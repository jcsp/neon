/// The attachment service mimics the aspects of the control plane API
/// that are required for a pageserver to operate.
///
/// This enables running & testing pageservers without a full-blown
/// deployment of the Neon cloud platform.
///
use anyhow::anyhow;
use clap::Parser;
use hyper::{Body, Request, Response};
use hyper::{Method, StatusCode};
use pageserver_api::models::{
    LocationConfig, LocationConfigMode, LocationConfigSecondary, ShardParameters, TenantConfig,
    TenantCreateRequest, TenantLocationConfigRequest, TenantShardSplitRequest,
    TenantShardSplitResponse, TimelineCreateRequest, TimelineInfo,
};
use pageserver_api::shard::{ShardCount, ShardIdentity, ShardNumber, TenantShardId};
use reqwest::Client;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, HashMap};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use utils::generation::Generation;
use utils::http::endpoint::request_span;
use utils::http::request::parse_request_param;
use utils::id::TenantId;
use utils::logging::{self, LogFormat};
use utils::seqwait::{MonotonicCounter, SeqWait, SeqWaitError};
use utils::signals::{ShutdownSignals, Signal};

use utils::{
    http::{
        endpoint::{self},
        error::ApiError,
        json::{json_request, json_response},
        RequestExt, RouterBuilder,
    },
    id::NodeId,
    tcp_listener,
};

use pageserver_api::control_api::{
    ReAttachRequest, ReAttachResponse, ReAttachResponseTenant, ValidateRequest, ValidateResponse,
    ValidateResponseTenant,
};

use control_plane::attachment_service::{
    AttachHookRequest, AttachHookResponse, InspectRequest, InspectResponse, NodeRegisterRequest,
    TenantCreateResponse, TenantCreateResponseShard, TenantLocateResponse,
    TenantLocateResponseShard,
};

#[derive(Parser)]
#[command(author, version, about, long_about = None)]
#[command(arg_required_else_help(true))]
struct Cli {
    /// Host and port to listen on, like `127.0.0.1:1234`
    #[arg(short, long)]
    listen: std::net::SocketAddr,

    /// Path to the .json file to store state (will be created if it doesn't exist)
    #[arg(short, long)]
    path: PathBuf,
}

/// Our latest knowledge of how this tenant is configured in the outside world.
///
/// Meaning:
///     * No instance of this type exists for a node: we are certain that we have nothing configured on that
///       node for this shard.
///     * Instance exists with conf==None: we *might* have some state on that node, but we don't know
///       what it is (e.g. we failed partway through configuring it)
///     * Instance exists with conf==Some: this tells us what we last successfully configured on this node,
///       and that configuration will still be present unless something external interfered.
#[derive(Serialize, Deserialize, Clone)]
struct ObservedStateLocation {
    /// If None, it means we do not know the status of this shard's location on this node, but
    /// we know that we might have some state on this node.
    conf: Option<LocationConfig>,
}

#[derive(Serialize, Deserialize, Clone)]
enum PlacementPolicy {
    /// Cheapest way to attach a tenant: just one pageserver, no secondary
    Single,
    /// Production-ready way to attach a tenant: one attached pageserver and
    /// some number of secondaries.
    Double(usize),
}

impl Default for PlacementPolicy {
    fn default() -> Self {
        PlacementPolicy::Double(1)
    }
}

#[derive(Serialize, Deserialize, Default, Clone)]
struct ObservedState {
    locations: HashMap<NodeId, ObservedStateLocation>,
}

#[derive(Serialize, Deserialize, Default, Clone)]
struct IntentState {
    attached: Option<NodeId>,
    secondary: Vec<NodeId>,
}

impl IntentState {
    fn all_pageservers(&self) -> Vec<NodeId> {
        let mut result = Vec::new();
        match self.attached {
            Some(p) => result.push(p),
            None => {}
        }

        result.extend(self.secondary.iter().map(|n| *n));

        result
    }

    fn single(node_id: Option<NodeId>) -> Self {
        Self {
            attached: node_id,
            secondary: vec![],
        }
    }
}

/// When a reconcile task completes, it sends this result object
/// to be applied to the primary TenantState.
struct ReconcileResult {
    sequence: Sequence,
    /// On errors, `observed` should be treated as an incompleted description
    /// of state (i.e. any nodes present in the result should override nodes
    /// present in the parent tenant state, but any unmentioned nodes should
    /// not be removed from parent tenant state)
    result: Result<(), ReconcileError>,

    tenant_shard_id: TenantShardId,
    generation: Generation,
    observed: ObservedState,
}

#[derive(Serialize, Deserialize, Ord, PartialOrd, Eq, PartialEq, Copy, Clone)]
struct Sequence(u64);

impl MonotonicCounter<Sequence> for Sequence {
    fn cnt_advance(&mut self, v: Sequence) {
        assert!(*self <= v);
        *self = v;
    }
    fn cnt_value(&self) -> Sequence {
        *self
    }
}

/// Having spawned a reconciler task, the tenant shard's state will carry enough
/// information to optionally cancel & await it later.
struct ReconcilerHandle {
    sequence: Sequence,
    handle: JoinHandle<()>,
    cancel: CancellationToken,
}

struct ReconcilerWaiter {
    // For observability purposes, remember the ID of the shard we're
    // waiting for.
    tenant_shard_id: TenantShardId,

    seq_wait: std::sync::Arc<SeqWait<Sequence, Sequence>>,
    seq: Sequence,
}

impl ReconcilerWaiter {
    async fn wait_timeout(&self, timeout: Duration) -> Result<(), SeqWaitError> {
        self.seq_wait.wait_for_timeout(self.seq, timeout).await
    }
}

fn default_waiter() -> std::sync::Arc<SeqWait<Sequence, Sequence>> {
    std::sync::Arc::new(SeqWait::new(Sequence(1)))
}

#[derive(Serialize, Deserialize)]
struct TenantState {
    tenant_shard_id: TenantShardId,

    shard: ShardIdentity,

    sequence: Sequence,

    // Latest generation number: next time we attach, increment this
    // and use the incremented number when attaching
    generation: Generation,

    // High level description of how the tenant should be set up.  Provided
    // externally.
    policy: PlacementPolicy,

    // Low level description of exactly which pageservers should fulfil
    // which role.  Generated by `Self::schedule`.
    intent: IntentState,

    // Low level description of how the tenant is configured on pageservers:
    // if this does not match `Self::intent` then the tenant needs reconciliation
    // with `Self::reconcile`.
    observed: ObservedState,

    config: TenantConfig,

    /// If a reconcile task is currently in flight, it may be joined here (it is
    /// only safe to join if either the result has been received or the reconciler's
    /// cancellation token has been fired)
    #[serde(skip)]
    reconciler: Option<ReconcilerHandle>,

    /// Optionally wait for reconciliation to complete up to a particular
    /// sequence number.
    #[serde(skip, default = "default_waiter")]
    waiter: std::sync::Arc<SeqWait<Sequence, Sequence>>,
}

#[derive(Serialize, Deserialize, Clone)]
struct NodeState {
    id: NodeId,

    listen_http_addr: String,
    listen_http_port: u16,

    listen_pg_addr: String,
    listen_pg_port: u16,
}

impl NodeState {
    fn base_url(&self) -> String {
        format!(
            "http://{}:{}/v1",
            self.listen_http_addr, self.listen_http_port
        )
    }
}

// Top level state available to all HTTP handlers
struct ServiceState {
    tenants: BTreeMap<TenantShardId, TenantState>,

    pageservers: Arc<HashMap<NodeId, NodeState>>,

    result_tx: tokio::sync::mpsc::UnboundedSender<ReconcileResult>,
}

/// State available to HTTP request handlers
#[derive(Clone)]
struct State {
    inner: Arc<std::sync::RwLock<ServiceState>>,
}

impl State {
    fn new(service_state: Arc<std::sync::RwLock<ServiceState>>) -> State {
        Self {
            inner: service_state,
        }
    }
}

#[inline(always)]
fn get_state(request: &Request<Body>) -> &State {
    request
        .data::<Arc<State>>()
        .expect("unknown state type")
        .as_ref()
}

impl TenantState {
    fn new(tenant_shard_id: TenantShardId, shard: ShardIdentity, policy: PlacementPolicy) -> Self {
        Self {
            tenant_shard_id,
            policy,
            intent: IntentState::default(),
            generation: Generation::new(0),
            shard,
            observed: ObservedState::default(),
            config: TenantConfig::default(),
            reconciler: None,
            sequence: Sequence(1),
            waiter: Arc::new(SeqWait::new(Sequence(0))),
        }
    }

    fn schedule(&mut self, scheduler: &mut Scheduler) -> Result<(), ScheduleError> {
        // TODO: before scheduling new nodes, check if any existing content in
        // self.intent refers to pageservers that are offline, and pick other
        // pageservers if so.

        // Build the set of pageservers already in use by this tenant, to avoid scheduling
        // more work on the same pageservers we're already using.
        let mut used_pageservers = self.intent.all_pageservers();
        let mut modified = false;

        use PlacementPolicy::*;
        match self.policy {
            Single => {
                todo!()
            }
            Double(secondary_count) => {
                // Should have exactly one attached
                if self.intent.attached.is_none() {
                    let node_id = scheduler.schedule_shard(&used_pageservers)?;
                    self.intent.attached = Some(node_id);
                    used_pageservers.push(node_id);
                    modified = true;
                }

                while self.intent.secondary.len() < secondary_count {
                    let node_id = scheduler.schedule_shard(&used_pageservers)?;
                    self.intent.secondary.push(node_id);
                    used_pageservers.push(node_id);
                    modified = true;
                }
            }
        }

        if modified {
            self.sequence.0 += 1;
        }

        Ok(())
    }

    fn dirty(&self) -> bool {
        if let Some(node_id) = self.intent.attached {
            let wanted_conf = attached_location_conf(self.generation, &self.shard, &self.config);
            match self.observed.locations.get(&node_id) {
                Some(conf) if conf.conf.as_ref() == Some(&wanted_conf) => {}
                Some(_) | None => {
                    return true;
                }
            }
        }

        for node_id in &self.intent.secondary {
            let wanted_conf = secondary_location_conf(&self.shard, &self.config);
            match self.observed.locations.get(&node_id) {
                Some(conf) if conf.conf.as_ref() == Some(&wanted_conf) => {}
                Some(_) | None => {
                    return true;
                }
            }
        }

        false
    }

    fn maybe_reconcile(
        &mut self,
        result_tx: tokio::sync::mpsc::UnboundedSender<ReconcileResult>,
        pageservers: &Arc<HashMap<NodeId, NodeState>>,
    ) -> Option<ReconcilerWaiter> {
        if !self.dirty() {
            return None;
        }

        // Reconcile already in flight for the current sequence?
        if let Some(handle) = &self.reconciler {
            if handle.sequence == self.sequence {
                return Some(ReconcilerWaiter {
                    tenant_shard_id: self.tenant_shard_id,
                    seq_wait: self.waiter.clone(),
                    seq: self.sequence,
                });
            }
        }

        // Reconcile in flight for a stale sequence?  Our sequence's task will wait for it before
        // doing our sequence's work.
        let old_handle = self.reconciler.take();

        let cancel = CancellationToken::new();
        let mut reconciler = Reconciler {
            tenant_shard_id: self.tenant_shard_id,
            shard: self.shard,
            generation: self.generation,
            intent: self.intent.clone(),
            config: self.config.clone(),
            observed: self.observed.clone(),
            pageservers: pageservers.clone(),
            cancel: cancel.clone(),
        };

        let reconcile_seq = self.sequence;

        let join_handle = tokio::task::spawn(async move {
            // Wait for any previous reconcile task to complete before we start
            if let Some(old_handle) = old_handle {
                old_handle.cancel.cancel();
                if let Err(e) = old_handle.handle.await {
                    // We can't do much with this other than log it: the task is done, so
                    // we may proceed with our work.
                    tracing::error!("Unexpected join error waiting for reconcile task: {e}");
                }
            }

            // Early check for cancellation before doing any work
            // TODO: wrap all remote API operations in cancellation check
            // as well.
            if reconciler.cancel.is_cancelled() {
                return ();
            }

            let result = reconciler.reconcile().await;
            result_tx
                .send(ReconcileResult {
                    sequence: reconcile_seq,
                    result,
                    tenant_shard_id: reconciler.tenant_shard_id,
                    generation: reconciler.generation,
                    observed: reconciler.observed,
                })
                .ok();
            ()
        });

        self.reconciler = Some(ReconcilerHandle {
            sequence: self.sequence,
            handle: join_handle,
            cancel,
        });

        Some(ReconcilerWaiter {
            tenant_shard_id: self.tenant_shard_id,
            seq_wait: self.waiter.clone(),
            seq: self.sequence,
        })
    }
}

/// Object with the lifetime of the background reconcile task that is created
/// for tenants which have a difference between their intent and observed states.
struct Reconciler {
    /// See [`TenantState`] for the meanings of these fields: they are a snapshot
    /// of a tenant's state from when we spawned a reconcile task.
    tenant_shard_id: TenantShardId,
    shard: ShardIdentity,
    generation: Generation,
    intent: IntentState,
    config: TenantConfig,
    observed: ObservedState,

    /// A snapshot of the pageservers as they were when we were asked
    /// to reconcile.
    pageservers: Arc<HashMap<NodeId, NodeState>>,

    /// A means to abort background reconciliation: it is essential to
    /// call this when something changes in the original TenantState that
    /// will make this reconciliation impossible or unnecessary, for
    /// example when a pageserver node goes offline, or the PlacementPolicy for
    /// the tenant is changed.
    cancel: CancellationToken,
}

fn attached_location_conf(
    generation: Generation,
    shard: &ShardIdentity,
    config: &TenantConfig,
) -> LocationConfig {
    LocationConfig {
        mode: LocationConfigMode::AttachedSingle,
        generation: generation.into(),
        secondary_conf: None,
        shard_number: shard.number.0,
        shard_count: shard.count.0,
        shard_stripe_size: shard.stripe_size.0,
        tenant_conf: config.clone(),
    }
}

fn secondary_location_conf(shard: &ShardIdentity, config: &TenantConfig) -> LocationConfig {
    LocationConfig {
        mode: LocationConfigMode::Secondary,
        generation: None,
        secondary_conf: Some(LocationConfigSecondary { warm: true }),
        shard_number: shard.number.0,
        shard_count: shard.count.0,
        shard_stripe_size: shard.stripe_size.0,
        tenant_conf: config.clone(),
    }
}

impl Reconciler {
    async fn location_config(
        &self,
        node: &NodeState,
        config: LocationConfig,
    ) -> anyhow::Result<()> {
        let configure_request = TenantLocationConfigRequest {
            tenant_shard_id: self.tenant_shard_id,
            config,
        };

        let client = Client::new();
        let response = client
            .request(
                Method::PUT,
                format!(
                    "{}/tenant/{}/location_config",
                    node.base_url(),
                    self.tenant_shard_id
                ),
            )
            .json(&configure_request)
            .send()
            .await?;
        response.error_for_status()?;

        Ok(())
    }

    async fn reconcile(&mut self) -> Result<(), ReconcileError> {
        // If the attached pageserver is not attached, do so now.
        if let Some(node_id) = self.intent.attached {
            let mut wanted_conf =
                attached_location_conf(self.generation, &self.shard, &self.config);
            match self.observed.locations.get(&node_id) {
                Some(conf) if conf.conf.as_ref() == Some(&wanted_conf) => {
                    // Nothing to do
                    tracing::info!("Observed configuration already correct.")
                }
                Some(_) | None => {
                    // If there is no observed configuration, or if its value does not equal our intent, then we must call out to the pageserver.
                    self.generation = self.generation.next();
                    wanted_conf.generation = self.generation.into();
                    tracing::info!("Observed configuration requires update.");
                    let node = self
                        .pageservers
                        .get(&node_id)
                        .expect("Pageserver may not be removed while referenced");
                    self.location_config(node, wanted_conf).await?;
                }
            }
        }

        // Configure secondary locations: if these were previously attached this
        // implicitly downgrades them from attached to secondary.
        for node_id in &self.intent.secondary {
            let wanted_conf = secondary_location_conf(&self.shard, &self.config);
            match self.observed.locations.get(&node_id) {
                Some(conf) if conf.conf.as_ref() == Some(&wanted_conf) => {
                    // Nothing to do
                    tracing::info!(%node_id, "Observed configuration already correct.")
                }
                Some(_) | None => {
                    // If there is no observed configuration, or if its value does not equal our intent, then we must call out to the pageserver.
                    tracing::info!(%node_id, "Observed configuration requires update.");
                    let node = self
                        .pageservers
                        .get(&node_id)
                        .expect("Pageserver may not be removed while referenced");
                    self.location_config(node, wanted_conf).await?;
                }
            }
        }

        // Detach any extraneous pageservers that are no longer referenced
        // by our intent.
        let all_pageservers = self.intent.all_pageservers();
        for node_id in self.observed.locations.keys() {
            if all_pageservers.contains(node_id) {
                // We are only detaching pageservers that aren't used at all.
                continue;
            }

            let node = self
                .pageservers
                .get(node_id)
                .expect("Pageserver may not be removed while referenced");
            self.location_config(
                node,
                LocationConfig {
                    mode: LocationConfigMode::Detached,
                    generation: None,
                    secondary_conf: None,
                    shard_number: self.shard.number.0,
                    shard_count: self.shard.count.0,
                    shard_stripe_size: self.shard.stripe_size.0,
                    tenant_conf: self.config.clone(),
                },
            )
            .await?;
        }

        Ok(())
    }
}

/// Pageserver calls into this on startup, to learn which tenants it should attach
async fn handle_re_attach(mut req: Request<Body>) -> Result<Response<Body>, ApiError> {
    let reattach_req = json_request::<ReAttachRequest>(&mut req).await?;

    let state = get_state(&req).inner.clone();
    let mut locked = state.write().unwrap();

    let mut response = ReAttachResponse {
        tenants: Vec::new(),
    };
    for (t, state) in &mut locked.tenants {
        if state.intent.attached == Some(reattach_req.node_id) {
            state.generation = state.generation.next();
            response.tenants.push(ReAttachResponseTenant {
                id: *t,
                gen: state.generation.into().unwrap(),
            });
        }
    }

    json_response(StatusCode::OK, response)
}

/// Pageserver calls into this before doing deletions, to confirm that it still
/// holds the latest generation for the tenants with deletions enqueued
async fn handle_validate(mut req: Request<Body>) -> Result<Response<Body>, ApiError> {
    let validate_req = json_request::<ValidateRequest>(&mut req).await?;

    let locked = get_state(&req).inner.read().unwrap();

    let mut response = ValidateResponse {
        tenants: Vec::new(),
    };

    for req_tenant in validate_req.tenants {
        if let Some(tenant_state) = locked.tenants.get(&req_tenant.id) {
            let valid = tenant_state.generation == Generation::new(req_tenant.gen);
            tracing::info!(
                "handle_validate: {}(gen {}): valid={valid} (latest {:?})",
                req_tenant.id,
                req_tenant.gen,
                tenant_state.generation
            );
            response.tenants.push(ValidateResponseTenant {
                id: req_tenant.id,
                valid,
            });
        }
    }

    json_response(StatusCode::OK, response)
}
/// Call into this before attaching a tenant to a pageserver, to acquire a generation number
/// (in the real control plane this is unnecessary, because the same program is managing
///  generation numbers and doing attachments).
async fn handle_attach_hook(mut req: Request<Body>) -> Result<Response<Body>, ApiError> {
    let attach_req = json_request::<AttachHookRequest>(&mut req).await?;

    let state = get_state(&req).inner.clone();
    let mut locked = state.write().unwrap();

    let tenant_state = locked
        .tenants
        .entry(attach_req.tenant_shard_id)
        .or_insert_with(|| {
            TenantState::new(
                attach_req.tenant_shard_id,
                ShardIdentity::unsharded(),
                PlacementPolicy::Single,
            )
        });

    if let Some(attaching_pageserver) = attach_req.node_id.as_ref() {
        tenant_state.generation = tenant_state.generation.next();
        tracing::info!(
            tenant_id = %attach_req.tenant_shard_id,
            ps_id = %attaching_pageserver,
            generation = ?tenant_state.generation,
            "issuing",
        );
    } else if let Some(ps_id) = tenant_state.intent.attached {
        tracing::info!(
            tenant_id = %attach_req.tenant_shard_id,
            %ps_id,
            generation = ?tenant_state.generation,
            "dropping",
        );
    } else {
        tracing::info!(
            tenant_id = %attach_req.tenant_shard_id,
            "no-op: tenant already has no pageserver");
    }
    tenant_state.intent.attached = attach_req.node_id;
    let generation = tenant_state.generation;

    tracing::info!(
        "handle_attach_hook: tenant {} set generation {:?}, pageserver {}",
        attach_req.tenant_shard_id,
        tenant_state.generation,
        attach_req.node_id.unwrap_or(utils::id::NodeId(0xfffffff))
    );

    json_response(
        StatusCode::OK,
        AttachHookResponse {
            gen: attach_req.node_id.map(|_| generation.into().unwrap()),
        },
    )
}

async fn handle_inspect(mut req: Request<Body>) -> Result<Response<Body>, ApiError> {
    let inspect_req = json_request::<InspectRequest>(&mut req).await?;

    let state = get_state(&req).inner.clone();
    let locked = state.write().unwrap();
    let tenant_state = locked.tenants.get(&inspect_req.tenant_shard_id);

    json_response(
        StatusCode::OK,
        InspectResponse {
            attachment: tenant_state.and_then(|s| {
                s.intent
                    .attached
                    .map(|ps| (s.generation.into().unwrap(), ps))
            }),
        },
    )
}

/// Scenarios in which we cannot find a suitable location for a tenant shard
#[derive(thiserror::Error, Debug)]
enum ScheduleError {
    #[error("No pageservers found")]
    NoPageservers,
    #[error("No pageserver found matching constraint")]
    ImpossibleConstraint,
}

impl From<ScheduleError> for ApiError {
    fn from(value: ScheduleError) -> Self {
        ApiError::Conflict(format!("Scheduling error: {}", value))
    }
}

#[derive(thiserror::Error, Debug)]
enum ReconcileError {
    #[error(transparent)]
    Other(#[from] anyhow::Error),
}

impl From<ReconcileError> for ApiError {
    fn from(value: ReconcileError) -> Self {
        ApiError::Conflict(format!("Reconciliation error: {}", value))
    }
}

struct Scheduler {
    tenant_counts: HashMap<NodeId, usize>,
}

impl Scheduler {
    fn new(service_state: &ServiceState) -> Self {
        let mut tenant_counts = HashMap::new();
        for node_id in service_state.pageservers.keys() {
            tenant_counts.insert(*node_id, 0);
        }

        for tenant in service_state.tenants.values() {
            if let Some(ps) = tenant.intent.attached {
                let entry = tenant_counts.entry(ps).or_insert(0);
                *entry += 1;
            }
        }
        Self { tenant_counts }
    }

    fn schedule_shard(&mut self, hard_exclude: &Vec<NodeId>) -> Result<NodeId, ScheduleError> {
        if self.tenant_counts.is_empty() {
            return Err(ScheduleError::NoPageservers);
        }

        let mut tenant_counts: Vec<(NodeId, usize)> = self
            .tenant_counts
            .iter()
            .filter_map(|(k, v)| {
                if hard_exclude.contains(k) {
                    None
                } else {
                    Some((*k, *v))
                }
            })
            .collect();
        tenant_counts.sort_by_key(|i| i.1);

        if tenant_counts.is_empty() {
            // After applying constraints, no pageservers were left
            return Err(ScheduleError::ImpossibleConstraint);
        }

        for (node_id, count) in &tenant_counts {
            tracing::info!("tenant_counts[{node_id}]={count}");
        }

        let node_id = tenant_counts.first().unwrap().0;
        tracing::info!("scheduler selected node {node_id}");
        *self.tenant_counts.get_mut(&node_id).unwrap() += 1;
        Ok(node_id)
    }
}

async fn handle_tenant_create(mut req: Request<Body>) -> Result<Response<Body>, ApiError> {
    let create_req = json_request::<TenantCreateRequest>(&mut req).await?;

    let state = get_state(&req).inner.clone();
    let (waiters, response_shards) = {
        let mut locked = state.write().unwrap();

        tracing::info!(
            "Creating tenant {}, shard_count={:?}, have {} pageservers",
            create_req.new_tenant_id,
            create_req.shard_parameters.count,
            locked.pageservers.len()
        );

        // This service expects to handle sharding itself: it is an error to try and directly create
        // a particular shard here.
        let tenant_id = if create_req.new_tenant_id.shard_count > ShardCount(1) {
            return Err(ApiError::BadRequest(anyhow::anyhow!(
                "Attempted to create a specific shard, this API is for creating the whole tenant"
            )));
        } else {
            create_req.new_tenant_id.tenant_id
        };

        // Shard count 0 is valid: it means create a single shard (ShardCount(0) means "unsharded")
        let literal_shard_count = if create_req.shard_parameters.is_unsharded() {
            1
        } else {
            create_req.shard_parameters.count.0
        };

        let mut response_shards = Vec::new();

        let mut scheduler = Scheduler::new(&locked);

        for i in 0..literal_shard_count {
            let shard_number = ShardNumber(i);

            let tenant_shard_id = TenantShardId {
                tenant_id,
                shard_number,
                shard_count: create_req.shard_parameters.count,
            };
            tracing::info!("Creating shard {tenant_shard_id}...");

            use std::collections::btree_map::Entry;
            match locked.tenants.entry(tenant_shard_id) {
                Entry::Occupied(mut entry) => {
                    tracing::info!("Tenant shard {tenant_shard_id} already exists while creating");

                    // TODO: schedule() should take an anti-affinity expression that pushes
                    // attached and secondary locations (independently) away frorm those
                    // pageservers also holding a shard for this tenant.

                    entry.get_mut().schedule(&mut scheduler).map_err(|e| {
                        ApiError::Conflict(format!(
                            "Failed to schedule shard {tenant_shard_id}: {e}"
                        ))
                    })?;

                    response_shards.push(TenantCreateResponseShard {
                        node_id: entry
                            .get()
                            .intent
                            .attached
                            .expect("We just set pageserver if it was None"),
                        generation: entry.get().generation.into().unwrap(),
                    });

                    continue;
                }
                Entry::Vacant(entry) => {
                    let mut state = TenantState::new(
                        tenant_shard_id,
                        ShardIdentity::from_params(shard_number, &create_req.shard_parameters),
                        PlacementPolicy::Double(1),
                    );

                    if let Some(create_gen) = create_req.generation {
                        state.generation = Generation::new(create_gen);
                    }
                    state.config = create_req.config.clone();

                    state.schedule(&mut scheduler).map_err(|e| {
                        ApiError::Conflict(format!(
                            "Failed to schedule shard {tenant_shard_id}: {e}"
                        ))
                    })?;

                    response_shards.push(TenantCreateResponseShard {
                        node_id: state
                            .intent
                            .attached
                            .expect("We just set pageserver if it was None"),
                        generation: state.generation.into().unwrap(),
                    });
                    entry.insert(state)
                }
            };
        }

        // Take a snapshot of pageservers
        let pageservers = locked.pageservers.clone();

        let mut waiters = Vec::new();
        let result_tx = locked.result_tx.clone();

        for (_tenant_shard_id, shard) in locked
            .tenants
            .range_mut(TenantShardId::tenant_range(tenant_id))
        {
            if let Some(waiter) = shard.maybe_reconcile(result_tx.clone(), &pageservers) {
                waiters.push(waiter);
            }
        }
        (waiters, response_shards)
    };

    let deadline = Instant::now().checked_add(Duration::from_secs(5)).unwrap();
    for waiter in waiters {
        let timeout = deadline.duration_since(Instant::now());
        waiter.wait_timeout(timeout).await.map_err(|_| {
            ApiError::Timeout(
                format!(
                    "Timeout waiting for reconciliation of tenant shard {}",
                    waiter.tenant_shard_id
                )
                .into(),
            )
        })?;
    }

    json_response(
        StatusCode::OK,
        TenantCreateResponse {
            shards: response_shards,
        },
    )
}

fn ensure_attached<'a>(
    mut locked: std::sync::RwLockWriteGuard<'a, ServiceState>,
    tenant_id: TenantId,
) -> Result<Vec<ReconcilerWaiter>, anyhow::Error> {
    let mut waiters = Vec::new();
    let result_tx = locked.result_tx.clone();
    let mut scheduler = Scheduler::new(&locked);
    let pageservers = locked.pageservers.clone();
    for (_tenant_shard_id, shard) in locked
        .tenants
        .range_mut(TenantShardId::tenant_range(tenant_id))
    {
        shard.schedule(&mut scheduler)?;

        if let Some(waiter) = shard.maybe_reconcile(result_tx.clone(), &pageservers) {
            waiters.push(waiter);
        }
    }
    Ok(waiters)
}

async fn timeline_create(
    tenant_shard_id: &TenantShardId,
    node: &NodeState,
    req: &TimelineCreateRequest,
) -> anyhow::Result<TimelineInfo> {
    let client = Client::new();
    let response = client
        .request(
            Method::POST,
            format!("{}/tenant/{}/timeline", node.base_url(), tenant_shard_id),
        )
        .json(req)
        .send()
        .await?;
    response.error_for_status_ref()?;

    Ok(response.json().await?)
}

async fn handle_tenant_timeline_create(mut req: Request<Body>) -> Result<Response<Body>, ApiError> {
    let tenant_id: TenantId = parse_request_param(&req, "tenant_id")?;
    let mut create_req = json_request::<TimelineCreateRequest>(&mut req).await?;

    let state = get_state(&req).inner.clone();

    let mut timeline_info = None;
    let ensure_waiters = {
        let locked = state.write().unwrap();

        tracing::info!(
            "Creating timeline {}/{}, have {} pageservers",
            tenant_id,
            create_req.new_timeline_id,
            locked.pageservers.len()
        );

        ensure_attached(locked, tenant_id).map_err(|e| ApiError::InternalServerError(e))?
    };

    let deadline = Instant::now().checked_add(Duration::from_secs(5)).unwrap();
    for waiter in ensure_waiters {
        let timeout = deadline.duration_since(Instant::now());
        waiter.wait_timeout(timeout).await.map_err(|_| {
            ApiError::Timeout(
                format!(
                    "Timeout waiting for reconciliation of tenant shard {}",
                    waiter.tenant_shard_id
                )
                .into(),
            )
        })?;
    }

    let targets = {
        let locked = state.read().unwrap();
        let mut targets = Vec::new();

        for (tenant_shard_id, shard) in locked.tenants.range(TenantShardId::tenant_range(tenant_id))
        {
            let node_id = shard.intent.attached.ok_or_else(|| {
                ApiError::InternalServerError(anyhow::anyhow!("Shard not scheduled"))
            })?;
            let node = locked
                .pageservers
                .get(&node_id)
                .expect("Pageservers may not be deleted while referenced");

            targets.push((*tenant_shard_id, node.clone()));
        }
        targets
    };

    for (tenant_shard_id, node) in targets {
        // TODO: issue shard timeline creates in parallel, once the 0th is done.

        let shard_timeline_info = timeline_create(&tenant_shard_id, &node, &create_req)
            .await
            .map_err(|e| ApiError::Conflict(format!("Failed to create timeline: {e}")))?;

        if timeline_info.is_none() {
            // If the caller specified an ancestor but no ancestor LSN, we are responsible for
            // propagating the LSN chosen by the first shard to the other shards: it is important
            // that all shards end up with the same ancestor_start_lsn.
            if create_req.ancestor_timeline_id.is_some() && create_req.ancestor_start_lsn.is_none()
            {
                create_req.ancestor_start_lsn = shard_timeline_info.ancestor_lsn;
            }

            // We will return the TimelineInfo from the first shard
            timeline_info = Some(shard_timeline_info);
        }
    }

    json_response(StatusCode::OK, timeline_info)
}

async fn handle_tenant_locate(req: Request<Body>) -> Result<Response<Body>, ApiError> {
    let tenant_id: TenantId = parse_request_param(&req, "tenant_id")?;

    let state = get_state(&req).inner.clone();
    let mut locked = state.write().unwrap();

    tracing::info!("Locating shards for tenant {tenant_id}");

    // Take a snapshot of pageservers
    let pageservers = locked.pageservers.clone();

    let mut result = Vec::new();
    let mut shard_params: Option<ShardParameters> = None;

    for (tenant_shard_id, shard) in locked
        .tenants
        .range_mut(TenantShardId::tenant_range(tenant_id))
    {
        let node_id = shard
            .intent
            .attached
            .ok_or(ApiError::BadRequest(anyhow::anyhow!(
                "Cannot locate a tenant that is not attached"
            )))?;

        let node = pageservers
            .get(&node_id)
            .expect("Pageservers may not be deleted while referenced");

        result.push(TenantLocateResponseShard {
            shard_id: *tenant_shard_id,
            node_id,
            listen_http_addr: node.listen_http_addr.clone(),
            listen_http_port: node.listen_http_port,
            listen_pg_addr: node.listen_pg_addr.clone(),
            listen_pg_port: node.listen_pg_port,
        });

        match &shard_params {
            None => {
                shard_params = Some(ShardParameters {
                    stripe_size: Some(shard.shard.stripe_size),
                    count: shard.shard.count,
                });
            }
            Some(params) => {
                if params.stripe_size != Some(shard.shard.stripe_size) {
                    // This should never happen.  We enforce at runtime because it's simpler than
                    // adding an extra per-tenant data structure to store the things that should be the same
                    return Err(ApiError::InternalServerError(anyhow::anyhow!(
                        "Inconsistent shard stripe size parameters!"
                    )));
                }
            }
        }
    }

    if result.is_empty() {
        return Err(ApiError::NotFound(
            anyhow::anyhow!("No shards for this tenant ID found").into(),
        ));
    }
    let shard_params = shard_params.expect("result is non-empty, therefore this is set");
    tracing::info!(
        "Located tenant {} with params {:?} on shards {}",
        tenant_id,
        shard_params,
        result
            .iter()
            .map(|s| format!("{:?}", s))
            .collect::<Vec<_>>()
            .join(",")
    );

    json_response(
        StatusCode::OK,
        TenantLocateResponse {
            shards: result,
            shard_params,
        },
    )
}

async fn handle_node_register(mut req: Request<Body>) -> Result<Response<Body>, ApiError> {
    let register_req = json_request::<NodeRegisterRequest>(&mut req).await?;
    let state = get_state(&req).inner.clone();
    let mut locked = state.write().unwrap();

    let mut new_pageservers = (*locked.pageservers).clone();

    new_pageservers.insert(
        register_req.node_id,
        NodeState {
            id: register_req.node_id,
            listen_http_addr: register_req.listen_http_addr,
            listen_http_port: register_req.listen_http_port,
            listen_pg_addr: register_req.listen_pg_addr,
            listen_pg_port: register_req.listen_pg_port,
        },
    );

    locked.pageservers = Arc::new(new_pageservers);

    tracing::info!(
        "Registered pageserver {}, now have {} pageservers",
        register_req.node_id,
        locked.pageservers.len()
    );

    json_response(StatusCode::OK, ())
}

async fn handle_tenant_shard_split(mut req: Request<Body>) -> Result<Response<Body>, ApiError> {
    let tenant_id: TenantId = parse_request_param(&req, "tenant_id")?;
    let split_req = json_request::<TenantShardSplitRequest>(&mut req).await?;
    let state = get_state(&req).inner.clone();

    let mut policy = None;
    let targets = {
        let mut locked = state.write().unwrap();

        let pageservers = locked.pageservers.clone();

        let mut targets = Vec::new();

        for (tenant_shard_id, shard) in locked
            .tenants
            .range_mut(TenantShardId::tenant_range(tenant_id))
        {
            if policy.is_none() {
                policy = Some(shard.policy.clone());
            }

            if tenant_shard_id.shard_count == ShardCount(split_req.new_shard_count) {
                tracing::warn!(
                    "Tenant shard {} already has shard count {}",
                    tenant_shard_id,
                    split_req.new_shard_count
                );
                continue;
            }

            let node_id = shard
                .intent
                .attached
                .ok_or(ApiError::BadRequest(anyhow::anyhow!(
                    "Cannot split a tenant that is not attached"
                )))?;

            let node = pageservers
                .get(&node_id)
                .expect("Pageservers may not be deleted while referenced");

            targets.push((*tenant_shard_id, node.clone()));
        }
        targets
    };

    let mut replacements = HashMap::new();
    for (tenant_shard_id, node) in targets {
        let client = Client::new();
        let response = client
            .request(
                Method::PUT,
                format!("{}/tenant/{}/shard_split", node.base_url(), tenant_shard_id),
            )
            .json(&TenantShardSplitRequest {
                new_shard_count: split_req.new_shard_count,
            })
            .send()
            .await
            .map_err(|e| {
                ApiError::Conflict(format!("Failed to split {}: {}", tenant_shard_id, e))
            })?;
        response.error_for_status_ref().map_err(|e| {
            ApiError::Conflict(format!("Failed to split {}: {}", tenant_shard_id, e))
        })?;
        let response: TenantShardSplitResponse = response.json().await.map_err(|e| {
            ApiError::InternalServerError(anyhow::anyhow!(
                "Malformed response from pageserver: {}",
                e
            ))
        })?;

        tracing::info!(
            "Split {} into {}",
            tenant_shard_id,
            response
                .new_shards
                .iter()
                .map(|s| format!("{:?}", s))
                .collect::<Vec<_>>()
                .join(",")
        );

        replacements.insert(tenant_shard_id, response.new_shards);
    }

    // TODO: concurrency: we're dropping the state lock while issuing split API calls.
    //       We should add some marker to the TenantState that causes any other change
    //       to refuse until the split is complete.  This will be related to a persistent
    //       splitting marker that will ensure resume after crash.

    // Replace all the shards we just split with their children
    let mut response = TenantShardSplitResponse {
        new_shards: Vec::new(),
    };
    {
        let mut locked = state.write().unwrap();
        for (replaced, children) in replacements.into_iter() {
            let (pageserver, generation, shard_ident, config) = {
                let old_state = locked
                    .tenants
                    .remove(&replaced)
                    .expect("It was present, we just split it");
                (
                    old_state.intent.attached.unwrap(),
                    old_state.generation,
                    old_state.shard,
                    old_state.config.clone(),
                )
            };

            locked.tenants.remove(&replaced);

            for child in children {
                let mut child_shard = shard_ident;
                child_shard.number = child.shard_number;
                child_shard.count = child.shard_count;

                let mut child_observed: HashMap<NodeId, ObservedStateLocation> = HashMap::new();
                child_observed.insert(
                    pageserver,
                    ObservedStateLocation {
                        conf: Some(attached_location_conf(generation, &child_shard, &config)),
                    },
                );

                let mut child_state = TenantState::new(
                    child,
                    child_shard,
                    policy
                        .clone()
                        .expect("We set this if any replacements are pushed"),
                );
                child_state.intent = IntentState::single(Some(pageserver));
                child_state.observed = ObservedState {
                    locations: child_observed,
                };
                child_state.generation = generation;
                child_state.config = config.clone();

                locked.tenants.insert(child, child_state);

                response.new_shards.push(child);
            }
        }
    }

    json_response(StatusCode::OK, response)
}

/// Status endpoint is just used for checking that our HTTP listener is up
async fn handle_status(_req: Request<Body>) -> Result<Response<Body>, ApiError> {
    json_response(StatusCode::OK, ())
}

fn make_router(
    service_state: Arc<std::sync::RwLock<ServiceState>>,
) -> RouterBuilder<hyper::Body, ApiError> {
    endpoint::make_router()
        .data(Arc::new(State::new(service_state)))
        .get("/status", |r| request_span(r, handle_status))
        .post("/re-attach", |r| request_span(r, handle_re_attach))
        .post("/validate", |r| request_span(r, handle_validate))
        .post("/attach-hook", |r| request_span(r, handle_attach_hook))
        .post("/inspect", |r| request_span(r, handle_inspect))
        .post("/node", |r| request_span(r, handle_node_register))
        .post("/tenant", |r| request_span(r, handle_tenant_create))
        .post("/tenant/:tenant_id/timeline", |r| {
            request_span(r, handle_tenant_timeline_create)
        })
        .get("/tenant/:tenant_id/locate", |r| {
            request_span(r, handle_tenant_locate)
        })
        .put("/tenant/:tenant_id/shard_split", |r| {
            request_span(r, handle_tenant_shard_split)
        })
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    logging::init(
        LogFormat::Plain,
        logging::TracingErrorLayerEnablement::Disabled,
        logging::Output::Stdout,
    )?;

    let args = Cli::parse();
    tracing::info!(
        "Starting, state at {}, listening on {}",
        args.path.to_string_lossy(),
        args.listen
    );

    let (result_tx, mut result_rx) = tokio::sync::mpsc::unbounded_channel();

    let service_state = Arc::new(std::sync::RwLock::new(ServiceState {
        tenants: BTreeMap::new(),
        pageservers: Arc::new(HashMap::new()),
        result_tx,
    }));

    let http_listener = tcp_listener::bind(args.listen)?;
    let router = make_router(service_state.clone())
        .build()
        .map_err(|err| anyhow!(err))?;
    let service = utils::http::RouterService::new(router).unwrap();
    let server = hyper::Server::from_tcp(http_listener)?.serve(service);

    tracing::info!("Serving on {0}", args.listen);

    tokio::task::spawn(server);

    tokio::task::spawn(async move {
        while let Some(result) = result_rx.recv().await {
            let mut locked = service_state.write().unwrap();
            if let Some(tenant) = locked.tenants.get_mut(&result.tenant_shard_id) {
                tenant.generation = result.generation;
                match result.result {
                    Ok(()) => {
                        tenant.observed = result.observed;
                        tenant.waiter.advance(result.sequence);
                    }
                    Err(e) => {
                        // TODO: some observability, record on teh tenant its last reconcile error
                        tracing::warn!(
                            "Reconcile error on tenant {}: {}",
                            tenant.tenant_shard_id,
                            e
                        );

                        for (node_id, o) in result.observed.locations {
                            tenant.observed.locations.insert(node_id, o);
                        }
                    }
                }
            }
        }
    });

    ShutdownSignals::handle(|signal| match signal {
        Signal::Interrupt | Signal::Terminate | Signal::Quit => {
            tracing::info!("Got {}. Terminating", signal.name());
            // We're just a test helper: no graceful shutdown.
            std::process::exit(0);
        }
    })?;

    Ok(())
}
