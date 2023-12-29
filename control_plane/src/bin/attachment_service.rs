/// The attachment service mimics the aspects of the control plane API
/// that are required for a pageserver to operate.
///
/// This enables running & testing pageservers without a full-blown
/// deployment of the Neon cloud platform.
///
use anyhow::anyhow;
use clap::Parser;
use control_plane::endpoint::ComputeControlPlane;
use control_plane::local_env::LocalEnv;
use control_plane::pageserver::PageServerNode;
use hyper::{Body, Request, Response};
use hyper::{Method, StatusCode};
use pageserver_api::models::{
    LocationConfig, LocationConfigMode, LocationConfigSecondary, ShardParameters, TenantConfig,
    TenantCreateRequest, TenantLocationConfigRequest, TenantShardSplitRequest,
    TenantShardSplitResponse, TimelineCreateRequest, TimelineInfo,
};
use pageserver_api::shard::{ShardCount, ShardIdentity, ShardIndex, ShardNumber, TenantShardId};
use postgres_connection::parse_host_port;
use reqwest::Client;
use std::collections::{BTreeMap, HashMap};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use utils::generation::Generation;
use utils::http::endpoint::request_span;
use utils::http::request::parse_request_param;
use utils::id::{TenantId, TimelineId};
use utils::logging::{self, LogFormat};
use utils::lsn::Lsn;
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
    AttachHookRequest, AttachHookResponse, InspectRequest, InspectResponse, NodeAvailability,
    NodeConfigureRequest, NodeRegisterRequest, NodeSchedulingPolicy, TenantCreateResponse,
    TenantCreateResponseShard, TenantLocateResponse, TenantLocateResponseShard,
    TenantShardMigrateRequest, TenantShardMigrateResponse,
};

const RECONCILE_TIMEOUT: Duration = Duration::from_secs(30);

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
#[derive(Clone)]
struct ObservedStateLocation {
    /// If None, it means we do not know the status of this shard's location on this node, but
    /// we know that we might have some state on this node.
    conf: Option<LocationConfig>,
}

#[derive(Clone)]
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

#[derive(Default, Clone)]
struct ObservedState {
    locations: HashMap<NodeId, ObservedStateLocation>,
}

#[derive(Default, Clone, Debug)]
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

    /// When a node goes offline, we update intents to avoid using it
    /// as their attached pageserver.
    ///
    /// Returns true if a change was made
    fn notify_offline(&mut self, node_id: NodeId) -> bool {
        if self.attached == Some(node_id) {
            self.attached = None;
            self.secondary.push(node_id);
            true
        } else {
            false
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

#[derive(Ord, PartialOrd, Eq, PartialEq, Copy, Clone)]
struct Sequence(u64);

impl std::fmt::Display for Sequence {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl MonotonicCounter<Sequence> for Sequence {
    fn cnt_advance(&mut self, v: Sequence) {
        assert!(*self <= v);
        *self = v;
    }
    fn cnt_value(&self) -> Sequence {
        *self
    }
}

impl Sequence {
    fn next(&self) -> Sequence {
        Sequence(self.0 + 1)
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

struct ComputeHookTenant {
    shards: Vec<(ShardIndex, NodeId)>,
}

impl ComputeHookTenant {
    async fn maybe_reconfigure(&mut self, tenant_id: TenantId) -> anyhow::Result<()> {
        // Find the highest shard count and drop any shards that aren't
        // for that shard count.
        let shard_count = self.shards.iter().map(|(k, _v)| k.shard_count).max();
        let Some(shard_count) = shard_count else {
            // No shards, nothing to do.
            tracing::info!("ComputeHookTenant::maybe_reconfigure: no shards");
            return Ok(());
        };

        self.shards.retain(|(k, _v)| k.shard_count == shard_count);
        self.shards
            .sort_by_key(|(shard, _node_id)| shard.shard_number);

        if self.shards.len() == shard_count.0 as usize {
            // We have pageservers for all the shards: proceed to reconfigure compute
            let env = LocalEnv::load_config().expect("Error loading config");
            let cplane = ComputeControlPlane::load(env.clone())
                .expect("Error loading compute control plane");

            let compute_pageservers = self
                .shards
                .iter()
                .map(|(_shard, node_id)| {
                    let ps_conf = env
                        .get_pageserver_conf(*node_id)
                        .expect("Unknown pageserver");
                    let (pg_host, pg_port) = parse_host_port(&ps_conf.listen_pg_addr)
                        .expect("Unable to parse listen_pg_addr");
                    (pg_host, pg_port.unwrap_or(5432))
                })
                .collect::<Vec<_>>();

            for (endpoint_name, endpoint) in &cplane.endpoints {
                if endpoint.tenant_id == tenant_id && endpoint.status() == "running" {
                    tracing::info!("🔁 Reconfiguring endpoint {}", endpoint_name,);
                    endpoint.reconfigure(compute_pageservers.clone()).await?;
                }
            }
        } else {
            tracing::info!(
                "ComputeHookTenant::maybe_reconfigure: not enough shards ({}/{})",
                self.shards.len(),
                shard_count.0
            );
        }

        Ok(())
    }
}

/// The compute hook is a destination for notifications about changes to tenant:pageserver
/// mapping.  It aggregates updates for the shards in a tenant, and when appropriate reconfigures
/// the compute connection string.
struct ComputeHook {
    state: tokio::sync::Mutex<HashMap<TenantId, ComputeHookTenant>>,
}

impl ComputeHook {
    fn new() -> Self {
        Self {
            state: Default::default(),
        }
    }

    async fn notify(&self, tenant_shard_id: TenantShardId, node_id: NodeId) -> anyhow::Result<()> {
        tracing::info!("ComputeHook::notify: {}->{}", tenant_shard_id, node_id);
        let mut locked = self.state.lock().await;
        let entry = locked
            .entry(tenant_shard_id.tenant_id)
            .or_insert_with(|| ComputeHookTenant { shards: Vec::new() });

        let shard_index = ShardIndex {
            shard_count: tenant_shard_id.shard_count,
            shard_number: tenant_shard_id.shard_number,
        };

        let mut set = false;
        for (existing_shard, existing_node) in &mut entry.shards {
            if *existing_shard == shard_index {
                *existing_node = node_id;
                set = true;
            }
        }
        if !set {
            entry.shards.push((shard_index, node_id));
        }

        entry.maybe_reconfigure(tenant_shard_id.tenant_id).await
    }
}

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
    reconciler: Option<ReconcilerHandle>,

    /// Optionally wait for reconciliation to complete up to a particular
    /// sequence number.
    waiter: std::sync::Arc<SeqWait<Sequence, Sequence>>,
}

#[derive(Clone)]
struct Node {
    id: NodeId,

    availability: NodeAvailability,
    scheduling: NodeSchedulingPolicy,

    listen_http_addr: String,
    listen_http_port: u16,

    listen_pg_addr: String,
    listen_pg_port: u16,
}

impl Node {
    fn base_url(&self) -> String {
        format!(
            "http://{}:{}/v1",
            self.listen_http_addr, self.listen_http_port
        )
    }

    /// Is this node elegible to have work scheduled onto it?
    fn may_schedule(&self) -> bool {
        match self.availability {
            NodeAvailability::Active => {}
            NodeAvailability::Offline => return false,
        }

        match self.scheduling {
            NodeSchedulingPolicy::Active => true,
            NodeSchedulingPolicy::Draining => false,
            NodeSchedulingPolicy::Filling => true,
            NodeSchedulingPolicy::Pause => false,
        }
    }
}

// Top level state available to all HTTP handlers
struct ServiceState {
    tenants: BTreeMap<TenantShardId, TenantState>,

    nodes: Arc<HashMap<NodeId, Node>>,

    compute_hook: Arc<ComputeHook>,

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
        pageservers: &Arc<HashMap<NodeId, Node>>,
        compute_hook: &Arc<ComputeHook>,
    ) -> Option<ReconcilerWaiter> {
        // If there are any ambiguous observed states, and the nodes they refer to are available,
        // we should reconcile to clean them up.
        let mut dirty_observed = false;
        for (node_id, observed_loc) in &self.observed.locations {
            let node = pageservers
                .get(&node_id)
                .expect("Nodes may not be removed while referenced");
            if observed_loc.conf.is_none()
                && !matches!(node.availability, NodeAvailability::Offline)
            {
                dirty_observed = true;
                break;
            }
        }

        if !self.dirty() && !dirty_observed {
            tracing::info!("Not dirty, no reconciliation needed.");
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
            compute_hook: compute_hook.clone(),
            cancel: cancel.clone(),
        };

        let reconcile_seq = self.sequence;

        tracing::info!("Spawning Reconciler for sequence {}", self.sequence);
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
    pageservers: Arc<HashMap<NodeId, Node>>,

    /// A hook to notify the running postgres instances when we change the location
    /// of a tenant
    compute_hook: Arc<ComputeHook>,

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
        &mut self,
        node_id: NodeId,
        config: LocationConfig,
    ) -> anyhow::Result<()> {
        let node = self
            .pageservers
            .get(&node_id)
            .expect("Pageserver may not be removed while referenced");

        self.observed
            .locations
            .insert(node.id, ObservedStateLocation { conf: None });

        let configure_request = TenantLocationConfigRequest {
            tenant_shard_id: self.tenant_shard_id,
            config: config.clone(),
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

        self.observed
            .locations
            .insert(node.id, ObservedStateLocation { conf: Some(config) });

        response.error_for_status()?;

        Ok(())
    }

    async fn maybe_live_migrate(&mut self) -> Result<(), ReconcileError> {
        let destination = if let Some(node_id) = self.intent.attached {
            match self.observed.locations.get(&node_id) {
                Some(conf) => {
                    // We will do a live migration only if the intended destination is not
                    // currently in an attached state.
                    match &conf.conf {
                        Some(conf) if conf.mode == LocationConfigMode::Secondary => {
                            // Fall through to do a live migration
                            node_id
                        }
                        None | Some(_) => {
                            // Attached or uncertain: don't do a live migration, proceed
                            // with a general-case reconciliation
                            tracing::info!("maybe_live_migrate: destination is None or attached");
                            return Ok(());
                        }
                    }
                }
                None => {
                    // Our destination is not attached: maybe live migrate if some other
                    // node is currently attached.  Fall through.
                    node_id
                }
            }
        } else {
            // No intent to be attached
            tracing::info!("maybe_live_migrate: no attached intent");
            return Ok(());
        };

        let mut origin = None;
        for (node_id, state) in &self.observed.locations {
            if let Some(observed_conf) = &state.conf {
                if observed_conf.mode == LocationConfigMode::AttachedSingle {
                    let node = self
                        .pageservers
                        .get(node_id)
                        .expect("Nodes may not be removed while referenced");
                    // We will only attempt live migration if the origin is not offline: this
                    // avoids trying to do it while reconciling after responding to an HA failover.
                    if !matches!(node.availability, NodeAvailability::Offline) {
                        origin = Some(*node_id);
                        break;
                    }
                }
            }
        }

        let Some(origin) = origin else {
            tracing::info!("maybe_live_migrate: no origin found");
            return Ok(());
        };

        // We have an origin and a destination: proceed to do the live migration
        let env = LocalEnv::load_config().expect("Error loading config");
        let origin_ps = PageServerNode::from_env(
            &env,
            env.get_pageserver_conf(origin)
                .expect("Conf missing pageserver"),
        );
        let destination_ps = PageServerNode::from_env(
            &env,
            env.get_pageserver_conf(destination)
                .expect("Conf missing pageserver"),
        );

        tracing::info!(
            "Live migrating {}->{}",
            origin_ps.conf.id,
            destination_ps.conf.id
        );
        self.live_migrate(origin_ps, destination_ps).await?;

        Ok(())
    }

    pub async fn live_migrate(
        &mut self,
        origin_ps: PageServerNode,
        dest_ps: PageServerNode,
    ) -> anyhow::Result<()> {
        // `maybe_live_migrate` is responsibble for sanity of inputs
        assert!(origin_ps.conf.id != dest_ps.conf.id);

        fn build_location_config(
            shard: &ShardIdentity,
            config: &TenantConfig,
            mode: LocationConfigMode,
            generation: Option<Generation>,
            secondary_conf: Option<LocationConfigSecondary>,
        ) -> LocationConfig {
            LocationConfig {
                mode,
                generation: generation.map(|g| g.into().unwrap()),
                secondary_conf,
                tenant_conf: config.clone(),
                shard_number: shard.number.0,
                shard_count: shard.count.0,
                shard_stripe_size: shard.stripe_size.0,
            }
        }

        async fn get_lsns(
            tenant_shard_id: TenantShardId,
            pageserver: &PageServerNode,
        ) -> anyhow::Result<HashMap<TimelineId, Lsn>> {
            let timelines = pageserver.timeline_list(&tenant_shard_id).await?;
            Ok(timelines
                .into_iter()
                .map(|t| (t.timeline_id, t.last_record_lsn))
                .collect())
        }

        async fn await_lsn(
            tenant_shard_id: TenantShardId,
            pageserver: &PageServerNode,
            baseline: HashMap<TimelineId, Lsn>,
        ) -> anyhow::Result<()> {
            loop {
                let latest = match get_lsns(tenant_shard_id, pageserver).await {
                    Ok(l) => l,
                    Err(e) => {
                        println!(
                            "🕑 Can't get LSNs on pageserver {} yet, waiting ({e})",
                            pageserver.conf.id
                        );
                        std::thread::sleep(Duration::from_millis(500));
                        continue;
                    }
                };

                let mut any_behind: bool = false;
                for (timeline_id, baseline_lsn) in &baseline {
                    match latest.get(timeline_id) {
                        Some(latest_lsn) => {
                            println!("🕑 LSN origin {baseline_lsn} vs destination {latest_lsn}");
                            if latest_lsn < baseline_lsn {
                                any_behind = true;
                            }
                        }
                        None => {
                            // Expected timeline isn't yet visible on migration destination.
                            // (IRL we would have to account for timeline deletion, but this
                            //  is just test helper)
                            any_behind = true;
                        }
                    }
                }

                if !any_behind {
                    println!("✅ LSN caught up.  Proceeding...");
                    break;
                } else {
                    std::thread::sleep(Duration::from_millis(500));
                }
            }

            Ok(())
        }

        tracing::info!(
            "🔁 Switching origin pageserver {} to stale mode",
            origin_ps.conf.id
        );

        // FIXME: it is incorrect to use self.generation here, we should use the generation
        // from the ObservedState of the origin pageserver (it might be older than self.generation)
        let stale_conf = build_location_config(
            &self.shard,
            &self.config,
            LocationConfigMode::AttachedStale,
            Some(self.generation),
            None,
        );
        origin_ps
            .location_config(
                self.tenant_shard_id,
                stale_conf,
                Some(Duration::from_secs(10)),
            )
            .await?;

        let baseline_lsns = Some(get_lsns(self.tenant_shard_id, &origin_ps).await?);

        // Increment generation before attaching to new pageserver
        self.generation = self.generation.next();

        let dest_conf = build_location_config(
            &self.shard,
            &self.config,
            LocationConfigMode::AttachedMulti,
            Some(self.generation),
            None,
        );

        tracing::info!("🔁 Attaching to pageserver {}", dest_ps.conf.id);
        dest_ps
            .location_config(self.tenant_shard_id, dest_conf, None)
            .await?;

        if let Some(baseline) = baseline_lsns {
            tracing::info!("🕑 Waiting for LSN to catch up...");
            await_lsn(self.tenant_shard_id, &dest_ps, baseline).await?;
        }

        tracing::info!("🔁 Notifying compute to use pageserver {}", dest_ps.conf.id);
        self.compute_hook
            .notify(self.tenant_shard_id, dest_ps.conf.id)
            .await?;

        // Downgrade the origin to secondary.  If the tenant's policy is PlacementPolicy::Single, then
        // this location will be deleted in the general case reconciliation that runs after this.
        let origin_secondary_conf = build_location_config(
            &self.shard,
            &self.config,
            LocationConfigMode::Secondary,
            None,
            Some(LocationConfigSecondary { warm: true }),
        );
        origin_ps
            .location_config(self.tenant_shard_id, origin_secondary_conf.clone(), None)
            .await?;
        // TODO: we should also be setting the ObservedState on earlier API calls, in case we fail
        // partway through.  In fact, all location conf API calls should be in a wrapper that sets
        // the observed state to None, then runs, then sets it to what we wrote.
        self.observed.locations.insert(
            origin_ps.conf.id,
            ObservedStateLocation {
                conf: Some(origin_secondary_conf),
            },
        );

        println!(
            "🔁 Switching to AttachedSingle mode on pageserver {}",
            dest_ps.conf.id
        );
        let dest_final_conf = build_location_config(
            &self.shard,
            &self.config,
            LocationConfigMode::AttachedSingle,
            Some(self.generation),
            None,
        );
        dest_ps
            .location_config(self.tenant_shard_id, dest_final_conf.clone(), None)
            .await?;
        self.observed.locations.insert(
            dest_ps.conf.id,
            ObservedStateLocation {
                conf: Some(dest_final_conf),
            },
        );

        println!("✅ Migration complete");

        Ok(())
    }

    /// Reconciling a tenant makes API calls to pageservers until the observed state
    /// matches the intended state.
    ///
    /// First we apply special case handling (e.g. for live migrations), and then a
    /// general case reconciliation where we walk through the intent by pageserver
    /// and call out to the pageserver to apply the desired state.
    async fn reconcile(&mut self) -> Result<(), ReconcileError> {
        // TODO: if any of self.observed is None, call to remote pageservers
        // to learn correct state.

        // Special case: live migration
        self.maybe_live_migrate().await?;

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
                    self.location_config(node_id, wanted_conf).await?;
                    if let Err(e) = self
                        .compute_hook
                        .notify(self.tenant_shard_id, node_id)
                        .await
                    {
                        tracing::warn!(
                            "Failed to notify compute of newly attached pageserver {node_id}: {e}"
                        );
                    }
                }
            }
        }

        // Configure secondary locations: if these were previously attached this
        // implicitly downgrades them from attached to secondary.
        let mut changes = Vec::new();
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
                    changes.push((*node_id, wanted_conf))
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

            changes.push((
                *node_id,
                LocationConfig {
                    mode: LocationConfigMode::Detached,
                    generation: None,
                    secondary_conf: None,
                    shard_number: self.shard.number.0,
                    shard_count: self.shard.count.0,
                    shard_stripe_size: self.shard.stripe_size.0,
                    tenant_conf: self.config.clone(),
                },
            ));
        }

        for (node_id, conf) in changes {
            self.location_config(node_id, conf).await?;
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
    fn new(tenants: &BTreeMap<TenantShardId, TenantState>, nodes: &HashMap<NodeId, Node>) -> Self {
        let mut tenant_counts = HashMap::new();
        for node_id in nodes.keys() {
            tenant_counts.insert(*node_id, 0);
        }

        for tenant in tenants.values() {
            if let Some(ps) = tenant.intent.attached {
                let entry = tenant_counts.entry(ps).or_insert(0);
                *entry += 1;
            }
        }

        for (node_id, node) in nodes {
            if !node.may_schedule() {
                tenant_counts.remove(node_id);
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

        // Sort by tenant count.  Nodes with the same tenant count are sorted by ID.
        tenant_counts.sort_by_key(|i| (i.1, i.0));

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
            locked.nodes.len()
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

        let mut scheduler = Scheduler::new(&locked.tenants, &locked.nodes);

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
        let pageservers = locked.nodes.clone();

        let mut waiters = Vec::new();
        let result_tx = locked.result_tx.clone();
        let compute_hook = locked.compute_hook.clone();

        for (_tenant_shard_id, shard) in locked
            .tenants
            .range_mut(TenantShardId::tenant_range(tenant_id))
        {
            if let Some(waiter) =
                shard.maybe_reconcile(result_tx.clone(), &pageservers, &compute_hook)
            {
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
    let compute_hook = locked.compute_hook.clone();
    let mut scheduler = Scheduler::new(&locked.tenants, &locked.nodes);
    let pageservers = locked.nodes.clone();

    for (_tenant_shard_id, shard) in locked
        .tenants
        .range_mut(TenantShardId::tenant_range(tenant_id))
    {
        shard.schedule(&mut scheduler)?;

        if let Some(waiter) = shard.maybe_reconcile(result_tx.clone(), &pageservers, &compute_hook)
        {
            waiters.push(waiter);
        }
    }
    Ok(waiters)
}

async fn timeline_create(
    tenant_shard_id: &TenantShardId,
    node: &Node,
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
            locked.nodes.len()
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
                .nodes
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
    let pageservers = locked.nodes.clone();

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

    let mut new_nodes = (*locked.nodes).clone();

    new_nodes.insert(
        register_req.node_id,
        Node {
            id: register_req.node_id,
            listen_http_addr: register_req.listen_http_addr,
            listen_http_port: register_req.listen_http_port,
            listen_pg_addr: register_req.listen_pg_addr,
            listen_pg_port: register_req.listen_pg_port,
            scheduling: NodeSchedulingPolicy::Filling,
            // TODO: we shouldn't really call this Active until we've heartbeated it.
            availability: NodeAvailability::Active,
        },
    );

    locked.nodes = Arc::new(new_nodes);

    tracing::info!(
        "Registered pageserver {}, now have {} pageservers",
        register_req.node_id,
        locked.nodes.len()
    );

    json_response(StatusCode::OK, ())
}

async fn handle_node_configure(mut req: Request<Body>) -> Result<Response<Body>, ApiError> {
    let node_id: NodeId = parse_request_param(&req, "node_id")?;
    let config_req = json_request::<NodeConfigureRequest>(&mut req).await?;
    let state = get_state(&req).inner.clone();
    let mut locked = state.write().unwrap();
    let result_tx = locked.result_tx.clone();
    let compute_hook = locked.compute_hook.clone();

    let mut new_nodes = (*locked.nodes).clone();

    let Some(node) = new_nodes.get_mut(&node_id) else {
        return Err(ApiError::NotFound(
            anyhow::anyhow!("Node not registered").into(),
        ));
    };

    let mut offline_transition = false;
    let mut active_transition = false;

    if let Some(availability) = &config_req.availability {
        match (availability, &node.availability) {
            (NodeAvailability::Offline, NodeAvailability::Active) => {
                tracing::info!("Node {} transition to offline", node_id);
                offline_transition = true;
            }
            (NodeAvailability::Active, NodeAvailability::Offline) => {
                tracing::info!("Node {} transition to active", node_id);
                active_transition = true;
            }
            _ => {
                tracing::info!("Node {} no change during config", node_id);
                // No change
            }
        };
        node.availability = *availability;
    }

    if let Some(scheduling) = config_req.scheduling {
        node.scheduling = scheduling;

        // TODO: once we have a background scheduling ticker for fill/drain, kick it
        // to wake up and start working.
    }

    let new_nodes = Arc::new(new_nodes);

    let mut scheduler = Scheduler::new(&locked.tenants, &new_nodes);
    if offline_transition {
        for (tenant_shard_id, tenant_state) in &mut locked.tenants {
            if let Some(observed_loc) = tenant_state.observed.locations.get_mut(&node_id) {
                // When a node goes offline, we set its observed configuration to None, indicating unknown: we will
                // not assume our knowledge of the node's configuration is accurate until it comes back online
                observed_loc.conf = None;
            }

            if tenant_state.intent.notify_offline(node_id) {
                tenant_state.sequence = tenant_state.sequence.next();
                match tenant_state.schedule(&mut scheduler) {
                    Err(e) => {
                        // It is possible that some tenants will become unschedulable when too many pageservers
                        // go offline: in this case there isn't much we can do other than make the issue observable.
                        // TODO: give TenantState a scheduling error attribute to be queried later.
                        tracing::warn!(%tenant_shard_id, "Scheduling error when marking pageserver {node_id} offline: {e}");
                    }
                    Ok(()) => {
                        tenant_state.maybe_reconcile(result_tx.clone(), &new_nodes, &compute_hook);
                    }
                }
            }
        }
    }

    if active_transition {
        // When a node comes back online, we must reconcile any tenant that has a None observed
        // location on the node.
        for (_tenant_shard_id, tenant_state) in &mut locked.tenants {
            if let Some(observed_loc) = tenant_state.observed.locations.get_mut(&node_id) {
                if observed_loc.conf.is_none() {
                    tenant_state.maybe_reconcile(result_tx.clone(), &new_nodes, &compute_hook);
                }
            }
        }

        // TODO: in the background, we should balance work back onto this pageserver
    }

    locked.nodes = new_nodes;

    json_response(StatusCode::OK, ())
}

async fn handle_tenant_shard_split(mut req: Request<Body>) -> Result<Response<Body>, ApiError> {
    let tenant_id: TenantId = parse_request_param(&req, "tenant_id")?;
    let split_req = json_request::<TenantShardSplitRequest>(&mut req).await?;
    let state = get_state(&req).inner.clone();

    let mut policy = None;
    let (targets, compute_hook) = {
        let mut locked = state.write().unwrap();

        let pageservers = locked.nodes.clone();

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

            // TODO: if any reconciliation is currently in progress for this shard, wait for it.

            targets.push((*tenant_shard_id, node.clone()));
        }
        (targets, locked.compute_hook.clone())
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
    let mut child_locations = Vec::new();
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

                child_locations.push((child, pageserver));

                locked.tenants.insert(child, child_state);
                response.new_shards.push(child);
            }
        }
    }

    for (child_id, child_ps) in child_locations {
        if let Err(e) = compute_hook.notify(child_id, child_ps).await {
            tracing::warn!("Failed to update compute of {}->{} during split, proceeding anyway to complete split ({e})",
                        child_id, child_ps);
        }
    }

    json_response(StatusCode::OK, response)
}

async fn handle_tenant_shard_migrate(mut req: Request<Body>) -> Result<Response<Body>, ApiError> {
    let tenant_shard_id: TenantShardId = parse_request_param(&req, "tenant_shard_id")?;
    let migrate_req = json_request::<TenantShardMigrateRequest>(&mut req).await?;
    let state = get_state(&req).inner.clone();
    let waiter = {
        let mut locked = state.write().unwrap();

        let result_tx = locked.result_tx.clone();
        let pageservers = locked.nodes.clone();
        let compute_hook = locked.compute_hook.clone();

        let Some(shard) = locked.tenants.get_mut(&tenant_shard_id) else {
            return Err(ApiError::NotFound(
                anyhow::anyhow!("Tenant shard not found").into(),
            ));
        };

        if shard.intent.attached == Some(migrate_req.node_id) {
            // No-op case: we will still proceed to wait for reconciliation in case it is
            // incomplete from an earlier update to the intent.
            tracing::info!("Migrating: intent is unchanged {:?}", shard.intent);
        } else {
            let old_attached = shard.intent.attached;

            shard.intent.attached = Some(migrate_req.node_id);
            match shard.policy {
                PlacementPolicy::Single => {
                    shard.intent.secondary.clear();
                }
                PlacementPolicy::Double(_n) => {
                    // If our new attached node was a secondary, it no longer should be.
                    shard.intent.secondary.retain(|s| s != &migrate_req.node_id);

                    // If we were already attached to something, demote that to a secondary
                    if let Some(old_attached) = old_attached {
                        shard.intent.secondary.push(old_attached);
                    }
                }
            }

            tracing::info!("Migrating: new intent {:?}", shard.intent);
            shard.sequence = shard.sequence.next();
        }

        shard.maybe_reconcile(result_tx, &pageservers, &compute_hook)
    };

    if let Some(waiter) = waiter {
        waiter
            .wait_timeout(RECONCILE_TIMEOUT)
            .await
            .map_err(|e| ApiError::Timeout(format!("{}", e).into()))?;
    } else {
        tracing::warn!("Migration is a no-op");
    }

    json_response(StatusCode::OK, TenantShardMigrateResponse {})
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
        .put("/node/:node_id/config", |r| {
            request_span(r, handle_node_configure)
        })
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
        .put("/tenant/:tenant_shard_id/migrate", |r| {
            request_span(r, handle_tenant_shard_migrate)
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
        nodes: Arc::new(HashMap::new()),
        result_tx,
        compute_hook: Arc::new(ComputeHook::new()),
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
            tracing::info!(
                "Reconcile result for sequence {}, ok={}",
                result.sequence,
                result.result.is_ok()
            );
            let mut locked = service_state.write().unwrap();
            if let Some(tenant) = locked.tenants.get_mut(&result.tenant_shard_id) {
                tenant.generation = result.generation;
                match result.result {
                    Ok(()) => {
                        for (node_id, loc) in &result.observed.locations {
                            if let Some(conf) = &loc.conf {
                                tracing::info!(
                                    "Updating observed location {}: {:?}",
                                    node_id,
                                    conf
                                );
                            } else {
                                tracing::info!("Setting observed location {} to None", node_id,)
                            }
                        }
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
