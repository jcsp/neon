use remote_storage::{GenericRemoteStorage, RemotePath};
use serde::Deserialize;
use serde::Serialize;
use serde_with::serde_as;
use tokio;
use tokio::time::{Duration, Instant};
use tracing::{self, debug, error, info, warn};
use utils::id::{TenantId, TimelineId};

use crate::{config::PageServerConf, tenant::storage_layer::LayerFileName};

// TODO: small value is just for testing, make this bigger
const DELETION_LIST_TARGET_SIZE: usize = 16;

// Ordinarily, we only flush periodically, to bound the window during
// which we might leak objects from not flushing a DeletionList after
// the objects are already unlinked from timeline metadata.
const FLUSH_DEFAULT_DEADLINE: Duration = Duration::from_millis(10000);

// If someone is waiting for a flush, only delay a little to accumulate
// more objects before doing the flush.
const FLUSH_EXPLICIT_DEADLINE: Duration = Duration::from_millis(100);

// TODO: metrics for queue length, deletions executed, deletion errors

// TODO: adminstrative "panic button" config property to disable all deletions

// TODO: implement admin API hook to flush deletion queue, for use in integration tests
//       that would like to assert deleted objects are gone

/// We aggregate object deletions from many tenants in one place, for several reasons:
/// - Coalesce deletions into fewer DeleteObjects calls
/// - Enable Tenant/Timeline lifetimes to be shorter than the time it takes
///   to flush any outstanding deletions.
/// - Globally control throughput of deletions, as these are a low priority task: do
///   not compete with the same S3 clients/connections used for higher priority uploads.
///
/// There are two parts ot this, frontend and backend, joined by channels:
/// - DeletionQueueWorker consumes the frontend queue: the "DeletionQueue" that makes up
///   the public interface and accepts deletion requests.
/// - BackendQueueWorker consumes the backend queue: a queue of DeletionList that have
///   already been written to S3 and are now eligible for final deletion.
///   
///
///
///
/// There are three queues internally:
/// - Incoming deletes (the DeletionQueue that the outside world sees)
/// - Persistent deletion blocks: these represent deletion lists that have already been written to S3 and
///   are pending execution.
/// - Deletions read back frorm the persistent deletion blocks, which are batched up into groups
///   of 1024 for execution via a DeleteObjects call.
#[derive(Clone)]
pub struct DeletionQueue {
    tx: tokio::sync::mpsc::Sender<QueueMessage>,
}

#[derive(Debug)]
enum QueueMessage {
    Delete(DeletionOp),
    Flush(FlushOp),
}

#[derive(Debug)]
struct DeletionOp {
    tenant_id: TenantId,
    timeline_id: TimelineId,
    layers: Vec<LayerFileName>,
}

#[derive(Debug)]
struct FlushOp {
    tx: tokio::sync::oneshot::Sender<()>,
}

impl FlushOp {
    fn fire(self) {
        if let Err(_) = self.tx.send(()) {
            // oneshot channel closed. This is legal: a client could be destroyed while waiting for a flush.
            debug!("deletion queue flush from dropped client");
        };
    }
}

#[derive(Clone)]
pub struct DeletionQueueClient {
    tx: tokio::sync::mpsc::Sender<QueueMessage>,
}

#[serde_as]
#[derive(Debug, Serialize, Deserialize)]
struct DeletionList {
    /// Used for constructing a unique key for each deletion list we write out.
    sequence: u64,

    /// These objects are elegible for deletion: they are unlinked from timeline metadata, and
    /// we are free to delete them at any time from their presence in this data structure onwards.
    objects: Vec<RemotePath>,
}

impl DeletionList {
    fn new(sequence: u64) -> Self {
        Self {
            sequence,
            objects: Vec::new(),
        }
    }
}

impl DeletionQueueClient {
    async fn do_push(&self, msg: QueueMessage) {
        match self.tx.send(msg).await {
            Ok(_) => {}
            Err(e) => {
                // This shouldn't happen, we should shut down all tenants before
                // we shut down the global delete queue.  If we encounter a bug like this,
                // we may leak objects as deletions won't be processed.
                error!("Deletion queue closed while pushing, shutting down? ({e})");
            }
        }
    }

    pub async fn push(
        &self,
        tenant_id: TenantId,
        timeline_id: TimelineId,
        layers: Vec<LayerFileName>,
    ) {
        self.do_push(QueueMessage::Delete(DeletionOp {
            tenant_id,
            timeline_id,
            layers,
        }))
        .await;
    }

    pub async fn flush(&self) {
        let (tx, rx) = tokio::sync::oneshot::channel();
        self.do_push(QueueMessage::Flush(FlushOp { tx })).await;
        if let Err(_) = rx.await {
            // This shouldn't happen if tenants are shut down before deletion queue.  If we
            // encounter a bug like this, then a flusher will incorrectly believe it has flushed
            // when it hasn't, possibly leading to leaking objects.
            error!("Deletion queue dropped flush op while client was still waiting");
        }
    }
}

pub struct BackendQueueWorker {
    remote_storage: Option<GenericRemoteStorage>,
    conf: &'static PageServerConf,
    rx: tokio::sync::mpsc::Receiver<BackendQueueMessage>,
}

impl BackendQueueWorker {
    pub async fn background(&mut self) {
        let remote_storage = match &self.remote_storage {
            Some(rs) => rs,
            None => {
                info!("No remote storage configured, deletion queue will not run");
                return;
            }
        };

        let _span = tracing::info_span!("deletion_backend");

        // TODO: if we would like to be able to defer deletions while a Layer still has
        // refs (but it will be elegible for deletion after process ends), then we may
        // add an ephemeral part to BackendQueueMessage::Delete that tracks which keys
        // in the deletion list may not be deleted yet, with guards to block on while
        // we wait to proceed.
        while let Some(msg) = self.rx.recv().await {
            match msg {
                BackendQueueMessage::Delete(list) => {
                    for key in list.objects {
                        // TODO: batch into DeleteObjects calls (the DeletionList is not
                        // necessarily one batch, it can be bigger or smaller)
                        //
                        // TODO: if the delete fails, push the DeletionList into a retry slot
                        // so that we try it again rather than pulling more from the channel
                        remote_storage.delete(&key).await.expect("TODO retry");
                    }
                }
                BackendQueueMessage::Flush(op) => {
                    // TODO: add an extra frrontend flush type that passes through to this flush
                    // We have implicitly already processed preceeding deletions
                    op.fire();
                }
            }
        }
    }
}

#[derive(Debug)]
enum BackendQueueMessage {
    Delete(DeletionList),
    Flush(FlushOp),
}

pub struct FrontendQueueWorker {
    remote_storage: Option<GenericRemoteStorage>,
    conf: &'static PageServerConf,

    // Incoming frontend requests to delete some keys
    rx: tokio::sync::mpsc::Receiver<QueueMessage>,

    // Outbound requests to the backend to execute deletion lists we have composed.
    tx: tokio::sync::mpsc::Sender<BackendQueueMessage>,

    // The list we are currently building, contains a buffer of keys to delete
    // and our next sequence number
    pending: DeletionList,

    // When we should next proactively flush if we have pending deletions, even if
    // the target deletion list size has not been reached.
    deadline: Instant,

    // These FlushOps should fire the next time we flush
    pending_flushes: Vec<FlushOp>,
}

impl FrontendQueueWorker {
    /// Try to flush `list` to persistent storage
    ///
    /// This does not return errors, because on failure to flush we do not lose
    /// any state: flushing will be retried implicitly on the next deadline
    async fn flush(&mut self) {
        let key = RemotePath::new(&self.conf.remote_deletion_list_path(self.pending.sequence))
            .expect("Failed to compose deletion list path");

        // We don't run this worker unless there is remote storage available.
        let remote_storage = self.remote_storage.as_ref().unwrap();

        let bytes = serde_json::to_vec(&self.pending).expect("Failed to serialize deletion list");
        let size = bytes.len();
        let source = tokio::io::BufReader::new(std::io::Cursor::new(bytes));

        match remote_storage.upload(source, size, &key, None).await {
            Ok(_) => {
                for f in self.pending_flushes.drain(..) {
                    f.fire();
                }

                let mut onward_list = DeletionList {
                    sequence: self.pending.sequence,
                    objects: Vec::new(),
                };
                std::mem::swap(&mut onward_list.objects, &mut self.pending.objects);
                self.pending.sequence += 1;

                if let Err(e) = self.tx.send(BackendQueueMessage::Delete(onward_list)).await {
                    // This is allowed to fail: it will only happen if the backend worker is shut down,
                    // so we can just drop this on the floor.
                    info!("Deletion list dropped, this is normal during shutdown ({e})");
                }
            }
            Err(e) => {
                warn!(
                    sequence = self.pending.sequence,
                    "Failed to flush deletion list, will retry later ({e})"
                )
            }
        }
    }

    /// This is the front-end ingest, where we bundle up deletion requests into DeletionList
    /// and write them out, for later
    pub async fn background(&mut self) {
        loop {
            let flush_delay = self.deadline.duration_since(Instant::now());

            // Wait for the next message, or to hit self.deadline
            let msg = tokio::select! {
                msg_opt = self.rx.recv() => {
                    match msg_opt {
                        None => {
                            break;
                        },
                        Some(msg)=> {msg}
                    }
                },
                _ = tokio::time::sleep(flush_delay) => {
                    if !self.pending.objects.is_empty() {
                        debug!("Flushing for deadline");
                        self.flush().await;
                    }
                    continue;
                }
            };

            match msg {
                QueueMessage::Delete(op) => {
                    let timeline_path = self.conf.timeline_path(&op.tenant_id, &op.timeline_id);

                    let _span = tracing::info_span!(
                        "execute_deletion",
                        tenant_id = %op.tenant_id,
                        timeline_id = %op.timeline_id,
                    );

                    for layer in op.layers {
                        // TODO go directly to remote path without composing local path
                        let local_path = timeline_path.join(layer.file_name());
                        let path = match self.conf.remote_path(&local_path) {
                            Ok(p) => p,
                            Err(e) => {
                                panic!("Can't make a timeline path! {e}");
                            }
                        };
                        self.pending.objects.push(path);
                    }
                }
                QueueMessage::Flush(op) => {
                    if self.pending.objects.is_empty() {
                        // Execute immediately
                        op.fire()
                    } else {
                        // Execute next time we flush
                        self.pending_flushes.push(op);

                        // Move up the deadline since we have been explicitly asked to flush
                        let flush_delay = self.deadline.duration_since(Instant::now());
                        if flush_delay > FLUSH_EXPLICIT_DEADLINE {
                            self.deadline = Instant::now() + FLUSH_EXPLICIT_DEADLINE;
                        }
                    }
                }
            }

            if self.pending.objects.len() > DELETION_LIST_TARGET_SIZE {
                debug!(sequence = self.pending.sequence, "Flushing for deadline");
                self.flush().await;
            }
        }
        info!("Deletion queue shut down.");
    }
}
impl DeletionQueue {
    pub fn new_client(&self) -> DeletionQueueClient {
        DeletionQueueClient {
            tx: self.tx.clone(),
        }
    }

    pub fn new(
        remote_storage: Option<GenericRemoteStorage>,
        conf: &'static PageServerConf,
    ) -> (Self, FrontendQueueWorker, BackendQueueWorker) {
        let (tx, rx) = tokio::sync::mpsc::channel(16384);

        let (backend_tx, backend_rx) = tokio::sync::mpsc::channel(16384);

        (
            Self { tx },
            FrontendQueueWorker {
                // TODO: on startup, recover sequence number by listing persistent list objects,
                // *or* if we implement generation numbers, we may start from 0 every time
                pending: DeletionList::new(0xdeadbeef),
                remote_storage: remote_storage.as_ref().map(|s| s.clone()),
                conf,
                rx,
                tx: backend_tx,
                deadline: Instant::now() + FLUSH_DEFAULT_DEADLINE,
                pending_flushes: Vec::new(),
            },
            BackendQueueWorker {
                remote_storage,
                conf,
                rx: backend_rx,
            },
        )
    }
}

/// A lightweight queue which can issue ordinary DeletionQueueClient objects, but doesn't do any persistence
/// or coalescing, and doesn't actually execute any deletions unless you call pump() to kick it.
pub mod mock {

    #[cfg(test)]
    pub struct MockDeletionQueue {
        tx: tokio::sync::mpsc::Sender<QueueMessage>,
        tx_pump: tokio::sync::mpsc::Sender<FlushOp>,
        executed: Arc<AtomicUsize>,
    }
    #[cfg(test)]
    impl MockDeletionQueue {
        pub fn new(
            remote_storage: Option<GenericRemoteStorage>,
            conf: &'static PageServerConf,
        ) -> Self {
            let (tx, mut rx) = tokio::sync::mpsc::channel(16384);
            let (tx_pump, mut rx_pump) = tokio::sync::mpsc::channel::<FlushOp>(1);

            let executed = Arc::new(AtomicUsize::new(0));
            let executed_bg = executed.clone();

            tokio::spawn(async move {
                let _span = tracing::info_span!("mock_deletion_queue");
                let remote_storage = match &remote_storage {
                    Some(rs) => rs,
                    None => {
                        info!("No remote storage configured, deletion queue will not run");
                        return;
                    }
                };
                info!("Running mock deletion queue");
                // Each time we are asked to pump, drain the queue of deletions
                while let Some(flush_op) = rx_pump.recv().await {
                    info!("Executing all pending deletions");
                    while let Ok(msg) = rx.try_recv() {
                        match msg {
                            QueueMessage::Delete(op) => {
                                let timeline_path =
                                    conf.timeline_path(&op.tenant_id, &op.timeline_id);

                                let _span = tracing::info_span!(
                                    "execute_deletion",
                                    tenant_id = %op.tenant_id,
                                    timeline_id = %op.timeline_id,
                                );

                                for layer in op.layers {
                                    let local_path = timeline_path.join(layer.file_name());
                                    let path = match conf.remote_path(&local_path) {
                                        Ok(p) => p,
                                        Err(e) => {
                                            panic!("Can't make a timeline path! {e}");
                                        }
                                    };
                                    info!("Executing deletion {path}");
                                    match remote_storage.delete(&path).await {
                                        Ok(_) => {
                                            debug!("Deleted {path}");
                                        }
                                        Err(e) => {
                                            error!(
                                                "Failed to delete {path}, leaking object! ({e})"
                                            );
                                        }
                                    }
                                    executed_bg.fetch_add(1, Ordering::Relaxed);
                                }
                            }
                            QueueMessage::Flush(op) => {
                                if let Err(_) = op.tx.send(()) {
                                    // oneshot channel closed. This is legal: a client could be destroyed while waiting for a flush.
                                    debug!("deletion queue flush from dropped client");
                                };
                            }
                        }
                        info!("All pending deletions have been executed");
                    }
                    flush_op
                        .tx
                        .send(())
                        .expect("Test called flush but dropped before finishing");
                }
            });

            Self {
                tx: tx,
                tx_pump,
                executed,
            }
        }

        pub fn get_executed(&self) -> usize {
            self.executed.load(Ordering::Relazed)
        }

        pub async fn pump(&self) {
            let (tx, rx) = tokio::sync::oneshot::channel();
            self.tx_pump
                .send(FlushOp { tx })
                .await
                .expect("pump called after deletion queue loop stopped");
            rx.await
                .expect("Mock delete queue shutdown while waiting to pump");
        }

        pub fn new_client(&self) -> DeletionQueueClient {
            DeletionQueueClient {
                tx: self.tx.clone(),
            }
        }
    }
}
