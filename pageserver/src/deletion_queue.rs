use remote_storage::{GenericRemoteStorage, RemotePath};
use tokio::sync::mpsc::{channel, Receiver, Sender};

use crate::{config::PageServerConf, tenant::storage_layer::LayerFileName};

/// We aggregate object deletions from many tenants in one place, for several reasons:
/// - Coalesce deletions into fewer DeleteObjects calls
/// - Enable Tenant/Timeline lifetimes to be shorter than the time it takes
///   to flush any outstanding deletions.
/// - Globally control throughput of deletions, as these are a low priority task: do
///   not compete with the same S3 clients/connections used for higher priority uploads.
///
/// DeletionQueue is the frontend that the rest of the pageserver interacts with.
pub struct DeletionQueue {
    tx: Sender<DeletionOp>,
}
struct DeletionOp {
    tenant_id: TenantId,
    timeline_id: TimelineId,
    layers: Vec<LayerFileName>,
}
pub struct DeletionQueueClient {
    tx: Sender<DeletionOp>,
}

impl DeletionQueueClient {
    pub async fn push(tenant_id: TenantId, timeline_id: TimelineId, layers: Vec<LayerFileName>) {}
}

impl DeletionQueue {
    pub fn new_client(&self) -> DeletionQueueClient {
        DeletionQueueClient {
            tx: self.tx.clone(),
        }
    }

    fn consume_loop(
        remote_storage: GenericRemoteStorage,
        conf: &'static PageServerConf,
        rx: Receiver<DeletionOp>,
    ) {
        while let Some(op) = rx.recv().await {
            let timeline_path = self.conf.timeline_path(&op.tenant_id, &op.timeline_id);
            let timeline_storage_path = conf.remote_path(&timeline_path)?;

            let span = tracing::info_span(
                "deletion",
                tenant_id = op.tenant_id,
                timeline_id = op.timeline_id,
            );

            for layer in op.layers {
                let path = timeline_storage_path.join(layer.file_name());
                match remote_storage.delete(&path).await {
                    Ok() => {
                        debug!("Deleted {path}");
                    }
                    Err(e) => {
                        error!("Failed to delete {path}, leaking object!");
                    }
                }
            }
        }
    }

    pub fn new(remote_storage: GenericRemoteStorage, conf: &'static PageServerConf) -> Self {
        let (tx, rx) = channel::new(16384);

        tokio::spawn(async move { consume_loop(remote_storage, conf, rx).await });

        Self { tx }
    }
}
