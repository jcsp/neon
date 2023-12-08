use crate::{background_process, local_env::LocalEnv};
use anyhow::anyhow;
use camino::Utf8PathBuf;
use hyper::{Method, StatusCode};
use pageserver_api::{
    models::{TenantCreateRequest, TimelineCreateRequest},
    shard::TenantShardId,
};
use postgres_connection::parse_host_port;
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use std::{path::PathBuf, process::Child};
use tracing::instrument;
use utils::id::{NodeId, TenantId};

pub struct AttachmentService {
    env: LocalEnv,
    listen: String,
    path: PathBuf,
    client: reqwest::Client,
}

const COMMAND: &str = "attachment_service";

#[derive(Serialize, Deserialize)]
pub struct AttachHookRequest {
    pub tenant_shard_id: TenantShardId,
    pub node_id: Option<NodeId>,
}

#[derive(Serialize, Deserialize)]
pub struct AttachHookResponse {
    pub gen: Option<u32>,
}

#[derive(Serialize, Deserialize)]
pub struct InspectRequest {
    pub tenant_shard_id: TenantShardId,
}

#[derive(Serialize, Deserialize)]
pub struct InspectResponse {
    pub attachment: Option<(u32, NodeId)>,
}

#[derive(Serialize, Deserialize)]
pub struct TenantCreateResponseShard {
    pub node_id: NodeId,
    pub generation: u32,
}

#[derive(Serialize, Deserialize)]
pub struct TenantCreateResponse {
    pub shards: Vec<TenantCreateResponseShard>,
}

#[derive(Serialize, Deserialize)]
pub struct NodeRegisterRequest {
    pub node_id: NodeId,

    pub listen_pg_addr: String,
    pub listen_pg_port: u16,

    pub listen_http_addr: String,
    pub listen_http_port: u16,
}

#[derive(Serialize, Deserialize)]
pub struct TenantLocateResponseShard {
    pub node_id: NodeId,

    pub listen_pg_addr: String,
    pub listen_pg_port: u16,

    pub listen_http_addr: String,
    pub listen_http_port: u16,
}

#[derive(Serialize, Deserialize)]
pub struct TenantLocateResponse {
    pub shards: Vec<TenantLocateResponseShard>,
}

impl AttachmentService {
    pub fn from_env(env: &LocalEnv) -> Self {
        let path = env.base_data_dir.join("attachments.json");

        // Makes no sense to construct this if pageservers aren't going to use it: assume
        // pageservers have control plane API set
        let listen_url = env.control_plane_api.clone().unwrap();

        let listen = format!(
            "{}:{}",
            listen_url.host_str().unwrap(),
            listen_url.port().unwrap()
        );

        Self {
            env: env.clone(),
            path,
            listen,
            client: reqwest::ClientBuilder::new()
                .build()
                .expect("Failed to construct http client"),
        }
    }

    fn pid_file(&self) -> Utf8PathBuf {
        Utf8PathBuf::from_path_buf(self.env.base_data_dir.join("attachment_service.pid"))
            .expect("non-Unicode path")
    }

    pub async fn start(&self) -> anyhow::Result<Child> {
        let path_str = self.path.to_string_lossy();

        let result = background_process::start_process(
            COMMAND,
            &self.env.base_data_dir,
            &self.env.attachment_service_bin(),
            ["-l", &self.listen, "-p", &path_str],
            [],
            background_process::InitialPidFile::Create(&self.pid_file()),
            || match self.status() {
                Ok(_) => Ok(true),
                Err(_) => Ok(false),
            },
        )
        .await;

        for ps_conf in &self.env.pageservers {
            let (pg_host, pg_port) =
                parse_host_port(&ps_conf.listen_pg_addr).expect("Unable to parse listen_pg_addr");
            let (http_host, http_port) = parse_host_port(&ps_conf.listen_http_addr)
                .expect("Unable to parse listen_http_addr");
            self.node_register(NodeRegisterRequest {
                node_id: ps_conf.id,
                listen_pg_addr: pg_host.to_string(),
                listen_pg_port: pg_port.unwrap_or(5432),
                listen_http_addr: http_host.to_string(),
                listen_http_port: http_port.unwrap_or(80),
            })?;
        }

        result
    }

    pub fn stop(&self, immediate: bool) -> anyhow::Result<()> {
        background_process::stop_process(immediate, COMMAND, &self.pid_file())
    }

    /// Simple HTTP request wrapper for calling into attachment service
    fn dispatch<RQ, RS>(
        &self,
        method: hyper::Method,
        path: String,
        body: Option<RQ>,
    ) -> anyhow::Result<RS>
    where
        RQ: Serialize + Sized,
        RS: DeserializeOwned + Sized,
    {
        let url = self
            .env
            .control_plane_api
            .clone()
            .unwrap()
            .join(&path)
            .unwrap();

        let mut builder = self.client.request(method, url);
        if let Some(body) = body {
            builder = builder.json(&body)
        }

        let response = builder.send()?;
        if response.status() != StatusCode::OK {
            return Err(anyhow!(
                "Unexpected status {} on {}",
                response.status(),
                path
            ));
        }

        Ok(response.json()?)
    }

    /// Call into the attach_hook API, for use before handing out attachments to pageservers
    #[instrument(skip(self))]
    pub async fn attach_hook(
        &self,
        tenant_shard_id: TenantShardId,
        pageserver_id: NodeId,
    ) -> anyhow::Result<Option<u32>> {
        let url = self
            .env
            .control_plane_api
            .clone()
            .unwrap()
            .join("attach-hook")
            .unwrap();

        let request = AttachHookRequest {
            tenant_shard_id,
            node_id: Some(pageserver_id),
        };

        let response = self.client.post(url).json(&request).send().await?;
        if response.status() != StatusCode::OK {
            return Err(anyhow!("Unexpected status {}", response.status()));
        }

        let response = response.json::<AttachHookResponse>().await?;
        Ok(response.gen)
    }

    #[instrument(skip(self))]
    pub async fn inspect(
        &self,
        tenant_shard_id: TenantShardId,
    ) -> anyhow::Result<Option<(u32, NodeId)>> {
        let url = self
            .env
            .control_plane_api
            .clone()
            .unwrap()
            .join("inspect")
            .unwrap();

        let request = InspectRequest { tenant_shard_id };

        let response = self.client.post(url).json(&request).send().await?;
        if response.status() != StatusCode::OK {
            return Err(anyhow!("Unexpected status {}", response.status()));
        }

        let response = response.json::<InspectResponse>().await?;
        Ok(response.attachment)
    }

    #[instrument(skip(self))]
    pub fn tenant_create(&self, req: TenantCreateRequest) -> anyhow::Result<TenantCreateResponse> {
        self.dispatch(Method::POST, "tenant".to_string(), Some(req))
    }

    #[instrument(skip(self))]
    pub fn tenant_locate(&self, tenant_id: TenantId) -> anyhow::Result<TenantLocateResponse> {
        self.dispatch::<(), _>(Method::GET, format!("tenant/{tenant_id}/locate"), None)
    }

    #[instrument(skip_all, fields(node_id=%req.node_id))]
    pub fn node_register(&self, req: NodeRegisterRequest) -> anyhow::Result<()> {
        self.dispatch::<_, ()>(Method::POST, "node".to_string(), Some(req))
    }

    #[instrument(skip(self))]
    pub fn status(&self) -> anyhow::Result<()> {
        self.dispatch::<(), ()>(Method::GET, "status".to_string(), None)
    }

    #[instrument(skip_all, fields(%tenant_id, timeline_id=%req.new_timeline_id))]
    pub fn tenant_timeline_create(
        &self,
        tenant_id: TenantId,
        req: TimelineCreateRequest,
    ) -> anyhow::Result<()> {
        self.dispatch(
            Method::POST,
            format!("tenant/{tenant_id}/timeline"),
            Some(req),
        )
    }
}
