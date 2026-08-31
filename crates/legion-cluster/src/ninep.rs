//! 9P2000.L protocol service on Legion's existing iroh endpoint.

use futures::{SinkExt, StreamExt};
use iroh::{
    EndpointId,
    endpoint::Connection,
    protocol::{AcceptError, ProtocolHandler, Router},
};
use jetstream_9p::server::Server as NinePServer;
use jetstream_rpc::{
    Frame, IntoError, Mux,
    client::ClientCodec,
    context::Context,
    server::{Server, ServerCodec},
};
use legion_namespace::{LegionNamespace, PeerNamespace};
use tokio::sync::mpsc;
use tokio_util::codec::{FramedRead, FramedWrite};

use crate::ClusterNode;

pub const NINEP_ALPN: &[u8] = b"9p";

type LegionNineP = NinePServer<LegionNamespace>;

/// Start serving the namespace over the node's authenticated iroh endpoint.
pub fn serve_namespace(node: &ClusterNode, namespace: LegionNamespace) -> Router {
    Router::builder(node.endpoint.clone())
        .accept(NINEP_ALPN, NamespaceProtocol::new(namespace))
        .spawn()
}

/// Build the one router that accepts both namespace and gossip traffic.
pub fn serve_namespace_and_gossip(
    node: &ClusterNode,
    namespace: LegionNamespace,
    gossip: iroh_gossip::net::Gossip,
) -> Router {
    Router::builder(node.endpoint.clone())
        .accept(NINEP_ALPN, NamespaceProtocol::new(namespace))
        .accept(iroh_gossip::ALPN, gossip)
        .spawn()
}

/// Minimal 9P client used for transparent peer path forwarding and integration tests.
pub struct NinePClient {
    mux: Mux<LegionNineP>,
}

impl NinePClient {
    pub async fn connect(node: &ClusterNode, endpoint_id: EndpointId) -> anyhow::Result<Self> {
        Self::connect_endpoint(&node.endpoint, endpoint_id).await
    }

    pub async fn connect_endpoint(
        endpoint: &iroh::Endpoint,
        endpoint_id: EndpointId,
    ) -> anyhow::Result<Self> {
        Self::connect_addr(endpoint, iroh::EndpointAddr::new(endpoint_id)).await
    }

    pub async fn connect_addr(
        endpoint: &iroh::Endpoint,
        endpoint_addr: iroh::EndpointAddr,
    ) -> anyhow::Result<Self> {
        let connection = endpoint.connect(endpoint_addr, NINEP_ALPN).await?;
        let (send, recv) = connection.open_bi().await?;
        let transport = tokio_util::codec::Framed::new(
            tokio::io::join(recv, send),
            ClientCodec::<LegionNineP>::default(),
        );
        let mux = Mux::new(256, Box::new(transport));
        let client = Self { mux };
        match client
            .rpc(jetstream_9p::server::Tmessage::Version(
                jetstream_9p::Tversion {
                    msize: jetstream_9p::DEFAULT_MSIZE,
                    version: "9P2000.L".into(),
                },
            ))
            .await?
        {
            jetstream_9p::server::Rmessage::Version(_) => Ok(client),
            response => anyhow::bail!("unexpected 9P version response: {response:?}"),
        }
    }

    async fn rpc(
        &self,
        request: jetstream_9p::server::Tmessage,
    ) -> anyhow::Result<jetstream_9p::server::Rmessage> {
        Ok(self.mux.rpc(Context::default(), request).await.await?.msg)
    }

    pub async fn read_path(&self, path: &str) -> anyhow::Result<Vec<u8>> {
        const FID: u32 = 1;
        const FILE_FID: u32 = 2;
        self.attach(FID).await?;
        self.walk(FID, FILE_FID, path).await?;
        match self
            .rpc(jetstream_9p::server::Tmessage::Read(jetstream_9p::Tread {
                fid: FILE_FID,
                offset: 0,
                count: jetstream_9p::DEFAULT_MSIZE,
            }))
            .await?
        {
            jetstream_9p::server::Rmessage::Read(response) => Ok(response.data.0),
            response => anyhow::bail!("unexpected 9P read response: {response:?}"),
        }
    }

    pub async fn write_path(&self, path: &str, data: Vec<u8>) -> anyhow::Result<Vec<u8>> {
        const FID: u32 = 3;
        const FILE_FID: u32 = 4;
        self.attach(FID).await?;
        self.walk(FID, FILE_FID, path).await?;
        match self
            .rpc(jetstream_9p::server::Tmessage::Write(
                jetstream_9p::Twrite {
                    fid: FILE_FID,
                    offset: 0,
                    data: jetstream_wireformat::Data(data),
                },
            ))
            .await?
        {
            jetstream_9p::server::Rmessage::Write(_) => self.read_path(path).await,
            response => anyhow::bail!("unexpected 9P write response: {response:?}"),
        }
    }

    async fn attach(&self, fid: u32) -> anyhow::Result<()> {
        match self
            .rpc(jetstream_9p::server::Tmessage::Attach(
                jetstream_9p::Tattach {
                    fid,
                    afid: u32::MAX,
                    uname: "legion".into(),
                    aname: "".into(),
                    n_uname: u32::MAX,
                },
            ))
            .await?
        {
            jetstream_9p::server::Rmessage::Attach(_) => Ok(()),
            response => anyhow::bail!("unexpected 9P attach response: {response:?}"),
        }
    }

    async fn walk(&self, fid: u32, newfid: u32, path: &str) -> anyhow::Result<()> {
        let wnames = path
            .trim_matches('/')
            .split('/')
            .filter(|part| !part.is_empty())
            .map(ToString::to_string)
            .collect();
        match self
            .rpc(jetstream_9p::server::Tmessage::Walk(jetstream_9p::Twalk {
                fid,
                newfid,
                wnames,
            }))
            .await?
        {
            jetstream_9p::server::Rmessage::Walk(_) => Ok(()),
            response => anyhow::bail!("unexpected 9P walk response: {response:?}"),
        }
    }
}

#[async_trait::async_trait]
impl PeerNamespace for NinePClient {
    async fn read(&self, path: &str) -> std::io::Result<Vec<u8>> {
        self.read_path(path).await.map_err(std::io::Error::other)
    }

    async fn write(&self, path: &str, data: &[u8]) -> std::io::Result<Vec<u8>> {
        self.write_path(path, data.to_vec())
            .await
            .map_err(std::io::Error::other)
    }
}

/// Iroh protocol handler for the Jetstream 9P server.
#[derive(Clone)]
pub struct NamespaceProtocol {
    inner: LegionNineP,
}

impl std::fmt::Debug for NamespaceProtocol {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("NamespaceProtocol")
            .finish_non_exhaustive()
    }
}

impl NamespaceProtocol {
    pub fn new(namespace: LegionNamespace) -> Self {
        Self {
            inner: LegionNineP::new(namespace),
        }
    }
}

impl ProtocolHandler for NamespaceProtocol {
    async fn accept(&self, connection: Connection) -> Result<(), AcceptError> {
        loop {
            let (send_stream, recv_stream) = match connection.accept_bi().await {
                Ok(streams) => streams,
                Err(_) => break,
            };
            let handler = self.inner.clone();
            tokio::spawn(async move {
                let mut reader = FramedRead::new(recv_stream, ServerCodec::<LegionNineP>::new());
                let mut writer = FramedWrite::new(send_stream, ServerCodec::<LegionNineP>::new());
                let (responses, mut response_rx) = mpsc::channel::<Frame<_>>(256);
                let writer_task = tokio::spawn(async move {
                    while let Some(response) = response_rx.recv().await {
                        if writer.send(response).await.is_err() {
                            break;
                        }
                    }
                });
                while let Some(request) = reader.next().await {
                    let Ok(request) = request else { break };
                    let mut handler = handler.clone();
                    let responses = responses.clone();
                    tokio::spawn(async move {
                        match handler
                            .rpc(jetstream_rpc::context::Context::default(), request)
                            .await
                        {
                            Ok(response) => {
                                let _ = responses.send(response).await;
                            }
                            Err(error) => {
                                tracing::warn!(error = %error.into_error(), "9P request failed");
                            }
                        }
                    });
                }
                drop(responses);
                let _ = writer_task.await;
            });
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use async_trait::async_trait;
    use legion_core::{error::Result as LegionResult, types::RunId};
    use legion_namespace::{Namespace, NamespaceResources};
    use serde_json::json;

    #[derive(Default)]
    struct TestSessionResources(tokio::sync::Mutex<Option<RunId>>);

    #[async_trait]
    impl NamespaceResources for TestSessionResources {
        async fn read(&self, path: &str) -> LegionResult<Option<Vec<u8>>> {
            if path == "/sessions/new" {
                return Ok(self
                    .0
                    .lock()
                    .await
                    .map(|run_id| run_id.to_string().into_bytes()));
            }
            Ok(None)
        }

        async fn write(&self, path: &str, _data: &[u8]) -> LegionResult<Option<Vec<u8>>> {
            if path == "/sessions/new" {
                let run_id = uuid::Uuid::new_v4();
                *self.0.lock().await = Some(run_id);
                return Ok(Some(run_id.to_string().into_bytes()));
            }
            Ok(None)
        }
    }

    fn config(path: std::path::PathBuf) -> crate::NodeConfig {
        crate::NodeConfig {
            data_dir: path,
            bind_addr: "127.0.0.1:0".into(),
            api_port: 0,
            mdns: false,
        }
    }

    #[tokio::test]
    async fn authenticated_iroh_9p_read_and_write_roundtrip() {
        let server_dir = tempfile::tempdir().unwrap();
        let client_dir = tempfile::tempdir().unwrap();
        let server = ClusterNode::start(config(server_dir.path().into()))
            .await
            .unwrap();
        let client = ClusterNode::start(config(client_dir.path().into()))
            .await
            .unwrap();
        let namespace = Namespace::new();
        namespace
            .set_json("/cluster/health", json!({"ok": true}))
            .await;
        let _router = serve_namespace(&server, LegionNamespace::new(namespace.clone()));

        let remote = NinePClient::connect_addr(&client.endpoint, server.endpoint.addr())
            .await
            .unwrap();
        let data = remote.read_path("/cluster/health").await.unwrap();
        assert_eq!(
            serde_json::from_slice::<serde_json::Value>(&data).unwrap(),
            json!({"ok": true})
        );
        let response = remote
            .write_path("/cluster/health", br#"{"ok":false}"#.to_vec())
            .await
            .unwrap();
        assert_eq!(
            serde_json::from_slice::<serde_json::Value>(&response).unwrap(),
            json!({"ok": false})
        );
        assert_eq!(
            namespace
                .get("/cluster/health")
                .await
                .unwrap()
                .kind
                .as_json(),
            Some(&json!({"ok": false}))
        );
    }

    #[tokio::test]
    async fn full_session_creation_roundtrip_over_ninep() {
        let server_dir = tempfile::tempdir().unwrap();
        let client_dir = tempfile::tempdir().unwrap();
        let server = ClusterNode::start(config(server_dir.path().into()))
            .await
            .unwrap();
        let client = ClusterNode::start(config(client_dir.path().into()))
            .await
            .unwrap();
        let namespace = LegionNamespace::new(Namespace::new())
            .with_resources(std::sync::Arc::new(TestSessionResources::default()));
        let _router = serve_namespace(&server, namespace);

        let remote = NinePClient::connect_addr(&client.endpoint, server.endpoint.addr())
            .await
            .unwrap();
        let response = remote
            .write_path("/sessions/new", br#"{"model":"faux/test"}"#.to_vec())
            .await
            .unwrap();
        let run_id = String::from_utf8(response).unwrap();
        assert!(run_id.parse::<uuid::Uuid>().is_ok());
        assert_eq!(
            String::from_utf8(remote.read_path("/sessions/new").await.unwrap()).unwrap(),
            run_id
        );
    }

    #[tokio::test]
    async fn one_router_accepts_gossip_and_ninep_alpns() {
        let server_dir = tempfile::tempdir().unwrap();
        let client_dir = tempfile::tempdir().unwrap();
        let server = ClusterNode::start(config(server_dir.path().into()))
            .await
            .unwrap();
        let client = ClusterNode::start(config(client_dir.path().into()))
            .await
            .unwrap();
        let namespace = Namespace::new();
        namespace
            .set_json("/cluster/health", json!({"ok": true}))
            .await;
        let gossip = iroh_gossip::net::Gossip::builder().spawn(server.endpoint.clone());
        let _router = serve_namespace_and_gossip(&server, LegionNamespace::new(namespace), gossip);

        let remote = NinePClient::connect_addr(&client.endpoint, server.endpoint.addr())
            .await
            .unwrap();
        assert!(remote.read_path("/cluster/health").await.is_ok());

        let gossip_connection = client
            .endpoint
            .connect(server.endpoint.addr(), iroh_gossip::ALPN)
            .await
            .unwrap();
        assert_eq!(gossip_connection.alpn(), iroh_gossip::ALPN);
    }
}
