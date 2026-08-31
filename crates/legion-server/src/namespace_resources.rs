//! Server-owned dynamic resources exposed through the transport-neutral 9P namespace hooks.

use std::sync::Arc;

use async_trait::async_trait;
use legion_cluster::ClusterNode;
use legion_core::error::{LegionError, Result};
use legion_deploy::{DeployJob, DeployPipeline};
use legion_namespace::{ClusterNamespace, DeployNamespace, Namespace, NodeKind};
use legion_runtime::manifest::FunctionRuntime;
use serde_json::{Value, json};

#[cfg(feature = "distributed")]
use legion_store::HiqliteStore;

pub struct ServerDeployNamespace {
    deployer: Arc<DeployPipeline>,
    namespace: Namespace,
}

impl ServerDeployNamespace {
    pub fn new(deployer: Arc<DeployPipeline>, namespace: Namespace) -> Self {
        Self {
            deployer,
            namespace,
        }
    }

    fn decode_job(data: &[u8]) -> Result<DeployJob> {
        serde_json::from_slice(data).map_err(LegionError::Serialization)
    }
}

#[async_trait]
impl DeployNamespace for ServerDeployNamespace {
    async fn read(&self, path: &str) -> Result<Option<Vec<u8>>> {
        let Some(node) = self.namespace.get(path).await else {
            return Ok(None);
        };
        let bytes = match node.kind {
            NodeKind::Json(value) => serde_json::to_vec(&value)?,
            NodeKind::Blob(bytes) => bytes.to_vec(),
            NodeKind::Dir => serde_json::to_vec(&self.namespace.ls(path).await)?,
        };
        Ok(Some(bytes))
    }

    async fn write(&self, path: &str, data: &[u8]) -> Result<Option<Vec<u8>>> {
        let response = match path {
            "/deploy/register" => {
                let outcome = self.deployer.deploy(Self::decode_job(data)?).await;
                serde_json::to_vec(&outcome)?
            }
            "/deploy/route" | "/deploy/promote" => {
                let value: Value = serde_json::from_slice(data)?;
                self.namespace.set_json(path, value.clone()).await;
                serde_json::to_vec(&value)?
            }
            _ if path.starts_with("/deploy/blobs/") => {
                self.namespace.set_blob(path, data.to_vec().into()).await;
                serde_json::to_vec(&json!({
                    "path": path,
                    "size": data.len(),
                }))?
            }
            _ => return Ok(None),
        };
        Ok(Some(response))
    }
}

pub struct ServerClusterNamespace {
    node: Arc<ClusterNode>,
    namespace: Namespace,
    #[cfg(feature = "distributed")]
    store: Arc<HiqliteStore>,
}

impl ServerClusterNamespace {
    #[cfg(feature = "distributed")]
    pub fn new(node: Arc<ClusterNode>, namespace: Namespace, store: Arc<HiqliteStore>) -> Self {
        Self {
            node,
            namespace,
            store,
        }
    }

    #[cfg(not(feature = "distributed"))]
    pub fn new(node: Arc<ClusterNode>, namespace: Namespace) -> Self {
        Self { node, namespace }
    }

    async fn value(&self, path: &str) -> Result<Option<Value>> {
        let value = match path {
            "/cluster/self" => json!({
                "endpoint_id": self.node.endpoint_id().to_string(),
                "short_id": self.node.short_id(),
                "api_port": self.node.config.api_port,
            }),
            "/cluster/health" => {
                let peers = self.namespace.ls("/cluster/peers").await.len();
                #[cfg(feature = "distributed")]
                {
                    json!({
                        "healthy": true,
                        "peers": peers,
                        "raft": self.store.raft_diagnostics().await,
                    })
                }
                #[cfg(not(feature = "distributed"))]
                {
                    json!({"healthy": true, "peers": peers, "raft": "disabled"})
                }
            }
            "/cluster/leader" => {
                #[cfg(feature = "distributed")]
                {
                    json!({"node_id": self.store.raft_leader().await})
                }
                #[cfg(not(feature = "distributed"))]
                {
                    Value::Null
                }
            }
            _ => return Ok(None),
        };
        Ok(Some(value))
    }
}

#[async_trait]
impl ClusterNamespace for ServerClusterNamespace {
    async fn read(&self, path: &str) -> Result<Option<Vec<u8>>> {
        self.value(path)
            .await?
            .map(|value| serde_json::to_vec(&value))
            .transpose()
            .map_err(LegionError::Serialization)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[tokio::test]
    async fn deployment_paths_register_and_read_blobs() {
        let dir = tempdir().unwrap();
        let namespace = Namespace::new();
        let deployer = Arc::new(DeployPipeline::new(
            dir.path().join("fn"),
            namespace.clone(),
        ));
        let resources = ServerDeployNamespace::new(deployer, namespace);
        let job = DeployJob::new("hello", FunctionRuntime::Bun, "test", "export default 1");

        let registered = resources
            .write("/deploy/register", &serde_json::to_vec(&job).unwrap())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            serde_json::from_slice::<Value>(&registered).unwrap()["status"],
            "success"
        );

        resources
            .write("/deploy/blobs/test", b"blob")
            .await
            .unwrap();
        assert_eq!(
            resources.read("/deploy/blobs/test").await.unwrap().unwrap(),
            b"blob"
        );
    }
}
