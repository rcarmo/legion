pub mod node;
pub mod discovery;
pub mod bonjour;
pub mod bootstrap;
pub mod membership;

pub use node::{ClusterNode, NodeConfig, NodeIdentity};
pub use bootstrap::BootstrapOutcome;
pub use membership::{start_membership, MembershipHandle, NodePresence};

#[cfg(test)]
mod tests {
    use super::*;
    use node::{ClusterNode, NodeConfig};
    use tempfile::TempDir;

    fn test_config(dir: &TempDir) -> NodeConfig {
        NodeConfig {
            data_dir: dir.path().to_path_buf(),
            bind_addr: "127.0.0.1:0".into(),
            api_port: 0,
            mdns: false, // don't blast mDNS in CI
        }
    }

    #[tokio::test]
    async fn node_starts_and_has_endpoint_id() {
        let dir = tempfile::tempdir().unwrap();
        let node = ClusterNode::start(test_config(&dir)).await.unwrap();
        let eid = node.endpoint_id();
        // Public key should be non-zero
        assert_ne!(eid.to_string(), "0000000000000000000000000000000000000000000000000000000000000000");
    }

    #[tokio::test]
    async fn node_keypair_is_stable_across_restarts() {
        let dir = tempfile::tempdir().unwrap();

        let eid1 = {
            let n = ClusterNode::start(test_config(&dir)).await.unwrap();
            n.endpoint_id().to_string()
        };
        let eid2 = {
            let n = ClusterNode::start(test_config(&dir)).await.unwrap();
            n.endpoint_id().to_string()
        };

        assert_eq!(eid1, eid2, "endpoint id must be stable across restarts");
    }
}
