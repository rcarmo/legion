//! Raft bootstrap logic: determines whether this node should form a new
//! single-node cluster or join an existing one.

use std::{collections::BTreeMap, sync::Arc};

use anyhow::Result;
use iroh::EndpointId;
use tracing::{info, warn};

use crate::bonjour::BonjourRegistration;
use crate::node::ClusterNode;

/// The decision the bootstrap phase arrives at.
#[derive(Debug, Clone)]
pub enum BootstrapOutcome {
    /// No other Legion nodes found — this node becomes the single-node leader.
    Bootstrap {
        endpoint_id: EndpointId,
        #[doc(hidden)]
        registration: Option<Arc<BonjourRegistration>>,
    },
    /// At least one other peer found — this node should join as a follower.
    Join {
        endpoint_id: EndpointId,
        peers: Vec<DiscoveredPeer>,
        #[doc(hidden)]
        registration: Option<Arc<BonjourRegistration>>,
    },
}

/// Raft coordinates advertised by a discovered Legion node.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DiscoveredPeer {
    pub endpoint_id: String,
    pub host: String,
    pub api_port: u16,
    pub raft_id: Option<u64>,
    pub raft_addr: Option<String>,
    pub raft_api_addr: Option<String>,
}

/// Local Raft coordinates advertised during discovery.
#[derive(Debug, Clone, Default)]
pub struct RaftAdvertisement {
    pub node_id: Option<u64>,
    pub raft_addr: Option<String>,
    pub api_addr: Option<String>,
}

/// Duration to listen for mDNS peers before deciding.
const DISCOVERY_WINDOW: tokio::time::Duration = tokio::time::Duration::from_secs(3);

/// Run the bootstrap probe: register on Bonjour, listen for other nodes,
/// then decide whether to become a single-node leader or join peers.
pub async fn run_bootstrap(node: &ClusterNode) -> Result<BootstrapOutcome> {
    run_bootstrap_with_raft(node, RaftAdvertisement::default()).await
}

/// Bootstrap with optional Raft coordinates published in Bonjour TXT records.
pub async fn run_bootstrap_with_raft(
    node: &ClusterNode,
    raft: RaftAdvertisement,
) -> Result<BootstrapOutcome> {
    run_bootstrap_with_window(node, raft, DISCOVERY_WINDOW).await
}

/// Bootstrap with a caller-supplied discovery window (used by integration tests).
pub async fn run_bootstrap_with_window(
    node: &ClusterNode,
    raft: RaftAdvertisement,
    discovery_window: tokio::time::Duration,
) -> Result<BootstrapOutcome> {
    let eid_str = node.endpoint_id().to_string();
    if !node.config.mdns {
        return Ok(BootstrapOutcome::Bootstrap {
            endpoint_id: node.endpoint_id(),
            registration: None,
        });
    }
    let ip_addr = local_ip().unwrap_or_else(|| "127.0.0.1".into());
    let hostname = format!(
        "legion-{}",
        &node.short_id()[..8.min(node.short_id().len())]
    );

    let mut properties = Vec::new();
    if let Some(node_id) = raft.node_id {
        properties.push(("raft_id", node_id.to_string()));
    }
    if let Some(address) = raft.raft_addr {
        properties.push(("raft_addr", address));
    }
    if let Some(address) = raft.api_addr {
        properties.push(("raft_api_addr", address));
    }
    let reg = BonjourRegistration::register(
        &eid_str,
        &hostname,
        &ip_addr,
        node.config.api_port,
        &properties,
    )?;

    let receiver = reg.browse()?;
    let mut peers = BTreeMap::new();
    let self_id = eid_str.clone();

    info!("bootstrap probe: {}ms window", discovery_window.as_millis());

    let deadline = tokio::time::Instant::now() + discovery_window;
    loop {
        if tokio::time::Instant::now() >= deadline {
            break;
        }
        match receiver.try_recv() {
            Ok(mdns_sd::ServiceEvent::ServiceResolved(info)) => {
                if let Some(pid_prop) = info.get_properties().get("node_id") {
                    let pid = pid_prop.val_str();
                    if pid != self_id {
                        info!(peer_id = %pid, "discovered peer during bootstrap");
                        let properties = info.get_properties();
                        peers.insert(
                            pid.to_string(),
                            DiscoveredPeer {
                                endpoint_id: pid.to_string(),
                                host: info.get_hostname().trim_end_matches('.').to_string(),
                                api_port: info.get_port(),
                                raft_id: properties
                                    .get("raft_id")
                                    .and_then(|value| value.val_str().parse().ok()),
                                raft_addr: BonjourRegistration::resolve_advertised_addr(
                                    &info,
                                    "raft_addr",
                                ),
                                raft_api_addr: BonjourRegistration::resolve_advertised_addr(
                                    &info,
                                    "raft_api_addr",
                                ),
                            },
                        );
                    }
                }
            }
            Ok(_) => {}
            Err(mdns_sd::TryRecvError::Empty) => {
                tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;
            }
            Err(e) => {
                warn!("mdns recv: {e}");
                break;
            }
        }
    }

    let outcome = if peers.is_empty() {
        info!(node = %self_id, "no peers — bootstrapping as single-node leader");
        BootstrapOutcome::Bootstrap {
            endpoint_id: node.endpoint_id(),
            registration: Some(Arc::new(reg)),
        }
    } else {
        let peer_list: Vec<DiscoveredPeer> = peers.into_values().collect();
        info!(node = %self_id, ?peer_list, "found peers — will join cluster");
        BootstrapOutcome::Join {
            endpoint_id: node.endpoint_id(),
            peers: peer_list,
            registration: Some(Arc::new(reg)),
        }
    };

    Ok(outcome)
}

/// Best-effort: return the first non-loopback IPv4 address of this machine.
fn local_ip() -> Option<String> {
    use std::net::{IpAddr, UdpSocket};
    let s = UdpSocket::bind("0.0.0.0:0").ok()?;
    s.connect("8.8.8.8:80").ok()?;
    match s.local_addr().ok()?.ip() {
        IpAddr::V4(v4) if !v4.is_loopback() => Some(v4.to_string()),
        _ => None,
    }
}
