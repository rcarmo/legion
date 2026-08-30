//! Raft bootstrap logic: determines whether this node should form a new
//! single-node cluster or join an existing one.

use std::collections::HashSet;
use std::time::Duration;

use anyhow::Result;
use iroh::EndpointId;
use tokio::time::timeout;
use tracing::{info, warn};

use crate::bonjour::BonjourRegistration;
use crate::node::ClusterNode;

/// The decision the bootstrap phase arrives at.
#[derive(Debug, Clone)]
pub enum BootstrapOutcome {
    /// No other Legion nodes found — this node becomes the single-node leader.
    Bootstrap { endpoint_id: EndpointId },
    /// At least one other peer found — this node should join as a follower.
    Join { endpoint_id: EndpointId, peers: Vec<String> },
}

/// Duration to listen for mDNS peers before deciding.
const DISCOVERY_WINDOW: Duration = Duration::from_secs(3);

/// Run the bootstrap probe: register on Bonjour, listen for other nodes,
/// then decide whether to become a single-node leader or join peers.
pub async fn run_bootstrap(node: &ClusterNode) -> Result<BootstrapOutcome> {
    let eid_str  = node.endpoint_id().to_string();
    let ip_addr  = local_ip().unwrap_or_else(|| "127.0.0.1".into());
    let hostname = format!("legion-{}", &node.short_id()[..8.min(node.short_id().len())]);

    let reg = BonjourRegistration::register(
        &eid_str,
        &hostname,
        &ip_addr,
        node.config.api_port,
    )?;

    let receiver   = reg.browse()?;
    let mut peers  = HashSet::new();
    let self_id    = eid_str.clone();

    info!("bootstrap probe: {}ms window", DISCOVERY_WINDOW.as_millis());

    // Non-blocking channel drain with timeout
    let _ = timeout(DISCOVERY_WINDOW, async {
        loop {
            match receiver.recv() {
                Ok(mdns_sd::ServiceEvent::ServiceResolved(info)) => {
                    if let Some(pid_prop) = info.get_properties().get("node_id") {
                            let pid = pid_prop.val_str();
                            if pid != self_id {
                                info!(peer_id = %pid, "discovered peer during bootstrap");
                                peers.insert(format!(
                                    "{}:{}",
                                    info.get_hostname(),
                                    info.get_port()
                                ));
                            }
                        }
                }
                Ok(_)  => {}
                Err(e) => {
                    warn!("mdns recv: {e}");
                    break;
                }
            }
        }
    }).await;

    let outcome = if peers.is_empty() {
        info!(node = %self_id, "no peers — bootstrapping as single-node leader");
        BootstrapOutcome::Bootstrap { endpoint_id: node.endpoint_id() }
    } else {
        let peer_list: Vec<String> = peers.into_iter().collect();
        info!(node = %self_id, ?peer_list, "found peers — will join cluster");
        BootstrapOutcome::Join {
            endpoint_id: node.endpoint_id(),
            peers: peer_list,
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
