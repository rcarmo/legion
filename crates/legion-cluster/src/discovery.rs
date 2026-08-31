//! Peer discovery via iroh-mdns-address-lookup (LAN only, no internet required).

use anyhow::{Context, Result};
use futures::StreamExt;
use iroh::EndpointId;
use iroh_mdns_address_lookup::{DiscoveryEvent as MdnsDiscoveryEvent, MdnsAddressLookup};
use tokio::sync::broadcast;
use tracing::{debug, info, warn};

use crate::node::ClusterNode;

/// A peer that was observed via mDNS.
#[derive(Debug, Clone)]
pub struct PeerInfo {
    pub endpoint_id: EndpointId,
    pub addresses: Vec<std::net::SocketAddr>,
}

#[derive(Debug, Clone)]
pub enum DiscoveryEvent {
    Discovered(PeerInfo),
    Expired(EndpointId),
}

/// A running mDNS discovery session.
pub struct MdnsDiscovery {
    tx: broadcast::Sender<DiscoveryEvent>,
    _lookup: MdnsAddressLookup,
}

impl MdnsDiscovery {
    /// Attach mDNS address lookup to an existing endpoint and start the discovery loop.
    pub async fn start(node: &ClusterNode) -> Result<Self> {
        let (tx, _rx) = broadcast::channel::<DiscoveryEvent>(64);
        let tx_clone = tx.clone();

        let mdns = MdnsAddressLookup::builder()
            .build(node.endpoint.id())
            .context("build mdns address lookup")?;

        node.endpoint
            .address_lookup()
            .map_err(|e| anyhow::anyhow!("address_lookup: {e}"))?
            .add(mdns.clone());

        info!(short_id = %node.short_id(), "mDNS discovery started");

        let lookup = mdns.clone();
        tokio::spawn(async move {
            let mut stream = mdns.subscribe().await;
            loop {
                match stream.next().await {
                    Some(MdnsDiscoveryEvent::Discovered { endpoint_info, .. }) => {
                        let pi = PeerInfo {
                            endpoint_id: endpoint_info.endpoint_id,
                            addresses: endpoint_info.ip_addrs().copied().collect(),
                        };
                        debug!(peer = %pi.endpoint_id, "mdns discovered peer");
                        let _ = tx_clone.send(DiscoveryEvent::Discovered(pi));
                    }
                    Some(MdnsDiscoveryEvent::Expired { endpoint_id }) => {
                        debug!(%endpoint_id, "mdns peer expired");
                        let _ = tx_clone.send(DiscoveryEvent::Expired(endpoint_id));
                    }
                    None => {
                        warn!("mdns discovery stream ended");
                        break;
                    }
                    Some(_) => {} // ignore unknown variants
                }
            }
        });

        Ok(Self {
            tx,
            _lookup: lookup,
        })
    }

    pub fn subscribe(&self) -> broadcast::Receiver<DiscoveryEvent> {
        self.tx.subscribe()
    }
}
