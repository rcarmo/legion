//! iroh-gossip peer membership: broadcasts node presence on a cluster topic
//! and notifies callers when peers join or leave.

use std::time::Duration;

use anyhow::{Context, Result};
use bytes::Bytes;
use futures::StreamExt;
use iroh_gossip::{
    net::Gossip,
    proto::TopicId,
    api::Event,
};
use serde::{Deserialize, Serialize};
use tokio::time::interval;
use tracing::{debug, info, warn};

use crate::node::ClusterNode;

// ── NodePresence ──────────────────────────────────────────────────────────────

/// Serialised presence announcement broadcast on the gossip topic.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodePresence {
    pub endpoint_id: String,
    pub short_id:    String,
    pub api_port:    u16,
    pub timestamp:   i64,
}

/// Returns the deterministic gossip topic ID for Legion cluster membership.
pub fn cluster_topic() -> TopicId {
    let hash = blake3::hash(b"legion-cluster-v1");
    TopicId::from_bytes(*hash.as_bytes())
}

// ── MembershipHandle ─────────────────────────────────────────────────────────

/// Active membership session (drop to leave the topic).
pub struct MembershipHandle {
    _gossip: Option<Gossip>,
}

impl MembershipHandle {
    /// A no-op handle for solo mode when gossip is unavailable.
    pub fn noop() -> Self { Self { _gossip: None } }
}

/// Start the membership gossip loop.
///
/// Returns a `MembershipHandle`; dropping it stops the gossip session.
pub async fn start_membership(
    node:      &ClusterNode,
    on_joined: impl Fn(NodePresence) + Send + Sync + 'static,
    on_left:   impl Fn(String) + Send + Sync + 'static,
    heartbeat: Duration,
) -> Result<MembershipHandle> {
    let topic    = cluster_topic();
    let short    = node.short_id().to_string();
    let api_port = node.config.api_port;
    let eid_str  = node.endpoint_id().to_string();

    // Spawn gossip on the existing endpoint
    let gossip = Gossip::builder()
        .spawn(node.endpoint.clone());

    info!(%short, "gossip membership starting");

    // Subscribe to the cluster topic (no bootstrap peers; rely on mDNS for initial contact)
    let topic_sub = gossip
        .subscribe(topic, vec![])
        .await
        .context("gossip subscribe")?;

    let (sender, mut receiver) = topic_sub.split();

    // Heartbeat task
    let eid_hb   = eid_str.clone();
    let short_hb = short.clone();
    tokio::spawn(async move {
        let mut tick = interval(heartbeat);
        loop {
            tick.tick().await;
            let presence = NodePresence {
                endpoint_id: eid_hb.clone(),
                short_id:    short_hb.clone(),
                api_port,
                timestamp:   chrono::Utc::now().timestamp_millis(),
            };
            if let Ok(bytes) = serde_json::to_vec(&presence) {
                if let Err(e) = sender.broadcast(Bytes::from(bytes)).await {
                    warn!("gossip broadcast: {e}");
                }
            }
        }
    });

    // Receive task
    tokio::spawn(async move {
        loop {
            match receiver.next().await {
                Some(Ok(Event::Received(msg))) => {
                    if let Ok(p) = serde_json::from_slice::<NodePresence>(&msg.content) {
                        if p.endpoint_id != eid_str {
                            debug!(peer = %p.short_id, "peer heartbeat");
                            on_joined(p);
                        }
                    }
                }
                Some(Ok(Event::NeighborUp(peer))) => {
                    info!(%peer, "gossip: neighbor up");
                }
                Some(Ok(Event::NeighborDown(peer))) => {
                    info!(%peer, "gossip: neighbor down");
                    on_left(peer.to_string());
                }
                Some(Ok(Event::Lagged)) => {
                    warn!("gossip: lagged");
                }
                Some(Err(e)) => warn!("gossip error: {e}"),
                None => {
                    info!("gossip: topic closed");
                    break;
                }
            }
        }
    });

    Ok(MembershipHandle { _gossip: Some(gossip) })
}
