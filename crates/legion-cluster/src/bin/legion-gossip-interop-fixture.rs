use std::net::Ipv4Addr;

use bytes::Bytes;
use iroh::{Endpoint, endpoint::presets, protocol::Router};
use iroh_gossip::{TopicId, ALPN, api::Event, net::Gossip};
use serde::{Deserialize, Serialize};
use tokio::io::{AsyncBufReadExt, BufReader};

#[derive(Serialize)]
#[serde(tag = "kind")]
enum Out {
    Ready { endpoint_id: String, addrs: Vec<String> },
    NeighborUp { peer: String },
    Received { content: String },
}

#[derive(Deserialize)]
#[serde(tag = "cmd")]
enum In {
    Broadcast { content: String },
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let topic = TopicId::from_bytes(*blake3::hash(b"legion-cluster-v1").as_bytes());
    let endpoint = Endpoint::builder(presets::Minimal)
        .bind_addr((Ipv4Addr::LOCALHOST, 0))?
        .alpns(vec![ALPN.to_vec()])
        .bind()
        .await?;
    let gossip = Gossip::builder().spawn(endpoint.clone());
    let router = Router::builder(endpoint.clone()).accept(ALPN, gossip.clone()).spawn();
    print_json(&Out::Ready {
        endpoint_id: endpoint.id().to_string(),
        addrs: endpoint.bound_sockets().into_iter().filter(|a| a.is_ipv4()).map(|a| a.to_string()).collect(),
    })?;
    let topic = gossip.subscribe(topic, Vec::new()).await?;
    let (sender, mut receiver) = topic.split();
    let events = tokio::spawn(async move {
        use futures::StreamExt;
        while let Some(event) = receiver.next().await {
            match event? {
                Event::NeighborUp(peer) => print_json(&Out::NeighborUp { peer: peer.to_string() })?,
                Event::Received(message) => print_json(&Out::Received { content: String::from_utf8_lossy(&message.content).into_owned() })?,
                Event::Lagged | Event::NeighborDown(_) => {}
            }
        }
        Ok::<(), Box<dyn std::error::Error + Send + Sync>>(())
    });
    let mut lines = BufReader::new(tokio::io::stdin()).lines();
    while let Some(line) = lines.next_line().await? {
        let In::Broadcast { content } = serde_json::from_str(&line)?;
        sender.broadcast(Bytes::from(content)).await?;
    }
    events.abort();
    router.shutdown().await?;
    Ok(())
}

fn print_json(value: &Out) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    println!("{}", serde_json::to_string(value)?);
    Ok(())
}
