use std::{env, net::SocketAddr};

use anyhow::{Context, Result};
use iroh::{
    Endpoint, RelayMode,
    endpoint::{Connection, presets},
    protocol::{AcceptError, ProtocolHandler, Router},
};
use serde::Serialize;

const ALPN: &[u8] = b"legion/interop/1";

#[derive(Debug, Clone)]
struct Echo;

impl ProtocolHandler for Echo {
    async fn accept(&self, connection: Connection) -> Result<(), AcceptError> {
        let (mut send, mut recv) = connection.accept_bi().await?;
        tokio::io::copy(&mut recv, &mut send).await?;
        send.finish()?;
        connection.closed().await;
        Ok(())
    }
}

#[derive(Serialize)]
struct Ready {
    endpoint_id: String,
    addrs: Vec<String>,
    relay_url: Option<String>,
    transport: &'static str,
}

#[tokio::main]
async fn main() -> Result<()> {
    let transport = env::args().nth(1).unwrap_or_else(|| "direct".to_owned());
    let mut builder = Endpoint::builder(presets::N0).alpns(vec![ALPN.to_vec()]);
    match transport.as_str() {
        "direct" => {
            let bind: SocketAddr = "127.0.0.1:0".parse()?;
            builder = builder.relay_mode(RelayMode::Disabled).bind_addr(bind)?;
        }
        "relay" => {
            builder = builder.relay_mode(RelayMode::Default).clear_ip_transports();
        }
        other => anyhow::bail!("unknown transport {other:?}; expected direct or relay"),
    }
    let endpoint = builder.bind().await.context("bind Rust iroh endpoint")?;
    if transport == "relay" {
        endpoint.online().await;
    }
    let addr = endpoint.addr();
    let ready = Ready {
        endpoint_id: endpoint.id().to_string(),
        addrs: addr.ip_addrs().map(ToString::to_string).collect(),
        relay_url: addr.relay_urls().next().map(ToString::to_string),
        transport: if transport == "relay" { "relay" } else { "direct" },
    };
    println!("LEGION_INTEROP_READY {}", serde_json::to_string(&ready)?);
    let router = Router::builder(endpoint).accept(ALPN, Echo).spawn();
    tokio::signal::ctrl_c().await?;
    router.shutdown().await?;
    Ok(())
}
