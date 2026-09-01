use std::{env, net::SocketAddr};

use anyhow::{Context, Result};
use iroh::{Endpoint, RelayMode, endpoint::presets};
use legion_cluster::{
    ClusterNode, NodeConfig,
    ninep::{NinePClient, serve_namespace},
};
use legion_namespace::{LegionNamespace, Namespace};
use serde::Serialize;
use serde_json::json;

#[derive(Serialize)]
struct Ready {
    endpoint_id: String,
    addrs: Vec<String>,
}

#[tokio::main]
async fn main() -> Result<()> {
    let mode = env::args().nth(1).unwrap_or_else(|| "server".into());
    match mode.as_str() {
        "server" => {
            let data_dir =
                env::temp_dir().join(format!("legion-ninep-fixture-{}", std::process::id()));
            let node = ClusterNode::start(NodeConfig {
                data_dir,
                bind_addr: "127.0.0.1:0".into(),
                api_port: 0,
                mdns: false,
            })
            .await?;
            let ns = Namespace::new();
            ns.set_json("/cluster/health", json!({"rust":true})).await;
            let addr = node.endpoint.addr();
            let ready = Ready {
                endpoint_id: node.endpoint_id().to_string(),
                addrs: addr.ip_addrs().map(ToString::to_string).collect(),
            };
            let router = serve_namespace(&node, LegionNamespace::new(ns));
            println!("LEGION_NINEP_READY {}", serde_json::to_string(&ready)?);
            tokio::signal::ctrl_c().await?;
            router.shutdown().await?;
        }
        "client" => {
            let endpoint_id = env::args().nth(2).context("endpoint id")?.parse()?;
            let socket = env::args().nth(3).context("socket addr")?.parse()?;
            let endpoint = Endpoint::builder(presets::N0)
                .relay_mode(RelayMode::Disabled)
                .bind_addr("127.0.0.1:0".parse::<SocketAddr>()?)?
                .bind()
                .await?;
            let client = NinePClient::connect_addr(
                &endpoint,
                iroh::EndpointAddr::new(endpoint_id).with_ip_addr(socket),
            )
            .await?;
            println!(
                "{}",
                String::from_utf8(client.read_path("/cluster/health").await?)?
            );
        }
        other => anyhow::bail!("unknown mode {other}"),
    };
    Ok(())
}
