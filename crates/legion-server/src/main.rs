//! Legion daemon — the executable that wires all crates together.
//!
//! Startup sequence:
//!  1. Load config from `legion.toml` (or defaults)
//!  2. Start the iroh cluster node (keypair + endpoint)
//!  3. Run the bootstrap probe (mDNS discovery, decide join vs. leader)
//!  4. Start the SQLite event store
//!  5. Start the agent loop
//!  6. Serve the REST API

mod api;
mod config;

use anyhow::Result;
use tracing::{info, warn};

use legion_cluster::{
    bootstrap::run_bootstrap,
    node::{ClusterNode, NodeConfig},
    BootstrapOutcome,
};
use legion_loop::driver::LegionLoop;
use legion_store::SqliteStore;

use config::ServerConfig;

#[tokio::main]
async fn main() -> Result<()> {
    // ── Tracing ──────────────────────────────────────────────────────────────
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "legion=info,warn".parse().unwrap()),
        )
        .compact()
        .init();

    // ── Config ───────────────────────────────────────────────────────────────
    let cfg = ServerConfig::load("legion.toml").unwrap_or_else(|_| {
        warn!("no legion.toml found; using defaults");
        ServerConfig::default()
    });

    info!(
        data_dir = %cfg.cluster.data_dir.display(),
        api_port = cfg.cluster.api_port,
        "legion starting"
    );

    // ── Cluster node ─────────────────────────────────────────────────────────
    let node = ClusterNode::start(cfg.cluster.clone()).await?;
    info!(node_id = %node.endpoint_id(), "cluster node ready");

    // ── Bootstrap probe ──────────────────────────────────────────────────────
    let outcome = run_bootstrap(&node).await?;
    match &outcome {
        BootstrapOutcome::Bootstrap { endpoint_id } => {
            info!(%endpoint_id, "bootstrapped as single-node leader");
        }
        BootstrapOutcome::Join { endpoint_id, peers } => {
            info!(%endpoint_id, ?peers, "joining existing cluster");
        }
    }

    // ── Event store ──────────────────────────────────────────────────────────
    let db_path = cfg.cluster.data_dir.join("sessions.db");
    let store   = SqliteStore::open(&db_path)?;
    info!(db = %db_path.display(), "event store ready");

    // ── Agent loop ───────────────────────────────────────────────────────────
    use std::sync::Arc;
    use legion_core::test_doubles::EchoToolRegistry;
    let tools = Arc::new(EchoToolRegistry::new());
    let _lp   = LegionLoop::new(Arc::new(store.clone()), tools);
    info!("agent loop ready");

    // ── REST API ─────────────────────────────────────────────────────────────
    let addr = format!("0.0.0.0:{}", cfg.cluster.api_port);
    info!(%addr, "starting REST API");
    api::serve(store, addr).await?;

    Ok(())
}
