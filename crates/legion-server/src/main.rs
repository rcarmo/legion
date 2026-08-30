//! Legion daemon — the executable that wires all crates together.

mod api;
mod config;
mod tools;

use std::sync::Arc;
use anyhow::Result;
use tracing::{info, warn};

use legion_cluster::{
    bootstrap::run_bootstrap,
    node::ClusterNode,
    BootstrapOutcome,
};
use legion_loop::driver::LegionLoop;
use legion_store::SqliteStore;

use api::AppState;
use config::ServerConfig;
use tools::BuiltinToolRegistry;

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
        data_dir  = %cfg.cluster.data_dir.display(),
        api_port  = cfg.cluster.api_port,
        model     = %cfg.model.default_model,
        "legion starting"
    );

    // ── Cluster node ─────────────────────────────────────────────────────────
    let node = Arc::new(ClusterNode::start(cfg.cluster.clone()).await?);
    info!(node_id = %node.endpoint_id(), "cluster node ready");

    // ── Bootstrap probe ──────────────────────────────────────────────────────
    let outcome = run_bootstrap(&node).await?;
    match &outcome {
        BootstrapOutcome::Bootstrap { endpoint_id } => {
            info!(%endpoint_id, "bootstrapped as single-node leader");
        }
        BootstrapOutcome::Join { endpoint_id, peers } => {
            info!(%endpoint_id, ?peers, "joining existing cluster (hiqlite handshake pending M2)");
        }
    }

    // ── Event store ──────────────────────────────────────────────────────────
    std::fs::create_dir_all(&cfg.cluster.data_dir)?;
    let db_path = cfg.cluster.data_dir.join("sessions.db");
    let store   = SqliteStore::open(&db_path)?;
    info!(db = %db_path.display(), "event store ready");

    // ── Tool registry + agent loop ───────────────────────────────────────────
    let arc_store = Arc::new(store.clone()) as Arc<dyn legion_core::traits::EventStore>;
    let tools     = Arc::new(BuiltinToolRegistry::new(arc_store.clone(), node.clone()));
    let lp        = Arc::new(LegionLoop::new(arc_store, tools));
    info!("agent loop ready (model: {})", cfg.model.default_model);

    // ── REST API ─────────────────────────────────────────────────────────────
    let state = Arc::new(AppState { store, lp });
    let addr  = format!("0.0.0.0:{}", cfg.cluster.api_port);
    api::serve(state, addr).await?;

    Ok(())
}
