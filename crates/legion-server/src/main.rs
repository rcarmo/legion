//! Legion daemon — the executable that wires all crates together.

mod api;
mod config;
mod tools;

use std::sync::Arc;
use anyhow::Result;
use tracing::{info, warn};

use legion_cluster::{bootstrap::run_bootstrap, node::ClusterNode, BootstrapOutcome};
use legion_core::ChainRegistry;
use legion_deploy::DeployPipeline;
use legion_loop::driver::LegionLoop;
use legion_namespace::Namespace;
use legion_runtime::{bun::BunRuntime, registry_bridge::RegistryBridge};
use legion_store::SqliteStore;

use api::AppState;
use config::ServerConfig;
use tools::BuiltinToolRegistry;

#[tokio::main]
async fn main() -> Result<()> {
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
        model    = %cfg.model.default_model,
        "legion starting"
    );

    // ── Cluster node ─────────────────────────────────────────────────────────
    let node = Arc::new(ClusterNode::start(cfg.cluster.clone()).await?);
    info!(node_id = %node.endpoint_id(), "cluster node ready");

    let outcome = run_bootstrap(&node).await?;
    match &outcome {
        BootstrapOutcome::Bootstrap { endpoint_id } =>
            info!(%endpoint_id, "bootstrapped as single-node leader"),
        BootstrapOutcome::Join { endpoint_id, peers } =>
            info!(%endpoint_id, ?peers, "joining cluster (hiqlite handshake pending M3)"),
    }

    // ── Storage ───────────────────────────────────────────────────────────────
    std::fs::create_dir_all(&cfg.cluster.data_dir)?;
    let db_path   = cfg.cluster.data_dir.join("sessions.db");
    let store     = SqliteStore::open(&db_path)?;
    let arc_store = Arc::new(store.clone()) as Arc<dyn legion_core::traits::EventStore>;
    info!(db = %db_path.display(), "event store ready");

    // ── Namespace ─────────────────────────────────────────────────────────────
    let namespace = Namespace::new();

    // ── Seed /cluster/self in namespace ──────────────────────────────────────
    namespace.set_json("/cluster/self", serde_json::json!({
        "endpoint_id": node.endpoint_id().to_string(),
        "short_id":    node.short_id(),
    })).await;

    // ── Deploy pipeline ───────────────────────────────────────────────────────
    let fn_root  = cfg.cluster.data_dir.join("fn");
    let deployer = Arc::new(DeployPipeline::new(fn_root.clone(), namespace.clone()));

    // ── Tool registries ───────────────────────────────────────────────────────
    let bun_runtime = Arc::new(BunRuntime { fn_root, ..Default::default() });
    let bridge   = Arc::new(RegistryBridge::new(namespace.clone(), bun_runtime));
    let builtins = Arc::new(BuiltinToolRegistry::new(
        arc_store.clone(),
        node.clone(),
        namespace.clone(),
    ));
    let tools = Arc::new(ChainRegistry::new(vec![
        builtins as Arc<dyn legion_core::traits::ToolRegistry>,
        bridge   as Arc<dyn legion_core::traits::ToolRegistry>,
    ]));

    // ── Agent loop ────────────────────────────────────────────────────────────
    let lp = Arc::new(LegionLoop::new(arc_store.clone(), tools));
    info!("agent loop ready (model: {})", cfg.model.default_model);

    // ── REST API ──────────────────────────────────────────────────────────────
    let state = Arc::new(AppState { store, lp, deployer, namespace });
    let addr  = format!("0.0.0.0:{}", cfg.cluster.api_port);
    api::serve(state, addr).await?;
    Ok(())
}
