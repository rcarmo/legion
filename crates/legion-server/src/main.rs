//! Legion daemon — the executable that wires all crates together.

mod api;
mod auth;
mod config;
mod tools;

use std::sync::Arc;
use anyhow::Result;
use tracing::{info, warn};

use legion_cluster::{bootstrap::run_bootstrap, membership::start_membership, node::ClusterNode, BootstrapOutcome};
use legion_core::ChainRegistry;
use legion_deploy::DeployPipeline;
use legion_loop::driver::LegionLoop;
use legion_namespace::Namespace;
use legion_runtime::{bun::BunRuntime, registry_bridge::RegistryBridge};
use legion_store::SqliteStore;
#[cfg(feature = "distributed")]
use legion_store::HiqliteStore;

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
    let mut cfg = ServerConfig::load("legion.toml").unwrap_or_else(|_| {
        warn!("no legion.toml found; using defaults");
        ServerConfig::default()
    });

    // Allow env overrides (useful for testing/CI)
    if let Ok(port) = std::env::var("LEGION_API_PORT") {
        if let Ok(p) = port.parse::<u16>() { cfg.cluster.api_port = p; }
    }
    if let Ok(dir) = std::env::var("LEGION_DATA_DIR") {
        cfg.cluster.data_dir = std::path::PathBuf::from(dir);
    }
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

    // Choose store: multi-node Raft (hiqlite) or single-node SQLite
    let arc_store: Arc<dyn legion_core::traits::EventStore>;
    // Keep SqliteStore in scope for AppState (single-node path)
    let sqlite_store_opt: Option<SqliteStore>;

    #[cfg(feature = "distributed")]
    if !cfg.raft_peers.is_empty() {
        use hiqlite::{Node, NodeConfig as HqlNodeConfig};
        let data_dir = cfg.cluster.data_dir.join("raft");
        let peers: Vec<Node> = cfg.raft_peers.iter().map(|p| Node {
            id:        p.id,
            addr_raft: p.addr_raft.clone(),
            addr_api:  p.addr_api.clone(),
        }).collect();
        let hql_cfg = HqlNodeConfig {
            node_id:     cfg.raft_node_id,
            nodes:       peers,
            data_dir:    data_dir.to_string_lossy().to_string().into(),
            secret_raft: cfg.raft_secret.clone(),
            secret_api:  cfg.raft_api_secret.clone(),
            ..Default::default()
        };
        info!(node_id = cfg.raft_node_id, peers = cfg.raft_peers.len(), "starting distributed hiqlite store");
        let hs = HiqliteStore::connect(hql_cfg).await?;
        arc_store = Arc::new(hs);
        sqlite_store_opt = None;
    } else {
        let db_path = cfg.cluster.data_dir.join("sessions.db");
        let s = SqliteStore::open(&db_path)?;
        info!(db = %db_path.display(), "event store ready (sqlite)");
        arc_store = Arc::new(s.clone());
        sqlite_store_opt = Some(s);
    }

    #[cfg(not(feature = "distributed"))]
    {
        let db_path = cfg.cluster.data_dir.join("sessions.db");
        let s = SqliteStore::open(&db_path)?;
        info!(db = %db_path.display(), "event store ready (sqlite)");
        arc_store = Arc::new(s.clone());
        sqlite_store_opt = Some(s);
    }

    // ── Namespace ─────────────────────────────────────────────────────────────
    let namespace = Namespace::new();
    namespace.set_json("/cluster/self", serde_json::json!({
        "endpoint_id": node.endpoint_id().to_string(),
        "short_id":    node.short_id(),
    })).await;

    // ── Gossip peer membership ─────────────────────────────────────────────────
    let ns_peers = namespace.clone();
    let _membership = start_membership(
        &node,
        move |p| {
            let ns = ns_peers.clone();
            tokio::spawn(async move {
                ns.set_json(
                    &format!("/cluster/peers/{}", p.short_id),
                    serde_json::json!({
                        "endpoint_id": p.endpoint_id,
                        "short_id":    p.short_id,
                        "api_port":    p.api_port,
                        "last_seen":   p.timestamp,
                    }),
                ).await;
            });
        },
        |eid| tracing::info!(%eid, "peer left cluster"),
        std::time::Duration::from_secs(5),
    ).await.unwrap_or_else(|e| {
        warn!("gossip membership unavailable (solo mode): {e}");
        legion_cluster::MembershipHandle::noop()
    });

    // ── Deploy pipeline ───────────────────────────────────────────────────────
    let fn_root  = cfg.cluster.data_dir.join("fn");
    let deployer = Arc::new(DeployPipeline::new(fn_root.clone(), namespace.clone()));

    // ── Tool registries ───────────────────────────────────────────────────────
    let bun_runtime = Arc::new(BunRuntime { fn_root, ..Default::default() });
    let bridge   = Arc::new(RegistryBridge::new(namespace.clone(), bun_runtime.clone()));
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
    let api_key = std::env::var("LEGION_API_KEY").ok().or(cfg.api_key.clone());
    if api_key.is_some() { info!("API key authentication enabled"); } else { warn!("no API key set — server is open"); }
    let state = Arc::new(AppState { store: arc_store, lp, deployer, namespace, invoker: bun_runtime as Arc<dyn legion_runtime::invoke::Invoker> });
    let addr  = format!("0.0.0.0:{}", cfg.cluster.api_port);
    api::serve(state, addr, api_key).await?;
    Ok(())
}
