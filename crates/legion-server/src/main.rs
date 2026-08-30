//! Legion daemon — the executable that wires all crates together.

mod api;
mod auth;
mod cli;
mod config;
mod rate_limit;
mod tools;

use std::sync::Arc;
use anyhow::{Context, Result};
use clap::Parser;
use tracing::{info, warn};

use legion_cluster::{bootstrap::run_bootstrap, membership::start_membership, node::ClusterNode, BootstrapOutcome};
use legion_core::ChainRegistry;
use legion_deploy::DeployPipeline;
use legion_loop::driver::LegionLoop;
use legion_namespace::Namespace;
use legion_runtime::{
    bun::BunRuntime, registry_bridge::RegistryBridge, BoundedInvoker,
    InvocationMetrics,
};
#[cfg(feature = "wasm")]
use legion_runtime::wasm::WasmRuntime;
use legion_store::SqliteStore;
#[cfg(feature = "distributed")]
use legion_store::HiqliteStore;

use api::AppState;
use config::ServerConfig;
use rate_limit::SessionRateLimiter;
use tools::BuiltinToolRegistry;

#[tokio::main]
async fn main() -> Result<()> {
    let command = cli::Cli::parse();
    if matches!(&command.command, Some(cli::Command::Serve) | None) {
        run_server().await
    } else {
        cli::run(command).await
    }
}

fn apply_env<T>(target: &mut T, name: &str)
where
    T: std::str::FromStr,
{
    if let Ok(value) = std::env::var(name) {
        if let Ok(parsed) = value.parse() {
            *target = parsed;
        } else {
            warn!(name, value, "ignoring invalid environment override");
        }
    }
}

async fn run_server() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "legion=info,warn".parse().unwrap()),
        )
        .compact()
        .init();

    // ── Config ───────────────────────────────────────────────────────────────
    let explicit_config = std::env::var("LEGION_CONFIG").ok();
    let config_path = explicit_config.as_deref().unwrap_or("legion.toml");
    let mut cfg = match ServerConfig::load(config_path) {
        Ok(config) => config,
        Err(error) if explicit_config.is_some() => {
            return Err(error).with_context(|| format!("load LEGION_CONFIG from {config_path}"));
        }
        Err(error) => {
            warn!(path = %config_path, %error, "configuration unavailable; using defaults");
            ServerConfig::default()
        }
    };

    // Allow env overrides (useful for testing/CI)
    if let Ok(port) = std::env::var("LEGION_API_PORT") {
        if let Ok(p) = port.parse::<u16>() { cfg.cluster.api_port = p; }
    }
    if let Ok(dir) = std::env::var("LEGION_DATA_DIR") {
        cfg.cluster.data_dir = std::path::PathBuf::from(dir);
    }
    apply_env(&mut cfg.invocation.timeout_ms, "LEGION_INVOKE_TIMEOUT_MS");
    apply_env(&mut cfg.invocation.max_input_bytes, "LEGION_INVOKE_MAX_INPUT_BYTES");
    apply_env(&mut cfg.invocation.max_output_bytes, "LEGION_INVOKE_MAX_OUTPUT_BYTES");
    apply_env(
        &mut cfg.invocation.max_concurrent_per_function,
        "LEGION_INVOKE_MAX_CONCURRENT_PER_FUNCTION",
    );
    apply_env(
        &mut cfg.invocation.max_requests_per_window,
        "LEGION_INVOKE_MAX_REQUESTS_PER_WINDOW",
    );
    apply_env(&mut cfg.invocation.rate_window_ms, "LEGION_INVOKE_RATE_WINDOW_MS");
    apply_env(
        &mut cfg.session_rate_limit.max_requests_per_window,
        "LEGION_SESSION_MAX_REQUESTS_PER_WINDOW",
    );
    apply_env(
        &mut cfg.session_rate_limit.window_ms,
        "LEGION_SESSION_RATE_WINDOW_MS",
    );
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

    // Choose store: multi-node Raft (hiqlite) or single-node SQLite.
    let arc_store: Arc<dyn legion_core::traits::EventStore>;

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
    } else {
        let db_path = cfg.cluster.data_dir.join("sessions.db");
        let s = SqliteStore::open(&db_path)?;
        info!(db = %db_path.display(), "event store ready (sqlite)");
        arc_store = Arc::new(s);
    }

    #[cfg(not(feature = "distributed"))]
    {
        let db_path = cfg.cluster.data_dir.join("sessions.db");
        let s = SqliteStore::open(&db_path)?;
        info!(db = %db_path.display(), "event store ready (sqlite)");
        arc_store = Arc::new(s);
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
    let invocation_metrics = Arc::new(InvocationMetrics::default());
    let bun_backend = Arc::new(BunRuntime { fn_root, ..Default::default() });
    let bun_runtime: Arc<dyn legion_runtime::invoke::Invoker> = Arc::new(BoundedInvoker::new(
        bun_backend,
        "bun",
        cfg.invocation.clone(),
        invocation_metrics.clone(),
    ));
    #[cfg(feature = "wasm")]
    let wasm_backend = Arc::new(WasmRuntime::with_timeout(
        cfg.cluster.data_dir.join("fn"),
        cfg.invocation.timeout_ms,
    ));
    #[cfg(feature = "wasm")]
    let wasm_runtime: Arc<dyn legion_runtime::invoke::Invoker> = Arc::new(BoundedInvoker::new(
        wasm_backend,
        "wasm",
        cfg.invocation.clone(),
        invocation_metrics.clone(),
    ));
    let bridge = RegistryBridge::new(namespace.clone(), bun_runtime.clone());
    #[cfg(feature = "wasm")]
    let bridge = bridge.with_wasm(wasm_runtime.clone());
    let bridge = Arc::new(bridge);
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
    let state = Arc::new(AppState {
        store: arc_store,
        lp,
        deployer,
        namespace,
        invoker_bun: bun_runtime,
        #[cfg(feature = "wasm")]
        invoker_wasm: wasm_runtime,
        invocation_metrics,
        session_rate_limiter: Arc::new(SessionRateLimiter::new(cfg.session_rate_limit)),
    });
    let addr  = format!("0.0.0.0:{}", cfg.cluster.api_port);
    api::serve(state, addr, api_key).await?;
    Ok(())
}
