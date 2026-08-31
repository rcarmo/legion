//! Server configuration loaded from `legion.toml`.

use anyhow::Result;
use serde::{Deserialize, Serialize};

use legion_cluster::node::NodeConfig;
use legion_runtime::InvocationLimits;

/// Hiqlite (Raft) peer descriptor — for multi-node distributed store.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RaftPeer {
    /// Raft node id (must be unique; node 1 is the bootstrap leader).
    pub id: u64,
    /// Internal Raft address, e.g. "192.168.1.10:17001".
    pub addr_raft: String,
    /// Public API address, e.g. "192.168.1.10:17002".
    pub addr_api: String,
}

/// Top-level config loaded from `legion.toml`.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct ServerConfig {
    #[serde(default)]
    pub cluster: NodeConfig,
    #[serde(default)]
    pub model: ModelConfig,
    /// Optional API key. Set to require `Authorization: Bearer <key>` on all requests.
    /// Also read from LEGION_API_KEY env var (env takes precedence).
    #[serde(default)]
    pub api_key: Option<String>,
    /// Hiqlite Raft peers for multi-node mode. When empty, uses single-node SQLite store.
    /// When populated, switches to HiqliteStore on the distributed feature.
    #[serde(default)]
    pub raft_peers: Vec<RaftPeer>,
    /// This node's Raft id (1-indexed). Ignored in single-node mode.
    #[serde(default = "default_raft_node_id")]
    pub raft_node_id: u64,
    /// Secrets for Raft and API channels. Must be the same across all peers.
    #[serde(default)]
    pub raft_secret: String,
    #[serde(default)]
    pub raft_api_secret: String,
    /// Raft bind address advertised through mDNS when distributed mode is enabled.
    #[serde(default = "default_raft_addr")]
    pub raft_addr: String,
    /// Hiqlite API bind address advertised through mDNS.
    #[serde(default = "default_raft_api_addr")]
    pub raft_api_addr: String,
    /// Shared limits applied to direct and agent-issued function invocations.
    #[serde(default)]
    pub invocation: InvocationLimits,
    /// Rate limit for execution-triggering requests to each session.
    #[serde(default)]
    pub session_rate_limit: SessionRateLimit,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct SessionRateLimit {
    pub max_requests_per_window: u32,
    pub window_ms: u64,
}

impl Default for SessionRateLimit {
    fn default() -> Self {
        Self {
            max_requests_per_window: 30,
            window_ms: 60_000,
        }
    }
}

fn default_raft_node_id() -> u64 {
    1
}

fn default_raft_addr() -> String {
    "0.0.0.0:17001".into()
}

fn default_raft_api_addr() -> String {
    "0.0.0.0:17002".into()
}

/// Model / provider settings.
/// API keys are read from environment variables by rs-ai automatically
/// (e.g. ANTHROPIC_API_KEY, OPENAI_API_KEY). Override here if needed.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ModelConfig {
    /// Default model string passed to rs-ai, e.g. "anthropic/claude-opus-4-5"
    pub default_model: String,
    /// Optional system prompt prepended to every session.
    pub system_prompt: Option<String>,
}

impl Default for ModelConfig {
    fn default() -> Self {
        Self {
            default_model: "anthropic/claude-haiku-3-5".into(),
            system_prompt: Some(
                "You are a Legion cluster agent. You manage durable function sessions \
                 and can introspect and control the cluster via built-in tools."
                    .into(),
            ),
        }
    }
}

impl ServerConfig {
    pub fn load(path: &str) -> Result<Self> {
        let raw = std::fs::read_to_string(path)?;
        Ok(toml::from_str(&raw)?)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn packaged_systemd_config_matches_schema() {
        let config: ServerConfig =
            toml::from_str(include_str!("../../../contrib/systemd/legion.toml")).unwrap();
        assert_eq!(config.cluster.data_dir.to_string_lossy(), "/var/lib/legion");
        assert_eq!(config.cluster.api_port, 8080);
        assert_eq!(config.raft_addr, "0.0.0.0:17001");
        assert_eq!(config.raft_api_addr, "0.0.0.0:17002");
        assert_eq!(config.invocation.timeout_ms, 30_000);
        assert_eq!(config.invocation.max_requests_per_window, 120);
        assert_eq!(config.invocation.wasm_fuel, 100_000_000);
        assert_eq!(config.invocation.wasm_max_memory_bytes, 64 * 1024 * 1024);
        assert_eq!(config.session_rate_limit.max_requests_per_window, 30);
    }
}
