//! Server configuration loaded from `legion.toml`.

use std::path::PathBuf;
use serde::{Deserialize, Serialize};
use anyhow::Result;

use legion_cluster::node::NodeConfig;

/// Top-level config loaded from `legion.toml`.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct ServerConfig {
    #[serde(default)]
    pub cluster: NodeConfig,
    #[serde(default)]
    pub model: ModelConfig,
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
                 and can introspect and control the cluster via built-in tools.".into()
            ),
        }
    }
}

impl ServerConfig {
    pub fn load(path: &str) -> Result<Self> {
        let raw = std::fs::read_to_string(path)?;
        Ok(toml::from_str(&raw)?)
    }

    /// Write a starter config file with commented defaults.
    pub fn write_example(path: &str) -> Result<()> {
        let example = r#"# Legion server configuration

[cluster]
data_dir  = "/var/lib/legion"
api_port  = 8080
mdns      = true

[model]
# Provider/model string passed to rs-ai.
# API keys are loaded from env: ANTHROPIC_API_KEY, OPENAI_API_KEY, etc.
default_model = "anthropic/claude-haiku-3-5"
system_prompt = "You are a Legion cluster agent."
"#;
        std::fs::write(path, example)?;
        Ok(())
    }
}
