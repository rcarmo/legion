//! Server configuration loaded from `legion.toml`.

use std::path::PathBuf;
use serde::{Deserialize, Serialize};
use anyhow::Result;

use legion_cluster::node::NodeConfig;

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct ServerConfig {
    #[serde(default)]
    pub cluster: NodeConfig,
}

impl ServerConfig {
    pub fn load(path: &str) -> Result<Self> {
        let raw = std::fs::read_to_string(path)?;
        Ok(toml::from_str(&raw)?)
    }
}
