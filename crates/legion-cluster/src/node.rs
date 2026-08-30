//! Node identity: keypair persistence and iroh endpoint management.

use std::path::{Path, PathBuf};
use anyhow::{Context, Result};
use iroh::{endpoint::presets, EndpointId, SecretKey};
use serde::{Deserialize, Serialize};
use tracing::info;

// ── NodeConfig ────────────────────────────────────────────────────────────────

/// Configuration for a single Legion cluster node.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeConfig {
    /// Directory where the keypair and state are persisted.
    pub data_dir: PathBuf,
    /// Address to bind the iroh QUIC endpoint on.
    #[serde(default = "default_bind_addr")]
    pub bind_addr: String,
    /// REST API port.
    #[serde(default = "default_api_port")]
    pub api_port: u16,
    /// Enable mDNS LAN discovery.
    #[serde(default = "default_true")]
    pub mdns: bool,
}

impl Default for NodeConfig {
    fn default() -> Self {
        Self {
            data_dir:  PathBuf::from("/var/lib/legion"),
            bind_addr: default_bind_addr(),
            api_port:  default_api_port(),
            mdns:      true,
        }
    }
}

fn default_bind_addr() -> String { "0.0.0.0:0".into() }
fn default_api_port()  -> u16    { 8080 }
fn default_true()      -> bool   { true }

// ── NodeIdentity ──────────────────────────────────────────────────────────────

/// The stable identity of a node: its iroh public key and derived display name.
#[derive(Debug, Clone)]
pub struct NodeIdentity {
    pub secret_key: SecretKey,
    /// Human-friendly abbreviated endpoint id (first 8 chars of string form).
    pub short_id:   String,
}

impl NodeIdentity {
    /// Load from disk, or generate and persist a new keypair.
    pub fn load_or_generate(data_dir: &Path) -> Result<Self> {
        let key_path = data_dir.join("node.key");
        std::fs::create_dir_all(data_dir)
            .with_context(|| format!("create data dir {}", data_dir.display()))?;

        let secret_key = if key_path.exists() {
            let bytes = std::fs::read(&key_path)
                .with_context(|| format!("read key from {}", key_path.display()))?;
            let arr: [u8; 32] = bytes.try_into()
                .map_err(|_| anyhow::anyhow!("invalid key file: expected 32 bytes"))?;
            SecretKey::from_bytes(&arr)
        } else {
            let key = SecretKey::generate();
            std::fs::write(&key_path, key.to_bytes())
                .with_context(|| format!("write key to {}", key_path.display()))?;
            info!(path = %key_path.display(), "generated new node keypair");
            key
        };

        let public  = secret_key.public();
        let short_id = public.to_string().chars().take(8).collect();

        Ok(Self { secret_key, short_id })
    }
}

// ── ClusterNode ───────────────────────────────────────────────────────────────

/// A running Legion cluster node: iroh endpoint + gossip + discovery.
pub struct ClusterNode {
    pub identity: NodeIdentity,
    pub endpoint: iroh::Endpoint,
    pub config:   NodeConfig,
}

impl ClusterNode {
    /// Bind the iroh endpoint and start background services.
    pub async fn start(config: NodeConfig) -> Result<Self> {
        let identity = NodeIdentity::load_or_generate(&config.data_dir)?;

        info!(
            short_id = %identity.short_id,
            "starting legion cluster node"
        );

        let endpoint = iroh::Endpoint::builder(presets::N0)
            .secret_key(identity.secret_key.clone())
            .bind()
            .await
            .context("bind iroh endpoint")?;

        let addr = endpoint.addr();
        info!(addr = ?addr, "iroh endpoint bound");

        Ok(Self { identity, endpoint, config })
    }

    /// Returns the iroh `EndpointId` (public key) for this node.
    pub fn endpoint_id(&self) -> EndpointId {
        self.endpoint.id()
    }

    /// Returns the short human-readable id.
    pub fn short_id(&self) -> &str {
        &self.identity.short_id
    }
}
