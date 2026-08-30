//! ChainRegistry — composite ToolRegistry that delegates across multiple backends.
//!
//! Definitions are the union of all registries (earlier registries win on name clash).
//! Dispatch routes to the first registry that recognises the tool name.

use std::sync::Arc;
use async_trait::async_trait;
use serde_json::Value;

use crate::{
    error::{LegionError, Result},
    traits::ToolRegistry,
    types::ToolDefinition,
};

pub struct ChainRegistry {
    registries: Vec<Arc<dyn ToolRegistry>>,
}

impl ChainRegistry {
    pub fn new(registries: Vec<Arc<dyn ToolRegistry>>) -> Self {
        Self { registries }
    }

    /// Add a registry at lowest priority.
    pub fn push(mut self, r: Arc<dyn ToolRegistry>) -> Self {
        self.registries.push(r);
        self
    }
}

#[async_trait]
impl ToolRegistry for ChainRegistry {
    async fn definitions(&self) -> Vec<ToolDefinition> {
        let mut seen  = std::collections::HashSet::new();
        let mut defs  = Vec::new();
        for r in &self.registries {
            for d in r.definitions().await {
                if seen.insert(d.name.clone()) {
                    defs.push(d);
                }
            }
        }
        defs
    }

    async fn dispatch(&self, name: &str, args: Value) -> Result<Value> {
        for r in &self.registries {
            let known: std::collections::HashSet<_> =
                r.definitions().await.into_iter().map(|d| d.name).collect();
            if known.contains(name) {
                return r.dispatch(name, args).await;
            }
        }
        Err(LegionError::ToolNotFound(name.into()))
    }
}
