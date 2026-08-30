//! RegistryBridge — exposes deployed namespace functions as `ToolDefinition`s.
//!
//! The LegionLoop dispatches tool calls through `ToolRegistry::dispatch`.
//! This bridge implements `ToolRegistry` by reading manifests from the namespace
//! and routing calls to the appropriate `Invoker` at dispatch time.

use std::sync::Arc;
use async_trait::async_trait;
use serde_json::Value;
use tracing::debug;

use legion_core::{
    error::{LegionError, Result},
    traits::ToolRegistry,
    types::{EffectClass, ToolDefinition},
};
use legion_namespace::Namespace;

use crate::invoke::{InvokeRequest, Invoker};
use crate::manifest::FunctionManifest;

/// Bridges the namespace function registry into the `ToolRegistry` trait.
pub struct RegistryBridge {
    pub namespace: Namespace,
    pub invoker:   Arc<dyn Invoker>,
}

impl RegistryBridge {
    pub fn new(namespace: Namespace, invoker: Arc<dyn Invoker>) -> Self {
        Self { namespace, invoker }
    }

    /// Load all deployed function manifests from `/fn/*/manifest.json`.
    pub async fn load_manifests(&self) -> Vec<FunctionManifest> {
        let names = self.namespace.ls("/fn").await;
        let mut manifests = Vec::new();
        for name in names {
            let path = format!("/fn/{}/manifest.json", name);
            if let Some(node) = self.namespace.get(&path).await {
                if let legion_namespace::NodeKind::Json(v) = node.kind {
                    if let Ok(m) = serde_json::from_value::<FunctionManifest>(v) {
                        manifests.push(m);
                    }
                }
            }
        }
        manifests
    }
}

#[async_trait]
impl ToolRegistry for RegistryBridge {
    fn definitions(&self) -> Vec<ToolDefinition> {
        // Synchronously read manifests — requires a blocking call in async context.
        // In production this is called once at context-build time; it's acceptable.
        let rt = tokio::runtime::Handle::current();
        let manifests = rt.block_on(self.load_manifests());
        manifests.into_iter().map(|m| ToolDefinition {
            name:        format!("fn.{}", m.name),
            description: m.description.clone(),
            parameters:  m.parameters.clone(),
            effect:      if m.idempotent {
                EffectClass::Idempotent
            } else {
                EffectClass::Write
            },
        }).collect()
    }

    async fn dispatch(&self, name: &str, args: Value) -> Result<Value> {
        let fn_name = name.strip_prefix("fn.")
            .ok_or_else(|| LegionError::ToolNotFound(name.into()))?;

        debug!(fn_name, "dispatching namespace function");

        let result = self.invoker.invoke(InvokeRequest {
            function_name: fn_name.to_string(),
            call_id:       uuid::Uuid::new_v4().to_string(),
            args,
        }).await?;

        if let Some(err) = result.error {
            return Err(LegionError::ToolError(format!("{fn_name}: {err}")));
        }
        Ok(result.output)
    }
}
