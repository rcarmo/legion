//! RegistryBridge — exposes deployed namespace functions as `ToolDefinition`s.
//!
//! The LegionLoop dispatches tool calls through `ToolRegistry::dispatch`.
//! This bridge reads manifests asynchronously and routes each call to the
//! runtime declared by the deployed function.

use std::sync::Arc;

use async_trait::async_trait;
use serde_json::Value;
use tracing::debug;

use legion_core::{
    error::{LegionError, Result},
    traits::ToolRegistry,
    types::{EffectClass, ToolDefinition},
};
use legion_namespace::{FunctionNamespace, Namespace, NodeKind};

use crate::invoke::{InvokeRequest, Invoker};
use crate::manifest::{FunctionManifest, FunctionRuntime};

/// Bridges namespace function manifests into the agent tool registry.
pub struct RegistryBridge {
    pub namespace: Namespace,
    bun_invoker: Arc<dyn Invoker>,
    wasm_invoker: Option<Arc<dyn Invoker>>,
}

impl RegistryBridge {
    pub fn new(namespace: Namespace, bun_invoker: Arc<dyn Invoker>) -> Self {
        Self {
            namespace,
            bun_invoker,
            wasm_invoker: None,
        }
    }

    pub fn with_wasm(mut self, wasm_invoker: Arc<dyn Invoker>) -> Self {
        self.wasm_invoker = Some(wasm_invoker);
        self
    }

    /// Load all deployed function manifests from `/fn/*/manifest.json`.
    pub async fn load_manifests(&self) -> Vec<FunctionManifest> {
        let names = self.namespace.ls("/fn").await;
        let mut manifests = Vec::new();
        for name in names {
            if let Some(manifest) = self.load_manifest(&name).await {
                manifests.push(manifest);
            }
        }
        manifests
    }

    async fn load_manifest(&self, name: &str) -> Option<FunctionManifest> {
        let path = format!("/fn/{name}/manifest.json");
        let node = self.namespace.get(&path).await?;
        let NodeKind::Json(value) = node.kind else {
            return None;
        };
        serde_json::from_value(value).ok()
    }

    fn invoker_for(&self, runtime: &FunctionRuntime) -> Result<Arc<dyn Invoker>> {
        match runtime {
            FunctionRuntime::Bun => Ok(self.bun_invoker.clone()),
            FunctionRuntime::Wasm => self.wasm_invoker.clone().ok_or_else(|| {
                LegionError::ToolError("server was built without WASM runtime support".into())
            }),
        }
    }
}

#[async_trait]
impl FunctionNamespace for RegistryBridge {
    async fn invoke(&self, name: &str, data: &[u8]) -> Result<Vec<u8>> {
        let args = serde_json::from_slice(data).map_err(LegionError::Serialization)?;
        let value = self.dispatch(&format!("fn.{name}"), args).await?;
        serde_json::to_vec(&value).map_err(LegionError::Serialization)
    }
}

#[async_trait]
impl ToolRegistry for RegistryBridge {
    async fn definitions(&self) -> Vec<ToolDefinition> {
        self.load_manifests()
            .await
            .into_iter()
            .map(|manifest| ToolDefinition {
                name: format!("fn.{}", manifest.name),
                description: manifest.description,
                parameters: manifest.parameters,
                effect: if manifest.idempotent {
                    EffectClass::Idempotent
                } else {
                    EffectClass::Write
                },
            })
            .collect()
    }

    async fn dispatch(&self, name: &str, args: Value) -> Result<Value> {
        let function_name = name
            .strip_prefix("fn.")
            .ok_or_else(|| LegionError::ToolNotFound(name.into()))?;
        let manifest = self
            .load_manifest(function_name)
            .await
            .ok_or_else(|| LegionError::ToolNotFound(name.into()))?;
        let invoker = self.invoker_for(&manifest.runtime)?;

        debug!(function_name, runtime = ?manifest.runtime, "dispatching namespace function");

        let result = invoker
            .invoke(InvokeRequest {
                function_name: function_name.to_string(),
                call_id: uuid::Uuid::new_v4().to_string(),
                artifact_cid: None,
                args,
            })
            .await?;

        if let Some(error) = result.error {
            return Err(LegionError::ToolError(format!("{function_name}: {error}")));
        }
        Ok(result.output)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::invoke::InvokeResult;

    struct TaggedInvoker(&'static str);

    #[async_trait]
    impl Invoker for TaggedInvoker {
        async fn invoke(&self, request: InvokeRequest) -> Result<InvokeResult> {
            Ok(InvokeResult {
                call_id: request.call_id,
                output: serde_json::json!({ "runtime": self.0 }),
                wall_ms: 0,
                error: None,
            })
        }
    }

    async fn register(namespace: &Namespace, name: &str, runtime: FunctionRuntime) {
        let manifest = FunctionManifest {
            name: name.into(),
            runtime,
            version: "1.0.0".into(),
            artifact_cid: None,
            deployed_at: 0,
            parameters: serde_json::json!({ "type": "object" }),
            description: format!("{name} test function"),
            idempotent: false,
        };
        namespace
            .set_json(
                &format!("/fn/{name}/manifest.json"),
                serde_json::to_value(manifest).unwrap(),
            )
            .await;
    }

    #[tokio::test]
    async fn definitions_are_loaded_without_blocking_runtime() {
        let namespace = Namespace::new();
        register(&namespace, "hello", FunctionRuntime::Bun).await;
        let bridge = RegistryBridge::new(namespace, Arc::new(TaggedInvoker("bun")));

        let definitions = bridge.definitions().await;
        assert_eq!(definitions.len(), 1);
        assert_eq!(definitions[0].name, "fn.hello");
    }

    #[tokio::test]
    async fn dispatches_by_manifest_runtime() {
        let namespace = Namespace::new();
        register(&namespace, "script", FunctionRuntime::Bun).await;
        register(&namespace, "module", FunctionRuntime::Wasm).await;
        let bridge = RegistryBridge::new(namespace, Arc::new(TaggedInvoker("bun")))
            .with_wasm(Arc::new(TaggedInvoker("wasm")));

        assert_eq!(
            bridge.dispatch("fn.script", Value::Null).await.unwrap()["runtime"],
            "bun"
        );
        assert_eq!(
            bridge.dispatch("fn.module", Value::Null).await.unwrap()["runtime"],
            "wasm"
        );
    }

    #[tokio::test]
    async fn wasm_dispatch_requires_wasm_invoker() {
        let namespace = Namespace::new();
        register(&namespace, "module", FunctionRuntime::Wasm).await;
        let bridge = RegistryBridge::new(namespace, Arc::new(TaggedInvoker("bun")));

        let error = bridge.dispatch("fn.module", Value::Null).await.unwrap_err();
        assert!(error.to_string().contains("without WASM runtime support"));
    }
}
