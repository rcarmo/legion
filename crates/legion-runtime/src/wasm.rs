//! WASM runtime via extism.
//!
//! Executes deployed WASM functions. The WASM module must export a `main` function
//! that reads JSON from stdin (extism input) and writes JSON to stdout (extism output).
//!
//! WASM modules are compiled at deploy time and cached by function name.

use std::path::PathBuf;
use std::sync::Mutex;
use std::time::Instant;
use std::collections::HashMap;

use async_trait::async_trait;
use extism::{Plugin, Wasm, Manifest};
use serde_json::Value;
use tracing::{debug, warn};

use legion_core::error::{LegionError, Result};
use crate::invoke::{InvokeRequest, InvokeResult, Invoker};

/// WASM function invoker using extism.
pub struct WasmRuntime {
    /// Root directory where WASM bundles live (e.g. `/var/lib/legion/fn`).
    pub fn_root: PathBuf,
    /// Compiled plugin cache (function_name → compiled bytes).
    cache: Mutex<HashMap<String, Vec<u8>>>,
}

impl WasmRuntime {
    pub fn new(fn_root: PathBuf) -> Self {
        Self { fn_root, cache: Mutex::new(HashMap::new()) }
    }

    fn load_wasm(&self, function_name: &str) -> Result<Vec<u8>> {
        let path = self.fn_root.join(function_name).join("index.wasm");
        if !path.exists() {
            return Err(LegionError::ToolNotFound(format!(
                "wasm function {} not found at {}",
                function_name,
                path.display()
            )));
        }
        std::fs::read(&path)
            .map_err(|e| LegionError::ToolError(format!("read wasm: {e}")))
    }
}

#[async_trait]
impl Invoker for WasmRuntime {
    async fn invoke(&self, req: InvokeRequest) -> Result<InvokeResult> {
        let function_name = req.function_name.clone();
        debug!(fn_name = %function_name, "invoking wasm function");

        // Load WASM bytes (check cache first)
        let wasm_bytes = {
            let cache = self.cache.lock().unwrap();
            cache.get(&function_name).cloned()
        };
        let wasm_bytes = match wasm_bytes {
            Some(b) => b,
            None => {
                let b = self.load_wasm(&function_name)?;
                self.cache.lock().unwrap().insert(function_name.clone(), b.clone());
                b
            }
        };

        let args_json = serde_json::to_string(&req.args)
            .map_err(|e| LegionError::Serialization(e))?;

        // Run in a blocking task to avoid blocking the async executor
        let call_id = req.call_id.clone();
        let result = tokio::task::spawn_blocking(move || {
            let start = Instant::now();
            let wasm   = Wasm::data(wasm_bytes);
            let manifest = Manifest::new([wasm]);
            let mut plugin = Plugin::new(manifest, [], true)
                .map_err(|e| LegionError::ToolError(format!("create plugin: {e}")))?;

            let output: Vec<u8> = plugin
                .call("main", args_json.as_bytes())
                .map_err(|e| LegionError::ToolError(format!("call main: {e}")))?;

            let output_val: Value = serde_json::from_slice(&output)
                .unwrap_or_else(|_| Value::String(String::from_utf8_lossy(&output).into_owned()));

            Ok::<InvokeResult, LegionError>(InvokeResult {
                call_id,
                output:  output_val,
                wall_ms: start.elapsed().as_millis() as u64,
                error:   None,
            })
        }).await
        .map_err(|e| LegionError::ToolError(format!("wasm task: {e}")))??;

        Ok(result)
    }
}
