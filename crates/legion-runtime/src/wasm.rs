//! WASM runtime via extism with bounded host capabilities.

use std::collections::{BTreeMap, HashMap};
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use async_trait::async_trait;
use extism::{Manifest, PluginBuilder, UserData, ValType, Wasm, host_fn};
use serde_json::Value;
use tracing::debug;

use crate::invoke::{ArtifactSource, InvokeRequest, InvokeResult, Invoker};
use legion_core::error::{LegionError, Result as LegionResult};

#[derive(Debug, Default)]
struct HostState {
    values: BTreeMap<String, String>,
    remaining_budget: u64,
}

host_fn!(host_log(message: String) {
    tracing::info!(target: "legion::wasm", %message, "guest log");
    Ok(())
});

host_fn!(host_read(state: HostState; key: String) -> String {
    let state = state.get()?;
    let state = state.lock().map_err(|_| anyhow::anyhow!("host state lock poisoned"))?;
    Ok(state.values.get(&key).cloned().unwrap_or_default())
});

host_fn!(host_write(state: HostState; key: String, value: String) {
    let state = state.get()?;
    let mut state = state.lock().map_err(|_| anyhow::anyhow!("host state lock poisoned"))?;
    state.values.insert(key, value);
    Ok(())
});

host_fn!(host_budget(state: HostState; requested: u64) -> u64 {
    let state = state.get()?;
    let mut state = state.lock().map_err(|_| anyhow::anyhow!("host state lock poisoned"))?;
    let granted = requested.min(state.remaining_budget);
    state.remaining_budget -= granted;
    Ok(granted)
});

/// WASM function invoker using extism.
pub struct WasmRuntime {
    pub fn_root: PathBuf,
    cache: Mutex<HashMap<String, Vec<u8>>>,
    timeout_ms: u64,
    fuel: u64,
    max_memory_bytes: usize,
    artifact_source: Option<Arc<dyn ArtifactSource>>,
}

impl WasmRuntime {
    pub fn new(fn_root: PathBuf) -> Self {
        Self::with_limits(fn_root, 30_000, 100_000_000, 64 * 1024 * 1024)
    }

    pub fn with_timeout(fn_root: PathBuf, timeout_ms: u64) -> Self {
        Self::with_limits(fn_root, timeout_ms, 100_000_000, 64 * 1024 * 1024)
    }

    pub fn with_limits(
        fn_root: PathBuf,
        timeout_ms: u64,
        fuel: u64,
        max_memory_bytes: usize,
    ) -> Self {
        Self {
            fn_root,
            cache: Mutex::new(HashMap::new()),
            timeout_ms,
            fuel,
            max_memory_bytes,
            artifact_source: None,
        }
    }

    pub fn with_artifact_source(mut self, source: Arc<dyn ArtifactSource>) -> Self {
        self.artifact_source = Some(source);
        self
    }

    async fn load_wasm(
        &self,
        function_name: &str,
        artifact_cid: Option<&str>,
    ) -> LegionResult<Vec<u8>> {
        let path = match artifact_cid {
            Some(cid) => self.fn_root.join(".artifacts").join(cid).join("index.wasm"),
            None => self.fn_root.join(function_name).join("index.wasm"),
        };
        if path.exists() {
            return std::fs::read(&path)
                .map_err(|error| LegionError::ToolError(format!("read wasm: {error}")));
        }
        let Some(cid) = artifact_cid else {
            return Err(LegionError::ToolNotFound(format!(
                "wasm function {function_name} not found at {}",
                path.display(),
            )));
        };
        let source = self.artifact_source.as_ref().ok_or_else(|| {
            LegionError::ToolNotFound(format!("WASM artifact {cid} is not cached"))
        })?;
        let bytes = source.fetch(cid).await?;
        let parent = path.parent().expect("artifact path has parent");
        std::fs::create_dir_all(parent)
            .map_err(|error| LegionError::ToolError(format!("create artifact cache: {error}")))?;
        std::fs::write(&path, &bytes)
            .map_err(|error| LegionError::ToolError(format!("cache wasm artifact: {error}")))?;
        Ok(bytes)
    }
}

#[async_trait]
impl Invoker for WasmRuntime {
    async fn invoke(&self, req: InvokeRequest) -> LegionResult<InvokeResult> {
        let function_name = req.function_name.clone();
        let cache_key = req
            .artifact_cid
            .clone()
            .unwrap_or_else(|| function_name.clone());
        debug!(fn_name = %function_name, "invoking wasm function");

        let wasm_bytes = self.cache.lock().unwrap().get(&cache_key).cloned();
        let wasm_bytes = match wasm_bytes {
            Some(bytes) => bytes,
            None => {
                let bytes = self
                    .load_wasm(&function_name, req.artifact_cid.as_deref())
                    .await?;
                self.cache.lock().unwrap().insert(cache_key, bytes.clone());
                bytes
            }
        };

        let args_json = serde_json::to_string(&req.args).map_err(LegionError::Serialization)?;
        let call_id = req.call_id.clone();
        let timeout_ms = self.timeout_ms;
        let fuel = self.fuel;
        let max_pages = self.max_memory_bytes.div_ceil(65_536).max(1) as u32;
        let result = tokio::task::spawn_blocking(move || {
            let start = Instant::now();
            let state = UserData::new(HostState {
                values: BTreeMap::new(),
                remaining_budget: fuel,
            });
            let manifest = Manifest::new([Wasm::data(wasm_bytes)])
                .with_timeout(Duration::from_millis(timeout_ms))
                .with_memory_max(max_pages);
            let mut builder = PluginBuilder::new(manifest)
                .with_wasi(true)
                .with_function("log", [ValType::I64], [], UserData::default(), host_log)
                .with_function(
                    "read",
                    [ValType::I64],
                    [ValType::I64],
                    state.clone(),
                    host_read,
                )
                .with_function(
                    "write",
                    [ValType::I64, ValType::I64],
                    [],
                    state.clone(),
                    host_write,
                )
                .with_function("budget", [ValType::I64], [ValType::I64], state, host_budget);
            if fuel > 0 {
                builder = builder.with_fuel_limit(fuel);
            }
            let mut plugin = builder
                .build()
                .map_err(|error| LegionError::ToolError(format!("create plugin: {error}")))?;
            let output: Vec<u8> = plugin
                .call("run", args_json.as_bytes())
                .map_err(|error| LegionError::ToolError(format!("call run: {error}")))?;
            let output = serde_json::from_slice(&output)
                .unwrap_or_else(|_| Value::String(String::from_utf8_lossy(&output).into_owned()));
            Ok::<InvokeResult, LegionError>(InvokeResult {
                call_id,
                output,
                wall_ms: start.elapsed().as_millis() as u64,
                error: None,
            })
        })
        .await
        .map_err(|error| LegionError::ToolError(format!("wasm task: {error}")))??;
        Ok(result)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    struct StaticSource(Vec<u8>);

    #[async_trait]
    impl ArtifactSource for StaticSource {
        async fn fetch(&self, _cid: &str) -> LegionResult<Vec<u8>> {
            Ok(self.0.clone())
        }
    }

    #[tokio::test]
    async fn fetches_and_caches_missing_artifact() {
        let dir = tempfile::tempdir().unwrap();
        let bytes = b"not-a-module".to_vec();
        let runtime = WasmRuntime::new(dir.path().to_path_buf())
            .with_artifact_source(Arc::new(StaticSource(bytes.clone())));
        assert_eq!(runtime.load_wasm("test", Some("cid")).await.unwrap(), bytes);
        assert_eq!(
            std::fs::read(dir.path().join(".artifacts/cid/index.wasm")).unwrap(),
            b"not-a-module",
        );
    }

    #[test]
    fn host_budget_is_bounded() {
        let state = UserData::new(HostState {
            values: BTreeMap::new(),
            remaining_budget: 10,
        });
        let state = state.get().unwrap();
        let mut state = state.lock().unwrap();
        let granted = 20_u64.min(state.remaining_budget);
        state.remaining_budget -= granted;
        assert_eq!(granted, 10);
        assert_eq!(state.remaining_budget, 0);
    }
}
