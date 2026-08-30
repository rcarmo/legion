//! Function manifest — the descriptor stored at `/fn/<name>/manifest.json`.

use serde::{Deserialize, Serialize};

/// Which execution engine a function requires.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum FunctionRuntime {
    /// WebAssembly via extism + wasmtime.
    Wasm,
    /// JavaScript/TypeScript via Bun subprocess.
    Bun,
}

/// The manifest for a deployed function.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FunctionManifest {
    /// Unique function name (DNS-label characters only).
    pub name:         String,
    /// Execution engine.
    pub runtime:      FunctionRuntime,
    /// Semver string, e.g. "1.0.0".
    pub version:      String,
    /// Unix timestamp (ms) of last deploy.
    pub deployed_at:  i64,
    /// JSON Schema for the function's input parameters (passed to LLM as tool schema).
    pub parameters:   serde_json::Value,
    /// Human description exposed to the LLM as a tool.
    pub description:  String,
    /// Whether the function produces side effects.
    #[serde(default)]
    pub idempotent:   bool,
}
