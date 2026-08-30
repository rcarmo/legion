//! Deploy job types.

use serde::{Deserialize, Serialize};
use uuid::Uuid;
use legion_runtime::manifest::FunctionRuntime;

/// A deploy request submitted to the pipeline.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DeployJob {
    pub id:          Uuid,
    pub name:        String,
    pub runtime:     FunctionRuntime,
    pub description: String,
    /// JSON Schema for function args (presented to the LLM).
    pub parameters:  serde_json::Value,
    /// JS/TS source code.
    pub code:        String,
    /// Raw WASM module bytes. Used only by the WASM runtime.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub wasm_bytes:  Option<Vec<u8>>,
    pub idempotent:  bool,
    pub submitted_at: i64,
}

impl DeployJob {
    pub fn new(
        name:        impl Into<String>,
        runtime:     FunctionRuntime,
        description: impl Into<String>,
        code:        impl Into<String>,
    ) -> Self {
        Self {
            id:          Uuid::new_v4(),
            name:        name.into(),
            runtime,
            description: description.into(),
            parameters:  serde_json::json!({ "type": "object", "properties": {} }),
            code:        code.into(),
            wasm_bytes:  None,
            idempotent:  false,
            submitted_at: chrono::Utc::now().timestamp_millis(),
        }
    }
}

/// Outcome of a deploy job.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DeployOutcome {
    pub job_id:  Uuid,
    pub name:    String,
    pub status:  DeployStatus,
    pub path:    Option<String>,
    pub error:   Option<String>,
    pub wall_ms: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum DeployStatus {
    Success,
    Failed,
}
