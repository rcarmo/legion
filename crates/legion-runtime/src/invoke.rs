//! Invoke request/result types and the `Invoker` trait.

use std::collections::BTreeMap;

use async_trait::async_trait;
use serde_json::Value;

use legion_core::error::Result;

/// A single function invocation.
#[derive(Debug, Clone)]
pub struct InvokeRequest {
    pub function_name: String,
    pub call_id: String,
    /// Optional CAS artifact override selected by weighted routing.
    pub artifact_cid: Option<String>,
    /// Explicit environment variables supplied by the deployment manifest.
    pub env: BTreeMap<String, String>,
    pub args: Value,
}

/// The result of a function invocation.
#[derive(Debug, Clone)]
pub struct InvokeResult {
    pub call_id: String,
    pub output: Value,
    pub wall_ms: u64,
    pub error: Option<String>,
}

/// Source used by runtimes to fetch missing content-addressed artifacts.
#[async_trait]
pub trait ArtifactSource: Send + Sync {
    async fn fetch(&self, cid: &str) -> Result<Vec<u8>>;
}

/// Trait implemented by each runtime backend.
#[async_trait]
pub trait Invoker: Send + Sync {
    async fn invoke(&self, req: InvokeRequest) -> Result<InvokeResult>;
}
