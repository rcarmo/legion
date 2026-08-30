//! Bun subprocess runtime.
//!
//! Invokes a deployed JS/TS function by running:
//!   `bun run /fn/<name>/index.ts`
//! with the JSON args on stdin, and parses stdout as the JSON result.

use std::path::PathBuf;
use std::time::Instant;
use async_trait::async_trait;
use serde_json::Value;
use tokio::io::AsyncWriteExt;
use tokio::process::Command;
use tracing::{debug, warn};

use legion_core::error::{LegionError, Result};
use crate::invoke::{InvokeRequest, InvokeResult, Invoker};

/// Bun function invoker.
pub struct BunRuntime {
    /// Root directory where function bundles live (e.g. `/var/lib/legion/fn`).
    pub fn_root: PathBuf,
    /// Path to the bun binary.
    pub bun_bin:  PathBuf,
}

impl Default for BunRuntime {
    fn default() -> Self {
        Self {
            fn_root: PathBuf::from("/var/lib/legion/fn"),
            bun_bin:  which_bun(),
        }
    }
}

#[async_trait]
impl Invoker for BunRuntime {
    async fn invoke(&self, req: InvokeRequest) -> Result<InvokeResult> {
        let script = self.fn_root
            .join(&req.function_name)
            .join("index.ts");

        if !script.exists() {
            return Err(LegionError::ToolNotFound(format!(
                "bun function {} not found at {}",
                req.function_name,
                script.display()
            )));
        }

        let args_json = serde_json::to_string(&req.args)
            .map_err(LegionError::Serialization)?;

        debug!(fn_name = %req.function_name, "invoking bun function");
        let start = Instant::now();

        let mut child = Command::new(&self.bun_bin)
            .arg("run")
            .arg(&script)
            .stdin(std::process::Stdio::piped())
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::piped())
            .kill_on_drop(true)
            .spawn()
            .map_err(|e| LegionError::ToolError(format!("spawn bun: {e}")))?;

        // Write args to stdin
        if let Some(mut stdin) = child.stdin.take() {
            stdin.write_all(args_json.as_bytes()).await
                .map_err(|e| LegionError::ToolError(format!("write stdin: {e}")))?;
        }

        let output = child.wait_with_output().await
            .map_err(|e| LegionError::ToolError(format!("bun wait: {e}")))?;

        let wall_ms = start.elapsed().as_millis() as u64;

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr).into_owned();
            warn!(fn_name = %req.function_name, %stderr, "bun function failed");
            return Ok(InvokeResult {
                call_id: req.call_id,
                output:  Value::Null,
                wall_ms,
                error:   Some(stderr),
            });
        }

        let stdout = String::from_utf8_lossy(&output.stdout);
        let result: Value = serde_json::from_str(stdout.trim())
            .unwrap_or_else(|_| serde_json::json!({ "output": stdout.trim() }));

        Ok(InvokeResult {
            call_id: req.call_id,
            output:  result,
            wall_ms,
            error:   None,
        })
    }
}

fn which_bun() -> PathBuf {
    // Check common locations; fall back to PATH
    for candidate in &[
        "/home/agent/.bun/bin/bun",
        "/usr/local/bin/bun",
        "/usr/bin/bun",
    ] {
        if std::path::Path::new(candidate).exists() {
            return PathBuf::from(candidate);
        }
    }
    PathBuf::from("bun") // rely on PATH
}
