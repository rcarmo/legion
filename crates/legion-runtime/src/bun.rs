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
        let script = match &req.artifact_cid {
            Some(cid) => self.fn_root.join(".artifacts").join(cid).join("index.ts"),
            None => self.fn_root.join(&req.function_name).join("index.ts"),
        };

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

        let mut command = Command::new(&self.bun_bin);
        command
            .arg("run")
            .arg(&script)
            .env_clear()
            .envs(base_environment())
            .envs(&req.env)
            .env("LEGION_FUNCTION_NAME", &req.function_name)
            .env("LEGION_CALL_ID", &req.call_id)
            .stdin(std::process::Stdio::piped())
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::piped())
            .kill_on_drop(true);
        let mut child = command
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

fn base_environment() -> impl Iterator<Item = (String, String)> {
    ["PATH", "HOME", "TMPDIR", "LANG", "TZ"]
        .into_iter()
        .filter_map(|name| std::env::var(name).ok().map(|value| (name.into(), value)))
}

fn which_bun() -> PathBuf {
    if let Ok(path) = std::env::var("LEGION_BUN_BIN") {
        return PathBuf::from(path);
    }
    // Prefer system-wide locations so the packaged service does not depend on
    // a particular user's home directory; fall back to PATH for development.
    for candidate in &["/usr/local/bin/bun", "/usr/bin/bun"] {
        if std::path::Path::new(candidate).exists() {
            return PathBuf::from(candidate);
        }
    }
    PathBuf::from("bun") // rely on PATH
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::os::unix::fs::PermissionsExt;

    use super::*;

    #[tokio::test]
    async fn injects_declared_and_legion_environment() {
        let dir = tempfile::tempdir().unwrap();
        let fn_dir = dir.path().join("fn/hello");
        std::fs::create_dir_all(&fn_dir).unwrap();
        std::fs::write(fn_dir.join("index.ts"), "unused").unwrap();
        let runner = dir.path().join("fake-bun");
        std::fs::write(
            &runner,
            "#!/bin/sh\nprintf '{\"declared\":\"%s\",\"name\":\"%s\",\"call\":\"%s\"}\\n' \"$GREETING\" \"$LEGION_FUNCTION_NAME\" \"$LEGION_CALL_ID\"\n",
        ).unwrap();
        std::fs::set_permissions(&runner, std::fs::Permissions::from_mode(0o755)).unwrap();
        let runtime = BunRuntime { fn_root: dir.path().join("fn"), bun_bin: runner };
        let result = runtime.invoke(InvokeRequest {
            function_name: "hello".into(),
            call_id: "call-1".into(),
            artifact_cid: None,
            env: BTreeMap::from([("GREETING".into(), "hello".into())]),
            args: serde_json::json!({}),
        }).await.unwrap();
        assert_eq!(result.output, serde_json::json!({
            "declared": "hello",
            "name": "hello",
            "call": "call-1",
        }));
    }
}
