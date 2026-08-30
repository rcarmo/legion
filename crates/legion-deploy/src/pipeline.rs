//! Deploy pipeline: validates, persists, and registers functions.

use std::path::PathBuf;
use std::time::Instant;

use anyhow::Result;
use tracing::{info, warn};

use legion_namespace::Namespace;
use legion_runtime::manifest::{FunctionManifest, FunctionRuntime};

use crate::job::{DeployJob, DeployOutcome, DeployStatus};

/// Validates and deploys functions into the namespace + data directory.
pub struct DeployPipeline {
    pub fn_root:   PathBuf,
    pub namespace: Namespace,
}

impl DeployPipeline {
    pub fn new(fn_root: PathBuf, namespace: Namespace) -> Self {
        Self { fn_root, namespace }
    }

    /// Run the full deploy pipeline for a job.
    pub async fn deploy(&self, job: DeployJob) -> DeployOutcome {
        let start = Instant::now();

        // Validate function name (DNS-label safe)
        if !is_valid_name(&job.name) {
            return DeployOutcome {
                job_id:  job.id,
                name:    job.name,
                status:  DeployStatus::Failed,
                path:    None,
                error:   Some("name must match [a-z0-9-]+".into()),
                wall_ms: start.elapsed().as_millis() as u64,
            };
        }

        // Determine file extension
        let ext = match &job.runtime {
            FunctionRuntime::Wasm => "wasm",
            FunctionRuntime::Bun  => "ts",
        };

        // Persist code to disk
        let fn_dir = self.fn_root.join(&job.name);
        if let Err(e) = std::fs::create_dir_all(&fn_dir) {
            return failed(job, start, format!("create dir: {e}"));
        }
        let code_path = fn_dir.join(format!("index.{ext}"));
        if let Err(e) = std::fs::write(&code_path, &job.code) {
            return failed(job, start, format!("write code: {e}"));
        }

        info!(name = %job.name, runtime = ?job.runtime, "function deployed to disk");

        // Build and register manifest in namespace
        let manifest = FunctionManifest {
            name:        job.name.clone(),
            runtime:     job.runtime.clone(),
            version:     "1.0.0".into(),
            deployed_at: chrono::Utc::now().timestamp_millis(),
            parameters:  job.parameters.clone(),
            description: job.description.clone(),
            idempotent:  job.idempotent,
        };

        match serde_json::to_value(&manifest) {
            Ok(v) => {
                self.namespace
                    .set_json(&format!("/fn/{}/manifest.json", job.name), v)
                    .await;
            }
            Err(e) => {
                return failed(job, start, format!("serialize manifest: {e}"));
            }
        }

        // Add deploy history entry
        let history_path = format!("/deploy/history/{}", job.id);
        self.namespace.set_json(&history_path, serde_json::json!({
            "job_id":       job.id.to_string(),
            "name":         job.name,
            "status":       "success",
            "deployed_at":  manifest.deployed_at,
        })).await;

        DeployOutcome {
            job_id:  job.id,
            name:    manifest.name,
            status:  DeployStatus::Success,
            path:    Some(code_path.display().to_string()),
            error:   None,
            wall_ms: start.elapsed().as_millis() as u64,
        }
    }

    /// Remove a deployed function.
    pub async fn undeploy(&self, name: &str) -> Result<()> {
        self.namespace.delete(&format!("/fn/{name}")).await;
        let fn_dir = self.fn_root.join(name);
        if fn_dir.exists() {
            std::fs::remove_dir_all(&fn_dir)?;
        }
        info!(%name, "function undeployed");
        Ok(())
    }
}

fn is_valid_name(name: &str) -> bool {
    !name.is_empty()
        && name.len() <= 64
        && name.chars().all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '-')
        && !name.starts_with('-')
        && !name.ends_with('-')
}

fn failed(job: DeployJob, start: Instant, error: String) -> DeployOutcome {
    warn!(name = %job.name, %error, "deploy failed");
    DeployOutcome {
        job_id:  job.id,
        name:    job.name,
        status:  DeployStatus::Failed,
        path:    None,
        error:   Some(error),
        wall_ms: start.elapsed().as_millis() as u64,
    }
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use legion_runtime::manifest::FunctionRuntime;
    use tempfile::TempDir;

    async fn pipeline(dir: &TempDir) -> DeployPipeline {
        DeployPipeline::new(
            dir.path().join("fn"),
            Namespace::new(),
        )
    }

    #[tokio::test]
    async fn deploy_bun_function() {
        let dir = tempfile::tempdir().unwrap();
        let p   = pipeline(&dir).await;

        let job = DeployJob::new(
            "hello",
            FunctionRuntime::Bun,
            "Greet the world",
            "console.log(JSON.stringify({ greeting: 'hello' }))",
        );
        let outcome = p.deploy(job).await;
        assert_eq!(outcome.status, DeployStatus::Success);
        assert!(outcome.path.unwrap().ends_with("index.ts"));
    }

    #[tokio::test]
    async fn deploy_rejects_invalid_name() {
        let dir = tempfile::tempdir().unwrap();
        let p   = pipeline(&dir).await;
        let job = DeployJob::new("My Invalid Name!", FunctionRuntime::Bun, "", "");
        let outcome = p.deploy(job).await;
        assert_eq!(outcome.status, DeployStatus::Failed);
    }

    #[tokio::test]
    async fn deploy_registers_manifest_in_namespace() {
        let dir  = tempfile::tempdir().unwrap();
        let ns   = Namespace::new();
        let p    = DeployPipeline::new(dir.path().join("fn"), ns.clone());
        let job  = DeployJob::new("greet", FunctionRuntime::Bun, "A greeter", "export default () => ({})");
        p.deploy(job).await;
        let node = ns.get("/fn/greet/manifest.json").await;
        assert!(node.is_some(), "manifest must be registered in namespace");
    }

    #[tokio::test]
    async fn undeploy_removes_from_namespace() {
        let dir = tempfile::tempdir().unwrap();
        let ns  = Namespace::new();
        let p   = DeployPipeline::new(dir.path().join("fn"), ns.clone());
        let job = DeployJob::new("temp", FunctionRuntime::Bun, "", "//noop");
        p.deploy(job).await;
        p.undeploy("temp").await.unwrap();
        assert!(ns.get("/fn/temp/manifest.json").await.is_none());
    }
}
