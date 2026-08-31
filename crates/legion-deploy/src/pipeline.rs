//! Deploy pipeline: validates, persists, and registers functions.

use std::path::PathBuf;
use std::time::Instant;

use anyhow::Result;
use tracing::{info, warn};

use legion_namespace::Namespace;
use legion_runtime::manifest::{FunctionManifest, FunctionRuntime};

use crate::blob_store::DeployBlobStore;
use crate::job::{DeployJob, DeployOutcome, DeployStatus};

/// Validates and deploys functions into the namespace + data directory.
pub struct DeployPipeline {
    pub fn_root:   PathBuf,
    pub namespace: Namespace,
    blob_store: DeployBlobStore,
}

impl DeployPipeline {
    pub async fn open(fn_root: PathBuf, namespace: Namespace) -> Result<Self> {
        let blob_root = fn_root.parent().unwrap_or(&fn_root).join("blobs");
        let blob_store = DeployBlobStore::open(blob_root).await?;
        Ok(Self { fn_root, namespace, blob_store })
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
                artifact_cid: None,
                error:   Some("name must match [a-z0-9-]+".into()),
                wall_ms: start.elapsed().as_millis() as u64,
            };
        }

        // Determine file extension
        let ext = match &job.runtime {
            FunctionRuntime::Wasm => "wasm",
            FunctionRuntime::Bun  => "ts",
        };

        let artifact = match (&job.runtime, &job.wasm_bytes) {
            (FunctionRuntime::Wasm, Some(bytes)) => bytes.clone(),
            _ => job.code.as_bytes().to_vec(),
        };
        let artifact_cid = match self.blob_store.put(artifact.clone()).await {
            Ok(cid) => cid,
            Err(error) => return failed(job, start, format!("store artifact: {error}")),
        };

        // Materialize the active version locally for the execution runtimes.
        let fn_dir = self.fn_root.join(&job.name);
        if let Err(e) = std::fs::create_dir_all(&fn_dir) {
            return failed(job, start, format!("create dir: {e}"));
        }
        let code_path = fn_dir.join(format!("index.{ext}"));
        let write_result = std::fs::write(&code_path, &artifact);
        if let Err(e) = write_result {
            return failed(job, start, format!("write code: {e}"));
        }

        info!(name = %job.name, runtime = ?job.runtime, "function deployed to disk");

        // Build and register manifest in namespace
        let manifest = FunctionManifest {
            name:        job.name.clone(),
            runtime:     job.runtime.clone(),
            version:     "1.0.0".into(),
            artifact_cid: Some(artifact_cid.clone()),
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

        self.namespace.set_json(
            &format!("/deploy/blobs/{artifact_cid}"),
            serde_json::json!({ "cid": artifact_cid, "size": artifact.len() }),
        ).await;

        // Add deploy history entry
        let history_path = format!("/deploy/history/{}", job.id);
        self.namespace.set_json(&history_path, serde_json::json!({
            "job_id":       job.id.to_string(),
            "name":         job.name,
            "status":       "success",
            "artifact_cid": artifact_cid,
            "deployed_at":  manifest.deployed_at,
        })).await;

        DeployOutcome {
            job_id:  job.id,
            name:    manifest.name,
            status:  DeployStatus::Success,
            path:    Some(code_path.display().to_string()),
            artifact_cid: Some(artifact_cid),
            error:   None,
            wall_ms: start.elapsed().as_millis() as u64,
        }
    }

    /// Register an existing CAS artifact as a deployed function.
    pub async fn register(&self, mut job: DeployJob, artifact_cid: &str) -> DeployOutcome {
        let artifact = match self.blob_store.get(artifact_cid).await {
            Ok(artifact) => artifact,
            Err(error) => return failed(job, Instant::now(), format!("load artifact: {error}")),
        };
        match job.runtime {
            FunctionRuntime::Wasm => job.wasm_bytes = Some(artifact),
            FunctionRuntime::Bun => match String::from_utf8(artifact) {
                Ok(code) => job.code = code,
                Err(error) => {
                    return failed(job, Instant::now(), format!("Bun artifact is not UTF-8: {error}"));
                }
            },
        }
        self.deploy(job).await
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
        artifact_cid: None,
        error:   Some(error),
        wall_ms: start.elapsed().as_millis() as u64,
    }
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use legion_namespace::NodeKind;
    use legion_runtime::manifest::FunctionRuntime;
    use tempfile::TempDir;

    async fn pipeline(dir: &TempDir) -> DeployPipeline {
        DeployPipeline::open(
            dir.path().join("fn"),
            Namespace::new(),
        ).await.unwrap()
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
        assert!(outcome.artifact_cid.is_some());
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
        let p    = DeployPipeline::open(dir.path().join("fn"), ns.clone()).await.unwrap();
        let job  = DeployJob::new("greet", FunctionRuntime::Bun, "A greeter", "export default () => ({})");
        let outcome = p.deploy(job).await;
        let node = ns.get("/fn/greet/manifest.json").await.unwrap();
        let NodeKind::Json(manifest) = node.kind else {
            panic!("manifest must be JSON");
        };
        assert_eq!(manifest["artifact_cid"], outcome.artifact_cid.unwrap());
        assert!(
            ns.get(&format!("/deploy/blobs/{}", manifest["artifact_cid"].as_str().unwrap()))
                .await
                .is_some(),
            "artifact metadata must be registered in namespace",
        );
    }

    #[tokio::test]
    async fn register_materializes_existing_artifact() {
        let dir = tempfile::tempdir().unwrap();
        let p = pipeline(&dir).await;
        let original = DeployJob::new("original", FunctionRuntime::Bun, "", "console.log('{}')");
        let cid = p.deploy(original).await.artifact_cid.unwrap();
        let registered = p.register(
            DeployJob::new("registered", FunctionRuntime::Bun, "", ""),
            &cid,
        ).await;
        assert_eq!(registered.artifact_cid.as_deref(), Some(cid.as_str()));
        assert_eq!(
            std::fs::read_to_string(dir.path().join("fn/registered/index.ts")).unwrap(),
            "console.log('{}')",
        );
    }

    #[tokio::test]
    async fn undeploy_removes_from_namespace() {
        let dir = tempfile::tempdir().unwrap();
        let ns  = Namespace::new();
        let p   = DeployPipeline::open(dir.path().join("fn"), ns.clone()).await.unwrap();
        let job = DeployJob::new("temp", FunctionRuntime::Bun, "", "//noop");
        p.deploy(job).await;
        p.undeploy("temp").await.unwrap();
        assert!(ns.get("/fn/temp/manifest.json").await.is_none());
    }
}
