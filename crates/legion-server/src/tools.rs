//! Built-in tool registry for the Legion server.
//!
//! Provides cluster introspection, session management, namespace access,
//! and function deployment/invocation tools to the agent loop.

use std::sync::Arc;
use serde_json::{json, Value};
use tracing::debug;
use uuid::Uuid;

use legion_cluster::node::ClusterNode;
use legion_core::{
    error::{LegionError, Result},
    traits::{EventStore, ToolRegistry},
    types::{
        EffectClass, ParkReason, RunId, SessionFilter, SessionStatus, ToolDefinition,
        TurnEvent,
    },
};
use legion_namespace::{Namespace, NodeKind};
use legion_runtime::manifest::{FunctionManifest, FunctionRuntime};

// ── BuiltinToolRegistry ───────────────────────────────────────────────────────

pub struct BuiltinToolRegistry {
    store:     Arc<dyn EventStore>,
    node:      Arc<ClusterNode>,
    namespace: Namespace,
}

impl BuiltinToolRegistry {
    pub fn new(
        store:     Arc<dyn EventStore>,
        node:      Arc<ClusterNode>,
        namespace: Namespace,
    ) -> Self {
        Self { store, node, namespace }
    }
}

#[async_trait::async_trait]
impl ToolRegistry for BuiltinToolRegistry {
    fn definitions(&self) -> Vec<ToolDefinition> {
        build_definitions()
    }

    async fn dispatch(&self, name: &str, args: Value) -> Result<Value> {
        debug!(tool = %name, "dispatching built-in tool");
        match name {
            // cluster
            "cluster.status"  => self.cluster_status().await,
            "cluster.peers"   => self.cluster_peers().await,
            "cluster.self"    => self.cluster_self().await,
            // sessions
            "sessions.list"   => self.sessions_list().await,
            "sessions.get"    => self.session_get(&args).await,
            "sessions.fork"   => self.session_fork(&args).await,
            "sessions.park"   => self.session_park(&args).await,
            "sessions.resume" => self.session_resume(&args).await,
            "sessions.cancel" => self.session_cancel(&args).await,
            // namespace
            "ns.ls"    => self.ns_ls(&args).await,
            "ns.read"  => self.ns_read(&args).await,
            "ns.write" => self.ns_write(&args).await,
            // functions
            "fn.list"   => self.fn_list().await,
            "fn.deploy" => self.fn_deploy(&args).await,
            "fn.delete" => self.fn_delete(&args).await,
            _ => Err(LegionError::ToolNotFound(name.into())),
        }
    }
}

// ── Cluster ───────────────────────────────────────────────────────────────────

impl BuiltinToolRegistry {
    async fn cluster_status(&self) -> Result<Value> {
        let eid  = self.node.endpoint_id();
        let addr = self.node.endpoint.addr();
        Ok(json!({
            "endpoint_id":  eid.to_string(),
            "short_id":     self.node.short_id(),
            "role":         "solo",
            "bound_addrs":  format!("{addr:?}"),
            "mdns_enabled": self.node.config.mdns,
        }))
    }

    async fn cluster_peers(&self) -> Result<Value> {
        let names = self.namespace.ls("/cluster/peers").await;
        let mut peers = vec![];
        for name in names {
            if let Some(node) = self.namespace.get(&format!("/cluster/peers/{name}")).await {
                if let NodeKind::Json(v) = node.kind { peers.push(v); }
            }
        }
        Ok(json!({ "peers": peers }))
    }

    async fn cluster_self(&self) -> Result<Value> {
        Ok(json!({
            "endpoint_id": self.node.endpoint_id().to_string(),
            "short_id":    self.node.short_id(),
            "data_dir":    self.node.config.data_dir.display().to_string(),
            "api_port":    self.node.config.api_port,
        }))
    }
}

// ── Sessions ──────────────────────────────────────────────────────────────────

impl BuiltinToolRegistry {
    async fn sessions_list(&self) -> Result<Value> {
        let sessions = self.store.list_sessions(SessionFilter::default()).await?;
        let rows: Vec<Value> = sessions.iter().map(|s| json!({
            "run_id":     s.run_id.to_string(),
            "status":     format!("{:?}", s.status),
            "model":      s.model,
            "created_at": s.created_at,
        })).collect();
        Ok(json!({ "sessions": rows }))
    }

    async fn session_get(&self, args: &Value) -> Result<Value> {
        let run_id = parse_run_id(args, "run_id")?;
        let status = self.store.session_status(run_id).await?;
        let recent = self.store.read_recent(run_id, 10).await?;
        let log: Vec<Value> = recent.iter().map(|e| json!({
            "seq":  e.seq,
            "kind": format!("{:?}", e.event.kind),
        })).collect();
        Ok(json!({
            "run_id":     run_id.to_string(),
            "status":     format!("{:?}", status),
            "recent_log": log,
        }))
    }

    async fn session_fork(&self, args: &Value) -> Result<Value> {
        let run_id = parse_run_id(args, "run_id")?;
        let at_seq = args.get("at_seq")
            .and_then(|v| v.as_u64())
            .ok_or_else(|| LegionError::ToolError("at_seq required".into()))?;

        let new_id = self.store.fork(run_id, at_seq).await?;
        Ok(json!({
            "forked_from": run_id.to_string(),
            "at_seq":      at_seq,
            "new_run_id":  new_id.to_string(),
        }))
    }

    async fn session_park(&self, args: &Value) -> Result<Value> {
        let run_id = parse_run_id(args, "run_id")?;
        self.store.set_status(run_id, SessionStatus::Parked {
            reason: ParkReason::AwaitingUserInput,
        }).await?;
        Ok(json!({ "run_id": run_id.to_string(), "status": "parked" }))
    }

    async fn session_resume(&self, args: &Value) -> Result<Value> {
        let run_id  = parse_run_id(args, "run_id")?;
        let content = args.get("message").and_then(|v| v.as_str()).unwrap_or("").to_string();
        self.store.append(run_id, TurnEvent::user_message(content)).await?;
        self.store.set_status(run_id, SessionStatus::Resuming).await?;
        Ok(json!({ "run_id": run_id.to_string(), "status": "resuming" }))
    }

    async fn session_cancel(&self, args: &Value) -> Result<Value> {
        let run_id = parse_run_id(args, "run_id")?;
        self.store.set_status(run_id, SessionStatus::Aborted).await?;
        Ok(json!({ "run_id": run_id.to_string(), "status": "aborted" }))
    }
}

// ── Namespace ─────────────────────────────────────────────────────────────────

impl BuiltinToolRegistry {
    async fn ns_ls(&self, args: &Value) -> Result<Value> {
        let path = args.get("path").and_then(|v| v.as_str())
            .ok_or_else(|| LegionError::ToolError("path required".into()))?;
        let children = self.namespace.ls(path).await;
        Ok(json!({ "path": path, "children": children }))
    }

    async fn ns_read(&self, args: &Value) -> Result<Value> {
        let path = args.get("path").and_then(|v| v.as_str())
            .ok_or_else(|| LegionError::ToolError("path required".into()))?;
        match self.namespace.get(path).await {
            None => Err(LegionError::ToolError(format!("not found: {path}"))),
            Some(node) => match node.kind {
                NodeKind::Json(v)  => Ok(json!({ "path": path, "kind": "json",  "value": v })),
                NodeKind::Blob(b)  => Ok(json!({ "path": path, "kind": "blob",  "size": b.len() })),
                NodeKind::Dir      => Ok(json!({ "path": path, "kind": "dir" })),
            },
        }
    }

    async fn ns_write(&self, args: &Value) -> Result<Value> {
        let path  = args.get("path").and_then(|v| v.as_str())
            .ok_or_else(|| LegionError::ToolError("path required".into()))?;
        let value = args.get("value").cloned()
            .ok_or_else(|| LegionError::ToolError("value required".into()))?;
        self.namespace.set_json(path, value).await;
        Ok(json!({ "path": path, "written": true }))
    }
}

// ── Functions ─────────────────────────────────────────────────────────────────

impl BuiltinToolRegistry {
    async fn fn_list(&self) -> Result<Value> {
        let names = self.namespace.ls("/fn").await;
        let mut fns = vec![];
        for name in &names {
            let mp = format!("/fn/{name}/manifest.json");
            if let Some(n) = self.namespace.get(&mp).await {
                if let NodeKind::Json(v) = n.kind { fns.push(v); }
            }
        }
        Ok(json!({ "functions": fns }))
    }

    async fn fn_deploy(&self, args: &Value) -> Result<Value> {
        let name = args.get("name").and_then(|v| v.as_str())
            .ok_or_else(|| LegionError::ToolError("name required".into()))?;
        let runtime_str = args.get("runtime").and_then(|v| v.as_str()).unwrap_or("bun");
        let description = args.get("description").and_then(|v| v.as_str()).unwrap_or("").to_string();
        let code  = args.get("code").and_then(|v| v.as_str())
            .ok_or_else(|| LegionError::ToolError("code required".into()))?;
        let params = args.get("parameters").cloned()
            .unwrap_or_else(|| json!({ "type": "object", "properties": {} }));

        let runtime = match runtime_str {
            "wasm" => FunctionRuntime::Wasm,
            _      => FunctionRuntime::Bun,
        };

        let manifest = FunctionManifest {
            name:        name.to_string(),
            runtime:     runtime.clone(),
            version:     "1.0.0".into(),
            deployed_at: chrono::Utc::now().timestamp_millis(),
            parameters:  params,
            description: description.clone(),
            idempotent:  args.get("idempotent").and_then(|v| v.as_bool()).unwrap_or(false),
        };

        let manifest_json = serde_json::to_value(&manifest)
            .map_err(|e| LegionError::Serialization(e))?;

        // Write manifest and code to namespace
        self.namespace.set_json(&format!("/fn/{name}/manifest.json"), manifest_json).await;
        self.namespace.set_json(
            &format!("/fn/{name}/code"),
            json!({ "source": code, "runtime": runtime_str }),
        ).await;

        // Persist code to data_dir/fn/<name>/index.ts (or index.wasm)
        let fn_dir = self.node.config.data_dir.join("fn").join(name);
        std::fs::create_dir_all(&fn_dir)
            .map_err(|e| LegionError::ToolError(format!("create fn dir: {e}")))?;
        let ext  = if matches!(runtime, FunctionRuntime::Wasm) { "wasm" } else { "ts" };
        let file = fn_dir.join(format!("index.{ext}"));
        std::fs::write(&file, code)
            .map_err(|e| LegionError::ToolError(format!("write code: {e}")))?;

        Ok(json!({
            "name":        name,
            "runtime":     runtime_str,
            "description": description,
            "path":        file.display().to_string(),
        }))
    }

    async fn fn_delete(&self, args: &Value) -> Result<Value> {
        let name = args.get("name").and_then(|v| v.as_str())
            .ok_or_else(|| LegionError::ToolError("name required".into()))?;

        self.namespace.delete(&format!("/fn/{name}")).await;

        let fn_dir = self.node.config.data_dir.join("fn").join(name);
        if fn_dir.exists() {
            std::fs::remove_dir_all(&fn_dir)
                .map_err(|e| LegionError::ToolError(format!("remove fn dir: {e}")))?;
        }
        Ok(json!({ "name": name, "deleted": true }))
    }
}

// ── Helpers ───────────────────────────────────────────────────────────────────

fn parse_run_id(args: &Value, field: &str) -> Result<RunId> {
    args.get(field)
        .and_then(|v| v.as_str())
        .and_then(|s| Uuid::parse_str(s).ok())
        .ok_or_else(|| LegionError::ToolError(format!("{field} must be a valid UUID")))
}

fn obj(description: &str, required: &[&str], props: Value) -> Value {
    json!({
        "type": "object",
        "description": description,
        "required": required,
        "properties": props,
    })
}

fn build_definitions() -> Vec<ToolDefinition> {
    vec![
        // cluster
        ToolDefinition { name: "cluster.status".into(), description: "Node cluster status: endpoint ID, role, bound addresses, mDNS.".into(), parameters: json!({ "type": "object", "properties": {} }), effect: EffectClass::Read },
        ToolDefinition { name: "cluster.peers".into(),  description: "List known iroh peers in the cluster.".into(), parameters: json!({ "type": "object", "properties": {} }), effect: EffectClass::Read },
        ToolDefinition { name: "cluster.self".into(),   description: "This node's full identity: endpoint ID, short ID, data dir, API port.".into(), parameters: json!({ "type": "object", "properties": {} }), effect: EffectClass::Read },
        // sessions
        ToolDefinition { name: "sessions.list".into(),   description: "List all sessions with status, model, and creation time.".into(), parameters: json!({ "type": "object", "properties": {} }), effect: EffectClass::Read },
        ToolDefinition { name: "sessions.get".into(),    description: "Get session metadata and recent log.".into(), parameters: obj("", &["run_id"], json!({ "run_id": { "type": "string" } })), effect: EffectClass::Read },
        ToolDefinition { name: "sessions.fork".into(),   description: "Fork a session at a given sequence number.".into(), parameters: obj("", &["run_id","at_seq"], json!({ "run_id": { "type": "string" }, "at_seq": { "type": "integer" } })), effect: EffectClass::Write },
        ToolDefinition { name: "sessions.park".into(),   description: "Suspend a session until an external event resumes it.".into(), parameters: obj("", &["run_id"], json!({ "run_id": { "type": "string" }, "reason": { "type": "string" } })), effect: EffectClass::Write },
        ToolDefinition { name: "sessions.resume".into(), description: "Resume a parked session by injecting a user message.".into(), parameters: obj("", &["run_id"], json!({ "run_id": { "type": "string" }, "message": { "type": "string" } })), effect: EffectClass::Write },
        ToolDefinition { name: "sessions.cancel".into(), description: "Abort a session.".into(), parameters: obj("", &["run_id"], json!({ "run_id": { "type": "string" } })), effect: EffectClass::Write },
        // namespace
        ToolDefinition { name: "ns.ls".into(),    description: "List direct children of a namespace path.".into(), parameters: obj("", &["path"], json!({ "path": { "type": "string" } })), effect: EffectClass::Read },
        ToolDefinition { name: "ns.read".into(),  description: "Read a namespace node (JSON value, blob size, or dir).".into(), parameters: obj("", &["path"], json!({ "path": { "type": "string" } })), effect: EffectClass::Read },
        ToolDefinition { name: "ns.write".into(), description: "Write a JSON value to a namespace path.".into(), parameters: obj("", &["path","value"], json!({ "path": { "type": "string" }, "value": { "type": "object" } })), effect: EffectClass::Write },
        // functions
        ToolDefinition { name: "fn.list".into(),   description: "List all deployed functions.".into(), parameters: json!({ "type": "object", "properties": {} }), effect: EffectClass::Read },
        ToolDefinition {
            name:        "fn.deploy".into(),
            description: "Deploy a Bun (JS/TS) or WASM function. Supply name, runtime, description, code (source text or wasm bytes), and optional JSON Schema parameters.".into(),
            parameters:  obj("", &["name","code"], json!({
                "name":        { "type": "string", "description": "Unique function name (DNS-label)" },
                "runtime":     { "type": "string", "enum": ["bun","wasm"], "description": "Execution engine" },
                "description": { "type": "string" },
                "code":        { "type": "string", "description": "JS/TS source or base64 WASM" },
                "parameters":  { "type": "object", "description": "JSON Schema for function args" },
                "idempotent":  { "type": "boolean" },
            })),
            effect: EffectClass::Write,
        },
        ToolDefinition { name: "fn.delete".into(), description: "Remove a deployed function.".into(), parameters: obj("", &["name"], json!({ "name": { "type": "string" } })), effect: EffectClass::Write },
    ]
}
