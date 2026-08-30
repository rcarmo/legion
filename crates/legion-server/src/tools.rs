//! Built-in tool registry for the Legion server.

use std::sync::Arc;
use async_trait as _; // used via #[async_trait::async_trait]
use serde_json::{json, Value};
use tracing::debug;
use uuid::Uuid;

use legion_cluster::node::ClusterNode;
use legion_core::{
    error::{LegionError, Result},
    traits::{EventStore, ToolRegistry},
    types::{
        EffectClass, ParkReason, RunId, SessionFilter, SessionStatus, ToolDefinition,
        TurnEvent, TurnEventKind,
    },
};

// ── BuiltinToolRegistry ───────────────────────────────────────────────────────

pub struct BuiltinToolRegistry {
    store: Arc<dyn EventStore>,
    node:  Arc<ClusterNode>,
}

impl BuiltinToolRegistry {
    pub fn new(store: Arc<dyn EventStore>, node: Arc<ClusterNode>) -> Self {
        Self { store, node }
    }
}

    #[async_trait::async_trait]
    impl ToolRegistry for BuiltinToolRegistry {
        fn definitions(&self) -> Vec<ToolDefinition> {
            build_definitions()
        }

        async fn dispatch(&self, name: &str, args: serde_json::Value) -> Result<serde_json::Value> {
        debug!(tool = %name, "dispatching built-in tool");
        match name {
            "cluster.status"  => self.cluster_status().await,
            "cluster.peers"   => self.cluster_peers().await,
            "cluster.self"    => self.cluster_self().await,
            "sessions.list"   => self.sessions_list().await,
            "sessions.get"    => self.session_get(&args).await,
            "sessions.fork"   => self.session_fork(&args).await,
            "sessions.park"   => self.session_park(&args).await,
            "sessions.resume" => self.session_resume(&args).await,
            "sessions.cancel" => self.session_cancel(&args).await,
            _ => Err(LegionError::ToolNotFound(name.into())),
        }
    }
}

// ── Cluster tools ─────────────────────────────────────────────────────────────

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
        Ok(json!({ "peers": [] }))
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

// ── Session tools ─────────────────────────────────────────────────────────────

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
            .ok_or_else(|| LegionError::ToolError("at_seq required".into()))? as u32;

        let log = self.store.read_log(run_id).await?;
        let config: legion_core::types::RunConfig = log.iter()
            .find_map(|e| {
                if matches!(e.event.kind, TurnEventKind::SessionStarted) {
                    e.event.payload.as_ref()
                        .and_then(|p| serde_json::from_value(p.clone()).ok())
                } else {
                    None
                }
            })
            .ok_or_else(|| LegionError::Store(format!("no SessionStarted in {run_id}")))?;

        let new_id = self.store.fork(run_id, at_seq as u64).await?;
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
        let content = args.get("message")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();
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
        ToolDefinition {
            name:        "cluster.status".into(),
            description: "Return this node's cluster status: endpoint ID, role, bound addresses, mDNS state.".into(),
            parameters:  json!({ "type": "object", "properties": {} }),
            effect:      EffectClass::Read,
        },
        ToolDefinition {
            name:        "cluster.peers".into(),
            description: "List currently known iroh peers in the cluster.".into(),
            parameters:  json!({ "type": "object", "properties": {} }),
            effect:      EffectClass::Read,
        },
        ToolDefinition {
            name:        "cluster.self".into(),
            description: "Return this node's full identity: endpoint ID, short ID, data dir, API port.".into(),
            parameters:  json!({ "type": "object", "properties": {} }),
            effect:      EffectClass::Read,
        },
        ToolDefinition {
            name:        "sessions.list".into(),
            description: "List all sessions with their status, model, and creation time.".into(),
            parameters:  json!({ "type": "object", "properties": {} }),
            effect:      EffectClass::Read,
        },
        ToolDefinition {
            name:        "sessions.get".into(),
            description: "Get metadata and recent log entries for a session.".into(),
            parameters:  obj(
                "Session to inspect",
                &["run_id"],
                json!({ "run_id": { "type": "string", "description": "Session UUID" } }),
            ),
            effect: EffectClass::Read,
        },
        ToolDefinition {
            name:        "sessions.fork".into(),
            description: "Fork a session at a given sequence number, creating a new branch.".into(),
            parameters:  obj(
                "Fork parameters",
                &["run_id", "at_seq"],
                json!({
                    "run_id": { "type": "string", "description": "Session UUID to fork" },
                    "at_seq": { "type": "integer", "description": "Sequence number to fork at" },
                }),
            ),
            effect: EffectClass::Write,
        },
        ToolDefinition {
            name:        "sessions.park".into(),
            description: "Suspend a session until an external event resumes it.".into(),
            parameters:  obj(
                "Park parameters",
                &["run_id"],
                json!({
                    "run_id": { "type": "string", "description": "Session UUID" },
                    "reason": { "type": "string", "description": "Human-readable reason" },
                }),
            ),
            effect: EffectClass::Write,
        },
        ToolDefinition {
            name:        "sessions.resume".into(),
            description: "Resume a parked session by injecting a user message.".into(),
            parameters:  obj(
                "Resume parameters",
                &["run_id"],
                json!({
                    "run_id":  { "type": "string", "description": "Session UUID" },
                    "message": { "type": "string", "description": "Message to inject" },
                }),
            ),
            effect: EffectClass::Write,
        },
        ToolDefinition {
            name:        "sessions.cancel".into(),
            description: "Abort a running or parked session.".into(),
            parameters:  obj(
                "Cancel parameters",
                &["run_id"],
                json!({ "run_id": { "type": "string", "description": "Session UUID" } }),
            ),
            effect: EffectClass::Write,
        },
    ]
}
