//! REST API — axum router for session management.
//!
//! Routes:
//!   POST   /sessions              — create a new session
//!   GET    /sessions/:id          — get session status
//!   POST   /sessions/:id/messages — send a user message + run one resolve turn
//!   GET    /sessions/:id/log      — get the full event log
//!   GET    /health                — liveness probe

use std::sync::Arc;

use anyhow::Result;
use axum::{
    extract::{Path, State},
    http::StatusCode,
    middleware,
    response::{Json, Sse, sse::Event as SseEvent},
    routing::{get, post},
    Router,
};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use tracing::{info, warn};
use uuid::Uuid;

use legion_core::traits::{AgentLoopTrait, EventStore};
use legion_core::types::{Budget, ExternalEvent, RunConfig};
use legion_deploy::{DeployJob, DeployPipeline};
use legion_loop::{driver::LegionLoop, SessionEvent};
use legion_namespace::{Namespace, NodeKind};
use legion_runtime::{invoke::{InvokeRequest, Invoker}, manifest::FunctionRuntime};

// ── AppState ──────────────────────────────────────────────────────────────────

#[derive(Clone)]
pub struct AppState {
    pub store:     Arc<dyn EventStore>,
    pub lp:        Arc<LegionLoop>,
    pub deployer:  Arc<DeployPipeline>,
    pub namespace: Namespace,
    pub invoker:   Arc<dyn Invoker>,
}

// ── Request / response types ──────────────────────────────────────────────────

#[derive(Debug, Deserialize)]
struct CreateSessionRequest {
    model:         String,
    system_prompt: Option<String>,
    budget:        Option<BudgetRequest>,
}

#[derive(Debug, Deserialize)]
struct BudgetRequest {
    max_turns:      Option<u32>,
    max_tokens_in:  Option<u64>,
    max_tokens_out: Option<u64>,
    max_wall_ms:    Option<u64>,
}

#[derive(Debug, Deserialize)]
struct DeployRequest {
    name:        String,
    runtime:     Option<String>,
    description: Option<String>,
    code:        String,
    idempotent:  Option<bool>,
    parameters:  Option<serde_json::Value>,
}

#[derive(Debug, Deserialize)]
struct SendMessageRequest {
    content: String,
}

// ── Handlers ──────────────────────────────────────────────────────────────────

async fn health() -> Json<Value> {
    Json(json!({ "ok": true, "version": env!("CARGO_PKG_VERSION") }))
}

async fn create_session(
    State(state): State<Arc<AppState>>,
    Json(req):    Json<CreateSessionRequest>,
) -> Result<(StatusCode, Json<Value>), (StatusCode, Json<Value>)> {
    let config = RunConfig {
        model:         req.model.clone(),
        system_prompt: req.system_prompt.clone(),
        tools:         vec![],
        metadata:      None,
        budget: req.budget.map(|b| Budget {
            max_turns:      b.max_turns,
            max_tokens_in:  b.max_tokens_in,
            max_tokens_out: b.max_tokens_out,
            max_wall_ms:    b.max_wall_ms,
            max_cost_usd:   None,
        }).unwrap_or_default(),
    };

    let run_id = state.lp.start(config).await
        .map_err(|e| server_error(e.to_string()))?;

    info!(run_id = %run_id, model = %req.model, "session created");

    Ok((StatusCode::CREATED, Json(json!({
        "id":     run_id.to_string(),
        "status": "idle",
    }))))
}

async fn get_session(
    State(state): State<Arc<AppState>>,
    Path(id):     Path<Uuid>,
) -> Result<Json<Value>, (StatusCode, Json<Value>)> {
    let status = state.store.session_status(id)
        .await
        .map_err(|e| not_found(e.to_string()))?;

    Ok(Json(json!({
        "id":     id.to_string(),
        "status": serde_json::to_value(status).unwrap_or(json!("unknown")),
    })))
}

async fn get_log(
    State(state): State<Arc<AppState>>,
    Path(id):     Path<Uuid>,
) -> Result<Json<Value>, (StatusCode, Json<Value>)> {
    let log = state.store.read_log(id)
        .await
        .map_err(|e| not_found(e.to_string()))?;

    let entries: Vec<_> = log.iter().map(|e| json!({
        "seq":        e.seq,
        "kind":       format!("{:?}", e.event.kind),
        "tokens_in":  e.event.tokens_in,
        "tokens_out": e.event.tokens_out,
        "wall_ms":    e.event.wall_ms,
        "created_at": e.created_at,
    })).collect();

    Ok(Json(json!({ "id": id.to_string(), "entries": entries })))
}

async fn send_message(
    State(state): State<Arc<AppState>>,
    Path(id):     Path<Uuid>,
    Json(req):    Json<SendMessageRequest>,
) -> Result<Json<Value>, (StatusCode, Json<Value>)> {
    // Inject the user message and run one resolve turn
    state.lp.resume(id, ExternalEvent::user_message(req.content.clone()))
        .await
        .map_err(|e| server_error(e.to_string()))?;

    let envelope = state.lp.resolve(id)
        .await
        .map_err(|e| {
            warn!(run_id = %id, err = %e, "resolve error");
            server_error(e.to_string())
        })?;

    let response_text = envelope.event.payload
        .as_ref()
        .and_then(|p| p.get("content"))
        .and_then(|c| c.as_str())
        .unwrap_or("")
        .to_string();

    Ok(Json(json!({
        "id":           id.to_string(),
        "seq":          envelope.seq,
        "response":     response_text,
        "tokens_in":    envelope.event.tokens_in,
        "tokens_out":   envelope.event.tokens_out,
        "wall_ms":      envelope.event.wall_ms,
    })))
}

async fn deploy_function(
    State(state): State<Arc<AppState>>,
    Json(req):    Json<DeployRequest>,
) -> Result<(StatusCode, Json<Value>), (StatusCode, Json<Value>)> {
    let runtime = match req.runtime.as_deref().unwrap_or("bun") {
        "wasm" => FunctionRuntime::Wasm,
        _      => FunctionRuntime::Bun,
    };
    let mut job = DeployJob::new(
        req.name,
        runtime,
        req.description.unwrap_or_default(),
        req.code,
    );
    if let Some(p) = req.parameters { job.parameters = p; }
    if let Some(i) = req.idempotent  { job.idempotent = i; }

    let outcome = state.deployer.deploy(job).await;
    if outcome.status == legion_deploy::DeployStatus::Success {
        Ok((StatusCode::CREATED, Json(serde_json::to_value(&outcome).unwrap())))
    } else {
        Err((StatusCode::UNPROCESSABLE_ENTITY, Json(serde_json::to_value(&outcome).unwrap())))
    }
}

async fn list_functions(
    State(state): State<Arc<AppState>>,
) -> Json<Value> {
    let names = state.namespace.ls("/fn").await;
    let mut fns = vec![];
    for name in names {
        let path = format!("/fn/{name}/manifest.json");
        if let Some(n) = state.namespace.get(&path).await {
            if let legion_namespace::NodeKind::Json(v) = n.kind {
                fns.push(v);
            }
        }
    }
    Json(json!({ "functions": fns }))
}

async fn delete_function(
    State(state): State<Arc<AppState>>,
    Path(name):   Path<String>,
) -> Result<Json<Value>, (StatusCode, Json<Value>)> {
    state.deployer.undeploy(&name).await
        .map_err(|e| server_error(e.to_string()))?;
    Ok(Json(json!({ "name": name, "deleted": true })))
}

/// GET /sessions/:id/stream — SSE stream of a single resolve turn.
/// Client sends the user message as a query param `?message=...` or in headers.
async fn stream_session(
    State(state): State<Arc<AppState>>,
    Path(id):     Path<Uuid>,
    axum::extract::Query(params): axum::extract::Query<std::collections::HashMap<String, String>>,
) -> Sse<impl futures::Stream<Item = Result<SseEvent, std::convert::Infallible>>> {
    use futures::stream;
    use std::convert::Infallible;

    let message = params.get("message").cloned().unwrap_or_default();
    let mut rx  = state.lp.clone().stream_resolve(id, message);

    let sse_stream = stream::unfold(rx, |mut rx| async move {
        match rx.recv().await {
            None     => None,
            Some(ev) => {
                let json  = serde_json::to_string(&ev).unwrap_or_default();
                let event = SseEvent::default().data(json);
                Some((Ok::<SseEvent, Infallible>(event), rx))
            }
        }
    });

    Sse::new(sse_stream)
        .keep_alive(axum::response::sse::KeepAlive::default())
}

async fn invoke_function(
    State(state): State<Arc<AppState>>,
    Path(name):   Path<String>,
    Json(args):   Json<Value>,
) -> Result<Json<Value>, (StatusCode, Json<Value>)> {
    // Check the function exists in namespace
    let manifest_path = format!("/fn/{name}/manifest.json");
    if state.namespace.get(&manifest_path).await.is_none() {
        return Err(not_found(format!("function not found: {name}")));
    }

    let result = state.invoker.invoke(InvokeRequest {
        function_name: name.clone(),
        call_id:       uuid::Uuid::new_v4().to_string(),
        args,
    }).await.map_err(|e| server_error(e.to_string()))?;

    if let Some(err) = result.error {
        return Err((StatusCode::UNPROCESSABLE_ENTITY, Json(json!({
            "function": name,
            "error":    err,
            "wall_ms":  result.wall_ms,
        }))));
    }

    Ok(Json(json!({
        "function": name,
        "output":   result.output,
        "wall_ms":  result.wall_ms,
    })))
}

/// POST /sessions/:id/events — inject an external trigger to resume a parked session.
async fn session_webhook(
    State(state): State<Arc<AppState>>,
    Path(id):     Path<Uuid>,
    Json(body):   Json<Value>,
) -> Result<Json<Value>, (StatusCode, Json<Value>)> {
    use legion_core::traits::AgentLoopTrait;

    let trigger_name = body.get("trigger")
        .and_then(|v| v.as_str())
        .unwrap_or("webhook")
        .to_string();
    let payload = body.get("payload").cloned().unwrap_or(Value::Null);

    state.lp.resume(id, ExternalEvent::ExternalTrigger {
        name:    trigger_name.clone(),
        payload: payload.clone(),
    }).await.map_err(|e| server_error(e.to_string()))?;

    info!(run_id = %id, trigger = %trigger_name, "external event injected");

    Ok(Json(json!({
        "id":      id.to_string(),
        "trigger": trigger_name,
        "status":  "resuming",
    })))
}

fn server_error(msg: String) -> (StatusCode, Json<Value>) {
    (StatusCode::INTERNAL_SERVER_ERROR, Json(json!({ "error": msg })))
}
fn not_found(msg: String) -> (StatusCode, Json<Value>) {
    (StatusCode::NOT_FOUND, Json(json!({ "error": msg })))
}

// ── Router ────────────────────────────────────────────────────────────────────

pub async fn serve(state: Arc<AppState>, addr: String, api_key: Option<String>) -> Result<()> {
    use crate::auth::require_api_key;

    let app = Router::new()
        .route("/health",                          get(health))
        .route("/sessions",                        post(create_session))
        .route("/sessions/{id}",                   get(get_session))
        .route("/sessions/{id}/log",               get(get_log))
        .route("/sessions/{id}/messages",          post(send_message))
        .route("/sessions/{id}/stream",            get(stream_session))
        .route("/sessions/{id}/events",            post(session_webhook))
        .route("/functions",                       get(list_functions).post(deploy_function))
        .route("/functions/{name}",                axum::routing::delete(delete_function))
        .route("/functions/{name}/invoke",         post(invoke_function))
        .with_state(state)
        .route_layer(middleware::from_fn_with_state(
            api_key,
            require_api_key,
        ))
        .layer(tower_http::trace::TraceLayer::new_for_http());

    let listener = tokio::net::TcpListener::bind(&addr).await?;
    info!(%addr, "REST API listening");
    axum::serve(listener, app).await?;
    Ok(())
}
