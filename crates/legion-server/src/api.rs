//! REST API — axum router for session management.
//!
//! Routes:
//!   POST   /sessions               — create a new session
//!   GET    /sessions/{id}          — get session status
//!   POST   /sessions/{id}/messages — send a user message + run one resolve turn
//!   GET    /sessions/{id}/log      — get the full event log
//!   GET    /sessions/{id}/stream   — stream one resolve turn via SSE
//!   GET    /health                 — liveness probe

use std::{collections::BTreeMap, sync::Arc};

use anyhow::Result;
use axum::{
    extract::{Path, State},
    http::{header, HeaderValue, StatusCode},
    middleware,
    response::{IntoResponse, Json, Sse, sse::Event as SseEvent},
    routing::{get, post},
    Router,
};
use serde::Deserialize;
use serde_json::{json, Value};
use tracing::{info, warn};
use uuid::Uuid;

use legion_core::traits::{AgentLoopTrait, EventStore};
use legion_core::types::{Budget, ExternalEvent, RunConfig, SessionFilter};
use legion_deploy::{DeployJob, DeployPipeline};
use legion_loop::driver::{LegionLoop, ReconcileAction};
use legion_namespace::Namespace;
use legion_runtime::{
    invoke::{InvokeRequest, Invoker},
    manifest::FunctionRuntime,
    InvocationMetrics,
};

use crate::rate_limit::SessionRateLimiter;

// ── AppState ──────────────────────────────────────────────────────────────────

#[derive(Clone)]
pub struct AppState {
    pub store:     Arc<dyn EventStore>,
    pub lp:        Arc<LegionLoop>,
    pub deployer:  Arc<DeployPipeline>,
    pub namespace:   Namespace,
    pub invoker_bun:  Arc<dyn Invoker>,
    #[cfg(feature = "wasm")]
    pub invoker_wasm: Arc<dyn Invoker>,
    pub invocation_metrics: Arc<InvocationMetrics>,
    pub session_rate_limiter: Arc<SessionRateLimiter>,
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
    max_tool_calls: Option<u32>,
    max_tokens_in:  Option<u64>,
    max_tokens_out: Option<u64>,
    max_wall_ms:    Option<u64>,
}

#[derive(Debug, Deserialize)]
struct DeployRequest {
    name:        String,
    runtime:     Option<String>,
    description: Option<String>,
    /// Inline source code (Bun/JS).
    code:        Option<String>,
    /// Base64-encoded WASM module bytes.
    wasm_b64:    Option<String>,
    idempotent:  Option<bool>,
    parameters:  Option<serde_json::Value>,
}

#[derive(Debug, Deserialize)]
struct RegisterRequest {
    name:        String,
    artifact_cid: String,
    runtime:     Option<String>,
    description: Option<String>,
    idempotent:  Option<bool>,
    parameters:  Option<serde_json::Value>,
}

#[derive(Debug, Deserialize)]
struct RouteRequest {
    name: String,
    artifact_cid: String,
    #[serde(default = "full_weight")]
    weight: u16,
}

#[derive(Debug, Deserialize)]
struct PromoteRequest {
    name: String,
    artifact_cid: String,
}

fn full_weight() -> u16 { 10_000 }

#[derive(Debug, Deserialize)]
struct SendMessageRequest {
    content: String,
}

#[derive(Debug, Deserialize)]
struct ReconcileRequest {
    action: String,
}

// ── Handlers ──────────────────────────────────────────────────────────────────

async fn health() -> Json<Value> {
    Json(json!({ "ok": true, "version": env!("CARGO_PKG_VERSION") }))
}

async fn metrics(
    State(state): State<Arc<AppState>>,
) -> Result<String, (StatusCode, Json<Value>)> {
    let mut output = state.invocation_metrics.render_prometheus();
    let mut by_model: BTreeMap<String, (u64, u64, u64, u64)> = BTreeMap::new();
    let mut offset = 0;

    loop {
        let sessions = state.store.list_sessions(SessionFilter {
            status: None,
            limit: Some(100),
            offset: Some(offset),
        }).await.map_err(|e| server_error(e.to_string()))?;
        if sessions.is_empty() { break; }
        offset += sessions.len();

        for session in sessions {
            let metric = by_model.entry(session.model).or_default();
            for entry in state.store.read_log(session.run_id).await
                .map_err(|e| server_error(e.to_string()))?
            {
                if matches!(entry.event.kind, legion_core::types::TurnEventKind::AssistantMessage) {
                    metric.0 += 1;
                    metric.1 += entry.event.tokens_in.unwrap_or(0) as u64;
                    metric.2 += entry.event.tokens_out.unwrap_or(0) as u64;
                    metric.3 += entry.event.wall_ms.unwrap_or(0);
                }
            }
        }
    }

    output.push_str("# HELP legion_session_turns_total Completed agent turns.\n# TYPE legion_session_turns_total counter\n");
    for (model, (turns, _, _, _)) in &by_model {
        output.push_str(&format!("legion_session_turns_total{{model=\"{}\"}} {turns}\n", prometheus_label(model)));
    }
    output.push_str("# HELP legion_session_tokens_total Agent tokens by direction.\n# TYPE legion_session_tokens_total counter\n");
    for (model, (_, tokens_in, tokens_out, _)) in &by_model {
        let model = prometheus_label(model);
        output.push_str(&format!("legion_session_tokens_total{{model=\"{model}\",direction=\"input\"}} {tokens_in}\n"));
        output.push_str(&format!("legion_session_tokens_total{{model=\"{model}\",direction=\"output\"}} {tokens_out}\n"));
    }
    output.push_str("# HELP legion_session_turn_wall_ms_total Total agent turn wall time.\n# TYPE legion_session_turn_wall_ms_total counter\n");
    for (model, (_, _, _, wall_ms)) in &by_model {
        output.push_str(&format!("legion_session_turn_wall_ms_total{{model=\"{}\"}} {wall_ms}\n", prometheus_label(model)));
    }
    output.push_str("# HELP legion_session_rate_limit_rejections_total Rejected session execution requests.\n# TYPE legion_session_rate_limit_rejections_total counter\n");
    output.push_str(&format!(
        "legion_session_rate_limit_rejections_total {}\n",
        state.session_rate_limiter.rejections(),
    ));
    Ok(output)
}

fn prometheus_label(value: &str) -> String {
    value.replace('\\', "\\\\").replace('"', "\\\"").replace('\n', "\\n")
}

async fn list_sessions(
    State(state): State<Arc<AppState>>,
    axum::extract::Query(filter): axum::extract::Query<SessionFilter>,
) -> Result<Json<Value>, (StatusCode, Json<Value>)> {
    let sessions = state.store.list_sessions(filter).await
        .map_err(|e| server_error(e.to_string()))?;
    Ok(Json(json!({ "sessions": sessions })))
}

async fn cluster_peers(
    State(state): State<Arc<AppState>>,
) -> Json<Value> {
    let self_node = state.namespace.get("/cluster/self").await.and_then(|node| {
        if let legion_namespace::NodeKind::Json(value) = node.kind { Some(value) } else { None }
    });
    let mut peers = Vec::new();
    for name in state.namespace.ls("/cluster/peers").await {
        if let Some(node) = state.namespace.get(&format!("/cluster/peers/{name}")).await {
            if let legion_namespace::NodeKind::Json(value) = node.kind {
                peers.push(value);
            }
        }
    }
    Json(json!({ "self": self_node, "peers": peers }))
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
            max_tool_calls: b.max_tool_calls,
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
) -> Result<Json<Value>, axum::response::Response> {
    check_session_rate(&state, id).await?;
    // Inject the user message and run one resolve turn
    state.lp.resume(id, ExternalEvent::user_message(req.content.clone()))
        .await
        .map_err(|e| server_error(e.to_string()).into_response())?;

    let envelope = state.lp.resolve(id)
        .await
        .map_err(|e| {
            warn!(run_id = %id, err = %e, "resolve error");
            server_error(e.to_string()).into_response()
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
        runtime.clone(),
        req.description.unwrap_or_default(),
        req.code.unwrap_or_default(),
    );
    if runtime == FunctionRuntime::Wasm {
        if let Some(encoded) = req.wasm_b64 {
            job.wasm_bytes = Some(base64_decode(&encoded).map_err(|e| {
                (StatusCode::BAD_REQUEST, Json(json!({
                    "error": format!("invalid wasm_b64: {e}")
                })))
            })?);
        } else {
            return Err((StatusCode::BAD_REQUEST, Json(json!({
                "error": "wasm runtime requires wasm_b64"
            }))));
        }
    }
    if let Some(p) = req.parameters { job.parameters = p; }
    if let Some(i) = req.idempotent  { job.idempotent = i; }

    let outcome = state.deployer.deploy(job).await;
    if outcome.status == legion_deploy::DeployStatus::Success {
        Ok((StatusCode::CREATED, Json(serde_json::to_value(&outcome).unwrap())))
    } else {
        Err((StatusCode::UNPROCESSABLE_ENTITY, Json(serde_json::to_value(&outcome).unwrap())))
    }
}

async fn register_function(
    State(state): State<Arc<AppState>>,
    Json(req): Json<RegisterRequest>,
) -> Result<(StatusCode, Json<Value>), (StatusCode, Json<Value>)> {
    let runtime = match req.runtime.as_deref().unwrap_or("bun") {
        "wasm" => FunctionRuntime::Wasm,
        "bun" => FunctionRuntime::Bun,
        other => return Err((StatusCode::BAD_REQUEST, Json(json!({
            "error": format!("unsupported runtime: {other}")
        })))),
    };
    let mut job = DeployJob::new(
        req.name,
        runtime,
        req.description.unwrap_or_default(),
        "",
    );
    if let Some(parameters) = req.parameters { job.parameters = parameters; }
    if let Some(idempotent) = req.idempotent { job.idempotent = idempotent; }
    let outcome = state.deployer.register(job, &req.artifact_cid).await;
    if outcome.status == legion_deploy::DeployStatus::Success {
        Ok((StatusCode::CREATED, Json(serde_json::to_value(&outcome).unwrap())))
    } else {
        Err((StatusCode::UNPROCESSABLE_ENTITY, Json(serde_json::to_value(&outcome).unwrap())))
    }
}

async fn route_function(
    State(state): State<Arc<AppState>>,
    Json(req): Json<RouteRequest>,
) -> Result<Json<Value>, (StatusCode, Json<Value>)> {
    if req.weight > 10_000 {
        return Err((StatusCode::BAD_REQUEST, Json(json!({ "error": "weight must be 0..=10000" }))));
    }
    let value = json!({
        "name": req.name,
        "artifact_cid": req.artifact_cid,
        "weight": req.weight,
        "updated_at": chrono::Utc::now().timestamp_millis(),
    });
    state.namespace.set_json(&format!("/deploy/routes/{}", req.name), value.clone()).await;
    Ok(Json(value))
}

async fn promote_function(
    State(state): State<Arc<AppState>>,
    Json(req): Json<PromoteRequest>,
) -> Result<Json<Value>, (StatusCode, Json<Value>)> {
    let path = format!("/fn/{}/manifest.json", req.name);
    let Some(node) = state.namespace.get(&path).await else {
        return Err(not_found(format!("function not found: {}", req.name)));
    };
    let legion_namespace::NodeKind::Json(mut manifest) = node.kind else {
        return Err(server_error("function manifest is not JSON".into()));
    };
    manifest["artifact_cid"] = Value::String(req.artifact_cid.clone());
    state.namespace.set_json(&path, manifest).await;
    let value = json!({
        "name": req.name,
        "artifact_cid": req.artifact_cid,
        "weight": 10_000,
        "promoted_at": chrono::Utc::now().timestamp_millis(),
    });
    state.namespace.set_json(&format!("/deploy/routes/{}", req.name), value.clone()).await;
    Ok(Json(value))
}

fn base64_decode(input: &str) -> anyhow::Result<Vec<u8>> {
    use base64::Engine;

    base64::engine::general_purpose::STANDARD
        .decode(input)
        .or_else(|_| base64::engine::general_purpose::URL_SAFE.decode(input))
        .map_err(|e| anyhow::anyhow!("base64: {e}"))
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

/// GET /sessions/{id}/stream — SSE stream of a single resolve turn.
/// Client sends the user message as a query param `?message=...` or in headers.
async fn stream_session(
    State(state): State<Arc<AppState>>,
    Path(id):     Path<Uuid>,
    axum::extract::Query(params): axum::extract::Query<std::collections::HashMap<String, String>>,
) -> Result<Sse<impl futures::Stream<Item = Result<SseEvent, std::convert::Infallible>>>, axum::response::Response> {
    use futures::stream;
    use std::convert::Infallible;

    check_session_rate(&state, id).await?;
    let message = params.get("message").cloned().unwrap_or_default();
    let rx = state.lp.clone().stream_resolve(id, message);

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

    Ok(Sse::new(sse_stream)
        .keep_alive(axum::response::sse::KeepAlive::default()))
}

async fn reconcile_session(
    State(state): State<Arc<AppState>>,
    Path(id): Path<Uuid>,
    Json(request): Json<ReconcileRequest>,
) -> Result<Json<Value>, (StatusCode, Json<Value>)> {
    let action = match request.action.as_str() {
        "skip" => ReconcileAction::Skip,
        "retry" => ReconcileAction::Retry,
        _ => return Err((StatusCode::BAD_REQUEST, Json(json!({
            "error": "action must be 'skip' or 'retry'"
        })))),
    };
    state.lp.reconcile(id, action).await
        .map_err(|e| (StatusCode::CONFLICT, Json(json!({ "error": e.to_string() }))))?;
    Ok(Json(json!({
        "id": id,
        "action": request.action,
        "status": "idle"
    })))
}

async fn invoke_function(
    State(state): State<Arc<AppState>>,
    Path(name):   Path<String>,
    Json(args):   Json<Value>,
) -> Result<Json<Value>, axum::response::Response> {
    let manifest_path = format!("/fn/{name}/manifest.json");
    let manifest = state.namespace.get(&manifest_path).await
        .ok_or_else(|| not_found(format!("function not found: {name}")).into_response())?;
    let runtime = if let legion_namespace::NodeKind::Json(ref value) = manifest.kind {
        value.get("runtime")
            .and_then(|v| serde_json::from_value(v.clone()).ok())
            .unwrap_or(FunctionRuntime::Bun)
    } else {
        FunctionRuntime::Bun
    };

    let request = InvokeRequest {
        function_name: name.clone(),
        call_id:       uuid::Uuid::new_v4().to_string(),
        args,
    };
    let result = match runtime {
        #[cfg(feature = "wasm")]
        FunctionRuntime::Wasm => state.invoker_wasm.invoke(request).await
            .map_err(invocation_error)?,
        #[cfg(not(feature = "wasm"))]
        FunctionRuntime::Wasm => {
            return Err((StatusCode::NOT_IMPLEMENTED, Json(json!({
                "error": "server was built without WASM runtime support"
            }))).into_response());
        }
        FunctionRuntime::Bun => state.invoker_bun.invoke(request).await
            .map_err(invocation_error)?,
    };

    if let Some(err) = result.error {
        return Err((StatusCode::UNPROCESSABLE_ENTITY, Json(json!({
            "function": name,
            "error":    err,
            "wall_ms":  result.wall_ms,
        }))).into_response());
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
) -> Result<Json<Value>, axum::response::Response> {
    use legion_core::traits::AgentLoopTrait;

    check_session_rate(&state, id).await?;
    let trigger_name = body.get("trigger")
        .and_then(|v| v.as_str())
        .unwrap_or("webhook")
        .to_string();
    let payload = body.get("payload").cloned().unwrap_or(Value::Null);

    state.lp.resume(id, ExternalEvent::ExternalTrigger {
        name:    trigger_name.clone(),
        payload: payload.clone(),
    }).await.map_err(|e| server_error(e.to_string()).into_response())?;

    info!(run_id = %id, trigger = %trigger_name, "external event injected");

    Ok(Json(json!({
        "id":      id.to_string(),
        "trigger": trigger_name,
        "status":  "resuming",
    })))
}

async fn check_session_rate(state: &AppState, run_id: Uuid) -> Result<(), axum::response::Response> {
    state.store.session_status(run_id).await
        .map_err(|e| not_found(e.to_string()).into_response())?;
    let retry_after_ms = match state.session_rate_limiter.check(run_id) {
        Ok(()) => return Ok(()),
        Err(retry_after_ms) => retry_after_ms,
    };
    let mut response = (
        StatusCode::TOO_MANY_REQUESTS,
        Json(json!({ "error": "session rate limit exceeded", "retry_after_ms": retry_after_ms })),
    ).into_response();
    let retry_after_secs = retry_after_ms.div_ceil(1000).max(1);
    if let Ok(value) = HeaderValue::from_str(&retry_after_secs.to_string()) {
        response.headers_mut().insert(header::RETRY_AFTER, value);
    }
    Err(response)
}

fn invocation_error(error: legion_core::error::LegionError) -> axum::response::Response {
    let (status, retry_after_ms) = match &error {
        legion_core::error::LegionError::InvocationLimitExceeded { .. } =>
            (StatusCode::PAYLOAD_TOO_LARGE, None),
        legion_core::error::LegionError::InvocationRateLimited { retry_after_ms, .. } =>
            (StatusCode::TOO_MANY_REQUESTS, Some(*retry_after_ms)),
        legion_core::error::LegionError::InvocationBusy(_) =>
            (StatusCode::TOO_MANY_REQUESTS, None),
        legion_core::error::LegionError::InvocationTimeout { .. } =>
            (StatusCode::GATEWAY_TIMEOUT, None),
        _ => (StatusCode::INTERNAL_SERVER_ERROR, None),
    };
    let mut response = (status, Json(json!({
        "error": error.to_string(),
        "retry_after_ms": retry_after_ms,
    }))).into_response();
    if let Some(ms) = retry_after_ms {
        if let Ok(value) = HeaderValue::from_str(&ms.div_ceil(1000).max(1).to_string()) {
            response.headers_mut().insert(header::RETRY_AFTER, value);
        }
    }
    response
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
        .route("/metrics",                         get(metrics))
        .route("/cluster/peers",                   get(cluster_peers))
        .route("/sessions",                        get(list_sessions).post(create_session))
        .route("/sessions/{id}",                   get(get_session))
        .route("/sessions/{id}/log",               get(get_log))
        .route("/sessions/{id}/messages",          post(send_message))
        .route("/sessions/{id}/stream",            get(stream_session))
        .route("/sessions/{id}/events",            post(session_webhook))
        .route("/sessions/{id}/reconcile",         post(reconcile_session))
        .route("/functions",                       get(list_functions).post(deploy_function))
        .route("/deploy/register",                  post(register_function))
        .route("/deploy/route",                     post(route_function))
        .route("/deploy/promote",                   post(promote_function))
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
