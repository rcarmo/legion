//! REST API — axum router for session management.
//!
//! Routes:
//!   POST   /sessions              — create a new session
//!   GET    /sessions/:id          — get session status
//!   POST   /sessions/:id/messages — send a user message (triggers resolve)
//!   GET    /sessions/:id/log      — get the full event log
//!   GET    /health                — liveness probe

use std::sync::Arc;

use anyhow::Result;
use axum::{
    extract::{Path, State},
    http::StatusCode,
    response::Json,
    routing::{get, post},
    Router,
};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use tracing::info;
use uuid::Uuid;

use legion_core::{
    traits::EventStore,
    types::{Budget, ExternalEvent, RunConfig},
};
use legion_store::SqliteStore;

// ── Types ─────────────────────────────────────────────────────────────────────

#[derive(Clone)]
struct AppState {
    store: SqliteStore,
}

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
struct SendMessageRequest {
    content: String,
}

#[derive(Debug, Serialize)]
struct SessionResponse {
    id:     String,
    status: String,
}

// ── Handlers ──────────────────────────────────────────────────────────────────

async fn health() -> Json<Value> {
    Json(json!({ "ok": true, "version": env!("CARGO_PKG_VERSION") }))
}

async fn create_session(
    State(state): State<Arc<AppState>>,
    Json(req):    Json<CreateSessionRequest>,
) -> Result<(StatusCode, Json<Value>), (StatusCode, Json<Value>)> {
    let run_id = Uuid::new_v4();
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

    state.store.create_session(run_id, &config)
        .await
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
        "created_at": e.created_at,
    })).collect();

    Ok(Json(json!({ "id": id.to_string(), "entries": entries })))
}

async fn send_message(
    State(state): State<Arc<AppState>>,
    Path(id):     Path<Uuid>,
    Json(req):    Json<SendMessageRequest>,
) -> Result<Json<Value>, (StatusCode, Json<Value>)> {
    use legion_core::types::{TurnEvent, SessionStatus};

    // Append the user message turn
    state.store.append(id, TurnEvent::user_message(req.content.clone()))
        .await
        .map_err(|e| server_error(e.to_string()))?;
    state.store.set_status(id, SessionStatus::Running)
        .await
        .map_err(|e| server_error(e.to_string()))?;

    info!(run_id = %id, "user message appended");

    Ok(Json(json!({
        "id":     id.to_string(),
        "status": "running",
        "note":   "message queued; call GET /sessions/:id for status",
    })))
}

// ── Error helpers ─────────────────────────────────────────────────────────────

fn server_error(msg: String) -> (StatusCode, Json<Value>) {
    (StatusCode::INTERNAL_SERVER_ERROR, Json(json!({ "error": msg })))
}
fn not_found(msg: String) -> (StatusCode, Json<Value>) {
    (StatusCode::NOT_FOUND, Json(json!({ "error": msg })))
}

// ── Router ────────────────────────────────────────────────────────────────────

pub async fn serve(store: SqliteStore, addr: String) -> Result<()> {
    let state = Arc::new(AppState { store });

    let app = Router::new()
        .route("/health",                        get(health))
        .route("/sessions",                      post(create_session))
        .route("/sessions/:id",                  get(get_session))
        .route("/sessions/:id/log",              get(get_log))
        .route("/sessions/:id/messages",         post(send_message))
        .with_state(state)
        .layer(tower_http::trace::TraceLayer::new_for_http());

    let listener = tokio::net::TcpListener::bind(&addr).await?;
    info!(%addr, "REST API listening");
    axum::serve(listener, app).await?;
    Ok(())
}
