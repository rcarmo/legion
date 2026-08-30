//! API key authentication middleware.
//!
//! When LEGION_API_KEY is set in the environment (or `api_key` in legion.toml),
//! all requests must carry `Authorization: Bearer <key>` or `X-Legion-Key: <key>`.
//! The health endpoint is always exempt.

use axum::{
    body::Body,
    extract::Request,
    http::{header, StatusCode},
    middleware::Next,
    response::{Json, Response},
};
use serde_json::json;
use tracing::warn;

/// Axum middleware that validates the API key.
/// Pass the expected key as shared state via `axum::middleware::from_fn_with_state`.
pub async fn require_api_key(
    axum::extract::State(expected): axum::extract::State<Option<String>>,
    req:  Request<Body>,
    next: Next,
) -> Result<Response, (StatusCode, Json<serde_json::Value>)> {
    // Health endpoint is always public
    if req.uri().path() == "/health" {
        return Ok(next.run(req).await);
    }

    // If no key configured, allow all
    let Some(key) = expected else {
        return Ok(next.run(req).await);
    };

    let provided = req.headers()
        .get(header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
        .and_then(|s| s.strip_prefix("Bearer "))
        .map(|s| s.to_string())
        .or_else(|| {
            req.headers()
                .get("x-legion-key")
                .and_then(|v| v.to_str().ok())
                .map(|s| s.to_string())
        });

    match provided {
        Some(ref k) if k == &key => Ok(next.run(req).await),
        Some(_) => {
            warn!("rejected request: invalid API key");
            Err((StatusCode::UNAUTHORIZED, Json(json!({ "error": "invalid API key" }))))
        }
        None => {
            Err((StatusCode::UNAUTHORIZED, Json(json!({ "error": "API key required" }))))
        }
    }
}
