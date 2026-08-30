use thiserror::Error;
use crate::types::{RunId, SeqNum, SessionStatus};

#[derive(Debug, Error)]
pub enum LegionError {
    #[error("store error: {0}")]
    Store(String),

    #[error("tamper-evident chain broken at run={0} seq={1}")]
    TamperEvident(RunId, SeqNum),

    #[error("session not found: {0}")]
    SessionNotFound(RunId),

    #[error("session already exists: {0}")]
    SessionAlreadyExists(RunId),

    #[error("session {run_id} in wrong state: expected {expected:?}, got {actual:?}")]
    SessionWrongState {
        run_id:   RunId,
        expected: SessionStatus,
        actual:   SessionStatus,
    },

    #[error("session {0} has a dangling write-ahead entry; reconcile before resuming")]
    PendingReconciliation(RunId),

    #[error("tool not found: {0}")]
    ToolNotFound(String),

    #[error("tool error: {0}")]
    ToolError(String),

    #[error("tool dispatch error for '{name}': {reason}")]
    ToolDispatch { name: String, reason: String },

    #[error("budget exceeded: {0}")]
    BudgetExceeded(String),

    #[error("LLM error: {0}")]
    LLMError(String),

    #[error("serialization error: {0}")]
    Serialization(#[from] serde_json::Error),
}

pub type Result<T> = std::result::Result<T, LegionError>;
