use async_trait::async_trait;
use crate::error::Result;
use crate::types::{
    ExternalEvent, RunConfig, RunId, SeqNum, SessionFilter, SessionStatus, SessionSummary,
    ToolDefinition, TurnEnvelope, TurnEvent,
};

// ── EventStore ────────────────────────────────────────────────────────────────

/// Persistent, ordered, hash-chained event log for agent sessions.
///
/// All writes are strongly consistent (go through the Raft leader when
/// backed by hiqlite). Reads may be eventually consistent unless noted.
#[async_trait]
pub trait EventStore: Send + Sync {
    /// Append an event to a session's log.
    /// Returns the assigned sequence number.
    async fn append(&self, run_id: RunId, event: TurnEvent) -> Result<SeqNum>;

    /// Read the full log for a session, verifying the hash chain.
    /// Returns `LegionError::TamperEvident` if the chain is broken.
    async fn read_log(&self, run_id: RunId) -> Result<Vec<TurnEnvelope>>;

    /// Read the most recent `n` turns (does NOT verify chain; fast path).
    async fn read_recent(&self, run_id: RunId, n: usize) -> Result<Vec<TurnEnvelope>>;

    /// Get the current status of a session (strongly consistent read).
    async fn session_status(&self, run_id: RunId) -> Result<SessionStatus>;

    /// Transition the session to a new status.
    async fn set_status(&self, run_id: RunId, status: SessionStatus) -> Result<()>;

    /// Fork a session at `at_seq`: new session shares history up to `at_seq`.
    async fn fork(&self, run_id: RunId, at_seq: SeqNum) -> Result<RunId>;

    /// List sessions matching a filter.
    async fn list_sessions(&self, filter: SessionFilter) -> Result<Vec<SessionSummary>>;

    /// Create the initial session record. Called before the first `append`.
    async fn create_session(&self, run_id: RunId, config: &RunConfig) -> Result<()>;
}

// ── AgentLoopTrait ────────────────────────────────────────────────────────────

/// A stateless executor for a single agent session.
///
/// All state lives in the `EventStore`. The loop reads history, drives
/// the LLM, dispatches tools, and writes results back — all via the store.
#[async_trait]
pub trait AgentLoopTrait: Send + Sync {
    /// Create a new session from config and return its `RunId`.
    async fn start(&self, config: RunConfig) -> Result<RunId>;

    /// Replay a crashed or interrupted session from the event log.
    /// Resolves any dangling write-ahead entries or returns
    /// `LegionError::PendingReconciliation` if a dangling write cannot be
    /// automatically resolved.
    async fn recover(&self, run_id: RunId) -> Result<()>;

    /// Inject an external event into a parked session and continue running.
    async fn resume(&self, run_id: RunId, event: ExternalEvent) -> Result<()>;

    /// Wait for a running session to complete and return the final assistant turn.
    async fn resolve(&self, run_id: RunId) -> Result<TurnEnvelope>;
}

// ── ToolRegistry ──────────────────────────────────────────────────────────────

/// Registry of tools available to agents.
#[async_trait]
pub trait ToolRegistry: Send + Sync {
    /// Return the definitions of all registered tools.
    fn definitions(&self) -> Vec<ToolDefinition>;

    /// Dispatch a tool call by name with the given JSON arguments.
    async fn dispatch(
        &self,
        name: &str,
        args: serde_json::Value,
    ) -> Result<serde_json::Value>;
}
