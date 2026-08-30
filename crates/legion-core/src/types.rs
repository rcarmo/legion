use serde::{Deserialize, Serialize};
use uuid::Uuid;

// ── Identity ─────────────────────────────────────────────────────────────────

/// Unique identifier for an agent session / run.
pub type RunId = Uuid;

/// Monotonic sequence number within a single session.
pub type SeqNum = u64;

// ── Budget ────────────────────────────────────────────────────────────────────

/// Hard limits enforced by the agent loop before each LLM call and after each
/// tool result. Any exceeded limit transitions the session to `BudgetHalt`.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct Budget {
    pub max_turns:      Option<u32>,
    pub max_tool_calls: Option<u32>,
    pub max_tokens_in:  Option<u64>,
    pub max_tokens_out: Option<u64>,
    pub max_wall_ms:    Option<u64>,
    pub max_cost_usd:   Option<f64>,
}

/// Accumulated spend tracked against the `Budget`.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct BudgetState {
    pub turns:      u32,
    pub tool_calls: u32,
    pub tokens_in:  u64,
    pub tokens_out: u64,
    pub wall_ms:    u64,
    pub cost_usd:   f64,
}

impl BudgetState {
    /// Check whether any budget limit has been exceeded.
    /// Returns the name of the first exceeded field, or `None`.
    pub fn exceeded_by(&self, budget: &Budget) -> Option<String> {
        if budget.max_turns.is_some_and(|m| self.turns >= m) {
            return Some("max_turns".into());
        }
        if budget.max_tool_calls.is_some_and(|m| self.tool_calls >= m) {
            return Some("max_tool_calls".into());
        }
        if budget.max_tokens_in.is_some_and(|m| self.tokens_in >= m) {
            return Some("max_tokens_in".into());
        }
        if budget.max_tokens_out.is_some_and(|m| self.tokens_out >= m) {
            return Some("max_tokens_out".into());
        }
        if budget.max_wall_ms.is_some_and(|m| self.wall_ms >= m) {
            return Some("max_wall_ms".into());
        }
        if budget.max_cost_usd.is_some_and(|m| self.cost_usd >= m) {
            return Some("max_cost_usd".into());
        }
        None
    }
}

// ── Run configuration ─────────────────────────────────────────────────────────

/// Configuration for a new agent session.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RunConfig {
    /// System prompt injected before the conversation.
    pub system_prompt: Option<String>,
    /// Model identifier in `provider/id` format (e.g. `anthropic/claude-opus-4-5`).
    pub model: String,
    /// Hard resource limits.
    #[serde(default)]
    pub budget: Budget,
    /// Tool names to enable. Empty = no tools.
    #[serde(default)]
    pub tools: Vec<String>,
    /// Arbitrary caller metadata stored with the session.
    #[serde(default)]
    pub metadata: Option<serde_json::Value>,
}

// ── Effect classification ─────────────────────────────────────────────────────

/// How a tool call should be treated during replay.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum EffectClass {
    /// Side-effect free; result is read from the log on replay.
    Read,
    /// Safe to re-execute; produces the same result.
    Idempotent,
    /// Non-idempotent side effect; write-ahead logged; dangling entry blocks resume.
    Write,
    /// LLM call; not idempotent; re-issued if in-flight at crash time.
    LlmCall,
}

// ── Tool definitions ──────────────────────────────────────────────────────────

/// Descriptor for a single tool exposed to the agent.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ToolDefinition {
    pub name:        String,
    pub description: String,
    /// JSON Schema for the tool's input parameters.
    pub parameters:  serde_json::Value,
    pub effect:      EffectClass,
}

// ── Park / external events ────────────────────────────────────────────────────

/// Why a session is waiting for an external signal.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ParkReason {
    AwaitingUserInput,
    AwaitingApproval   { description: String },
    AwaitingExternalEvent { event_name: String },
}

/// Signal that can resume a parked session.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ExternalEvent {
    UserMessage(String),
    ApprovalGranted,
    ApprovalDenied,
    ExternalTrigger { name: String, payload: serde_json::Value },
}

// ── Session status ────────────────────────────────────────────────────────────

/// Lifecycle state of an agent session.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "status", rename_all = "snake_case")]
pub enum SessionStatus {
    Idle,
    Running,
    ToolPending,
    Parked         { reason: ParkReason },
    Resuming,
    Completed,
    BudgetHalt     { budget_field: String },
    /// A write-ahead intent was recorded but no result arrived (crash mid-write).
    /// The session cannot resume until the dangling entry is resolved.
    PendingReconciliation { tool_name: String, call_id: String },
    Aborted,
}

impl SessionStatus {
    pub fn is_terminal(&self) -> bool {
        matches!(self, Self::Completed | Self::BudgetHalt { .. } | Self::Aborted)
    }
}

// ── Turn phase (internal loop state machine) ──────────────────────────────────

/// Phase within a single turn of the agent loop.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TurnPhase {
    Setup,
    Running,
    Tools,
    Finalizing,
    Completed,
    Aborted,
}

// ── Turn events ───────────────────────────────────────────────────────────────

/// What kind of event is being appended.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum TurnEventKind {
    // External input
    UserMessage,
    // Write-ahead intents
    ModelCallIntent,
    ToolCallIntent   { tool_name: String, call_id: String, effect: EffectClass },
    // Completions
    AssistantMessage,
    ToolResult       { call_id: String },
    ToolCallReconciled { call_id: String, action: String },
    // Session lifecycle
    SessionStarted,
    SessionForked    { parent_run_id: RunId, at_seq: SeqNum },
    SessionParked    { reason: ParkReason },
    SessionResumed,
    SessionCompleted,
    SessionBudgetHalt     { budget_field: String },
    SessionPendingReconciliation { tool_name: String, call_id: String },
    SessionAborted,
}

/// A turn event ready to be appended to the store.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TurnEvent {
    pub kind:        TurnEventKind,
    /// Inline content for small payloads (< 8 KB).
    pub payload:     Option<serde_json::Value>,
    /// iroh-blobs CID for large payloads; NULL if inline.
    pub payload_cid: Option<String>,
    /// Model identifier (for LLM turns).
    pub model:       Option<String>,
    pub tokens_in:   Option<u32>,
    pub tokens_out:  Option<u32>,
    pub wall_ms:     Option<u64>,
}

impl TurnEvent {
    pub fn user_message(content: impl Into<String>) -> Self {
        Self {
            kind:        TurnEventKind::UserMessage,
            payload:     Some(serde_json::json!({ "content": content.into() })),
            payload_cid: None,
            model:       None,
            tokens_in:   None,
            tokens_out:  None,
            wall_ms:     None,
        }
    }

    pub fn model_call_intent() -> Self {
        Self {
            kind:        TurnEventKind::ModelCallIntent,
            payload:     None,
            payload_cid: None,
            model:       None,
            tokens_in:   None,
            tokens_out:  None,
            wall_ms:     None,
        }
    }

    pub fn tool_call_intent(
        tool_name: impl Into<String>,
        call_id: impl Into<String>,
        effect: EffectClass,
        arguments: serde_json::Value,
    ) -> Self {
        Self {
            kind: TurnEventKind::ToolCallIntent {
                tool_name: tool_name.into(),
                call_id:   call_id.into(),
                effect,
            },
            payload:     Some(serde_json::json!({ "arguments": arguments })),
            payload_cid: None,
            model:       None,
            tokens_in:   None,
            tokens_out:  None,
            wall_ms:     None,
        }
    }

    pub fn tool_result(call_id: impl Into<String>, result: serde_json::Value) -> Self {
        Self {
            kind:        TurnEventKind::ToolResult { call_id: call_id.into() },
            payload:     Some(result),
            payload_cid: None,
            model:       None,
            tokens_in:   None,
            tokens_out:  None,
            wall_ms:     None,
        }
    }

    pub fn session_budget_halt(field: impl Into<String>) -> Self {
        let field = field.into();
        Self {
            kind: TurnEventKind::SessionBudgetHalt { budget_field: field.clone() },
            payload: Some(serde_json::json!({ "budget_field": field })),
            payload_cid: None,
            model: None,
            tokens_in: None,
            tokens_out: None,
            wall_ms: None,
        }
    }

    pub fn tool_call_reconciled(call_id: impl Into<String>, action: impl Into<String>) -> Self {
        let call_id = call_id.into();
        let action = action.into();
        Self {
            kind: TurnEventKind::ToolCallReconciled {
                call_id: call_id.clone(),
                action: action.clone(),
            },
            payload: Some(serde_json::json!({ "call_id": call_id, "action": action })),
            payload_cid: None,
            model: None,
            tokens_in: None,
            tokens_out: None,
            wall_ms: None,
        }
    }

    pub fn assistant_message(content: serde_json::Value, model: impl Into<String>, tokens_in: u32, tokens_out: u32, wall_ms: u64) -> Self {
        Self {
            kind:        TurnEventKind::AssistantMessage,
            payload:     Some(content),
            payload_cid: None,
            model:       Some(model.into()),
            tokens_in:   Some(tokens_in),
            tokens_out:  Some(tokens_out),
            wall_ms:     Some(wall_ms),
        }
    }

    pub fn session_started(config: &RunConfig) -> Self {
        Self {
            kind:        TurnEventKind::SessionStarted,
            payload:     Some(serde_json::to_value(config).unwrap_or_default()),
            payload_cid: None,
            model:       None,
            tokens_in:   None,
            tokens_out:  None,
            wall_ms:     None,
        }
    }
}

/// A stored turn envelope: event + positional metadata + hash chain link.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TurnEnvelope {
    pub run_id:     RunId,
    pub seq:        SeqNum,
    /// SHA-256 of the CBOR/JSON encoding of the previous TurnEnvelope.
    /// All zeros for seq == 0.
    pub prev_hash:  [u8; 32],
    pub event:      TurnEvent,
    pub created_at: i64, // Unix timestamp ms
}

// ── Session summary ───────────────────────────────────────────────────────────

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SessionSummary {
    pub run_id:     RunId,
    pub status:     SessionStatus,
    pub model:      String,
    pub turns:      u64,
    pub created_at: i64,
    pub updated_at: i64,
}

/// Filter for listing sessions.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct SessionFilter {
    pub status: Option<String>,
    pub limit:  Option<usize>,
    pub offset: Option<usize>,
}
