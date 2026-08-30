//! SSE-compatible streaming events from a session resolve turn.
//!
//! `SessionEvent` mirrors rs-ai `Event` but is serialisable and carries
//! Legion-specific metadata (run_id, seq).

use serde::{Deserialize, Serialize};

/// An event stream item produced while resolving a session turn.
///
/// These are emitted over SSE as `data: <json>\n\n`.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum SessionEvent {
    /// A text delta from the LLM.
    TextDelta { delta: String },
    /// A thinking delta (if the model supports it).
    ThinkingDelta { delta: String },
    /// A tool call was issued.
    ToolCall { name: String, call_id: String },
    /// A tool result was received.
    ToolResult { call_id: String, output: serde_json::Value },
    /// The LLM turn is complete; full response text and usage stats.
    Done {
        content:    String,
        seq:        u64,
        tokens_in:  u32,
        tokens_out: u32,
        wall_ms:    u64,
    },
    /// An error occurred.
    Error { message: String },
}
