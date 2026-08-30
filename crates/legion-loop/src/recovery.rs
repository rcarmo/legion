//! Crash-recovery logic: replay a committed log and determine where to resume.

use legion_core::{
    error::Result,
    traits::EventStore,
    types::{RunId, SessionStatus, TurnEnvelope, TurnEventKind},
};

/// Outcome of replaying a session log.
#[derive(Debug)]
pub enum RecoveryOutcome {
    /// Session was already complete; nothing to do.
    AlreadyComplete,
    /// Session had no prior LLM call; start fresh.
    StartFresh,
    /// Session crashed mid-LLM call; re-issue from last committed user message.
    RetryLLMCall,
    /// Session crashed mid-tool call with a dangling write-ahead entry.
    /// The caller must reconcile before resuming.
    DanglingToolWrite { tool_name: String, call_id: String },
    /// Session is parked; wait for an external event.
    Parked,
}

/// Replay the committed log for `run_id` and determine the recovery action.
pub async fn recover_session(
    store: &dyn EventStore,
    run_id: RunId,
) -> Result<RecoveryOutcome> {
    let status = store.session_status(run_id).await?;

    // Already in a terminal state — nothing to do
    if status.is_terminal() {
        return Ok(RecoveryOutcome::AlreadyComplete);
    }

    if let SessionStatus::Parked { .. } = status {
        return Ok(RecoveryOutcome::Parked);
    }

    // Load and verify the full log
    let log = store.read_log(run_id).await?;

    // Scan for dangling write-ahead entries
    for (i, env) in log.iter().enumerate() {
        if let TurnEventKind::ToolCallIntent { tool_name, call_id, .. } = &env.event.kind {
            // Check if a ToolResult for this call_id follows
            let has_result = log[i + 1..].iter().any(|e| {
                matches!(&e.event.kind, TurnEventKind::ToolResult { call_id: rid } if rid == call_id)
            });
            if !has_result {
                // Dangling write-ahead intent — need human reconciliation
                store.set_status(
                    run_id,
                    SessionStatus::PendingReconciliation {
                        tool_name: tool_name.clone(),
                        call_id:   call_id.clone(),
                    },
                ).await?;
                return Ok(RecoveryOutcome::DanglingToolWrite {
                    tool_name: tool_name.clone(),
                    call_id:   call_id.clone(),
                });
            }
        }
    }

    // Check for a dangling model call intent (crashed between intent and Done)
    let last_model_intent = last_model_intent_seq(&log);
    let last_assistant_seq = last_event_seq(&log, |k| matches!(k, TurnEventKind::AssistantMessage));

    match (last_model_intent, last_assistant_seq) {
        (Some(intent_seq), Some(assistant_seq)) if intent_seq > assistant_seq => {
            // ModelCallIntent was logged but no AssistantMessage followed → retry
            Ok(RecoveryOutcome::RetryLLMCall)
        }
        (Some(_), None) => Ok(RecoveryOutcome::RetryLLMCall),
        (None, _) => Ok(RecoveryOutcome::StartFresh),
        _ => Ok(RecoveryOutcome::StartFresh),
    }
}

fn last_model_intent_seq(log: &[TurnEnvelope]) -> Option<u64> {
    log.iter().rev().find_map(|e| {
        if matches!(e.event.kind, TurnEventKind::ModelCallIntent) {
            Some(e.seq)
        } else {
            None
        }
    })
}

fn last_event_seq<F>(log: &[TurnEnvelope], pred: F) -> Option<u64>
where
    F: Fn(&TurnEventKind) -> bool,
{
    log.iter().rev().find_map(|e| {
        if pred(&e.event.kind) { Some(e.seq) } else { None }
    })
}
