//! In-memory test doubles for `EventStore` and `ToolRegistry`.
//! These are suitable for unit tests; they have no I/O.

use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;
use async_trait::async_trait;
use uuid::Uuid;

use crate::error::{LegionError, Result};
use crate::traits::{EventStore, ToolRegistry};
use crate::types::{
    EffectClass, ExternalEvent, RunConfig, RunId, SeqNum, SessionFilter, SessionStatus,
    SessionSummary, ToolDefinition, TurnEnvelope, TurnEvent,
};

// ── MemoryEventStore ──────────────────────────────────────────────────────────

#[derive(Debug, Clone)]
struct SessionState {
    log:    Vec<TurnEnvelope>,
    status: SessionStatus,
    config: Option<RunConfig>,
}

/// An in-memory `EventStore` with no I/O. Suitable for unit tests.
/// NOT for production — state is lost on drop.
#[derive(Debug, Default, Clone)]
pub struct MemoryEventStore {
    inner: Arc<RwLock<HashMap<RunId, SessionState>>>,
}

impl MemoryEventStore {
    pub fn new() -> Self {
        Self::default()
    }
}

#[async_trait]
impl EventStore for MemoryEventStore {
    async fn create_session(&self, run_id: RunId, config: &RunConfig) -> Result<()> {
        let mut map = self.inner.write().await;
        if map.contains_key(&run_id) {
            return Err(LegionError::SessionAlreadyExists(run_id));
        }
        map.insert(run_id, SessionState {
            log:    vec![],
            status: SessionStatus::Idle,
            config: Some(config.clone()),
        });
        Ok(())
    }

    async fn append(&self, run_id: RunId, event: TurnEvent) -> Result<SeqNum> {
        let mut map = self.inner.write().await;
        let session = map.get_mut(&run_id)
            .ok_or(LegionError::SessionNotFound(run_id))?;

        let seq = session.log.len() as SeqNum;
        let prev_hash = if seq == 0 {
            [0u8; 32]
        } else {
            hash_envelope(session.log.last().unwrap())
        };

        let now = chrono::Utc::now().timestamp_millis();
        session.log.push(TurnEnvelope {
            run_id,
            seq,
            prev_hash,
            event,
            created_at: now,
        });
        Ok(seq)
    }

    async fn read_log(&self, run_id: RunId) -> Result<Vec<TurnEnvelope>> {
        let map = self.inner.read().await;
        let session = map.get(&run_id)
            .ok_or(LegionError::SessionNotFound(run_id))?;
        verify_chain(&session.log, run_id)?;
        Ok(session.log.clone())
    }

    async fn read_recent(&self, run_id: RunId, n: usize) -> Result<Vec<TurnEnvelope>> {
        let map = self.inner.read().await;
        let session = map.get(&run_id)
            .ok_or(LegionError::SessionNotFound(run_id))?;
        let log = &session.log;
        let start = log.len().saturating_sub(n);
        Ok(log[start..].to_vec())
    }

    async fn session_status(&self, run_id: RunId) -> Result<SessionStatus> {
        let map = self.inner.read().await;
        let session = map.get(&run_id)
            .ok_or(LegionError::SessionNotFound(run_id))?;
        Ok(session.status.clone())
    }

    async fn set_status(&self, run_id: RunId, status: SessionStatus) -> Result<()> {
        let mut map = self.inner.write().await;
        let session = map.get_mut(&run_id)
            .ok_or(LegionError::SessionNotFound(run_id))?;
        session.status = status;
        Ok(())
    }

    async fn fork(&self, run_id: RunId, at_seq: SeqNum) -> Result<RunId> {
        let mut map = self.inner.write().await;
        let parent = map.get(&run_id)
            .ok_or(LegionError::SessionNotFound(run_id))?;

        let history: Vec<TurnEnvelope> = parent.log
            .iter()
            .filter(|e| e.seq <= at_seq)
            .cloned()
            .collect();
        let config = parent.config.clone();

        let new_id = Uuid::new_v4();
        map.insert(new_id, SessionState {
            log:    history,
            status: SessionStatus::Idle,
            config,
        });
        Ok(new_id)
    }

    async fn list_sessions(&self, filter: SessionFilter) -> Result<Vec<SessionSummary>> {
        let map = self.inner.read().await;
        let now = chrono::Utc::now().timestamp_millis();
        let mut summaries: Vec<SessionSummary> = map.iter().map(|(id, s)| {
            SessionSummary {
                run_id:     *id,
                status:     s.status.clone(),
                model:      s.config.as_ref().map(|c| c.model.clone()).unwrap_or_default(),
                turns:      s.log.len() as u64,
                created_at: s.log.first().map(|e| e.created_at).unwrap_or(now),
                updated_at: s.log.last().map(|e| e.created_at).unwrap_or(now),
            }
        }).collect();

        if let Some(status) = filter.status {
            let status_str = status.to_lowercase();
            summaries.retain(|s| format!("{:?}", s.status).to_lowercase().contains(&status_str));
        }

        let offset = filter.offset.unwrap_or(0);
        let limit  = filter.limit.unwrap_or(usize::MAX);
        Ok(summaries.into_iter().skip(offset).take(limit).collect())
    }
}

// ── Hash chain helpers ────────────────────────────────────────────────────────

fn hash_envelope(env: &TurnEnvelope) -> [u8; 32] {
    use sha2::{Sha256, Digest};
    // Hash only content fields (not run_id) so forked chains remain valid.
    let content = serde_json::json!({
        "seq":        env.seq,
        "prev_hash":  env.prev_hash,
        "event":      env.event,
        "created_at": env.created_at,
    });
    let json = serde_json::to_vec(&content).unwrap_or_default();
    let mut hasher = Sha256::new();
    hasher.update(&json);
    hasher.finalize().into()
}

fn verify_chain(log: &[TurnEnvelope], run_id: RunId) -> Result<()> {
    for (i, env) in log.iter().enumerate() {
        let expected_prev = if i == 0 {
            [0u8; 32]
        } else {
            hash_envelope(&log[i - 1])
        };
        if env.prev_hash != expected_prev {
            return Err(LegionError::TamperEvident(run_id, env.seq));
        }
    }
    Ok(())
}

// ── EchoToolRegistry ──────────────────────────────────────────────────────────

/// A `ToolRegistry` that echoes its input back as the result.
/// Suitable for testing the agent loop's tool-dispatch path.
#[derive(Debug, Default, Clone)]
pub struct EchoToolRegistry {
    tools: Vec<ToolDefinition>,
}

impl EchoToolRegistry {
    pub fn new() -> Self {
        Self {
            tools: vec![
                ToolDefinition {
                    name:        "echo".into(),
                    description: "Returns its input unchanged.".into(),
                    parameters:  serde_json::json!({
                        "type": "object",
                        "properties": {
                            "message": { "type": "string" }
                        },
                        "required": ["message"]
                    }),
                    effect: EffectClass::Read,
                },
                ToolDefinition {
                    name:        "fail".into(),
                    description: "Always returns a tool dispatch error. Used in error-path tests.".into(),
                    parameters:  serde_json::json!({
                        "type": "object",
                        "properties": {
                            "reason": { "type": "string" }
                        }
                    }),
                    effect: EffectClass::Write,
                },
            ],
        }
    }
}

#[async_trait]
impl ToolRegistry for EchoToolRegistry {
    async fn definitions(&self) -> Vec<ToolDefinition> {
        self.tools.clone()
    }

    async fn dispatch(&self, name: &str, args: serde_json::Value) -> Result<serde_json::Value> {
        match name {
            "echo" => Ok(args),
            "fail" => {
                let reason = args.get("reason")
                    .and_then(|r| r.as_str())
                    .unwrap_or("intentional test failure")
                    .to_string();
                Err(LegionError::ToolDispatch { name: name.into(), reason })
            }
            other => Err(LegionError::ToolNotFound(other.into())),
        }
    }
}

// ── ExternalEvent helper (for tests) ─────────────────────────────────────────

impl ExternalEvent {
    pub fn user_message(s: impl Into<String>) -> Self {
        Self::UserMessage(s.into())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{TurnEvent, RunConfig, Budget, BudgetState};

    fn test_config() -> RunConfig {
        RunConfig {
            system_prompt: Some("You are a test assistant.".into()),
            model:         "faux/test".into(),
            budget:        Budget::default(),
            tools:         vec!["echo".into()],
            metadata:      None,
        }
    }

    #[tokio::test]
    async fn memory_store_append_and_read() {
        let store = MemoryEventStore::new();
        let run_id = Uuid::new_v4();

        store.create_session(run_id, &test_config()).await.unwrap();
        let seq0 = store.append(run_id, TurnEvent::user_message("hello")).await.unwrap();
        let seq1 = store.append(run_id, TurnEvent::model_call_intent()).await.unwrap();

        assert_eq!(seq0, 0);
        assert_eq!(seq1, 1);

        let log = store.read_log(run_id).await.unwrap();
        assert_eq!(log.len(), 2);
        assert_eq!(log[0].seq, 0);
        assert_eq!(log[1].seq, 1);
    }

    #[tokio::test]
    async fn memory_store_hash_chain_valid() {
        let store = MemoryEventStore::new();
        let run_id = Uuid::new_v4();
        store.create_session(run_id, &test_config()).await.unwrap();
        for i in 0..5u32 {
            store.append(run_id, TurnEvent::user_message(format!("msg {i}"))).await.unwrap();
        }
        // read_log verifies the chain; should not error
        store.read_log(run_id).await.unwrap();
    }

    #[tokio::test]
    async fn memory_store_session_status_transitions() {
        let store = MemoryEventStore::new();
        let run_id = Uuid::new_v4();
        store.create_session(run_id, &test_config()).await.unwrap();
        assert_eq!(store.session_status(run_id).await.unwrap(), SessionStatus::Idle);

        store.set_status(run_id, SessionStatus::Running).await.unwrap();
        assert_eq!(store.session_status(run_id).await.unwrap(), SessionStatus::Running);

        store.set_status(run_id, SessionStatus::Completed).await.unwrap();
        assert!(store.session_status(run_id).await.unwrap().is_terminal());
    }

    #[tokio::test]
    async fn memory_store_fork() {
        let store = MemoryEventStore::new();
        let run_id = Uuid::new_v4();
        store.create_session(run_id, &test_config()).await.unwrap();
        store.append(run_id, TurnEvent::user_message("a")).await.unwrap();
        store.append(run_id, TurnEvent::user_message("b")).await.unwrap();
        store.append(run_id, TurnEvent::user_message("c")).await.unwrap();

        let fork_id = store.fork(run_id, 1).await.unwrap();
        let fork_log = store.read_log(fork_id).await.unwrap();
        assert_eq!(fork_log.len(), 2); // seq 0 and 1
    }

    #[tokio::test]
    async fn echo_tool_dispatch() {
        let reg = EchoToolRegistry::new();
        let result = reg.dispatch("echo", serde_json::json!({"message": "hi"})).await.unwrap();
        assert_eq!(result["message"], "hi");
    }

    #[tokio::test]
    async fn echo_tool_fail_dispatch() {
        let reg = EchoToolRegistry::new();
        let err = reg.dispatch("fail", serde_json::json!({"reason": "test"})).await;
        assert!(err.is_err());
    }

    #[tokio::test]
    async fn budget_state_exceeded() {
        let budget = Budget { max_turns: Some(3), ..Default::default() };
        let mut state = BudgetState::default();
        assert!(state.exceeded_by(&budget).is_none());
        state.turns = 3;
        assert_eq!(state.exceeded_by(&budget).as_deref(), Some("max_turns"));

        let tool_budget = Budget { max_tool_calls: Some(2), ..Default::default() };
        state = BudgetState::default();
        state.tool_calls = 2;
        assert_eq!(state.exceeded_by(&tool_budget).as_deref(), Some("max_tool_calls"));
    }
}
