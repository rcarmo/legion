//! The main agent loop driver.
//!
//! `LegionLoop` implements `AgentLoopTrait`. It drives one agent session
//! using rs-ai's `EventStream` and writes every step to the `EventStore`.

use std::sync::Arc;
use std::time::Instant;
use async_trait::async_trait;
use futures::StreamExt;
use tracing::{debug, info, warn};
use uuid::Uuid;

use rs_ai::{
    events::Event,
    registry,
    types::{Context, StreamOptions, Tool},
};

use legion_core::{
    error::{LegionError, Result},
    traits::{AgentLoopTrait, EventStore, ToolRegistry},
    types::{
        BudgetState, EffectClass, ExternalEvent, RunConfig, RunId, SessionStatus,
        TurnEnvelope, TurnEvent, TurnEventKind,
    },
};

use crate::context::build_messages;
use crate::recovery::{recover_session, RecoveryOutcome};

use crate::stream::SessionEvent;

// ── LegionLoop ────────────────────────────────────────────────────────────────

/// Stateless agent loop that drives sessions stored in an `EventStore`.
pub struct LegionLoop {
    store:   Arc<dyn EventStore>,
    tools:   Arc<dyn ToolRegistry>,
    /// Maximum turns to read for the context window.
    context_window: usize,
}

impl LegionLoop {
    pub fn new(
        store:   Arc<dyn EventStore>,
        tools:   Arc<dyn ToolRegistry>,
    ) -> Self {
        Self { store, tools, context_window: 40 }
    }

    pub fn with_context_window(mut self, n: usize) -> Self {
        self.context_window = n;
        self
    }

    /// Run one full turn: build context → call LLM → dispatch tools → commit.
    /// Returns the `TurnEnvelope` of the assistant message, or an error.
    async fn run_one_turn(
        &self,
        run_id:  RunId,
        config:  &RunConfig,
        budget:  &mut BudgetState,
    ) -> Result<TurnEnvelope> {
        let start = Instant::now();

        // ── Check budget before calling the model ─────────────────────────────
        if let Some(field) = budget.exceeded_by(&config.budget) {
            self.store.set_status(run_id, SessionStatus::BudgetHalt {
                budget_field: field.clone(),
            }).await?;
            return Err(LegionError::BudgetExceeded(field));
        }

        // ── Build context window ──────────────────────────────────────────────
        let recent = self.store.read_recent(run_id, self.context_window).await?;
        let messages = build_messages(&recent);

        // ── Build tool definitions for rs-ai ─────────────────────────────────
        let rs_tools: Vec<Tool> = self.tools.definitions().iter().map(|td| {
            Tool {
                name:        td.name.clone(),
                description: td.description.clone(),
                parameters:  td.parameters.clone(),
                constrained_sampling: None,
            }
        }).collect();

        let ctx = Context {
            system_prompt: config.system_prompt.clone(),
            messages,
            tools: rs_tools,
        };

        let opts = StreamOptions::default();

        // ── Write-ahead: record that we're about to call the model ────────────
        self.store.append(run_id, TurnEvent::model_call_intent()).await?;
        self.store.set_status(run_id, SessionStatus::Running).await?;

        // ── Stream the LLM response ───────────────────────────────────────────
        let model = rs_ai::registry::get_model(&config.model.split('/').next().unwrap_or(""), &config.model.splitn(2, '/').nth(1).unwrap_or(&config.model))
            .ok_or_else(|| LegionError::LLMError(format!("model not found: {}", config.model)))?;

        let mut stream = registry::stream(&model, &ctx, &opts);
        let mut text_buf      = String::new();
        let mut tool_calls: Vec<(String, String, serde_json::Value)> = vec![]; // (id, name, args)
        let mut tokens_in: u32  = 0;
        let mut tokens_out: u32 = 0;

        while let Some(event) = stream.next().await {
            match event {
                Event::TextDelta { .. } => {}
                Event::TextStart => {}
                Event::TextEnd   => {}

                Event::ThinkingDelta { .. } | Event::ThinkingStart | Event::ThinkingEnd => {
                    // Thinking blocks are not stored in the turn log
                }

                Event::ToolCallStart { id, name } => {
                    debug!(run_id = %run_id, tool = %name, "tool call start");
                    tool_calls.push((id, name, serde_json::Value::Null));
                }

                Event::ToolCallDelta { delta } => {
                    if let Some(last) = tool_calls.last_mut() {
                        // Accumulate JSON delta into args string
                        if last.2.is_null() {
                            last.2 = serde_json::Value::String(delta);
                        } else if let serde_json::Value::String(s) = &mut last.2 {
                            s.push_str(&delta);
                        }
                    }
                }

                Event::ToolCallEnd { id, name, arguments } => {
                    debug!(run_id = %run_id, tool = %name, "tool call end");
                    // Ensure args are parsed (ToolCallEnd gives finalised Value)
                    if let Some(tc) = tool_calls.iter_mut().find(|t| t.0 == id) {
                        tc.2 = arguments.clone();
                    } else {
                        tool_calls.push((id, name, arguments));
                    }
                }

                Event::Done { message, .. } => {
                    if let Some(usage) = &message.usage {
                        tokens_in  = usage.input;
                        tokens_out = usage.output;
                    }
                    // Collect text from content blocks
                    for block in &message.content {
                        if let rs_ai::types::ContentBlock::Text { text, .. } = block {
                            text_buf.push_str(text);
                        }
                    }
                    break;
                }

                Event::Error { error, .. } => {
                    return Err(LegionError::LLMError(error.to_string()));
                }

                Event::Start { .. } => {}
            }
        }

        let wall_ms = start.elapsed().as_millis() as u64;

        // ── Dispatch tool calls (if any) ──────────────────────────────────────
        for (call_id, tool_name, args) in &tool_calls {
            let effect = self.tools.definitions()
                .iter()
                .find(|td| td.name == *tool_name)
                .map(|td| td.effect.clone())
                .unwrap_or(EffectClass::Write);

            // Write-ahead intent
            self.store.append(
                run_id,
                TurnEvent::tool_call_intent(tool_name, call_id, effect.clone()),
            ).await?;
            self.store.set_status(run_id, SessionStatus::ToolPending).await?;

            // Dispatch
            let result = match self.tools.dispatch(tool_name, args.clone()).await {
                Ok(r)  => r,
                Err(e) => {
                    warn!(tool = %tool_name, err = %e, "tool dispatch error");
                    serde_json::json!({ "error": e.to_string() })
                }
            };

            // Commit result
            self.store.append(run_id, TurnEvent::tool_result(call_id, result)).await?;
        }

        // ── Commit assistant message ──────────────────────────────────────────
        let payload = serde_json::json!({
            "content": text_buf,
            "tool_calls": tool_calls.iter().map(|(id, name, args)| {
                serde_json::json!({ "id": id, "name": name, "args": args })
            }).collect::<Vec<_>>(),
        });

        let assistant_event = TurnEvent::assistant_message(
            payload,
            &config.model,
            tokens_in,
            tokens_out,
            wall_ms,
        );

        let seq = self.store.append(run_id, assistant_event.clone()).await?;

        // ── Update budget ─────────────────────────────────────────────────────
        budget.turns      += 1;
        budget.tokens_in  += tokens_in as u64;
        budget.tokens_out += tokens_out as u64;
        budget.wall_ms    += wall_ms;

        info!(run_id = %run_id, seq, tokens_in, tokens_out, wall_ms, "turn complete");

        let envelope = TurnEnvelope {
            run_id,
            seq,
            prev_hash: [0u8; 32], // placeholder; actual hash from store
            event:     assistant_event,
            created_at: chrono::Utc::now().timestamp_millis(),
        };

        Ok(envelope)
    }
}

// ── AgentLoopTrait impl ───────────────────────────────────────────────────────

#[async_trait]
impl AgentLoopTrait for LegionLoop {
    async fn start(&self, config: RunConfig) -> Result<RunId> {
        let run_id = Uuid::new_v4();
        self.store.create_session(run_id, &config).await?;
        self.store.append(run_id, TurnEvent::session_started(&config)).await?;
        self.store.set_status(run_id, SessionStatus::Idle).await?;
        info!(run_id = %run_id, model = %config.model, "session started");
        Ok(run_id)
    }

    async fn recover(&self, run_id: RunId) -> Result<()> {
        let outcome = recover_session(self.store.as_ref(), run_id).await?;
        match outcome {
            RecoveryOutcome::AlreadyComplete => {
                info!(run_id = %run_id, "recovery: already complete");
            }
            RecoveryOutcome::StartFresh | RecoveryOutcome::RetryLLMCall => {
                info!(run_id = %run_id, "recovery: resuming from last checkpoint");
                self.store.set_status(run_id, SessionStatus::Idle).await?;
            }
            RecoveryOutcome::DanglingToolWrite { tool_name, call_id } => {
                warn!(run_id = %run_id, %tool_name, %call_id,
                      "recovery: dangling write-ahead; pending reconciliation");
                return Err(LegionError::PendingReconciliation(run_id));
            }
            RecoveryOutcome::Parked => {
                info!(run_id = %run_id, "recovery: session is parked; awaiting resume");
            }
        }
        Ok(())
    }

    async fn resume(&self, run_id: RunId, event: ExternalEvent) -> Result<()> {
        // Append the external event as a UserMessage (or other event type)
        match event {
            ExternalEvent::UserMessage(content) => {
                self.store.append(run_id, TurnEvent::user_message(content)).await?;
                self.store.set_status(run_id, SessionStatus::Running).await?;
            }
            ExternalEvent::ApprovalGranted => {
                self.store.append(run_id, TurnEvent {
                    kind:        TurnEventKind::SessionResumed,
                    payload:     Some(serde_json::json!({"approval": "granted"})),
                    payload_cid: None,
                    model:       None,
                    tokens_in:   None,
                    tokens_out:  None,
                    wall_ms:     None,
                }).await?;
                self.store.set_status(run_id, SessionStatus::Resuming).await?;
            }
            ExternalEvent::ApprovalDenied => {
                self.store.set_status(run_id, SessionStatus::Aborted).await?;
            }
            ExternalEvent::ExternalTrigger { name, payload } => {
                self.store.append(run_id, TurnEvent {
                    kind:        TurnEventKind::SessionResumed,
                    payload:     Some(serde_json::json!({"trigger": name, "payload": payload})),
                    payload_cid: None,
                    model:       None,
                    tokens_in:   None,
                    tokens_out:  None,
                    wall_ms:     None,
                }).await?;
                self.store.set_status(run_id, SessionStatus::Resuming).await?;
            }
        }
        Ok(())
    }

    async fn resolve(&self, run_id: RunId) -> Result<TurnEnvelope> {
        // Load config from the SessionStarted event
        let log = self.store.read_log(run_id).await?;
        let config: RunConfig = log.iter()
            .find_map(|e| {
                if matches!(e.event.kind, TurnEventKind::SessionStarted) {
                    e.event.payload.as_ref()
                        .and_then(|p| serde_json::from_value(p.clone()).ok())
                } else {
                    None
                }
            })
            .ok_or_else(|| LegionError::Store(format!("no SessionStarted event for {run_id}")))?;

        let mut budget = BudgetState::default();

        // Accumulate prior usage from log
        for env in &log {
            if matches!(env.event.kind, TurnEventKind::AssistantMessage) {
                budget.turns      += 1;
                budget.tokens_in  += env.event.tokens_in.unwrap_or(0) as u64;
                budget.tokens_out += env.event.tokens_out.unwrap_or(0) as u64;
                budget.wall_ms    += env.event.wall_ms.unwrap_or(0);
            }
        }

        // Run one turn
        let result = self.run_one_turn(run_id, &config, &mut budget).await?;
        self.store.set_status(run_id, SessionStatus::Completed).await?;
        Ok(result)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use legion_core::test_doubles::{EchoToolRegistry, MemoryEventStore};
    use legion_core::types::{Budget, RunConfig};

    fn echo_loop() -> LegionLoop {
        LegionLoop::new(
            Arc::new(MemoryEventStore::new()),
            Arc::new(EchoToolRegistry::new()),
        )
    }

    #[tokio::test]
    async fn start_creates_session() {
        let lp = echo_loop();
        let run_id = lp.start(RunConfig {
            system_prompt: Some("test".into()),
            model:         "faux/test".into(),
            budget:        Budget::default(),
            tools:         vec!["echo".into()],
            metadata:      None,
        }).await.unwrap();

        // Check session was created and is Idle
        let status = lp.store.session_status(run_id).await.unwrap();
        assert!(matches!(status, SessionStatus::Idle));
    }

    #[tokio::test]
    async fn resume_appends_user_message() {
        let lp = echo_loop();
        let run_id = lp.start(RunConfig {
            system_prompt: None,
            model:         "faux/test".into(),
            budget:        Budget::default(),
            tools:         vec![],
            metadata:      None,
        }).await.unwrap();

        lp.resume(run_id, ExternalEvent::user_message("hello")).await.unwrap();

        let recent = lp.store.read_recent(run_id, 5).await.unwrap();
        let has_user_msg = recent.iter().any(|e| matches!(e.event.kind, TurnEventKind::UserMessage));
        assert!(has_user_msg, "expected a UserMessage turn after resume");
    }

    #[tokio::test]
    async fn recover_fresh_session_ok() {
        let lp = echo_loop();
        let run_id = lp.start(RunConfig {
            system_prompt: None,
            model:         "faux/test".into(),
            budget:        Budget::default(),
            tools:         vec![],
            metadata:      None,
        }).await.unwrap();

        // No dangling writes; recover should succeed
        lp.recover(run_id).await.unwrap();
        let status = lp.store.session_status(run_id).await.unwrap();
        assert!(matches!(status, SessionStatus::Idle));
    }
}

impl LegionLoop {
    /// Streaming variant of `resolve`: injects a user message, runs one turn,
    /// and emits `SessionEvent`s via the returned channel receiver.
    ///
    /// The caller should poll the receiver and forward events to the SSE stream.
    /// The channel closes (returns `None`) when the turn completes or errors.
    pub fn stream_resolve(
        self: Arc<Self>,
        run_id:  RunId,
        message: String,
    ) -> tokio::sync::mpsc::Receiver<SessionEvent> {
        let (tx, rx) = tokio::sync::mpsc::channel::<SessionEvent>(32);
        let lp       = self.clone();

        tokio::spawn(async move {
            // Inject user message
            if let Err(e) = lp.store.append(run_id, TurnEvent::user_message(message)).await {
                let _ = tx.send(SessionEvent::Error { message: e.to_string() }).await;
                return;
            }

            // Load config
            let log = match lp.store.read_log(run_id).await {
                Ok(l)  => l,
                Err(e) => { let _ = tx.send(SessionEvent::Error { message: e.to_string() }).await; return; }
            };
            let config: RunConfig = match log.iter().find_map(|e| {
                if matches!(e.event.kind, TurnEventKind::SessionStarted) {
                    e.event.payload.as_ref().and_then(|p| serde_json::from_value(p.clone()).ok())
                } else { None }
            }) {
                Some(c) => c,
                None => { let _ = tx.send(SessionEvent::Error { message: "no config".into() }).await; return; }
            };

            // Build context + tools
            let recent   = match lp.store.read_recent(run_id, lp.context_window).await {
                Ok(r)  => r,
                Err(e) => { let _ = tx.send(SessionEvent::Error { message: e.to_string() }).await; return; }
            };
            let messages = crate::context::build_messages(&recent);
            let rs_tools: Vec<rs_ai::types::Tool> = lp.tools.definitions().iter().map(|td| {
                rs_ai::types::Tool {
                    name:        td.name.clone(),
                    description: td.description.clone(),
                    parameters:  td.parameters.clone(),
                    constrained_sampling: None,
                }
            }).collect();
            let ctx  = rs_ai::types::Context { system_prompt: config.system_prompt.clone(), messages, tools: rs_tools };
            let opts = rs_ai::types::StreamOptions::default();

            // Look up model
            let parts: Vec<&str> = config.model.splitn(2, '/').collect();
            let (provider, mid) = if parts.len() == 2 { (parts[0], parts[1]) } else { ("", config.model.as_str()) };
            let model = match rs_ai::registry::get_model(provider, mid) {
                Some(m) => m,
                None => {
                    let _ = tx.send(SessionEvent::Error { message: format!("model not found: {}", config.model) }).await;
                    return;
                }
            };

            // Write-ahead
            let _ = lp.store.append(run_id, TurnEvent::model_call_intent()).await;
            let _ = lp.store.set_status(run_id, SessionStatus::Running).await;

            // Stream
            let start  = std::time::Instant::now();
            let mut stream = rs_ai::registry::stream(&model, &ctx, &opts);
            let mut text_buf    = String::new();
            let mut tokens_in   = 0u32;
            let mut tokens_out  = 0u32;
            let mut last_seq    = 0u64;

            while let Some(ev) = stream.next().await {
                match ev {
                    rs_ai::events::Event::TextDelta { delta } => {
                        text_buf.push_str(&delta);
                        let _ = tx.send(SessionEvent::TextDelta { delta }).await;
                    }
                    rs_ai::events::Event::ThinkingDelta { delta } => {
                        let _ = tx.send(SessionEvent::ThinkingDelta { delta }).await;
                    }
                    rs_ai::events::Event::ToolCallEnd { id, name, .. } => {
                        let _ = tx.send(SessionEvent::ToolCall { name, call_id: id }).await;
                    }
                    rs_ai::events::Event::Done { message, .. } => {
                        if let Some(u) = message.usage { tokens_in = u.input; tokens_out = u.output; }
                        let wall_ms = start.elapsed().as_millis() as u64;
                        let ev = TurnEvent::assistant_message(
                            serde_json::json!({ "content": text_buf }),
                            &config.model, tokens_in, tokens_out, wall_ms,
                        );
                        if let Ok(seq) = lp.store.append(run_id, ev).await {
                            last_seq = seq;
                        }
                        let _ = lp.store.set_status(run_id, SessionStatus::Completed).await;
                        let _ = tx.send(SessionEvent::Done {
                            content: text_buf.clone(),
                            seq:     last_seq,
                            tokens_in,
                            tokens_out,
                            wall_ms,
                        }).await;
                        break;
                    }
                    rs_ai::events::Event::Error { error, .. } => {
                        let _ = tx.send(SessionEvent::Error { message: error.to_string() }).await;
                        break;
                    }
                    _ => {}
                }
            }
        });

        rx
    }
}
