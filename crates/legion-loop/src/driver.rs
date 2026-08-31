//! The main agent loop driver.
//!
//! `LegionLoop` implements `AgentLoopTrait`. It drives one agent session
//! using rs-ai's `EventStream` and writes every step to the `EventStore`.

use async_trait::async_trait;
use futures::StreamExt;
use std::sync::Arc;
use std::time::Instant;
use tracing::{Instrument, info, info_span, warn};
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
        BudgetState, EffectClass, ExternalEvent, RunConfig, RunId, SessionStatus, TurnEnvelope,
        TurnEvent, TurnEventKind, TurnPhase,
    },
};

use crate::context::build_messages;
use crate::recovery::{RecoveryOutcome, recover_session};

use crate::stream::SessionEvent;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReconcileAction {
    Skip,
    Retry,
}

/// Explicit phase tracker for a single model/tool turn.
///
/// Keeping phase transitions in one place makes invalid driver changes fail
/// loudly in tests instead of silently creating an unrecoverable event order.
#[derive(Debug)]
struct TurnState {
    phase: TurnPhase,
}

impl TurnState {
    fn new() -> Self {
        Self {
            phase: TurnPhase::Setup,
        }
    }

    fn transition(&mut self, expected: TurnPhase, next: TurnPhase) -> Result<()> {
        if self.phase != expected {
            return Err(LegionError::Store(format!(
                "invalid turn phase transition: {:?} -> {:?} (expected {:?})",
                self.phase, next, expected
            )));
        }
        self.phase = next;
        Ok(())
    }
}

impl ReconcileAction {
    fn as_str(self) -> &'static str {
        match self {
            Self::Skip => "skip",
            Self::Retry => "retry",
        }
    }
}

// ── LegionLoop ────────────────────────────────────────────────────────────────

/// Stateless agent loop that drives sessions stored in an `EventStore`.
pub struct LegionLoop {
    store: Arc<dyn EventStore>,
    tools: Arc<dyn ToolRegistry>,
    /// Maximum turns to read for the context window.
    context_window: usize,
}

impl LegionLoop {
    pub fn new(store: Arc<dyn EventStore>, tools: Arc<dyn ToolRegistry>) -> Self {
        Self {
            store,
            tools,
            context_window: 40,
        }
    }

    pub fn with_context_window(mut self, n: usize) -> Self {
        self.context_window = n;
        self
    }

    /// Resolve the currently pending tool call after an operator decision.
    /// A failed retry leaves the session pending so it can be reconciled again.
    pub async fn reconcile(&self, run_id: RunId, action: ReconcileAction) -> Result<()> {
        let status = self.store.session_status(run_id).await?;
        let SessionStatus::PendingReconciliation { tool_name, call_id } = status else {
            return Err(LegionError::SessionWrongState {
                run_id,
                expected: SessionStatus::PendingReconciliation {
                    tool_name: "<tool>".into(),
                    call_id: "<call>".into(),
                },
                actual: status,
            });
        };

        let log = self.store.read_log(run_id).await?;
        let intent = log
            .iter()
            .rev()
            .find(|entry| {
                matches!(
                    &entry.event.kind,
                    TurnEventKind::ToolCallIntent { call_id: id, .. } if id == &call_id
                )
            })
            .ok_or_else(|| {
                LegionError::Store(format!("pending tool intent not found for call {call_id}"))
            })?;

        match action {
            ReconcileAction::Skip => {
                self.store
                    .append(
                        run_id,
                        TurnEvent::tool_result(
                            &call_id,
                            serde_json::json!({ "skipped": true, "reconciled": true }),
                        ),
                    )
                    .await?;
            }
            ReconcileAction::Retry => {
                let arguments = intent
                    .event
                    .payload
                    .as_ref()
                    .and_then(|payload| payload.get("arguments"))
                    .cloned()
                    .ok_or_else(|| {
                        LegionError::ToolError(
                            "cannot retry legacy tool intent without stored arguments".into(),
                        )
                    })?;
                let result = self.tools.dispatch(&tool_name, arguments).await?;
                self.store
                    .append(run_id, TurnEvent::tool_result(&call_id, result))
                    .await?;
            }
        }

        self.store
            .append(
                run_id,
                TurnEvent::tool_call_reconciled(&call_id, action.as_str()),
            )
            .await?;
        self.store.set_status(run_id, SessionStatus::Idle).await?;
        Ok(())
    }

    async fn halt_budget(&self, run_id: RunId, field: String) -> Result<()> {
        self.store
            .append(run_id, TurnEvent::session_budget_halt(&field))
            .await?;
        self.store
            .set_status(
                run_id,
                SessionStatus::BudgetHalt {
                    budget_field: field,
                },
            )
            .await
    }

    fn stream_options(run_id: RunId, config: &RunConfig) -> StreamOptions {
        let metadata = config.metadata.as_ref().and_then(|value| {
            value.as_object().map(|object| {
                object
                    .iter()
                    .map(|(key, value)| (key.clone(), value.clone()))
                    .collect()
            })
        });
        StreamOptions {
            session_id: Some(run_id.to_string()),
            telemetry_context: config.metadata.clone(),
            metadata,
            ..Default::default()
        }
    }

    fn budget_from_log(log: &[TurnEnvelope]) -> BudgetState {
        let mut budget = BudgetState::default();
        for envelope in log {
            match envelope.event.kind {
                TurnEventKind::AssistantMessage => {
                    budget.turns += 1;
                    budget.tokens_in += envelope.event.tokens_in.unwrap_or(0) as u64;
                    budget.tokens_out += envelope.event.tokens_out.unwrap_or(0) as u64;
                    budget.wall_ms += envelope.event.wall_ms.unwrap_or(0);
                }
                TurnEventKind::ToolResult { .. } => budget.tool_calls += 1,
                _ => {}
            }
        }
        budget
    }

    fn config_from_log(log: &[TurnEnvelope], run_id: RunId) -> Result<RunConfig> {
        log.iter()
            .find_map(|envelope| {
                if matches!(envelope.event.kind, TurnEventKind::SessionStarted) {
                    envelope
                        .event
                        .payload
                        .as_ref()
                        .and_then(|payload| serde_json::from_value(payload.clone()).ok())
                } else {
                    None
                }
            })
            .ok_or_else(|| LegionError::Store(format!("no SessionStarted event for {run_id}")))
    }

    async fn emit(sender: Option<&tokio::sync::mpsc::Sender<SessionEvent>>, event: SessionEvent) {
        if let Some(sender) = sender {
            let _ = sender.send(event).await;
        }
    }

    /// Run a complete model/tool turn, including every model continuation needed
    /// to turn tool results into a final assistant response.
    async fn run_one_turn(
        &self,
        run_id: RunId,
        config: &RunConfig,
        budget: &mut BudgetState,
        sender: Option<&tokio::sync::mpsc::Sender<SessionEvent>>,
    ) -> Result<TurnEnvelope> {
        if let Some(field) = budget.exceeded_by(&config.budget) {
            self.halt_budget(run_id, field.clone()).await?;
            Self::emit(
                sender,
                SessionEvent::BudgetHalt {
                    budget_field: field.clone(),
                },
            )
            .await;
            return Err(LegionError::BudgetExceeded(field));
        }

        let mut turn_state = TurnState::new();
        let recent = self.store.read_recent(run_id, self.context_window).await?;
        let mut context = Context {
            system_prompt: config.system_prompt.clone(),
            messages: build_messages(&recent),
            tools: Vec::new(),
        };
        let options = Self::stream_options(run_id, config);
        let (provider, model_id) = config
            .model
            .split_once('/')
            .unwrap_or(("", config.model.as_str()));
        let model = registry::get_model(provider, model_id)
            .ok_or_else(|| LegionError::LLMError(format!("model not found: {}", config.model)))?;

        loop {
            if let Some(field) = budget.exceeded_by(&config.budget) {
                self.halt_budget(run_id, field.clone()).await?;
                Self::emit(
                    sender,
                    SessionEvent::BudgetHalt {
                        budget_field: field.clone(),
                    },
                )
                .await;
                return Err(LegionError::BudgetExceeded(field));
            }

            turn_state.transition(TurnPhase::Setup, TurnPhase::Running)?;

            // Refresh definitions on every model step so deployed or promoted tools
            // become visible without splitting streaming and non-streaming behavior.
            let tool_definitions = self.tools.definitions().await;
            let enabled_definitions = tool_definitions
                .iter()
                .filter(|definition| config.tools.iter().any(|name| name == &definition.name))
                .collect::<Vec<_>>();
            context.tools = enabled_definitions
                .iter()
                .map(|definition| Tool {
                    name: definition.name.clone(),
                    description: definition.description.clone(),
                    parameters: definition.parameters.clone(),
                    constrained_sampling: None,
                })
                .collect();

            self.store
                .append(run_id, TurnEvent::model_call_intent())
                .await?;
            self.store
                .set_status(run_id, SessionStatus::Running)
                .await?;

            let started_at = Instant::now();
            let mut stream = registry::stream(&model, &context, &options);
            let mut terminal_message = None;

            while let Some(event) = stream.next().await {
                match event {
                    Event::TextDelta { delta } => {
                        Self::emit(sender, SessionEvent::TextDelta { delta }).await;
                    }
                    Event::ThinkingDelta { delta } => {
                        Self::emit(sender, SessionEvent::ThinkingDelta { delta }).await;
                    }
                    Event::Done { message, .. } => {
                        terminal_message = Some(message);
                        break;
                    }
                    Event::Error { error, .. } => {
                        return Err(LegionError::LLMError(error.to_string()));
                    }
                    Event::Start { .. }
                    | Event::TextStart
                    | Event::TextEnd
                    | Event::ThinkingStart
                    | Event::ThinkingEnd
                    | Event::ToolCallStart { .. }
                    | Event::ToolCallDelta { .. }
                    | Event::ToolCallEnd { .. } => {}
                }
            }

            drop(stream);
            let message = terminal_message.ok_or_else(|| {
                LegionError::LLMError("model stream ended without a terminal message".into())
            })?;
            let wall_ms = started_at.elapsed().as_millis() as u64;
            let (tokens_in, tokens_out, cache_read, cache_write) =
                message.usage.as_ref().map_or((0, 0, 0, 0), |usage| {
                    (
                        usage.input,
                        usage.output,
                        usage.cache_read,
                        usage.cache_write,
                    )
                });
            crate::telemetry::record_token_usage(
                &config.model,
                tokens_in,
                tokens_out,
                cache_read,
                cache_write,
            );
            let content = rs_ai::harness::get_text_content(&message);
            let tool_calls = message
                .content
                .iter()
                .filter_map(|block| {
                    if let rs_ai::types::ContentBlock::ToolCall {
                        id,
                        name,
                        arguments,
                        ..
                    } = block
                    {
                        Some((
                            id.clone(),
                            name.clone(),
                            serde_json::to_value(arguments).unwrap_or_default(),
                        ))
                    } else {
                        None
                    }
                })
                .collect::<Vec<_>>();
            let payload = serde_json::json!({
                "content": content,
                "tool_calls": tool_calls.iter().map(|(id, name, arguments)| {
                    serde_json::json!({ "id": id, "name": name, "args": arguments })
                }).collect::<Vec<_>>(),
                "message": message,
            });
            let assistant_event = TurnEvent::assistant_message(
                payload,
                &config.model,
                tokens_in,
                tokens_out,
                wall_ms,
            );
            let seq = self.store.append(run_id, assistant_event.clone()).await?;
            let envelope = TurnEnvelope {
                run_id,
                seq,
                prev_hash: [0; 32],
                event: assistant_event,
                created_at: chrono::Utc::now().timestamp_millis(),
            };

            budget.turns += 1;
            budget.tokens_in += tokens_in as u64;
            budget.tokens_out += tokens_out as u64;
            budget.wall_ms += wall_ms;
            info!(run_id = %run_id, seq, tokens_in, tokens_out, wall_ms, "model step complete");

            // The assistant tool-call message must precede all corresponding tool
            // results in both durable history and the next provider context.
            context = rs_ai::harness::append_assistant_message(context, &message);

            if tool_calls.is_empty() {
                turn_state.transition(TurnPhase::Running, TurnPhase::Finalizing)?;
                let halt_field = budget.exceeded_by(&config.budget);
                if let Some(field) = &halt_field {
                    self.halt_budget(run_id, field.clone()).await?;
                } else {
                    self.store
                        .set_status(run_id, SessionStatus::Completed)
                        .await?;
                }
                Self::emit(
                    sender,
                    SessionEvent::Done {
                        content,
                        seq,
                        tokens_in,
                        tokens_out,
                        wall_ms,
                    },
                )
                .await;
                if let Some(budget_field) = halt_field {
                    Self::emit(sender, SessionEvent::BudgetHalt { budget_field }).await;
                }
                turn_state.transition(TurnPhase::Finalizing, TurnPhase::Completed)?;
                return Ok(envelope);
            }

            turn_state.transition(TurnPhase::Running, TurnPhase::Tools)?;

            // Turn/token/time budgets are step limits. Halt before dispatch only
            // when another model continuation is no longer permitted; the tool-call
            // budget is enforced individually below so its final allowed call runs.
            if let Some(field) = budget.exceeded_by(&config.budget)
                && field != "max_tool_calls"
            {
                self.halt_budget(run_id, field.clone()).await?;
                Self::emit(
                    sender,
                    SessionEvent::BudgetHalt {
                        budget_field: field.clone(),
                    },
                )
                .await;
                return Err(LegionError::BudgetExceeded(field));
            }

            for (call_id, tool_name, arguments) in tool_calls {
                if config
                    .budget
                    .max_tool_calls
                    .is_some_and(|max| budget.tool_calls >= max)
                {
                    let field = "max_tool_calls".to_string();
                    self.halt_budget(run_id, field.clone()).await?;
                    Self::emit(
                        sender,
                        SessionEvent::BudgetHalt {
                            budget_field: field.clone(),
                        },
                    )
                    .await;
                    return Err(LegionError::BudgetExceeded(field));
                }

                Self::emit(
                    sender,
                    SessionEvent::ToolCall {
                        name: tool_name.clone(),
                        call_id: call_id.clone(),
                    },
                )
                .await;
                let effect = enabled_definitions
                    .iter()
                    .find(|definition| definition.name == tool_name)
                    .map(|definition| definition.effect.clone())
                    .unwrap_or(EffectClass::Write);
                self.store
                    .append(
                        run_id,
                        TurnEvent::tool_call_intent(
                            &tool_name,
                            &call_id,
                            effect,
                            arguments.clone(),
                        ),
                    )
                    .await?;
                self.store
                    .set_status(run_id, SessionStatus::ToolPending)
                    .await?;

                let tool_span = info_span!("agent.tool", tool.name = %tool_name);
                let (result, is_error) = match self
                    .tools
                    .dispatch(&tool_name, arguments)
                    .instrument(tool_span)
                    .await
                {
                    Ok(result) => (result, false),
                    Err(error) => {
                        warn!(tool = %tool_name, err = %error, "tool dispatch error");
                        (serde_json::json!({ "error": error.to_string() }), true)
                    }
                };
                self.store
                    .append(run_id, TurnEvent::tool_result(&call_id, result.clone()))
                    .await?;
                budget.tool_calls += 1;
                context = rs_ai::harness::append_tool_result(
                    context,
                    &call_id,
                    &tool_name,
                    &result.to_string(),
                    is_error,
                );
                Self::emit(
                    sender,
                    SessionEvent::ToolResult {
                        call_id,
                        output: result,
                        is_error,
                    },
                )
                .await;

                if let Some(field) = budget.exceeded_by(&config.budget)
                    && field != "max_tool_calls"
                {
                    self.halt_budget(run_id, field.clone()).await?;
                    Self::emit(
                        sender,
                        SessionEvent::BudgetHalt {
                            budget_field: field.clone(),
                        },
                    )
                    .await;
                    return Err(LegionError::BudgetExceeded(field));
                }
            }

            turn_state.transition(TurnPhase::Tools, TurnPhase::Setup)?;
        }
    }
}

// ── AgentLoopTrait impl ───────────────────────────────────────────────────────

#[async_trait]
impl AgentLoopTrait for LegionLoop {
    #[tracing::instrument(name = "agent.start", skip_all, fields(model = %config.model))]
    async fn start(&self, config: RunConfig) -> Result<RunId> {
        let run_id = Uuid::new_v4();
        self.store.create_session(run_id, &config).await?;
        self.store
            .append(run_id, TurnEvent::session_started(&config))
            .await?;
        self.store.set_status(run_id, SessionStatus::Idle).await?;
        info!(run_id = %run_id, model = %config.model, "session started");
        Ok(run_id)
    }

    #[tracing::instrument(name = "agent.recover", skip_all)]
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

    #[tracing::instrument(name = "agent.resume", skip_all)]
    async fn resume(&self, run_id: RunId, event: ExternalEvent) -> Result<()> {
        // Append the external event as a UserMessage (or other event type)
        match event {
            ExternalEvent::UserMessage(content) => {
                self.store
                    .append(run_id, TurnEvent::user_message(content))
                    .await?;
                self.store
                    .set_status(run_id, SessionStatus::Running)
                    .await?;
            }
            ExternalEvent::ApprovalGranted => {
                self.store
                    .append(
                        run_id,
                        TurnEvent {
                            kind: TurnEventKind::SessionResumed,
                            payload: Some(serde_json::json!({"approval": "granted"})),
                            payload_cid: None,
                            model: None,
                            tokens_in: None,
                            tokens_out: None,
                            wall_ms: None,
                        },
                    )
                    .await?;
                self.store
                    .set_status(run_id, SessionStatus::Resuming)
                    .await?;
            }
            ExternalEvent::ApprovalDenied => {
                self.store
                    .set_status(run_id, SessionStatus::Aborted)
                    .await?;
            }
            ExternalEvent::ExternalTrigger { name, payload } => {
                self.store
                    .append(
                        run_id,
                        TurnEvent {
                            kind: TurnEventKind::SessionResumed,
                            payload: Some(serde_json::json!({"trigger": name, "payload": payload})),
                            payload_cid: None,
                            model: None,
                            tokens_in: None,
                            tokens_out: None,
                            wall_ms: None,
                        },
                    )
                    .await?;
                self.store
                    .set_status(run_id, SessionStatus::Resuming)
                    .await?;
            }
        }
        Ok(())
    }

    #[tracing::instrument(name = "agent.resolve", skip_all)]
    async fn resolve(&self, run_id: RunId) -> Result<TurnEnvelope> {
        let log = self.store.read_log(run_id).await?;
        let config = Self::config_from_log(&log, run_id)?;
        let mut budget = Self::budget_from_log(&log);
        self.run_one_turn(run_id, &config, &mut budget, None).await
    }
}

impl LegionLoop {
    /// Streaming variant of `resolve`: inject a user message and run the same
    /// complete model/tool engine while forwarding observable step events.
    pub fn stream_resolve(
        self: Arc<Self>,
        run_id: RunId,
        message: String,
    ) -> tokio::sync::mpsc::Receiver<SessionEvent> {
        let (sender, receiver) = tokio::sync::mpsc::channel(32);

        tokio::spawn(async move {
            if let Err(error) = self
                .store
                .append(run_id, TurnEvent::user_message(message))
                .await
            {
                Self::emit(
                    Some(&sender),
                    SessionEvent::Error {
                        message: error.to_string(),
                    },
                )
                .await;
                return;
            }
            let result = async {
                let log = self.store.read_log(run_id).await?;
                let config = Self::config_from_log(&log, run_id)?;
                let mut budget = Self::budget_from_log(&log);
                self.run_one_turn(run_id, &config, &mut budget, Some(&sender))
                    .await
            }
            .await;
            if let Err(error) = result
                && !matches!(error, LegionError::BudgetExceeded(_))
            {
                Self::emit(
                    Some(&sender),
                    SessionEvent::Error {
                        message: error.to_string(),
                    },
                )
                .await;
            }
        });

        receiver
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

    #[test]
    fn turn_phase_state_machine_rejects_invalid_transitions() {
        let mut state = TurnState::new();
        state
            .transition(TurnPhase::Setup, TurnPhase::Running)
            .unwrap();
        assert!(
            state
                .transition(TurnPhase::Setup, TurnPhase::Tools)
                .is_err()
        );
        state
            .transition(TurnPhase::Running, TurnPhase::Finalizing)
            .unwrap();
        state
            .transition(TurnPhase::Finalizing, TurnPhase::Completed)
            .unwrap();
    }

    #[tokio::test]
    async fn start_creates_session() {
        let lp = echo_loop();
        let run_id = lp
            .start(RunConfig {
                system_prompt: Some("test".into()),
                model: "faux/test".into(),
                budget: Budget::default(),
                tools: vec!["echo".into()],
                metadata: None,
            })
            .await
            .unwrap();

        // Check session was created and is Idle
        let status = lp.store.session_status(run_id).await.unwrap();
        assert!(matches!(status, SessionStatus::Idle));
    }

    #[tokio::test]
    async fn resume_appends_user_message() {
        let lp = echo_loop();
        let run_id = lp
            .start(RunConfig {
                system_prompt: None,
                model: "faux/test".into(),
                budget: Budget::default(),
                tools: vec![],
                metadata: None,
            })
            .await
            .unwrap();

        lp.resume(run_id, ExternalEvent::user_message("hello"))
            .await
            .unwrap();

        let recent = lp.store.read_recent(run_id, 5).await.unwrap();
        let has_user_msg = recent
            .iter()
            .any(|e| matches!(e.event.kind, TurnEventKind::UserMessage));
        assert!(has_user_msg, "expected a UserMessage turn after resume");
    }

    #[tokio::test]
    async fn reconcile_skip_closes_dangling_call() {
        let store = Arc::new(MemoryEventStore::new());
        let lp = LegionLoop::new(store.clone(), Arc::new(EchoToolRegistry::new()));
        let run_id = lp
            .start(RunConfig {
                system_prompt: None,
                model: "faux/test".into(),
                budget: Budget::default(),
                tools: vec!["echo".into()],
                metadata: None,
            })
            .await
            .unwrap();
        store
            .append(
                run_id,
                TurnEvent::tool_call_intent(
                    "echo",
                    "call-1",
                    EffectClass::Write,
                    serde_json::json!({"message":"hello"}),
                ),
            )
            .await
            .unwrap();
        lp.recover(run_id).await.unwrap_err();

        lp.reconcile(run_id, ReconcileAction::Skip).await.unwrap();

        assert_eq!(
            store.session_status(run_id).await.unwrap(),
            SessionStatus::Idle
        );
        let log = store.read_log(run_id).await.unwrap();
        assert!(log.iter().any(|entry| matches!(
            &entry.event.kind,
            TurnEventKind::ToolCallReconciled { call_id, action }
                if call_id == "call-1" && action == "skip"
        )));
    }

    #[tokio::test]
    async fn reconcile_retry_dispatches_stored_arguments() {
        let store = Arc::new(MemoryEventStore::new());
        let lp = LegionLoop::new(store.clone(), Arc::new(EchoToolRegistry::new()));
        let run_id = lp
            .start(RunConfig {
                system_prompt: None,
                model: "faux/test".into(),
                budget: Budget::default(),
                tools: vec!["echo".into()],
                metadata: None,
            })
            .await
            .unwrap();
        store
            .append(
                run_id,
                TurnEvent::tool_call_intent(
                    "echo",
                    "call-2",
                    EffectClass::Idempotent,
                    serde_json::json!({"message":"again"}),
                ),
            )
            .await
            .unwrap();
        lp.recover(run_id).await.unwrap_err();

        lp.reconcile(run_id, ReconcileAction::Retry).await.unwrap();

        let log = store.read_log(run_id).await.unwrap();
        let result = log
            .iter()
            .find(|entry| {
                matches!(
                    &entry.event.kind,
                    TurnEventKind::ToolResult { call_id } if call_id == "call-2"
                )
            })
            .unwrap();
        assert_eq!(result.event.payload.as_ref().unwrap()["message"], "again");
    }

    #[tokio::test]
    async fn resolve_persists_budget_halt_before_model_call() {
        let store = Arc::new(MemoryEventStore::new());
        let lp = LegionLoop::new(store.clone(), Arc::new(EchoToolRegistry::new()));
        let run_id = lp
            .start(RunConfig {
                system_prompt: None,
                model: "faux/test".into(),
                budget: Budget {
                    max_turns: Some(1),
                    ..Default::default()
                },
                tools: vec![],
                metadata: None,
            })
            .await
            .unwrap();
        store
            .append(
                run_id,
                TurnEvent::assistant_message(
                    serde_json::json!({"content":"prior"}),
                    "faux/test",
                    1,
                    1,
                    1,
                ),
            )
            .await
            .unwrap();

        let error = lp.resolve(run_id).await.unwrap_err();

        assert!(matches!(error, LegionError::BudgetExceeded(ref field) if field == "max_turns"));
        assert_eq!(
            store.session_status(run_id).await.unwrap(),
            SessionStatus::BudgetHalt {
                budget_field: "max_turns".into()
            },
        );
        assert!(
            store
                .read_log(run_id)
                .await
                .unwrap()
                .iter()
                .any(|entry| matches!(
                    &entry.event.kind,
                    TurnEventKind::SessionBudgetHalt { budget_field } if budget_field == "max_turns"
                ))
        );
    }

    fn faux_message(
        content: Vec<rs_ai::types::ContentBlock>,
        reason: rs_ai::types::StopReason,
    ) -> rs_ai::types::Message {
        rs_ai::types::Message {
            role: rs_ai::types::Role::Assistant,
            content,
            timestamp: 0,
            api: None,
            provider: None,
            model: None,
            response_id: None,
            response_model: None,
            diagnostics: Vec::new(),
            usage: None,
            stop_reason: Some(reason),
            deferred: None,
            error_message: None,
            raw_stop_reason: None,
            end_turn: None,
            tool_call_id: None,
            tool_name: None,
            is_error: false,
            details: None,
            added_tool_names: Vec::new(),
        }
    }

    fn tool_call(
        call_id: &str,
        name: &str,
        key: &str,
        value: serde_json::Value,
    ) -> rs_ai::types::Message {
        faux_message(
            vec![rs_ai::types::ContentBlock::ToolCall {
                id: call_id.into(),
                name: name.into(),
                arguments: std::collections::HashMap::from([(key.into(), value)]),
                thought_signature: None,
                namespace: None,
            }],
            rs_ai::types::StopReason::ToolUse,
        )
    }

    fn text_response(text: &str) -> rs_ai::types::Message {
        faux_message(
            vec![rs_ai::types::ContentBlock::Text {
                text: text.into(),
                text_signature: None,
            }],
            rs_ai::types::StopReason::Stop,
        )
    }

    fn register_faux(test_name: &str) -> (String, Arc<rs_ai::provider::faux::FauxProvider>) {
        let unique = Uuid::new_v4().simple().to_string();
        let api = format!("legion-{test_name}-{unique}");
        let provider = format!("legion-{test_name}-{unique}");
        let model_id = "model";
        let faux = rs_ai::provider::faux::FauxProvider::new(&api, &provider);
        rs_ai::registry::register_api(faux.clone());
        rs_ai::registry::register_model(rs_ai::types::Model {
            id: model_id.into(),
            name: "Legion Faux".into(),
            api,
            provider: provider.clone(),
            base_url: String::new(),
            reasoning: false,
            thinking_level_map: None,
            input: vec!["text".into()],
            cost: Default::default(),
            context_window: 128_000,
            max_tokens: 4096,
            sampling_params: None,
            headers: None,
            api_key: None,
            compat: Default::default(),
        });
        (format!("{provider}/{model_id}"), faux)
    }

    async fn new_faux_loop(
        test_name: &str,
        responses: Vec<rs_ai::types::Message>,
        budget: Budget,
        metadata: Option<serde_json::Value>,
    ) -> (
        LegionLoop,
        Arc<MemoryEventStore>,
        Arc<rs_ai::provider::faux::FauxProvider>,
        RunId,
    ) {
        let (model, faux) = register_faux(test_name);
        faux.set_responses(responses);
        let store = Arc::new(MemoryEventStore::new());
        let lp = LegionLoop::new(store.clone(), Arc::new(EchoToolRegistry::new()));
        let run_id = lp
            .start(RunConfig {
                system_prompt: Some("test system".into()),
                model,
                budget,
                tools: vec!["echo".into(), "fail".into()],
                metadata,
            })
            .await
            .unwrap();
        lp.resume(run_id, ExternalEvent::user_message("begin"))
            .await
            .unwrap();
        (lp, store, faux, run_id)
    }

    #[tokio::test]
    async fn resolve_executes_tool_and_continues_to_final_response() {
        let (lp, store, faux, run_id) = new_faux_loop(
            "one-round",
            vec![
                tool_call("call-1", "echo", "message", serde_json::json!("hello")),
                text_response("finished"),
            ],
            Budget::default(),
            Some(serde_json::json!({"trace_id":"turn-1"})),
        )
        .await;

        let final_turn = lp.resolve(run_id).await.unwrap();

        assert_eq!(
            final_turn.event.payload.as_ref().unwrap()["content"],
            "finished"
        );
        assert_eq!(faux.call_count(), 2);
        assert_eq!(faux.pending_response_count(), 0);
        assert_eq!(
            store.session_status(run_id).await.unwrap(),
            SessionStatus::Completed
        );
        let log = store.read_log(run_id).await.unwrap();
        let kinds = log
            .iter()
            .filter_map(|entry| match &entry.event.kind {
                TurnEventKind::AssistantMessage => Some("assistant"),
                TurnEventKind::ToolCallIntent { .. } => Some("intent"),
                TurnEventKind::ToolResult { .. } => Some("result"),
                _ => None,
            })
            .collect::<Vec<_>>();
        assert_eq!(kinds, vec!["assistant", "intent", "result", "assistant"]);

        let contexts = faux.contexts();
        assert_eq!(contexts.len(), 2);
        assert_eq!(contexts[1].messages.len(), contexts[0].messages.len() + 2);
        assert!(matches!(
            contexts[1].messages[contexts[0].messages.len()].role,
            rs_ai::types::Role::Assistant
        ));
        let tool_result = contexts[1].messages.last().unwrap();
        assert_eq!(tool_result.role, rs_ai::types::Role::ToolResult);
        assert_eq!(tool_result.tool_call_id.as_deref(), Some("call-1"));
        assert_eq!(tool_result.tool_name.as_deref(), Some("echo"));
        assert!(!tool_result.is_error);

        assert_eq!(
            faux.telemetry_contexts(),
            vec![
                Some(serde_json::json!({"trace_id":"turn-1"})),
                Some(serde_json::json!({"trace_id":"turn-1"})),
            ]
        );
        let options = LegionLoop::stream_options(
            run_id,
            &RunConfig {
                system_prompt: None,
                model: "ignored/model".into(),
                budget: Budget::default(),
                tools: vec![],
                metadata: Some(serde_json::json!({"trace_id":"turn-1"})),
            },
        );
        assert_eq!(
            options.session_id.as_deref(),
            Some(run_id.to_string().as_str())
        );
        assert_eq!(
            options
                .metadata
                .as_ref()
                .and_then(|metadata| metadata.get("trace_id")),
            Some(&serde_json::json!("turn-1")),
        );
    }

    #[tokio::test]
    async fn resolve_handles_multiple_sequential_tool_rounds() {
        let (lp, store, faux, run_id) = new_faux_loop(
            "many-rounds",
            vec![
                tool_call("call-1", "echo", "message", serde_json::json!("one")),
                tool_call("call-2", "echo", "message", serde_json::json!("two")),
                text_response("all done"),
            ],
            Budget::default(),
            None,
        )
        .await;

        let final_turn = lp.resolve(run_id).await.unwrap();

        assert_eq!(
            final_turn.event.payload.as_ref().unwrap()["content"],
            "all done"
        );
        assert_eq!(faux.call_count(), 3);
        let log = store.read_log(run_id).await.unwrap();
        assert_eq!(
            log.iter()
                .filter(|entry| matches!(entry.event.kind, TurnEventKind::AssistantMessage))
                .count(),
            3
        );
        assert_eq!(
            log.iter()
                .filter(|entry| matches!(entry.event.kind, TurnEventKind::ToolResult { .. }))
                .count(),
            2
        );
    }

    #[tokio::test]
    async fn tool_errors_are_visible_to_the_followup_model_call() {
        let (lp, store, faux, run_id) = new_faux_loop(
            "tool-error",
            vec![
                tool_call("call-fail", "fail", "reason", serde_json::json!("broken")),
                text_response("recovered"),
            ],
            Budget::default(),
            None,
        )
        .await;

        lp.resolve(run_id).await.unwrap();

        let tool_result = faux.contexts()[1].messages.last().unwrap().clone();
        assert!(tool_result.is_error);
        assert_eq!(tool_result.tool_name.as_deref(), Some("fail"));
        let log = store.read_log(run_id).await.unwrap();
        let stored = log
            .iter()
            .find(|entry| matches!(entry.event.kind, TurnEventKind::ToolResult { .. }))
            .unwrap();
        assert!(
            stored.event.payload.as_ref().unwrap()["error"]
                .as_str()
                .unwrap()
                .contains("broken")
        );
    }

    #[tokio::test]
    async fn step_budget_halts_before_executing_requested_tools() {
        let (lp, store, faux, run_id) = new_faux_loop(
            "step-budget",
            vec![tool_call(
                "call-1",
                "echo",
                "message",
                serde_json::json!("never run"),
            )],
            Budget {
                max_turns: Some(1),
                ..Default::default()
            },
            None,
        )
        .await;

        let error = lp.resolve(run_id).await.unwrap_err();

        assert!(matches!(error, LegionError::BudgetExceeded(ref field) if field == "max_turns"));
        assert_eq!(faux.call_count(), 1);
        assert_eq!(
            store.session_status(run_id).await.unwrap(),
            SessionStatus::BudgetHalt {
                budget_field: "max_turns".into()
            }
        );
        let log = store.read_log(run_id).await.unwrap();
        assert!(!log.iter().any(|entry| matches!(
            entry.event.kind,
            TurnEventKind::ToolCallIntent { .. } | TurnEventKind::ToolResult { .. }
        )));
    }

    #[tokio::test]
    async fn max_tool_call_budget_allows_the_final_call_then_halts() {
        let (lp, store, faux, run_id) = new_faux_loop(
            "tool-budget",
            vec![tool_call(
                "call-1",
                "echo",
                "message",
                serde_json::json!("one allowed call"),
            )],
            Budget {
                max_tool_calls: Some(1),
                ..Default::default()
            },
            None,
        )
        .await;

        let error = lp.resolve(run_id).await.unwrap_err();

        assert!(
            matches!(error, LegionError::BudgetExceeded(ref field) if field == "max_tool_calls")
        );
        assert_eq!(faux.call_count(), 1);
        let log = store.read_log(run_id).await.unwrap();
        assert_eq!(
            log.iter()
                .filter(|entry| matches!(entry.event.kind, TurnEventKind::ToolResult { .. }))
                .count(),
            1
        );
        assert_eq!(
            store.session_status(run_id).await.unwrap(),
            SessionStatus::BudgetHalt {
                budget_field: "max_tool_calls".into()
            }
        );
    }

    #[tokio::test]
    async fn stream_resolve_uses_the_same_multi_step_engine() {
        let (model, faux) = register_faux("stream");
        faux.set_responses(vec![
            tool_call(
                "call-stream",
                "echo",
                "message",
                serde_json::json!("streamed"),
            ),
            text_response("stream complete"),
        ]);
        let store = Arc::new(MemoryEventStore::new());
        let lp = Arc::new(LegionLoop::new(
            store.clone(),
            Arc::new(EchoToolRegistry::new()),
        ));
        let run_id = lp
            .start(RunConfig {
                system_prompt: None,
                model,
                budget: Budget::default(),
                tools: vec!["echo".into()],
                metadata: None,
            })
            .await
            .unwrap();

        let mut receiver = lp.stream_resolve(run_id, "begin".into());
        let mut events = Vec::new();
        while let Some(event) = receiver.recv().await {
            events.push(event);
        }

        assert!(
            matches!(events.first(), Some(SessionEvent::ToolCall { call_id, .. }) if call_id == "call-stream")
        );
        assert!(events.iter().any(|event| matches!(event, SessionEvent::ToolResult { call_id, is_error: false, .. } if call_id == "call-stream")));
        assert!(events.iter().any(|event| matches!(event, SessionEvent::TextDelta { delta } if delta.contains("stream complete"))));
        assert!(
            matches!(events.last(), Some(SessionEvent::Done { content, .. }) if content == "stream complete")
        );
        assert!(!events.iter().any(|event| matches!(
            event,
            SessionEvent::Error { .. } | SessionEvent::BudgetHalt { .. }
        )));
        assert_eq!(faux.call_count(), 2);
        assert_eq!(
            store.session_status(run_id).await.unwrap(),
            SessionStatus::Completed
        );
    }

    #[tokio::test]
    async fn stream_budget_halt_is_terminal_without_done_or_error() {
        let (model, faux) = register_faux("stream-budget");
        faux.set_responses(vec![tool_call(
            "call-stream",
            "echo",
            "message",
            serde_json::json!("no"),
        )]);
        let store = Arc::new(MemoryEventStore::new());
        let lp = Arc::new(LegionLoop::new(store, Arc::new(EchoToolRegistry::new())));
        let run_id = lp
            .start(RunConfig {
                system_prompt: None,
                model,
                budget: Budget {
                    max_turns: Some(1),
                    ..Default::default()
                },
                tools: vec!["echo".into()],
                metadata: None,
            })
            .await
            .unwrap();

        let mut receiver = lp.stream_resolve(run_id, "begin".into());
        let mut events = Vec::new();
        while let Some(event) = receiver.recv().await {
            events.push(event);
        }

        assert!(
            matches!(events.last(), Some(SessionEvent::BudgetHalt { budget_field }) if budget_field == "max_turns")
        );
        assert!(!events.iter().any(|event| matches!(
            event,
            SessionEvent::Done { .. } | SessionEvent::Error { .. } | SessionEvent::ToolCall { .. }
        )));
    }
    #[tokio::test]
    async fn recover_fresh_session_ok() {
        let lp = echo_loop();
        let run_id = lp
            .start(RunConfig {
                system_prompt: None,
                model: "faux/test".into(),
                budget: Budget::default(),
                tools: vec![],
                metadata: None,
            })
            .await
            .unwrap();

        // No dangling writes; recover should succeed
        lp.recover(run_id).await.unwrap();
        let status = lp.store.session_status(run_id).await.unwrap();
        assert!(matches!(status, SessionStatus::Idle));
    }
}
