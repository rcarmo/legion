# Agent Loop

The agent loop (`legion-loop`) is the stateful execution engine for a single agent session. It is built directly on `rs-ai`'s `EventStream` and stores its state via the `EventStore` trait from `legion-core`.

## Design Principles

- **No LLM logic** — the loop only reacts to `rs-ai` events; it does not parse provider-specific formats
- **No storage logic** — all persistence is through the `EventStore` trait
- **Crash-resumable** — every step is write-ahead logged before execution
- **Pure state machine** — the `TurnPhase` enum drives the loop; no implicit state

## TurnPhase State Machine

```
Idle
  │ start(config)
  ▼
Running ◄──────────────────────────────────────────────────┐
  │                                                        │
  ├── build_context(last N turns)                          │
  ├── EventStore.append(ModelCallIntent) ← write-ahead     │
  ├── rs_ai.stream(history, tools) → EventStream           │
  │                                                        │
  │   Event::TextDelta       → buffer                      │
  │   Event::ThinkingDelta   → buffer                      │
  │   Event::ToolCallEnd ────► ToolPending                 │
  │                              │                         │
  │                              ├── EventStore.append(ToolCallIntent)
  │                              ├── classify_effect(name) │
  │                              ├── dispatch_tool(args)   │
  │                              └── EventStore.append(ToolResult)
  │                                    └─────────────────► │ (loop)
  │   Event::Done ───────────► Finalizing                  │
  │                              │                         │
  │                              ├── EventStore.append(AssistantMessage)
  │                              └── emit to namespace     │
  │                                    └─────────────────► Complete (terminal)
  │
  ├── budget_exceeded? ──────► BudgetHalt (terminal)
  └── should_park? ──────────► Parked
                                  │ ExternalEvent::Resume
                                  └─────────────────────► Resuming → Running
```

## Effect Classification

Tool calls are classified before dispatch. This classification is stored in the `ToolCallIntent` event so replay can skip completed steps correctly.

| Class | Meaning | Replay behaviour |
|---|---|---|
| `Read` | Side-effect free; deterministic result | Read result from log; do not re-execute |
| `Idempotent` | Safe to re-execute; same result | Re-execute on replay |
| `Write` | Non-idempotent side effect | Write-ahead logged; dangling write blocks resume |
| `LLMCall` | Model invocation | Not idempotent; re-issued on crash during call |

Tools declare their class via the `ToolDefinition`:

```rust
pub struct ToolDefinition {
    pub name:        String,
    pub description: String,
    pub parameters:  serde_json::Value,  // JSON Schema
    pub effect:      EffectClass,
}
```

## Crash Resume

On any restart, `AgentLoop::recover(run_id)` replays the committed log:

```
1. EventStore.read_log(run_id) → Vec<TurnEnvelope>
   (read_log verifies the hash chain; tampered logs fail loudly)

2. For each committed event:
   - ModelCallIntent → skip (will retry from here if incomplete)
   - ToolCallIntent  → if ToolResult follows, skip both; otherwise: dangling write
   - ToolResult      → skip
   - AssistantMessage → skip (turn complete)

3. Find last committed boundary:
   - AssistantMessage present → session was Complete before crash; nothing to do
   - ToolCallIntent without ToolResult → PendingReconciliation (block for human)
   - ModelCallIntent without Done → restart from here (re-issue LLM call)

4. Resume from last safe boundary
```

Dangling writes (write-ahead intent present but no result) transition the session to `SessionStatus::PendingReconciliation`. A human or operator tool must resolve this before the session can resume.

## Budget Enforcement

Budgets are declared in `RunConfig` and enforced at the loop level, before each LLM call and after each tool result:

```rust
pub struct Budget {
    pub max_turns:     Option<u32>,
    pub max_tokens_in: Option<u64>,
    pub max_tokens_out: Option<u64>,
    pub max_wall_ms:   Option<u64>,
    pub max_cost_usd:  Option<f64>,
}
```

Budget state accumulates in the `RunState` struct. When any limit is exceeded, the session transitions to `SessionStatus::BudgetHalt` (terminal) rather than silently truncating.

## Context Window Management

The loop calls `EventStore.read_recent(run_id, n)` to fetch the last N turns for the context window. Turn content may be stored inline (small payloads) or as iroh-blobs CIDs (large payloads). The context builder fetches large payloads on demand before passing history to rs-ai.

Context window boundary detection follows picoclaw's `context_budget.go` approach:
- Find the most recent `UserMessage` boundary within the token budget
- Prefer backward boundary (drop oldest turns) to preserve recent context
- Never split a tool call / tool result pair

## Session Forking

Because the turn log is a CAS-linked chain, sessions can be forked at any committed sequence number:

```rust
// Fork from seq 5 of session A → new session B shares history up to seq 5
let fork_id = loop.fork(run_id_a, seq_num: 5).await?;
```

This enables: prompt A/B testing, exploratory branching, rollback to a known-good decision point.

## Example Usage

```rust
let store = HiqliteStore::new(cluster_config).await?;
let tools = MyToolRegistry::new();
let loop_  = LegionLoop::new(rs_ai_provider, Arc::new(store), Arc::new(tools));

// Start a new session
let run_id = loop_.start(RunConfig {
    system_prompt: "You are a helpful assistant.".into(),
    model: "anthropic/claude-opus-4-5".into(),
    budget: Budget { max_turns: Some(20), ..Default::default() },
    ..Default::default()
}).await?;

// Inject a user message
loop_.resume(run_id, ExternalEvent::UserMessage("Hello!".into())).await?;

// Wait for completion
let result = loop_.resolve(run_id).await?;
```
