# Turn Store

The turn store is the event log for all agent sessions. It is implemented in `legion-store` via the `EventStore` trait defined in `legion-core`.

## Design Goals

- **Append-only** — turns are never updated or deleted, only appended
- **Hash-chained** — each turn references its predecessor's hash; tampering is detectable
- **Distributed** — replicated via hiqlite (openraft + SQLite) across all cluster nodes
- **Large-payload friendly** — turn content is stored inline for small payloads or as iroh-blobs CIDs for large ones
- **Fork-capable** — sessions can branch at any sequence number (same CAS chain semantics as git)

## Schema

### `turns` table

```sql
CREATE TABLE turns (
    run_id      TEXT    NOT NULL,
    seq         INTEGER NOT NULL,
    prev_hash   BLOB    NOT NULL,   -- SHA-256 of previous TurnEnvelope (0x00*32 for seq=0)
    kind        TEXT    NOT NULL,   -- see TurnEventKind
    payload     JSONB,              -- inline content (small payloads)
    payload_cid TEXT,               -- iroh-blobs CID (large content; NULL if inline)
    model       TEXT,               -- model used (NULL for non-LLM turns)
    tokens_in   INTEGER,
    tokens_out  INTEGER,
    wall_ms     INTEGER,
    created_at  INTEGER NOT NULL,   -- Unix timestamp ms
    PRIMARY KEY (run_id, seq)
);

CREATE INDEX idx_turns_run_status ON turns(run_id, kind);
```

### `sessions` table

```sql
CREATE TABLE sessions (
    run_id      TEXT    PRIMARY KEY,
    parent_run  TEXT,               -- non-NULL if this is a fork
    fork_seq    INTEGER,            -- seq of parent at fork point
    status      TEXT    NOT NULL,   -- SessionStatus
    config      JSONB   NOT NULL,   -- RunConfig (system prompt, model, budget)
    created_at  INTEGER NOT NULL,
    updated_at  INTEGER NOT NULL
);
```

### `functions` table

```sql
CREATE TABLE functions (
    name        TEXT    NOT NULL,
    cid         TEXT    NOT NULL,   -- iroh-blobs content hash
    runtime     TEXT    NOT NULL,   -- 'wasm' | 'bun'
    schema      JSONB,              -- input/output JSON Schema
    version     TEXT,               -- semver string (informational)
    created_at  INTEGER NOT NULL,
    PRIMARY KEY (name, cid)
);

CREATE TABLE function_routes (
    name        TEXT    PRIMARY KEY,
    default_cid TEXT    NOT NULL,   -- current default CID for this name
    routes      JSONB               -- canary/weighted routing config
);
```

## TurnEventKind

```rust
pub enum TurnEventKind {
    // User input
    UserMessage,
    // Write-ahead intents
    ModelCallIntent,
    ToolCallIntent,
    // Completions
    AssistantMessage,
    ToolResult,
    // Control
    SessionStarted,
    SessionForked,
    SessionParked { reason: ParkReason },
    SessionResumed,
    SessionCompleted,
    SessionBudgetHalt { budget_field: String },
    SessionPendingReconciliation { tool_name: String, call_id: String },
}
```

## Hash Chain

Each turn envelope is hashed before storing, and the hash becomes the `prev_hash` of the next turn:

```
seq=0: prev_hash = [0u8; 32]
       envelope  = TurnEnvelope { seq: 0, run_id, kind, payload, ... }
       hash      = sha256(cbor_encode(envelope))

seq=1: prev_hash = hash(seq=0)
       ...
```

`EventStore::read_log` recomputes the chain before returning events. Any gap or hash mismatch returns `StoreError::TamperEvident`. This is purely for integrity detection — it does not replace Raft's own ordering guarantees.

## Large Payload Handling

Payloads above a configurable threshold (default: 8KB) are stored in iroh-blobs:

```
Turn with large payload:
  payload     = NULL
  payload_cid = "bafkrei..."   ← iroh-blobs CID

Retrieval:
  1. Read turn from SQLite
  2. If payload_cid is non-NULL: fetch blob from iroh-blobs
  3. Decode and return
```

This keeps the SQLite WAL small and enables content-deduplication across turns (same image referenced in multiple turns = stored once).

## EventStore Trait

```rust
#[async_trait]
pub trait EventStore: Send + Sync {
    /// Append an event; returns the assigned sequence number.
    async fn append(&self, run_id: RunId, event: TurnEvent) -> Result<SeqNum>;

    /// Read the full log (verifies hash chain).
    async fn read_log(&self, run_id: RunId) -> Result<Vec<TurnEnvelope>>;

    /// Read the last N turns (optimised; no chain verification).
    async fn read_recent(&self, run_id: RunId, n: usize) -> Result<Vec<TurnEnvelope>>;

    /// Get current session status.
    async fn session_status(&self, run_id: RunId) -> Result<SessionStatus>;

    /// Transition session status.
    async fn set_status(&self, run_id: RunId, status: SessionStatus) -> Result<()>;

    /// Fork a session at a given sequence number.
    async fn fork(&self, run_id: RunId, at_seq: SeqNum) -> Result<RunId>;

    /// List sessions matching a filter.
    async fn list_sessions(&self, filter: SessionFilter) -> Result<Vec<SessionSummary>>;
}
```

## Consistency Model

All writes go through the hiqlite leader:
- `EXECUTE` queries (writes): forwarded to leader over the Raft network; committed before returning
- `QUERY` queries (reads): served locally from any node by default; use `query_consistent` for linearizable reads

For the agent loop, all `EventStore::append` calls use hiqlite's strongly-consistent execute path. `read_recent` for context window building uses the local (eventually consistent) path — a slightly stale context is acceptable; a stale write-ahead log is not.

## Migrations

Migrations live in `crates/legion-store/src/migrations/` as numbered SQL files:

```
0001_initial.sql       -- turns, sessions
0002_functions.sql     -- functions, function_routes
0003_cluster_state.sql -- peer registry, leader cache
```

hiqlite applies migrations automatically on startup. All nodes run the same migration set; Raft ensures they execute in order.
