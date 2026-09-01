# Roadmap

Legion is in early design stage, which means that I am still yelling at AI to discuss some of this even after my initial notes (_especially_ because it lost my initial notes and did this). This roadmap tracks work by milestone.

Go-port checklist reset on 2026-09-01. Rust implementation evidence remains in `main`; every item below must be re-verified in Go before completion.

## Milestone 0: Foundation

**Goal**: Go package skeleton, core interfaces, and single-node turn store working end-to-end.

- [x] Go 1.26 module scaffold with pure-Go package boundaries and `CGO_ENABLED=0` gates
- [x] `internal/core`: types + interfaces (no I/O)
  - [x] `TurnEvent`, `TurnEnvelope`, `RunId`, `SeqNum`, `SessionStatus`
  - [x] `EventStore` interface
  - [x] `AgentLoop` interface (4 verbs: start, recover, resume, resolve)
  - [x] `ToolRegistry` interface
  - [x] `Budget`, `RunConfig`, `TurnPhase`
  - [x] Test doubles (`MemoryEventStore`, `EchoToolRegistry`)
- [x] `internal/store`: pure-Go SQLite-backed `EventStore` (single-node, no Raft yet)
  - [x] `modernc.org/sqlite` (or benchmark-proven pure-Go alternative) + WAL mode
  - [x] Migrations: `0001_initial.sql`, `0002_functions.sql`
  - [x] Hash chain implementation
  - [x] `read_log` with chain verification
  - [x] `fork` implementation
- [x] `internal/agent`: Basic agent loop on `rcarmo/go-ai`
  - [x] `TurnPhase` state machine
  - [x] go-ai event-channel integration
  - [x] Write-ahead intent logging
  - [x] Effect classification
  - [x] Budget enforcement
  - [x] Crash recovery from log
- [x] Unit tests for loop + store with deterministic Go test doubles

**Exit criteria**: An agent session can run to completion and be replayed from the event log on a single node.

Verified on 2026-09-01 with Go 1.26.5 and `CGO_ENABLED=0`: `make go-check`,
`go mod verify`, and an uncached `go test -count=1 ./...` pass. The end-to-end
SQLite test runs a tool-using session to completion, closes and reopens the
database, verifies and reconstructs the complete event chain, then recovers the
terminal session without inference. A Rust-generated golden vector also fixes
the mixed-language event-envelope hash contract.

---

## Milestone 1: Cluster (networking)

**Goal**: 3-node cluster with Raft-replicated state and mDNS discovery.

- [ ] `internal/cluster`: go-iroh + mDNS bootstrap
  - [ ] `tmc/go-iroh` endpoint setup + keypair persistence
  - [ ] mixed Rust/Go direct and relay interoperability
  - [ ] Bonjour/mDNS LAN discovery and service registration
  - [ ] go-iroh gossip membership with Rust interoperability
  - [ ] Hashicorp Raft bootstrap (stable node IDs/addresses; join as nonvoter then voter)
- [ ] `internal/raftstore`: replicate the SQLite-backed store
  - [ ] versioned typed Raft commands wrapping the `EventStore`
  - [ ] `raft-boltdb/v2`/bbolt durable Raft log and stable store
  - [ ] transactional pure-Go SQLite materialized state on every voter
  - [ ] local notification mechanism for park/resume wakeup
  - [ ] leader barriers/leases for leader-only operations
- [ ] Integration test: 3-node cluster, session survives leader kill
- [ ] `cmd/legion`: Node startup sequence

**Exit criteria**: 3-node cluster forms automatically on LAN; any node can resume a session after another node crashes.

---

## Milestone 2: Namespace (9P)

**Goal**: All cluster resources accessible via 9P paths.

- [ ] `internal/namespace`: `hugelgupf/p9` 9P2000.L integration
  - [ ] `LegionNamespace` implementing the p9 server interfaces
  - [ ] All Milestone 2 path handlers (see [07-9p-namespace.md](07-9p-namespace.md))
  - [ ] Remote proxy (`/peers/<key>/...`) over authenticated iroh 9P RPC
  - [ ] Streaming/blocking reads for `/sessions/<id>/turns`
- [ ] `cmd/legion`: Expose namespace and gossip through one go-iroh QUIC endpoint
- [ ] REST API shim on port 8080
- [ ] CLI: `legion session`, `legion cluster`, `legion call`
- [ ] Integration test: authenticated 9P read/write over go-iroh, including Rust interoperability and dynamic session resources

**Exit criteria**: A human can run a full agent session using only `9p read/write` shell commands.

---

## Milestone 3: Deployment (functions)

**Goal**: Functions can be deployed as WASM or Joker bundles and invoked from the namespace.

- [ ] `internal/deploy`: CAS deployment (artifacts are stored by BLAKE3 CID and materialized locally for execution)
  - [ ] go-iroh blobs integration with Rust ticket/CID compatibility
  - [ ] `push`, `register`, `route`, `promote` commands
  - [ ] Canary weighted routing
- [ ] `internal/runtime/wasm`: general WASM executor
  - [ ] wazero + Extism Go SDK integration
  - [ ] Host functions (log, read, write, budget)
  - [ ] Context/listener-based CPU and wall-time limits (wazero has no portable fuel contract)
  - [ ] Memory limit enforcement
  - [ ] Blob fetch + local cache
- [ ] `internal/runtime/joker`: bundled `rcarmo/go-joker` executor
  - [ ] pinned Joker worker + newline-delimited JSON stdio protocol
  - [ ] Timeout + process termination
  - [ ] Environment variable injection
- [ ] CLI: `legion deploy`
- [ ] Integration tests: deploy and invoke WASM and Joker functions

**Exit criteria**: A Joker function and a WASM function can be deployed and invoked via `legion call` and via the 9P namespace.

---

## Milestone 4: Production Hardening

**Goal**: Production-ready cluster with observability, backup, and security.

- [ ] Authenticated encryption for go-iroh connections, proven interoperable with Rust Iroh endpoints
- [ ] Authentication for namespace access (9P attach bearer capability, independent from REST API-key authentication)
- [ ] Off-cluster, restorable backups
  - [ ] Backend-neutral snapshot workflow with at least one production backend implemented
  - [ ] Restic repositories supported (local, SFTP, REST, or object-storage backed)
  - [ ] Quiesce or use a database-consistent snapshot before restic capture; never copy live SQLite/Raft files blindly
  - [ ] Documented and automated restore procedure
  - [ ] Successful restore drill from a clean node, with state integrity verified
- [ ] OpenTelemetry traces for agent loop steps
- [ ] OpenTelemetry metrics export for token consumption
  - [ ] Monotonic input, output, cache-read, and cache-write token counters where providers expose them
  - [ ] Low-cardinality dimensions for provider, model, node, and outcome; never session IDs, run IDs, prompts, or user content
  - [ ] OTLP configuration plus an integration test proving token usage reaches an OTEL collector
- [ ] Built-in metrics endpoint: turn latency, token counts, function invocation times
- [ ] `legion session reconcile` — resolve `pending_reconciliation` sessions
- [ ] Rate limiting per session / per function
- [ ] Go `legion` server systemd unit file
- [ ] Comprehensive load tests (three-node Raft/SQLite replicated batch gate ≥24.5k inserts/s; HTTP capacity, p95, error-rate, and overload-shedding gates)

---

## Milestone 5: Agent Ecosystem

**Goal**: Multi-agent workflows, agent-as-tool composition, picoclaw channel adapters.

- [ ] Agents callable as tools (`AgentProfile` registration exposes `agent.<name>` through the shared `ToolRegistry`)
- [ ] picoclaw-compatible channel adapters (Telegram long polling, framework-neutral web chat, durable conversation routing)
- [ ] Sub-agent sessions (verified parent sequence, durable fork, assignment injection, supervised child result/status)
- [ ] Workflow graph execution (validated DAG, dependency outputs, concurrent deterministic waves, cycle rejection)
- [ ] `@legion/client` npm package for Bun/Node.js (Node-targeted ESM plus declarations, authenticated REST contracts)
- [ ] Dashboard UI (embedded session list/detail/log plus agents, functions, cluster and workflow views)
- [ ] 9P client compatibility: retain the TypeScript adapter for external Bun/Node consumers and add Joker namespace helpers for bundled functions

---

## Design Decisions Log

| Decision | Rationale |
|---|---|
| go-ai over a new provider abstraction | It is our direct Go pi-ai port and already has compatible streaming, tools, reasoning, provider and token-usage contracts |
| Hashicorp Raft + pure-Go SQLite over rqlite/dqlite | Preserves an embedded typed state machine while enforcing `CGO_ENABLED=0`; rqlite and dqlite depend on C SQLite components |
| bbolt for Raft log/stable storage | Mature pure-Go backend; SQLite remains the queryable materialized state and snapshot payload |
| tmc/go-iroh over immediate libp2p substitution | Preserves Iroh identities, QUIC/relay behavior, blobs and gossip; mixed Rust/Go interop is a mandatory gate |
| hugelgupf/p9 over custom RPC | Maintained pure-Go 9P2000.L client/server is the direct Jetstream protocol match |
| Extism Go SDK + wazero | Preserves the Extism guest ABI while using a pure-Go runtime |
| Joker over Bun | Owned pure-Go Lisp runtime with I/O namespaces and internal wazero optimization; initially isolated as supervised workers |
| picoclaw as channel/gateway reference | Mature Go channel lifecycle and normalization patterns, without introducing a second inference abstraction |
| salvor as durability reference | Event sourcing and four-verb loop remain language-independent design constraints |

## Known Risks

| Risk | Mitigation |
|---|---|
| go-iroh v0.1 maturity | Pin a reviewed commit, isolate it behind a transport interface, and require mixed Rust/Go direct, relay, blob and gossip gates |
| Joker process-global environment | Use supervised worker processes first; permit warm/in-process pools only after isolation and race evidence |
| Joker EPL-1.0 distribution obligations | Keep a separately identifiable bundled runtime, ship notices and revision/source pointers, and publish modifications to covered files |
| Raft/SQLite dual-store consistency | Replicate typed commands, apply one SQLite transaction per FSM command, and verify snapshot/restore plus hash chains under failover |
| Pure-Go SQLite performance/binary size | Benchmark modernc against ncruces before final selection without weakening `CGO_ENABLED=0` |
| go-ai tracking pi-ai | We own the port; pin reviewed revisions and coordinate schema/event changes with golden compatibility fixtures |
