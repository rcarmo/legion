# Roadmap

Legion is in early design stage, which means that I am still yelling at AI to discuss some of this even after my initial notes (_especially_ because it lost my initial notes and did this). This roadmap tracks work by milestone.

Checklist last reconciled with the implementation and test suite on 2026-08-31. A parent item remains unchecked while any of its required children are incomplete.

## Milestone 0: Foundation

**Goal**: Crate skeleton, core traits, and single-node turn store working end-to-end.

- [x] Cargo workspace scaffold with all 8 crates
- [x] `legion-core`: types + traits (no I/O)
  - [x] `TurnEvent`, `TurnEnvelope`, `RunId`, `SeqNum`, `SessionStatus`
  - [x] `EventStore` trait
  - [x] `AgentLoop` trait (4 verbs: start, recover, resume, resolve)
  - [x] `ToolRegistry` trait
  - [x] `Budget`, `RunConfig`, `TurnPhase`
  - [x] Test doubles (`MemoryEventStore`, `EchoToolRegistry`)
- [x] `legion-store`: SQLite-backed `EventStore` (single-node, no Raft yet)
  - [x] `rusqlite` + WAL mode
  - [x] Migrations: `0001_initial.sql`, `0002_functions.sql`
  - [x] Hash chain implementation
  - [x] `read_log` with chain verification
  - [x] `fork` implementation
- [x] `legion-loop`: Basic agent loop on rs-ai
  - [x] `TurnPhase` state machine
  - [x] rs-ai `EventStream` integration
  - [x] Write-ahead intent logging
  - [x] Effect classification
  - [x] Budget enforcement
  - [x] Crash recovery from log
- [x] Unit tests for loop + store with `MemoryEventStore`

**Exit criteria**: An agent session can run to completion and be replayed from the event log on a single node.

---

## Milestone 1: Cluster (networking)

**Goal**: 3-node cluster with Raft-replicated state and mDNS discovery.

- [x] `legion-cluster`: iroh + mDNS bootstrap
  - [x] iroh endpoint setup + keypair persistence
  - [x] `iroh-mdns-address-lookup` integration
  - [x] `mdns-sd` Bonjour registration
  - [x] `iroh-gossip` membership
  - [x] Raft bootstrap logic (stable node IDs/addresses advertised via mDNS; hiqlite joins as learner then voter)
- [x] `legion-store`: Swap SQLite for hiqlite
  - [x] hiqlite `Client` wrapping
  - [x] fjall as Raft log store (through hiqlite)
  - [x] All `EventStore` methods over hiqlite
  - [x] `listen_notify_local` for park/resume wakeup
  - [x] `dlock` for leader-only operations
- [x] Integration test: 3-node cluster, session survives leader kill
- [x] `legion-server`: Node startup sequence

**Exit criteria**: 3-node cluster forms automatically on LAN; any node can resume a session after another node crashes.

---

## Milestone 2: Namespace (9P)

**Goal**: All cluster resources accessible via 9P paths.

- [x] `legion-namespace`: jetstream 9P2000.L integration
  - [x] `LegionNamespace` implementing jetstream `NineP200L`
  - [x] All Milestone 2 path handlers (see [07-9p-namespace.md](07-9p-namespace.md))
  - [x] Remote proxy (`/peers/<key>/...`) over authenticated iroh 9P RPC
  - [x] Streaming/blocking reads for `/sessions/<id>/turns`
- [x] `legion-server`: Expose namespace and gossip through one iroh QUIC router
- [x] REST API shim on port 8080
- [x] CLI: `legion session`, `legion cluster`, `legion call`
- [x] Integration test: authenticated 9P read/write over iroh (including dynamic session resources)

**Exit criteria**: A human can run a full agent session using only `9p read/write` shell commands.

---

## Milestone 3: Deployment (functions)

**Goal**: Functions can be deployed as WASM or Bun bundles and invoked from the namespace.

- [x] `legion-deploy`: CAS deployment (artifacts are stored by BLAKE3 CID and materialized locally for execution)
  - [x] iroh-blobs integration
  - [x] `push`, `register`, `route`, `promote` commands
  - [x] Canary weighted routing
- [x] `legion-runtime`: WASM executor
  - [x] wasmtime + extism integration
  - [x] Host functions (log, read, write, budget)
  - [x] Fuel-based CPU limit
  - [x] Memory limit enforcement
  - [x] Blob fetch + local cache
- [x] `legion-runtime`: Bun executor
  - [x] Subprocess spawn + stdio protocol
  - [x] Timeout + process termination
  - [x] Environment variable injection
- [x] CLI: `legion deploy`
- [x] Integration tests: deploy and invoke WASM and Bun functions

**Exit criteria**: A Bun function and a WASM function can be deployed and invoked via `legion call` and via the 9P namespace.

---

## Milestone 4: Production Hardening

**Goal**: Production-ready cluster with observability, backup, and security.

- [x] Authenticated encryption for iroh connections (built into iroh QUIC endpoints)
- [ ] Authentication for namespace access (capability tokens; REST API-key authentication exists)
- [ ] Off-cluster, restorable backups
  - [ ] Automated encrypted state snapshots to storage outside the Legion cluster (initially via hiqlite's S3-compatible backup transport)
  - [ ] Documented and automated restore procedure
  - [ ] Successful restore drill from a clean node, with state integrity verified
- [ ] OpenTelemetry traces for agent loop steps
- [x] Metrics: turn latency, token counts, function invocation times
- [x] `legion session reconcile` — resolve `pending_reconciliation` sessions
- [x] Rate limiting per session / per function
- [x] `legion-server` systemd unit file
- [ ] Comprehensive load tests (hiqlite bench: 24.5k inserts/s target)

---

## Milestone 5: Agent Ecosystem

**Goal**: Multi-agent workflows, agent-as-tool composition, picoclaw channel adapters.

- [ ] Agents callable as tools (function registration of agent sessions)
- [ ] picoclaw-compatible channel adapters (Telegram, web chat)
- [ ] Sub-agent sessions (fork + supervised child runs)
- [ ] Workflow graph execution (inspired by salvor's `run_graph`)
- [ ] `@legion/client` npm package for Bun/Node.js
- [ ] Dashboard UI (hiqlite's built-in dashboard extended with session view)
- [ ] `legion-bun-client`: TypeScript 9P adapter for Bun functions

---

## Design Decisions Log

| Decision | Rationale |
|---|---|
| rs-ai over rig | We own rs-ai; tracks pi-ai API; no breaking change risk; already has all needed providers |
| hiqlite over raw openraft + SQLite | hiqlite provides the full integration (Raft + SQLite + migrations + dlock) as one crate |
| fjall over RocksDB | Pure Rust, no C deps, LSM suited to append-heavy Raft log |
| iroh over libp2p | Public-key routing, built-in NAT traversal, simpler API, mDNS plugin from same org |
| jetstream over custom RPC | 9P is a proven minimal protocol; jetstream has iroh transport built in |
| extism over raw wasmtime | Typed PDK, multiple guest languages, lower authoring friction |
| Bun over Deno | Already used throughout the existing stack; Bun FFI for native integration |
| picoclaw as reference | 30K-star validated agent loop design in Go; picoclaw-rs port has matching module structure |
| salvor as reference | Best-in-class event sourcing design for single-node durable execution; EventStore trait pattern |

## Known Risks

| Risk | Mitigation |
|---|---|
| jetstream "not production-ready" | Pin version; contribute upstream or fork if needed |
| openraft alpha version label | Deployed in production at Databend; label is cosmetic |
| Bun subprocess overhead | ~50-200ms cold start; mitigate with warm pool (future milestone) |
| fjall/hiqlite storage format stability | Pin versions, test upgrades, and maintain restore-tested off-cluster backups (initially through hiqlite's S3-compatible transport) |
| rs-ai tracking upstream pi-ai | We own both; coordinate breaking changes explicitly |
