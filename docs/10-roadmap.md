# Roadmap

Legion is in early design stage. This roadmap tracks work by milestone.

## Milestone 0: Foundation (current)

**Goal**: Crate skeleton, core traits, and single-node turn store working end-to-end.

- [ ] Cargo workspace scaffold with all 8 crates
- [ ] `legion-core`: types + traits (no I/O)
  - [ ] `TurnEvent`, `TurnEnvelope`, `RunId`, `SeqNum`, `SessionStatus`
  - [ ] `EventStore` trait
  - [ ] `AgentLoop` trait (4 verbs: start, recover, resume, resolve)
  - [ ] `ToolRegistry` trait
  - [ ] `Budget`, `RunConfig`, `TurnPhase`
  - [ ] Test doubles (`MemoryEventStore`, `EchoToolRegistry`)
- [ ] `legion-store`: SQLite-backed `EventStore` (single-node, no Raft yet)
  - [ ] `rusqlite` + WAL mode
  - [ ] Migrations: `0001_initial.sql`, `0002_functions.sql`
  - [ ] Hash chain implementation
  - [ ] `read_log` with chain verification
  - [ ] `fork` implementation
- [ ] `legion-loop`: Basic agent loop on rs-ai
  - [ ] `TurnPhase` state machine
  - [ ] rs-ai `EventStream` integration
  - [ ] Write-ahead intent logging
  - [ ] Effect classification
  - [ ] Budget enforcement
  - [ ] Crash recovery from log
- [ ] Unit tests for loop + store with `MemoryEventStore`

**Exit criteria**: An agent session can run to completion and be replayed from the event log on a single node.

---

## Milestone 1: Cluster (networking)

**Goal**: 3-node cluster with Raft-replicated state and mDNS discovery.

- [ ] `legion-cluster`: iroh + mDNS bootstrap
  - [ ] iroh endpoint setup + keypair persistence
  - [ ] `iroh-mdns-address-lookup` integration
  - [ ] `mdns-sd` Bonjour registration
  - [ ] `iroh-gossip` membership
  - [ ] Raft bootstrap logic (join vs. single-node)
- [ ] `legion-store`: Swap SQLite for hiqlite
  - [ ] hiqlite `Client` wrapping
  - [ ] fjall as Raft log store
  - [ ] All `EventStore` methods over hiqlite
  - [ ] `listen_notify_local` for park/resume wakeup
  - [ ] `dlock` for leader-only operations
- [ ] Integration test: 3-node cluster, session survives leader kill
- [ ] `legion-server`: Node startup sequence

**Exit criteria**: 3-node cluster forms automatically on LAN; any node can resume a session after another node crashes.

---

## Milestone 2: Namespace (9P)

**Goal**: All cluster resources accessible via 9P paths.

- [ ] `legion-namespace`: jetstream integration
  - [ ] `LegionNamespace` implementing jetstream `FileSystem`
  - [ ] All path handlers (see [07-9p-namespace.md](07-9p-namespace.md))
  - [ ] Remote proxy (`/peers/<key>/...`)
  - [ ] Streaming reads for `/sessions/<id>/turns`
- [ ] `legion-server`: Expose namespace on iroh QUIC transport
- [ ] REST API shim on port 8080
- [ ] CLI: `legion session`, `legion cluster`, `legion call`
- [ ] Integration test: full session via 9P read/write

**Exit criteria**: A human can run a full agent session using only `9p read/write` shell commands.

---

## Milestone 3: Deployment (functions)

**Goal**: Functions can be deployed as WASM or Bun bundles and invoked from the namespace.

- [ ] `legion-deploy`: CAS deployment
  - [ ] iroh-blobs integration
  - [ ] `push`, `register`, `route`, `promote` commands
  - [ ] Canary weighted routing
- [ ] `legion-runtime`: WASM executor
  - [ ] wasmtime + extism integration
  - [ ] Host functions (log, read, write, budget)
  - [ ] Fuel-based CPU limit
  - [ ] Memory limit enforcement
  - [ ] Blob fetch + local cache
- [ ] `legion-runtime`: Bun executor
  - [ ] Subprocess spawn + stdio protocol
  - [ ] Timeout + SIGKILL
  - [ ] Environment variable injection
- [ ] CLI: `legion deploy`
- [ ] Integration test: deploy and invoke WASM + Bun functions

**Exit criteria**: A Bun function and a WASM function can be deployed and invoked via `legion call` and via the 9P namespace.

---

## Milestone 4: Production Hardening

**Goal**: Production-ready cluster with observability, backup, and security.

- [ ] TLS for all iroh connections (built-in; verify config)
- [ ] Authentication for namespace access (capability tokens)
- [ ] hiqlite S3 backup integration
- [ ] OpenTelemetry traces for agent loop steps
- [ ] Metrics: turn latency, token counts, function invocation times
- [ ] `legion session reconcile` — resolve `pending_reconciliation` sessions
- [ ] Rate limiting per session / per function
- [ ] `legion-server` systemd unit file
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
| fjall/hiqlite storage format stability | Pin versions; test upgrades; S3 backup for recovery |
| rs-ai tracking upstream pi-ai | We own both; coordinate breaking changes explicitly |
