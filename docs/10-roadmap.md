# Roadmap

Legion is in early design stage, which means that I am still yelling at AI to discuss some of this even after my initial notes (_especially_ because it lost my initial notes and did this). This roadmap tracks work by milestone.

Checklist last reconciled with the implementation and test suite on 2026-09-02. A parent item remains unchecked while any of its required children are incomplete.

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
- [x] Authentication for namespace access (9P attach bearer capability, independent from REST API-key authentication)
- [x] Off-cluster, restorable backups
  - [x] Backend-neutral snapshot workflow with at least one production backend implemented
  - [x] Restic repositories supported (local, SFTP, REST, or object-storage backed); hiqlite's native S3-compatible transport remains an optional future backend
  - [x] Quiesce or use a database-consistent snapshot before restic capture; never copy live SQLite/Raft files blindly
  - [x] Documented and automated restore procedure
  - [x] Successful restore drill from a clean node, with state integrity verified
- [x] OpenTelemetry traces for agent loop steps
- [x] OpenTelemetry metrics export for token consumption
  - [x] Monotonic input, output, cache-read, and cache-write token counters where providers expose them
  - [x] Low-cardinality dimensions for provider, model, node, and outcome; never session IDs, run IDs, prompts, or user content
  - [x] OTLP configuration plus an integration test proving token usage reaches an OTEL collector
- [x] Built-in metrics endpoint: turn latency, token counts, function invocation times
- [x] `legion session reconcile` — resolve `pending_reconciliation` sessions
- [x] Rate limiting per session / per function
- [x] `legion-server` systemd unit file
- [x] Comprehensive load tests (three-node hiqlite replicated batch gate ≥24.5k inserts/s; HTTP capacity, p95, error-rate, and overload-shedding gates)

---

## Milestone 5: Agent Ecosystem

**Goal**: Multi-agent workflows, agent-as-tool composition, picoclaw channel adapters.

- [x] Agents callable as tools (`AgentProfile` registration exposes `agent.<name>` through the shared `ToolRegistry`)
- [x] picoclaw-compatible channel adapters (Telegram long polling, framework-neutral web chat, durable conversation routing)
- [x] Sub-agent sessions (verified parent sequence, durable fork, assignment injection, supervised child result/status)
- [x] Workflow graph execution (validated DAG, dependency outputs, concurrent deterministic waves, cycle rejection)
- [x] `@legion/client` npm package for Bun/Node.js (Node-targeted ESM plus declarations, authenticated REST contracts)
- [x] Dashboard UI (embedded session list/detail/log plus agents, functions, cluster and workflow views)
- [x] `legion-bun-client`: TypeScript 9P adapter for Bun functions (native 9P2000.L over opt-in loopback TCP, capability authenticated)

---

## Milestone 6: Cluster-wide Function Availability and Execution

**Goal**: A function deployed through one node becomes callable through every healthy node. Gateways resolve one replicated route, obtain the verified artifact from a known provider when necessary, and execute it on a healthy compatible node without client-side peer selection.

Current peer discovery, authenticated iroh connectivity, gossip membership, Raft storage, and explicit `/peers/<key>/...` access provide the foundations. Function manifests and routes still live in each process's in-memory namespace, deployment blobs are served only from the receiving node's local store, and normal function invocation uses that node's local runtime.

### M6.1: Replicated function registry

- [ ] Add a transport-neutral `FunctionRegistry` trait and shared function, route, and version types
- [ ] Implement the registry for single-node SQLite and distributed hiqlite
- [ ] Add a migration for complete manifest data, including runtime, version, schema, description, idempotency, environment binding names, deployment time, artifact size, and format
- [ ] Make the existing `functions` and `function_routes` tables the source of record
- [ ] Project registry data into `/fn`, `/deploy/routes`, REST, CLI, dashboard, and agent tool definitions
- [ ] Route deploy, register, promote, rollback, and undeploy operations through the registry instead of the process-local namespace
- [ ] Load the registry view after startup and after a node joins
- [ ] Use consistent reads for route-selection boundaries and local reads for listings where bounded staleness is acceptable
- [ ] Preserve manifest and route compatibility during rolling upgrades

**Gate**: Deploy through node A, restart all three nodes, then retrieve the same manifest, versions, default CID, and canary route through A, B, and C.

### M6.2: Native artifact serving and transfer

- [ ] Expose `DeployBlobStore` as an `iroh_blobs::api::Store`
- [ ] Register `iroh_blobs::BlobsProtocol` and `/iroh-bytes/4` on the shared iroh router beside 9P and gossip
- [ ] Add a downloader that accepts one or more authenticated provider endpoint IDs
- [ ] Verify every transfer against its BLAKE3 CID before making it available
- [ ] Coalesce concurrent downloads of the same CID into one transfer
- [ ] Pin active function artifacts and protect blobs with active execution leases from garbage collection
- [ ] Materialise downloads to a temporary path and atomically rename them into the CID-specific runtime path
- [ ] Remove partial downloads and materialisations after failure
- [ ] Enforce transfer, artifact-size, and disk-space limits

**Gate**: Upload an artifact only to node A, execute it on clean node B after native iroh-blobs transfer, and prove that truncated or corrupt content never executes.

### M6.3: Provider discovery and replica policy

- [ ] Maintain a durable CID-to-provider set for deployment replicas
- [ ] Advertise transient cache locality through bounded gossip data without writing every cache hit through Raft
- [ ] Remove stale providers after heartbeat expiry or failed transfer
- [ ] Try multiple known providers before returning an unavailable-artifact error
- [ ] Start with `replication = all` for active function artifacts in a three-node cluster
- [ ] Store and verify replicas before publishing a new function route
- [ ] Reconcile active CIDs when a node joins, restarts, or returns after extended downtime
- [ ] Add a configurable replication factor and placement labels after the all-node policy passes
- [ ] Report under-replicated and unavailable artifacts through health, metrics, CLI, and dashboard views

`iroh-blobs` supplies transport and multi-provider download, but not provider discovery. Legion owns provider records and reconciliation; it does not assume an iroh DHT.

**Gate**: Deploy through A, wait for route publication, stop A, then invoke the function successfully through B and C. A newly joined empty node obtains every active CID without redeployment.

### M6.4: Deterministic cluster routing

- [ ] Accept or generate a globally unique `call_id` before route selection
- [ ] Select exactly one immutable artifact CID from the replicated route and `call_id`
- [ ] Keep the selected CID across admission attempts, executor retries, and gateway retries
- [ ] Make weighted canary selection deterministic on every gateway
- [ ] Commit promote and rollback changes through hiqlite
- [ ] Define promotion at the commit boundary: calls started after a successful consistent route read use the new route; calls already started retain their selected CID
- [ ] Return `call_id`, selected CID, route revision, gateway, and executor in invocation metadata
- [ ] Reject reuse of a `call_id` with different function arguments or selected CID

**Gate**: The same `call_id` submitted through A, B, and C selects the same CID. After promotion returns, every newly started invocation uses the promoted default while in-flight calls retain their original CID.

### M6.5: Authenticated remote invocation

- [ ] Add a versioned `legion/invoke/1` ALPN to the shared iroh router
- [ ] Define bounded request, admission, result, and stable error frames
- [ ] Carry `call_id`, function name, selected CID, runtime, arguments hash, absolute deadline, idempotency, execution limits, and W3C trace context
- [ ] Authorise executor requests by authenticated iroh endpoint ID and cluster policy
- [ ] Reserve destination capacity before returning `accepted`
- [ ] Apply the strictest executor, manifest, and request limits
- [ ] Fetch and materialise a missing selected CID before execution
- [ ] Invoke the existing `BoundedInvoker` for Bun and WASM
- [ ] Return output, queue time, wall time, cache status, and executor identity
- [ ] Support explicit remote placement first, then replace direct backend calls with one `ClusterInvoker`
- [ ] Keep authenticated 9P for namespace access and diagnostics rather than using it as the scheduler transport

**Gate**: Node A explicitly invokes Bun and WASM functions on B; B enforces its own limits, fetches a missing artifact from a known provider, and reports its executor identity.

### M6.6: Membership, admission, and scheduling

- [ ] Extend `NodePresence` with protocol and runtime ABI versions, supported runtimes, capacity, inflight work, drain state, architecture, operator labels, latency, error rate, and bounded cache locality
- [ ] Expire scheduling candidates after a configured number of missed heartbeats
- [ ] Exclude unauthorised, stale, draining, incompatible, partitioned, and circuit-broken nodes
- [ ] Treat the local node as an ordinary candidate unless placement requires local execution
- [ ] Add `local`, `spread`, `affinity`, `pinned`, and `leader` placement modes
- [ ] Use power-of-two choices for default spread placement, scored by load, latency, recent errors, and artifact locality
- [ ] Use rendezvous hashing for affinity placement with a deterministic healthy fallback order
- [ ] Keep destination admission authoritative because gossip load is eventually consistent
- [ ] Return HTTP 429 with bounded retry guidance when aggregate eligible capacity is exhausted
- [ ] Do not introduce an unbounded central or per-node queue

**Gate**: Calls entering any of three nodes distribute within 20% of capacity-weighted expectation over 10,000 idempotent calls at concurrency 96, and no executor exceeds its configured per-function concurrency ceiling.

### M6.7: Retry, deduplication, and failure semantics

- [ ] Propagate one absolute deadline across connection, admission, transfer, queue, execution, and every retry
- [ ] Retry failures before executor acceptance on another eligible node
- [ ] Add bounded deduplication records and retained terminal results keyed by cluster-global `call_id`, request identity, and selected CID
- [ ] Retry idempotent calls after executor failure with the original `call_id`
- [ ] Return a stored terminal result when a gateway or client repeats a completed call
- [ ] Add durable ownership and leases before allowing failover of non-idempotent calls
- [ ] Return an explicit ambiguous outcome when an external side effect may have completed
- [ ] Never retry an ambiguous non-idempotent invocation automatically
- [ ] Expose invocation inspection and administrator-authorised reconciliation through REST, CLI, 9P, and dashboard
- [ ] Add best-effort cancellation for queued work, Bun process groups, and WASM interruption

Legion can provide deduplicated execution records. Exactly-once effects in an external system require that system to accept `call_id` as an idempotency key or participate in the transaction.

**Gate**: Killing an executor during idempotent load yields one retained result per `call_id`. An ambiguous non-idempotent failure produces one reconciliation record and no automatic second execution.

### M6.8: Draining, security, and observability

- [ ] Add drain and uncordon controls with a bounded grace period
- [ ] Reject new reservations on draining nodes while accepted work finishes
- [ ] Add an execution endpoint allow-list independent of discovery
- [ ] Resolve secrets from executor-local or cluster-managed bindings without placing secret values in gossip, requests, logs, or traces
- [ ] Bound protocol frames, inline payloads, retained results, provider lists, and locality advertisements
- [ ] Add low-cardinality metrics for scheduling, admission, remote execution, artifact transfer, cache hits, retries, deduplication, ambiguity, and per-runtime capacity
- [ ] Trace gateway routing, executor selection, admission, transfer, runtime execution, and response as one operation
- [ ] Add executor freshness, capacity, runtime support, drain state, invocation distribution, replica health, and reconciliation views to the dashboard
- [ ] Document rolling upgrade, rollback, partition recovery, replica repair, and call reconciliation procedures

**Gate**: A three-node rolling restart completes under load without routing new work to the draining node. Metrics and traces identify the gateway, executor, selected CID, route revision, attempts, queue time, cache state, and outcome without secret or high-cardinality labels.

### M6.9: Integration and capacity gates

- [ ] Add isolated three-node tests with separate endpoint IDs, ports, stores, function roots, and Raft data directories
- [ ] Test deploy-through-any-node and invoke-through-any-node for Bun and WASM
- [ ] Test clean-node artifact reconciliation and execution after the original uploader stops
- [ ] Test weighted routes, promote, rollback, and stable selection across retries
- [ ] Test overload shedding, stale heartbeat, partition, drain, mixed protocol/runtime versions, corrupt transfer, insufficient disk, executor crash, gateway crash, deadline, and cancellation
- [ ] Test x86-64 and AArch64 nodes when hardware is available; use explicit emulated capability labels in CI otherwise
- [ ] Archive machine-readable counts, capacity weights, p50/p95/p99 latency, retries, HTTP 429 responses, cache hits, transfer bytes, and errors
- [ ] Keep the existing single-node deployment and invocation paths working throughout rollout

**Exit criteria**: A function deployed through any node is durably registered, replicated according to policy, callable through any healthy gateway, and executable on any healthy compatible executor. Route changes have one replicated commit boundary, idempotent retries do not duplicate completed work, ambiguous non-idempotent outcomes require reconciliation, and the three-node capacity and failure gates pass.

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
| fjall/hiqlite storage format stability | Pin versions, test upgrades, and maintain restore-tested off-cluster backups through hiqlite snapshots or database-consistent restic captures |
| rs-ai tracking upstream pi-ai | We own both; coordinate breaking changes explicitly |
