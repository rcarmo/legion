# Pure-Go Port Analysis

**Status:** proposed baseline for branch `go`

**Date:** 2026-09-01

**Constraint:** the Legion server and bundled runtimes must build with `CGO_ENABLED=0`.

This branch is a clean Go reimplementation of Legion's contracts. The Rust implementation remains on `main` as an executable specification and interoperability oracle. A roadmap item is complete only after the corresponding Go behavior has its own test evidence.

## Conclusions

1. Use direct ports and wire-compatible implementations wherever feasible.
2. Use [`rcarmo/go-ai`](https://github.com/rcarmo/go-ai) as the sole inference/provider abstraction. It is the direct Go counterpart of `rs-ai` and preserves pi-ai-compatible messages, streaming events, tools, usage and costs.
3. Use [`tmc/go-iroh`](https://github.com/tmc/go-iroh) as the preferred Iroh implementation. It is a clean-room, pure-Go implementation with Rust wire interoperability for endpoints, relay/NAT traversal, blobs and gossip. Keep a transport interface and a go-libp2p contingency until mixed Rust/Go cluster tests pass.
4. Use [`hugelgupf/p9`](https://github.com/hugelgupf/p9) for 9P2000.L. It is the maintained direct protocol match, extracted from gVisor.
5. Replace Bun functions with bundled [`rcarmo/go-joker`](https://github.com/rcarmo/go-joker) workers. Joker is pure Go, already includes rich I/O/HTTP namespaces, and internally uses wazero for optimized Joker IR.
6. Keep general WASM functions distinct from Joker's internal optimizer. Use `wazero` directly, with `extism/go-sdk` for the existing Extism ABI where appropriate. Both are pure Go.
7. Replace hiqlite with a typed Hashicorp Raft state machine whose durable Raft log uses bbolt and whose materialized query view uses pure-Go SQLite. Do not use rqlite or dqlite: current rqlite builds on a C SQLite fork, and dqlite requires its C library.

## Direct-match matrix

| Rust/current component | Go choice | Match | Pure Go | Confidence and required proof |
|---|---|---:|---:|---|
| `rs-ai` | `github.com/rcarmo/go-ai` | Direct owned port | Yes | **High.** Channel-based events cover text, thinking, tool calls, done/error; messages and JSON schemas track pi-ai; usage includes input, output, cache-read and cache-write. Prove deterministic faux-provider replay and tool-call recovery. |
| `iroh`, `iroh-blobs`, `iroh-gossip` | `github.com/tmc/go-iroh` | Clean-room wire-compatible port | Yes | **Medium.** v0.1.0, active, Rust vectors/live interop tests, endpoint/relay/NAT, blobs, gossip and DNS packages. Prove mixed Rust/Go dialing, relay fallback, blob transfer and gossip before removing the transport contingency. Legion must still provide LAN mDNS service advertisement if the library does not. |
| Jetstream 9P2000.L | `github.com/hugelgupf/p9` | Exact protocol implementation | Yes | **High.** Maintained v0.4.1, Apache-2.0, extracted from gVisor. Prove Legion path semantics and Rust/Bun 9P client interoperability; transport it over go-iroh streams and loopback TCP. |
| hiqlite/openraft/fjall | `github.com/hashicorp/raft` + `raft-boltdb/v2` + pure-Go SQLite | Architectural equivalent | Yes | **Medium-high.** Hashicorp Raft is mature and exposes FSM/snapshot/restore. Replicate typed commands, not arbitrary SQL. Apply commands transactionally to each node's SQLite materialized view. Prove leader failover, barriers, snapshots and deterministic hash chains. |
| `rusqlite` | `modernc.org/sqlite` | Driver equivalent | Yes | **High.** No CGO. Use WAL for the single-node store and the same schema/query code behind the Raft FSM. Keep `ncruces/go-sqlite3` as a benchmark alternative if modernc binary size or performance is unacceptable. |
| iroh content IDs / BLAKE3 | `tmc/go-iroh/blobs` + `lukechampine.com/blake3` | Wire-compatible | Yes | **Medium.** Prefer go-iroh blob/ticket types to preserve mixed-cluster compatibility. Prove CID/ticket vectors and on-demand cross-language fetch. |
| `wasmtime` | `github.com/tetratelabs/wazero` | Runtime equivalent | Yes | **High.** Mature v1.12, compiler/interpreter modes, no CGO. Prove fuel-equivalent execution limits with context deadlines/listeners and memory-page caps. |
| Extism Rust SDK | `github.com/extism/go-sdk` | Official SDK | Yes | **High.** Uses wazero, not Wasmtime/CGO. Pin a version compatible with the root wazero version and prove the existing fixture ABI, host functions, timeout and memory limits. |
| Bun runtime | bundled `github.com/rcarmo/go-joker` | Deliberate replacement | Yes | **Medium-high.** Active owned fork, EPL-1.0, Go 1.26. Joker's evaluator is currently coupled to `GLOBAL_ENV`; isolate executions in supervised worker processes first. In-process pools require an upstream per-runtime environment API and race tests. |
| Bun's use of WASM | Joker's internal wazero compiler | Functional replacement for Joker optimization | Yes | **High within Joker only.** Joker translates eligible numeric IR to WASM and caches wazero modules. This is not a general Extism function host and must not be exposed as one. |
| Axum/Tokio | `net/http` + `go-chi/chi/v5`; goroutines/contexts | Idiomatic equivalent | Yes | **High.** Keep handlers thin and preserve REST/SSE contracts. Prefer standard library primitives over framework-specific state. |
| Clap | `spf13/cobra` | CLI equivalent | Yes | **High.** Preserve command names, JSON output and exit codes. |
| `serde`/`serde_json` | `encoding/json` | Standard equivalent | Yes | **High.** Add golden compatibility fixtures generated by `main`. |
| UUID | `google/uuid` | Direct equivalent | Yes | **High.** UUID values remain wire-compatible strings/16-byte values. |
| BLAKE3 | `lukechampine.com/blake3` | Direct algorithm | Yes | **High.** Cross-check fixed vectors with Rust. |
| OpenTelemetry Rust | `go.opentelemetry.io/otel` | Official implementation | Yes | **High.** Preserve span names and low-cardinality token metric labels; collector smoke test remains mandatory. |
| Prometheus metrics | `prometheus/client_golang` | Standard implementation | Yes | **High.** Preserve endpoint names where practical. |
| Restic scripts | unchanged external `restic` orchestration | Same backend | N/A server dependency | **High.** Preserve service quiescing, manifests and clean-node restore drill. |
| systemd | same unit and shell tooling | Same platform | N/A | **High.** Change executable path and add Go runtime hardening only after a live install smoke. |
| TypeScript REST client/channels | retain existing packages | Wire client, language-neutral | Yes for server | **High.** They target HTTP and remain useful to Node/browser clients even though Bun is no longer a function runtime. Add Go client only if a concrete consumer needs it. |

## Storage design

There is no pure-Go, embeddable, drop-in hiqlite port. The closest products fail at least one constraint:

- **rqlite** is operationally mature but is a separate server architecture and currently replaces `mattn/go-sqlite3` with `rqlite/go-sqlite3`; the SQLite C code remains. It violates `CGO_ENABLED=0` and would add a second service boundary.
- **dqlite/go-dqlite** is embeddable but requires Canonical's C dqlite library.
- **Dragonboat** is pure Go and fast, but is a larger multi-Raft system than Legion requires and would not preserve hiqlite's SQL behavior by itself.

The recommended store is:

```text
EventStore method
  -> validate and encode versioned Command (JSON or deterministic binary)
  -> hashicorp/raft.Apply(command)
  -> FSM.Apply on every voter
  -> one pure-Go SQLite transaction updates sessions/turns/functions/cluster_state
  -> response returned from leader
```

Rules:

- Never replicate arbitrary SQL strings. Replicate versioned typed commands so all nodes execute the same state transition.
- Reads that require linearizability call `raft.Barrier`; stale/local reads are explicit.
- Raft logs/stable state use `raft-boltdb/v2` (bbolt). SQLite is the queryable materialized state and snapshot payload, not the Raft log.
- FSM snapshots use SQLite's online backup/snapshot facilities into an immutable temporary file, then stream it through Raft's snapshot sink. Restore replaces state only while the store is quiesced.
- Schema migrations are embedded, numbered SQL files and execute before joining/serving. A protocol/schema compatibility version is advertised during cluster join.

## Networking design

Define a narrow internal transport interface (`Identity`, `Listen`, `Dial`, `Discover`, `BlobStore`, `Gossip`) and implement it first with go-iroh. This keeps Iroh semantics while containing v0.1 API churn.

Mandatory go-iroh proof gates:

1. Go endpoint dials a Rust Legion/Iroh endpoint by public key and ALPN.
2. Rust endpoint dials Go endpoint directly and through a relay.
3. Forced relay fallback succeeds; direct address loss/recovery does not split identity.
4. Rust and Go exchange a fixed BLAKE3 blob/ticket in both directions.
5. Rust and Go join one gossip topic and exchange membership records.
6. LAN bootstrap finds peers. If go-iroh lacks compatible mDNS address discovery, Legion owns a small `zeroconf`/mDNS adapter that publishes signed endpoint information; do not replace the rest of Iroh.

Fallback is `go-libp2p` only if those interop gates fail. It is not a direct protocol replacement and would create a Go-only cluster boundary.

## Inference and agent loop

`go-ai` replaces `rs-ai` directly. Legion's loop consumes its event channel and persists write-ahead intents before external effects:

| Legion phase | go-ai event/data |
|---|---|
| model call starts | `StartEvent` |
| streamed answer | text start/delta/end events |
| hidden reasoning | thinking start/delta/end events |
| function request | `ToolCallStartEvent`, delta, `ToolCallEndEvent` |
| completion | `DoneEvent` with `Message`, stop reason and `Usage` |
| failure | `ErrorEvent` with partial message where available |

`go-ai.Usage` already exposes input, output, cache-read, cache-write, reasoning and cost fields. No Picoclaw provider adapter is needed. Picoclaw remains a reference for channel normalization, gateway lifecycle and configuration—not a second inference layer.

## Joker runtime

### Initial execution model

Build a pinned `go-joker` worker binary as part of Legion's release and supervise it with `os/exec`:

- one newline-delimited JSON request/response envelope over stdin/stdout;
- a fresh process per invocation initially, followed by a measured warm-worker pool;
- deadline via `context.Context`, process-group termination on timeout, bounded stdout/stderr and temporary directory;
- explicit allowlisted environment and filesystem roots;
- Legion host operations through authenticated loopback 9P or a narrow RPC bridge;
- artifact contains source plus manifest; runtime version and go-joker commit are recorded in deployment metadata.

This is pure Go and avoids cross-session contamination from Joker's process-global `GLOBAL_ENV` and namespace state. In-process embedding is deferred until go-joker exposes an independently constructible runtime/environment with cancellation and concurrent isolation.

### WASM relationship

Joker already uses wazero v1.12 internally to compile eligible Joker numeric IR. Legion should benefit from that automatically, but must not route arbitrary `.wasm`/Extism modules through Joker's private `WasmProgram` APIs. The general WASM executor independently owns:

- module validation and compilation cache;
- WASI policy;
- memory limits;
- wall-clock cancellation and deterministic host calls;
- Extism host ABI compatibility;
- result and log capture.

A later optimization may share a process-wide wazero compilation cache only if Joker exposes a supported injection point.

### License consequence

`go-joker` is EPL-1.0. Bundling or modifying it requires preserving its license/notices and making modifications to EPL-covered files available under EPL terms. Keep it as a pinned, separately identifiable worker/module boundary; do not copy its source into Legion packages. Confirm release artifacts include notices and source/revision pointers.

## Proposed Go module layout

```text
cmd/legion/             server and CLI entrypoint
internal/core/          pure types and interfaces; no disk/network imports
internal/store/         SQLite store, migrations, hash chain
internal/raftstore/     Hashicorp Raft FSM, snapshots, membership
internal/agent/         go-ai event loop, recovery, budgets
internal/namespace/     p9 filesystem and resource handlers
internal/cluster/       go-iroh adapter, discovery and membership
internal/runtime/       executor interface
internal/runtime/joker/ supervised bundled Joker workers
internal/runtime/wasm/  wazero and Extism executor
internal/deploy/        go-iroh blob CAS, registration and routing
internal/ecosystem/     agent tools, child runs and workflow DAGs
internal/api/           REST/SSE handlers, auth and dashboard
packages/               retained TypeScript REST/channel clients
```

Only `internal/core` defines contracts. It must not import SQLite, Raft, go-iroh, p9, go-ai providers, Joker or wazero.

## Toolchain and dependency policy

- Baseline **Go 1.26** because current go-joker and go-iroh require it; go-ai itself requires Go 1.24.
- `CGO_ENABLED=0` is set in every build/test target and CI job.
- Pin go-ai, go-iroh and go-joker to reviewed commits until stable versioning is sufficient. Record those revisions in `docs/` and deployment metadata.
- Run `govulncheck`, license checks and a CycloneDX SBOM before release milestones.
- Keep interfaces around go-iroh and Joker because they are the youngest/highest-risk dependencies.

## Implementation order and proof gates

### Phase A — compatibility spike

- Scaffold the Go module and pure `internal/core` types/interfaces.
- Golden-decode Rust event logs and re-encode byte-for-byte compatible JSON.
- Consume a deterministic go-ai faux stream, including thinking and tool-call events.
- Execute one Joker function through the worker envelope with timeout/kill tests.
- Execute existing Extism/WASM fixtures under wazero with memory/time limits.
- Mount the minimal namespace with p9 and read it using the existing Bun 9P client.
- Complete all six mixed Rust/Go go-iroh interop gates.

Do not start the full port until the compatibility spike resolves any wire/schema changes.

### Phase B — Milestone 0 single node

Implement pure core contracts, SQLite EventStore, hash verification/fork, go-ai loop, recovery and deterministic test doubles. Exit criterion remains replayable single-node sessions.

### Phase C — clustering and namespace

Add typed Raft FSM/snapshot/restore, go-iroh identity/discovery/gossip/blob transport, then p9 namespace over go-iroh and loopback TCP. Prove three-node failover and mixed-language protocol compatibility.

### Phase D — runtimes and ecosystem

Add Joker deployment/execution, general wazero/Extism execution, CAS routing, REST/CLI, agent tools, supervised children, workflows, channels and dashboard. Preserve external HTTP/9P schemas unless a versioned migration is documented.

### Phase E — hardening

Repeat the Rust implementation's authentication, telemetry, backup/restore, reconciliation, rate-limit and load gates with Go-specific race, fuzz, SBOM, vulnerability and license checks.

## Decisions still requiring measurements

1. `modernc.org/sqlite` versus `ncruces/go-sqlite3`: benchmark event append/read, snapshot size, binary size and memory. Default to modernc for conventional `database/sql` behavior.
2. Fresh Joker process versus warm workers: measure latency, RSS and namespace leakage before pooling.
3. Extism SDK versus a minimal direct wazero ABI: retain Extism for compatibility first; simplify only with fixture parity.
4. go-iroh's unsupported blob request variants: determine whether Legion exercises them and add cross-language tests before production claims.
5. mDNS discovery ownership: retain Legion's current Bonjour service contract even if endpoint discovery differs internally.

## Primary sources inspected

- `rcarmo/go-ai` at `abd95ba55b58b3986961b03fcc5c014d6d775c0c` (2026-08-29), MIT, Go 1.24.
- `rcarmo/go-joker` at `f9696e21c9b7025fbabe442e458fbeb86b0b44a2` (v42.10.1, 2026-08-07), EPL-1.0, Go 1.26 toolchain; wazero v1.12.
- `tmc/go-iroh` at `d017bbf60c5ae3a6e3fd59dc200137e460e1c3f5` (2026-08-30), v0.1.0, MIT, Go 1.26.
- `hugelgupf/p9` v0.4.1 (2026-05-21), Apache-2.0.
- `hashicorp/raft` v1.7.3, MPL-2.0; `raft-boltdb/v2` and bbolt are pure Go.
- `modernc.org/sqlite`, BSD-3-Clause, pure-Go SQLite.
- `tetratelabs/wazero` v1.12.0, Apache-2.0, zero dependency/non-CGO runtime.
- `extism/go-sdk` v1.7.1, BSD-3-Clause; implementation uses wazero and showed no CGO/Wasmtime dependency.
- rqlite v10.2.7 source and module manifest: current SQLite dependency is its fork of `mattn/go-sqlite3`; project changelog states the SQLite C code remains unchanged.
- Canonical dqlite/go-dqlite documentation: Go client still requires the C dqlite library.
- Picoclaw provider/channel source as an architectural reference only; inference is assigned to go-ai.
