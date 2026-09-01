# Distributed execution

Milestone 6 makes function execution cluster-wide. Any healthy node can accept an invocation, select an eligible executor, transfer the immutable artifact when needed, and return the result without requiring the client to choose a peer.

The current implementation replicates session state, discovers peers, and supports explicit peer namespace access. `POST /functions/{name}/invoke` still uses the receiving node's local Bun or WASM backend. Gossip does not advertise execution capacity, and no cluster scheduler exists. This milestone closes that gap.

## Scope

Milestone 6 covers:

- authenticated remote function invocation over iroh;
- decentralised, load-aware executor selection;
- node capability and load advertisements;
- authoritative destination-side admission control;
- content-addressed artifact transfer and cache locality;
- placement, affinity, draining, and runtime compatibility;
- deadlines, cancellation, retries, deduplication, and result reconciliation;
- invocation records and failure semantics for idempotent and non-idempotent work;
- REST, CLI, 9P, metrics, traces, and dashboard changes;
- mixed-architecture and node-failure acceptance tests.

Agent sessions and workflow nodes benefit when they invoke `fn.<name>`, because the function registry bridge uses the same cluster invoker. Model inference placement and migration of an active agent turn are separate work.

## Required behaviour

The existing invocation endpoint keeps its contract:

```http
POST /functions/{name}/invoke
Content-Type: application/json

{"key":"value"}
```

The node receiving the request is the **gateway**. The node running the selected artifact is the **executor**. They may be the same node.

For each call, the gateway must:

1. resolve the function name to one immutable artifact CID and runtime;
2. create or accept a globally unique `call_id`;
3. apply the function placement policy;
4. select an eligible executor from fresh membership data;
5. obtain an admission decision from that executor;
6. execute locally or send an authenticated remote request;
7. enforce the original deadline across all attempts;
8. return the result and execution metadata;
9. record metrics and the final invocation outcome.

A client does not need to know cluster membership or retry a different node itself.

## Invariants

- The gateway selects the artifact CID before it selects an executor. Every attempt for one `call_id` uses that CID.
- The executor verifies artifact bytes against the CID before execution.
- Gossip informs scheduling but never grants capacity. The executor's local semaphore and rate limiter make the final admission decision.
- The executor applies the strictest value from its local configuration, the function manifest, and the request. A gateway cannot raise an executor's limits.
- A retry retains the original `call_id`, artifact CID, arguments hash, and deadline.
- An executor rejects a reused `call_id` when the function name, CID, or arguments hash differs.
- A deadline is an absolute Unix timestamp. Each hop reduces its remaining budget; retries do not reset it.
- Nodes with stale presence, incompatible runtimes, no capacity, or draining state are ineligible.
- Scheduler state is soft state. Losing it may change placement but cannot change function version or retry safety.
- Raft is not required for every idempotent invocation. Durable ownership for non-idempotent work uses the replicated store.
- Hidden environment values and API credentials do not enter gossip, logs, traces, or scheduling responses.

## Remote invocation protocol

`legion-cluster` adds an iroh protocol handler with ALPN:

```text
legion/invoke/1
```

The existing node key authenticates both peers through iroh QUIC. The protocol uses one bidirectional stream with length-delimited JSON control frames. The executor sends its admission response and terminal response on that stream. Inputs or results above the inline threshold use CIDs and the blob transfer path instead of unbounded frames.

### Request

```json
{
  "protocol": 1,
  "call_id": "018f...",
  "function": "image-resize",
  "artifact_cid": "bafk...",
  "runtime": "wasm",
  "args": {"source_cid": "bafk..."},
  "args_hash": "blake3:...",
  "deadline_unix_ms": 1788290000000,
  "idempotent": true,
  "limits": {
    "max_input_bytes": 1048576,
    "max_output_bytes": 4194304,
    "wasm_fuel": 100000000,
    "wasm_max_memory_bytes": 67108864
  },
  "trace_context": {
    "traceparent": "00-...",
    "tracestate": ""
  }
}
```

Exactly one of `args` or `args_cid` is present. `args_cid` names a JSON value in the cluster blob store and includes its decoded byte length. Terminal responses use `output` or `output_cid` under the same rule. The default inline threshold is 1 MiB and cannot exceed the configured frame limit.

Function environment comes from the executor's replicated manifest and node-local secret bindings. The gateway does not send plaintext secrets in the request.

### Responses

The executor returns one of these states before or after execution:

- `accepted`: the executor owns the attempt and supplies an executor-local attempt ID;
- `busy`: no local permit is available; includes bounded retry guidance;
- `incompatible`: runtime, ABI, label, or resource requirements are not met;
- `missing_artifact`: transfer failed or no provider could serve the CID;
- `completed`: includes output, timing, and executor identity;
- `failed`: execution produced a known error before the deadline;
- `unknown`: the executor accepted the call but cannot prove whether a side effect completed.

Example terminal response:

```json
{
  "status": "completed",
  "call_id": "018f...",
  "executor": "98b4e950",
  "output": {"width": 1280, "height": 720},
  "wall_ms": 83,
  "queue_ms": 4,
  "artifact_cache": "hit"
}
```

Protocol frames have configured size limits. Malformed, unauthorised, expired, or replay-inconsistent requests are rejected before admission.

## Membership and capacity

The gossip heartbeat extends `NodePresence` with an execution advertisement:

```json
{
  "endpoint_id": "mX...",
  "short_id": "98b4e950",
  "api_port": 18080,
  "timestamp": 1788290000000,
  "draining": false,
  "runtimes": {
    "bun": {"version": "1.4.0", "abi": "legion-bun-1", "capacity": 8, "inflight": 3},
    "wasm": {"engine": "wasmtime", "abi": "legion-wasm-1", "capacity": 8, "inflight": 1}
  },
  "labels": {"arch": "x86_64", "zone": "office"},
  "available_memory_bytes": 4294967296,
  "ewma_latency_ms": 92,
  "recent_error_rate": 0.01,
  "cache_bloom_epoch": 41
}
```

Advertisements are bounded and contain no per-call, per-user, function-argument, or secret data. Artifact locality uses a bounded Bloom filter or equivalent summary; it must not grow with the complete cache inventory.

A node expires from the scheduler when its heartbeat is older than three heartbeat intervals. A node may still reject a request after selection because gossip is eventually consistent.

`NeighborDown`, mDNS expiry, and heartbeat expiry remove a node from new scheduling decisions. Existing accepted work continues until its deadline or local cancellation.

## Eligibility

An executor is eligible when all these conditions hold:

- its presence is fresh;
- it is not draining;
- it supports the function runtime and required ABI;
- it satisfies required placement labels;
- it meets declared memory, fuel, architecture, or device constraints;
- it has advertised capacity;
- a circuit breaker has not excluded it;
- policy permits remote execution for the function.

The local node enters the same candidate set and is scored by the same rules. `placement.mode = "local"` is the only exception.

## Scheduling

The default `spread` scheduler uses power of two choices:

1. choose two eligible nodes using a per-request pseudorandom seed;
2. score each node;
3. ask the lower-scoring node for admission;
4. try another eligible node on `busy`, stale membership, or a connection failure before acceptance;
5. fall back to the local node only when policy permits it.

The initial score is:

```text
(inflight / max(capacity, 1))
+ normalised EWMA latency
+ recent error penalty
+ artifact cache-miss penalty
+ retry penalty
```

Weights are configuration values with conservative defaults. The scheduler must expose its decision fields through traces and bounded debug output so changes can be measured.

Power of two choices avoids a leader bottleneck and does not require strongly consistent load counters. Destination admission corrects stale estimates.

### Affinity

`affinity` placement uses rendezvous hashing over:

```text
(function name, artifact CID, affinity key, eligible endpoint IDs)
```

The highest-ranked healthy node receives the call. The next ranked node is the fallback. The raw affinity key is not logged or advertised; traces contain only a keyed hash.

Affinity is a placement preference, not durable actor ownership. Stateful durable objects need a separate ownership and migration contract.

## Placement policy

`FunctionManifest` gains a backward-compatible placement block:

```json
{
  "placement": {
    "mode": "spread",
    "required_labels": {"arch": "aarch64"},
    "preferred_labels": {"zone": "office"},
    "prefer_cached": true,
    "allow_local_fallback": true,
    "affinity_argument": "/customer_id",
    "max_attempts": 3
  }
}
```

Supported modes:

| Mode | Behaviour |
|---|---|
| `local` | Execute only on the gateway. This preserves current behaviour. |
| `spread` | Select any eligible node using load and locality. |
| `affinity` | Use rendezvous hashing from a configured input field. |
| `pinned` | Require explicit labels or endpoint IDs; fail when none qualify. |
| `leader` | Execute on the current Raft leader. Use only for leader-owned maintenance work. |

The default for existing manifests is `local` during rollout. Operators may change the cluster default to `spread` after mixed-version checks pass. A later migration may make `spread` the default for new deployments.

Clients may set these optional HTTP headers without changing the function-input JSON body:

| Header | Value |
|---|---|
| `Idempotency-Key` | Client-selected `call_id`; retries of the same logical call reuse it. |
| `X-Legion-Placement` | `local`, `spread`, `affinity`, `pinned`, or `leader`. |
| `X-Legion-Affinity-Key` | Affinity value when the manifest permits client-supplied affinity. |
| `X-Legion-Deadline-Ms` | Absolute Unix deadline in milliseconds. |

The CLI maps its placement, affinity, call-ID, and deadline options to these headers. A client may request a stricter placement mode, but cannot weaken manifest restrictions, raise execution limits, or select an unauthorised endpoint.

## Admission and backpressure

Each executor retains its existing per-function `BoundedInvoker` controls:

- concurrency semaphore;
- request rate window;
- input and output size limits;
- runtime timeout;
- WASM fuel and memory limits.

Remote admission reserves a permit before returning `accepted`. The reservation has a short expiry and is consumed exactly once by the matching request. This closes the race between a load advertisement and execution.

The executor returns `busy` immediately when it cannot reserve capacity. Milestone 6 does not add an unbounded cluster queue. Gateways try another eligible node or return HTTP 429 with `Retry-After`.

A configurable bounded local queue may be enabled per runtime. Queue depth and maximum queue delay are hard limits. Expired queued work is removed without execution.

## Artifact distribution

The present deployment store is local `iroh-blobs` storage. Milestone 6 must connect blob providers across cluster nodes and prove fetch-on-demand between separate data directories.

Before execution, an executor:

1. checks its local CAS for the selected CID;
2. fetches the blob from the gateway or another known provider on a miss;
3. verifies the received BLAKE3 hash;
4. materialises it under the runtime function root using an atomic rename;
5. records cache hit, source, bytes, and transfer time;
6. executes only after verification succeeds.

Concurrent requests for the same missing CID share one transfer. Failed partial transfers do not become cache entries. Cache eviction never removes a blob with an active execution lease.

Canary selection occurs once at the gateway. Remote scheduling cannot select a different function version.

## Retry and deduplication

### Idempotent functions

Calls whose manifest sets `idempotent = true` may be retried automatically when:

- connection fails before `accepted`;
- the executor returns `busy` or `incompatible`;
- artifact transfer fails and another provider or executor is available;
- an accepted executor disappears before returning a terminal result.

All attempts reuse the same `call_id`. Executors keep a bounded deduplication record and return the stored terminal result when they receive the same call again.

### Non-idempotent functions

Non-idempotent calls use a replicated ownership record before execution:

```text
call_id → function, CID, arguments hash, owner endpoint, lease, status, result reference
```

Only the owner holding the valid lease may begin execution. Ownership transfer is allowed only when the previous owner rejected the call before execution or when the durable record proves it never started.

An executor that may have completed an external side effect but failed before recording the result produces `unknown`. The gateway returns an ambiguous-outcome error and a reconciliation identifier. It does not retry automatically.

Legion cannot guarantee exactly-once effects in an external system that does not participate in the invocation transaction. Functions should use the Legion `call_id` as an idempotency key when calling external services.

### Deduplication retention

Retention must exceed the maximum client retry horizon and invocation deadline. Completed inline results may be retained directly; large results use a CID. Expiry is a leader-owned maintenance task protected by a distributed lock.

## Cancellation and deadlines

The gateway propagates an absolute deadline. Connection, admission, transfer, queue, and execution time all consume the same budget.

Client disconnect does not cancel accepted work by default. A client may request cancellable execution. Cancellation is best effort:

- queued work is removed;
- Bun process groups receive termination and then forced kill after a grace period;
- WASM execution uses epoch interruption or an equivalent runtime interrupt;
- completed effects are not rolled back;
- the durable invocation record states whether cancellation happened before or after execution began.

Draining nodes reject new reservations, finish accepted work up to a configured grace period, and cancel or hand back only work whose retry policy permits it.

## Failure semantics

| Failure | Required result |
|---|---|
| Candidate disappears before admission | Select another eligible node. |
| Executor returns `busy` | Select another node or return 429. |
| Artifact provider fails | Try another provider within the deadline. |
| Artifact hash differs | Reject, quarantine the source, and record a security metric. |
| Idempotent executor fails after acceptance | Query dedup state, then retry with the same `call_id` when safe. |
| Non-idempotent executor fails before start | Transfer ownership through the durable record. |
| Non-idempotent outcome is ambiguous | Return `unknown`; require reconciliation. |
| Gateway fails after remote completion | A retry with the same `call_id` returns the stored result. |
| Network partition | Schedule only fresh reachable nodes; quorum-dependent ownership changes pause without quorum. |
| Raft leader changes | Ordinary idempotent scheduling continues; durable ownership operations wait for the replicated store. |
| Mixed-version peer | Exclude it when protocol or runtime ABI does not match. |
| Deadline expires | Cancel when permitted and return 504 with execution metadata. |

A scheduler must not route new work to a node solely because mDNS still lists it. Fresh authenticated gossip and successful transport reachability are required.

## Security

- iroh node identities authenticate the remote invocation channel.
- Cluster configuration defines the endpoint IDs allowed to execute work. Discovery alone does not grant execution rights.
- Protocol version, runtime ABI, function policy, limits, and deadline are validated before admission. The executor intersects requested limits with its local and manifest limits.
- REST credentials remain at the gateway; they are not forwarded to executors.
- Function secrets resolve from named node-local or cluster-managed bindings. Gossip and invocation traces contain binding names only.
- Arguments and outputs follow existing size limits and redaction policy.
- Trace propagation accepts only valid W3C fields and starts a new trace when supplied context is malformed.
- Remote errors expose stable codes to clients. Internal paths, environment values, and transport details remain in protected logs.
- Admission reservations bind `call_id` to the authenticated gateway that obtained the permit. Deduplication and ownership records use the cluster-global `call_id`, request identity, and artifact CID so retries may enter through another authenticated gateway. Each accepted gateway identity is retained for audit.

## API and CLI

The invocation response gains execution metadata while preserving `output` and `wall_ms`:

```json
{
  "function": "image-resize",
  "output": {"width": 1280},
  "wall_ms": 83,
  "execution": {
    "call_id": "018f...",
    "gateway": "a1b2c3d4",
    "executor": "98b4e950",
    "remote": true,
    "attempts": 1,
    "queue_ms": 4,
    "artifact_cache": "hit"
  }
}
```

HTTP status mapping:

- `200`: completed;
- `400`: invalid placement, affinity, or call identity;
- `409`: reused `call_id` conflicts with the stored request;
- `422`: no compatible executor or function failure;
- `429`: cluster capacity unavailable;
- `502`: remote protocol or executor failure;
- `504`: original deadline expired;
- `425`: non-idempotent outcome is unknown and requires reconciliation.

Management endpoints:

```http
GET  /cluster/executors
POST /cluster/executors/{endpoint-id}/drain
POST /cluster/executors/{endpoint-id}/uncordon
GET  /invocations/{call-id}
POST /invocations/{call-id}/reconcile
```

Drain requests contain `{"grace_ms":30000}`. Reconciliation requests contain `{"action":"mark_failed"}` or `{"action":"record_result","output":...}` and require the existing administrative REST credential.

CLI additions:

```sh
legion call my-fn --placement spread --json '{"x":1}'
legion call my-fn --affinity customer-123 --call-id 018f...
legion invocation get 018f...
legion invocation reconcile 018f... --action mark-failed
legion cluster executors
legion cluster drain <endpoint-id> --grace 30s
legion cluster uncordon <endpoint-id>
```

The 9P namespace adds:

```text
/cluster/executors                 current bounded executor view
/cluster/executors/<id>/status     capacity, load, runtimes, draining
/invocations/<call-id>/status      durable public invocation state
/invocations/<call-id>/result      terminal result or CID
```

`/peers/<id>/fn/<name>` remains the explicit peer path for diagnostics and pinned operation. Normal `/fn/<name>` writes use the cluster invoker when placement permits.

## Observability

Metrics use function, runtime, outcome, and node labels. They do not use call IDs, arguments, affinity keys, or user data as metric labels.

Required metrics:

```text
legion_scheduler_decisions_total{result,placement}
legion_scheduler_attempts_total{outcome}
legion_scheduler_no_candidate_total{reason}
legion_executor_inflight{node,runtime}
legion_executor_capacity{node,runtime}
legion_executor_admission_total{result,runtime}
legion_remote_invocations_total{outcome,runtime}
legion_remote_invocation_wall_ms_total{outcome,runtime}
legion_remote_queue_ms_total{runtime}
legion_artifact_fetch_total{outcome,source}
legion_artifact_fetch_bytes_total
legion_artifact_fetch_wall_ms_total{outcome}
legion_invocation_dedup_hits_total{status}
legion_invocation_unknown_total{runtime}
```

One trace spans gateway scheduling, remote connection, admission, artifact fetch, runtime execution, and response. Span fields include endpoint IDs, function name, CID prefix, runtime, placement, attempt number, cache status, queue time, and stable outcome code.

The dashboard adds:

- executor freshness, runtime capability, inflight work, capacity, and drain state;
- invocation distribution by node and function;
- scheduler rejection and failover counts;
- artifact cache hit rate and transfer latency;
- recent unknown non-idempotent outcomes requiring reconciliation.

## Operations

A node starts unready for remote execution until:

- its runtime backends initialise;
- its invocation protocol handler is listening;
- required secret bindings and storage paths pass validation;
- its protocol and runtime ABI versions are known;
- it has joined the configured execution allow-list.

Rolling upgrade procedure:

1. mark one node draining;
2. wait for `inflight = 0` or the grace deadline;
3. stop and upgrade it;
4. verify protocol/runtime compatibility and health;
5. uncordon it;
6. continue with the next node.

Mixed versions may coexist only when their advertised invocation protocol and runtime ABI overlap. The scheduler excludes incompatible nodes automatically.

Configuration adds:

```toml
[execution]
enabled = true
default_placement = "local"
heartbeat_ms = 5000
presence_ttl_ms = 15000
max_attempts = 3
admission_reservation_ms = 2000
result_retention_hours = 24
allow_endpoints = ["mX..."]

[execution.scheduler]
load_weight = 1.0
latency_weight = 0.25
error_weight = 2.0
cache_miss_weight = 0.2

[execution.drain]
grace_ms = 30000
```

## Delivery plan

### M6.1 — Protocol and explicit remote execution

- Define versioned request, admission, result, and error types in `legion-core` without adding I/O there.
- Add the authenticated `legion/invoke/1` iroh handler and client in `legion-cluster`.
- Execute one explicitly selected remote Bun or WASM function through the existing bounded runtime.
- Preserve `call_id`, CID, deadline, limits, and trace context.
- Add protocol compatibility and malformed-frame tests.

Acceptance: node A invokes a function explicitly on node B, receives the correct executor identity, and node B enforces its local limits.

### M6.2 — Capability advertisement and scheduling

- Extend presence with runtimes, ABI versions, capacity, inflight counts, labels, drain state, and bounded locality data.
- Expire stale executors.
- Add eligibility filtering and power-of-two scheduling.
- Add local fallback, circuit breaking, and destination admission reservations.
- Return execution metadata through REST, CLI, tools, and 9P.

Acceptance: repeated calls entering any node spread across three eligible nodes without exceeding any executor's concurrency limit.

### M6.3 — Artifact transfer and heterogeneous nodes

- Connect artifact providers through iroh-blobs.
- Fetch and verify missing CIDs on demand with single-flight transfer.
- Advertise runtime and architecture constraints.
- Exclude incompatible executors before admission.
- Test Bun and WASM on separate data directories and at least two CPU architectures when hardware is available; CI uses emulated capability labels where necessary.

Acceptance: a function uploaded to one node executes on a clean peer after verified artifact transfer, and an incompatible peer receives no work.

### M6.4 — Retry safety and durable ownership

- Add deduplication records and result retention.
- Retry idempotent calls with one `call_id` across executor failure.
- Add replicated ownership and leases for non-idempotent functions.
- Return `unknown` for ambiguous effects and provide inspection/reconciliation commands.
- Add deadline and best-effort cancellation propagation.

Acceptance: gateway and executor failure tests produce one stored result for idempotent work and no automatic duplicate attempt for ambiguous non-idempotent work.

### M6.5 — Placement, draining, and operations

- Add manifest placement policy, affinity, labels, pinned and leader modes.
- Add drain/uncordon controls and rolling-upgrade checks.
- Add cluster executor, invocation, and reconciliation views to REST, CLI, 9P, and dashboard.
- Document security, configuration, runbooks, and recovery.

Acceptance: a three-node rolling restart completes while traffic continues on eligible nodes, with pinned and affinity policies preserved.

### M6.6 — Capacity and failure gates

- Add deterministic scheduler unit tests.
- Add isolated multi-node integration tests with separate ports and data directories.
- Add sustained and burst load tests.
- Add partition, stale heartbeat, transfer failure, executor crash, gateway crash, deadline, drain, mixed-version, and malformed-peer tests.
- Archive machine-readable distribution and latency evidence in CI.

Acceptance gates are listed below.

## Acceptance gates

Milestone 6 is complete when all these gates pass:

1. Three nodes form one cluster and advertise compatible execution capacity.
2. A call submitted to any node may execute on any eligible node without client-side peer selection.
3. At least 10,000 idempotent invocations at concurrency 96 distribute within 20% of capacity-weighted expectation across three equal nodes.
4. No executor exceeds its configured per-function concurrency ceiling.
5. A burst above total cluster capacity produces bounded HTTP 429 responses and no unbounded queue growth.
6. Killing one executor during load preserves successful idempotent calls through bounded retry with the original `call_id`.
7. Replaying a completed `call_id` returns the stored result and does not execute the function again.
8. A non-idempotent ambiguous failure returns `unknown` and creates one reconciliation record; Legion does not retry it automatically.
9. A clean executor fetches a missing artifact from another node, verifies its CID, and executes it. Corrupt bytes never execute.
10. Canary version selection remains fixed across remote attempts.
11. Affinity sends a stable key to the same eligible node and moves it to the next rendezvous candidate after failure.
12. Draining removes a node from new placement and lets accepted work finish within the configured grace period.
13. Stale, unauthorised, incompatible, and partitioned peers receive no new work.
14. Absolute deadlines bound connection, queue, transfer, execution, and retries together.
15. Metrics and traces identify gateway, executor, placement, attempts, queue time, cache status, and outcome without high-cardinality or secret labels.
16. REST, CLI, agent function tools, workflows, and `/fn/<name>` use the same cluster invocation path.
17. Existing `placement = "local"` behaviour and single-node tests continue to pass.
18. A documented rolling upgrade and rollback drill succeeds on three isolated nodes.

The distribution gate reports observed counts, capacity weights, p50/p95/p99 latency, retries, 429 responses, cache hits, and errors as JSON. Tests use fixed seeds where scheduler determinism matters and real randomness only in the load-distribution gate.

## Non-goals

Milestone 6 does not include:

- Kubernetes-style container orchestration or arbitrary process scheduling;
- migration of a running Bun process or WASM instance;
- GPU model inference scheduling;
- global autoscaling or creation of new VMs;
- WAN federation between administrative trust domains;
- exactly-once external side effects without cooperation from the external system;
- durable actor ownership, actor migration, or globally serialised affinity keys;
- an unbounded central work queue;
- speculative duplicate execution or hedged requests;
- cross-cluster billing or tenant accounting.

These may use the protocol and scheduling foundations later, but they need separate consistency and security contracts.
