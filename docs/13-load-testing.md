# Load testing

Milestone 4 has two reproducible capacity gates. Run them on an otherwise idle machine with at least 6 GiB free:

```sh
make load-test
```

## Replicated storage

`make load-test-hiqlite` starts three local hiqlite nodes on ports 38101–38103 and 38201–38203, writes 25,000 rows in 500-row Raft transactions, and requires at least **24,500 committed inserts/second**. It uses a release build and one test thread. Override code constants deliberately when changing the workload; do not lower the gate to accommodate a busy machine.

This measures the hiqlite replicated state-machine batch-insert target. Legion's hash-chained `EventStore::append` intentionally performs a tail read and one Raft commit per event and is a latency/durability path, not comparable to bulk SQL throughput.

## HTTP function invocation

`make load-test-http` starts an isolated Legion node, deploys a Bun echo function, and invokes it with 1,000 requests at the configured per-function concurrency ceiling of 8. Defaults require:

- at least 60 requests/second;
- p95 latency no higher than 200 ms;
- error rate no higher than 0.1%;
- a deliberate concurrency-32 overload burst must contain both successful work and HTTP 429 load shedding rather than unbounded queuing.

Tune the workload or gates without editing files by setting `LEGION_LOAD_REQUESTS`, `LEGION_LOAD_CONCURRENCY`, `LEGION_LOAD_MIN_RPS`, `LEGION_LOAD_MAX_P95_MS`, and `LEGION_LOAD_MAX_ERROR_RATE`.

The script emits one JSON result suitable for archiving in CI or release evidence. These tests are opt-in because they consume significant CPU, disk, and fixed local ports; ordinary `make check` does not run them.
