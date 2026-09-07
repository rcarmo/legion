# Agent Ecosystem

Milestone 5 adds one composition model across agent tools, supervised child sessions, workflow graphs, channels, TypeScript applications, and Bun functions.

## Agent profiles and supervised runs

An `AgentProfile` is a named `RunConfig`. Registering a profile adds `agent.<name>` to the live `ToolRegistry`, so LLM tool dispatch, workflows, and REST calls all use the same execution path.

```http
POST /agents
{"name":"researcher","description":"Research a topic","config":{"model":"anthropic/claude-haiku-3-5","tools":["fn.search"]}}

POST /agents/researcher/invoke
{"prompt":"Find primary sources"}
```

To supervise a child from an existing session, provide both `parent_run_id` and `at_seq`. Legion verifies that sequence, forks the durable event log, injects the assignment, resolves the child, and returns its run ID, parent ID, terminal status, and output.

```json
{"prompt":"Check this claim","parent_run_id":"…","at_seq":5}
```

Profiles are currently node-local runtime configuration. Child sessions and all their events remain durable in the configured `EventStore`.

## Workflow graphs

`POST /workflows/run` accepts a directed acyclic graph:

```json
{
  "nodes": [
    {"id":"research","tool":"agent.researcher","args":{"prompt":"Collect evidence"}},
    {"id":"review","tool":"agent.reviewer","args":{"prompt":"Review evidence"},"depends_on":["research"]}
  ]
}
```

Legion rejects empty graphs, duplicate IDs, missing references, self-dependencies, and cycles. Ready nodes run concurrently in deterministic waves. Each dependent node receives predecessor results in its `dependencies` argument. The response includes all outputs and the wave plan as supervision evidence.

## TypeScript packages

- `@legion/client` — zero-dependency REST client for Bun and Node.js 20+. Covers sessions, functions, profiles, supervised invocations, and workflows.
- `@legion/channels` — Picoclaw-shaped `ChannelAdapter`, Telegram long polling, framework-neutral web chat, and `LegionChannelRouter`, which maps each conversation to a durable session.
- `legion-bun-client` — native Bun 9P2000.L client with `readFile`, `writeFile`, JSON helpers, and function invocation.

Run strict type checks and package tests with `make js-test`.

## Bun 9P bridge

Bun functions connect to the same capability-protected `LegionNamespace` through an opt-in local TCP projection:

```sh
bin/legion --9p-addr 127.0.0.1:5640 --9p-capability replace-me
```

Only loopback bind addresses and clients are accepted. Cluster traffic continues over authenticated iroh QUIC. Set `LEGION_NAMESPACE_CAPABILITY` in functions and pass it to `createLegionFs`.

`make go-m5-smoke` starts an isolated Go server and proves version negotiation, capability attach, file open/read/write/clunk, durable session creation/readback, and wrong-capability denial.

## Dashboard

`/` and `/dashboard` serve a dependency-free UI embedded in the Legion binary. It provides:

- recent sessions, status, model, turn count, and event-log detail;
- registered agent profiles and functions;
- cluster identity and peers;
- interactive workflow graph execution.

The shell is public so it can present an API-key field. Every data request remains protected by the normal API middleware; the key is stored only in browser local storage. `make go-m5-smoke` verifies the UI against a real Go server in Chromium.

## Joker namespace helpers

The pinned Joker worker preloads the `legion` namespace. Helpers use the authenticated
REST projection of the same namespace; they do not implement a separate 9P client.
Supply `LEGION_URL` and, for a protected server, `LEGION_API_KEY` explicitly in the
function's `env` manifest. Server credentials are never inherited automatically.

```clojure
(defn run [args]
  {"self" (legion/read-json "/cluster/self")
   "child" (legion/write-json "/sessions/new" {"model" "openai/gpt-4o-mini"})
   "result" (legion/invoke "echo" args)})
```

`legion/read-file` and `legion/write-file` handle raw strings. JSON helpers parse
responses; HTTP failures become worker errors. The existing worker timeout bounds
helper calls. `legion/invoke` returns the function result envelope.

## Go verification

Verified on 2026-09-07:

- `make go-check`: formatting, vet and all Go package tests.
- `make go-test-channels`: real HTTP, SQLite, GoAI, web/Telegram adapters, agent
  invocation, supervised forks, DAGs, LLM-to-agent tool dispatch and database reopen.
  Inference and Telegram transport use deterministic test doubles.
- `make go-m5-smoke`: real Go process, TypeScript REST client, Bun functions in
  workflow waves, capability-protected Bun 9P and Chromium dashboard navigation.
- `LEGION_JOKER_TEST_BIN=$PWD/bin/joker make go-check`: pinned Joker worker execution
  and helper read/write/invoke tests, including wrong-key rejection.

`make go-test-opencode` is an optional live check restricted to model IDs ending
in `-free`. OpenCode rejected the 2026-09-07 attempt with `MissingSessionID` and
“OpenCode's free tier can only be used in OpenCode”; this gate has not passed.

Profiles and channel-to-session mappings remain process-local, as in the Rust
reference. Session logs survive restart. Automatic cluster-wide function scheduling
is outside Milestone 5.
