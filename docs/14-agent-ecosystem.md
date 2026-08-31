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

```toml
namespace_capability = "replace-me"
ninep_tcp_addr = "127.0.0.1:5640"
```

Only loopback bind addresses and clients are accepted. Cluster traffic continues over authenticated iroh QUIC. Set `LEGION_NAMESPACE_CAPABILITY` in functions and pass it to `createLegionFs`.

`make bun-ninep-integration-test` starts a prebuilt Legion server and proves version negotiation, capability attach, durable session creation/readback, and wrong-capability denial.

## Dashboard

`/` and `/dashboard` serve a dependency-free UI embedded in the Legion binary. It provides:

- recent sessions, status, model, turn count, and event-log detail;
- registered agent profiles and functions;
- cluster identity and peers;
- interactive workflow graph execution.

The shell is public so it can present an API-key field. Every data request remains protected by the normal API middleware; the key is stored only in browser local storage. `make dashboard-integration-test` verifies the UI against a real server in Chromium.
