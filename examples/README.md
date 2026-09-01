# Legion examples

These examples target a running Legion node. By default they use `http://127.0.0.1:18080`; override it with `LEGION_URL` and set `LEGION_API_KEY` when authentication is enabled.

| Example | Purpose | External requirement |
|---|---|---|
| [`hello-bun`](hello-bun/) | Deploy and invoke a Bun function | None |
| [`hello-wasm`](hello-wasm/) | Build, deploy, and invoke portable WASM | Rust `wasm32-wasip1` target |
| [`durable-chat`](durable-chat/) | Durable model conversation and history | Model provider credentials |
| [`research-team`](research-team/) | Parallel agent profiles and review DAG | Model provider credentials |
| [`supervised-child`](supervised-child/) | Fork and supervise a child agent | Model provider credentials |
| [`web-chat`](web-chat/) | Browser UI backed by a durable session | Model provider credentials |
| [`telegram-bot`](telegram-bot/) | Telegram conversations mapped to sessions | `TELEGRAM_BOT_TOKEN`, model credentials |
| [`bun-9p`](bun-9p/) | Native Bun access to Legion's 9P namespace | Local 9P bridge enabled |
| [`approval-workflow`](approval-workflow/) | Park, inspect, and resume via webhook | None |
| [`canary-deployment`](canary-deployment/) | CAS registration, weighted route, promotion | None |
| [`cluster-inspector`](cluster-inspector/) | CLI inventory of peers, sessions, functions | None |
| [`backup-drill`](backup-drill/) | Encrypted restic backup/restore drill | `restic`, root/systemd access |

## Verification

```sh
make examples-test
```

The gate type-checks all TypeScript, tests pure helpers, and runs deterministic examples against an isolated prebuilt Legion server. Provider credentials are never required by the test suite.
