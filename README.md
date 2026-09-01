# Legion

![Legion icon](docs/icon-256.png)

> A self-hosted, self-healing durable functions platform with Raft consensus, content-addressed storage, and a 9P namespace — running AI agents and WASM/Bun functions across a LAN-bootstrapped P2P cluster.

Legion is the open, self-hostable equivalent of Cloudflare Agents / Durable Objects, built entirely in Rust. Consider it a learning experience in various ways, informed by my interest in Plan9, general concerns about doing lifecycle management The Right Way<sup>TM</sup>, and wanting something I could run locally across Intel and ARM machines (often very low powered SBCs).

It's also a stab at making [`piclaw`](https://github.com/rcarmo/piclaw)'s back-end nearly impossible to kill without hitting the main breaker.

## What It Is

- **Durable agent loop** — AI agent turns are event-sourced and crash-resumable on any cluster node
- **Distributed by default** — Raft consensus via hiqlite (openraft + SQLite) replicates all state
- **Content-addressed deployment** — Functions are WASM modules or Bun bundles stored by hash via iroh-blobs; deployment is push-a-blob + register
- **Self-healing** — iroh P2P reconnects by public key, not IP; mDNS bootstraps the cluster with zero config on a LAN
- **9P namespace** — Every cluster resource (functions, sessions, peers) is accessible as a file path via jetstream

## What It's Not

The answer to everyone's needs. Or finished.

## Architecture at a Glance

After a few weeks of pondering, this is what I came up with, and substantiated in `docs` after yelling at AI for a long time. When it stopped yelling back about "best practices", I wagered this was good enough for a first cut:

```
┌─────────────────────────────────────────────────────┐
│  Clients: Bun workers · WASM guests · CLI · REST    │
└──────────────────────┬──────────────────────────────┘
                       │ 9P (jetstream over iroh QUIC)
┌──────────────────────▼──────────────────────────────┐
│  9P Namespace  /fn/* · /sessions/* · /cluster/*     │
└──────────────────────┬──────────────────────────────┘
                       │
┌──────────────────────▼──────────────────────────────┐
│  Agent Loop (legion-loop)                           │
│  rs-ai streaming · tool dispatch · budget tracking  │
└──────────────────────┬──────────────────────────────┘
                       │
┌──────────────────────▼──────────────────────────────┐
│  EventStore (legion-store)                          │
│  hiqlite: openraft + rusqlite                       │
│  Raft log: fjall (pure-Rust LSM)                    │
│  Large payloads: iroh-blobs (CAS)                   │
└──────────────────────┬──────────────────────────────┘
                       │
┌──────────────────────▼──────────────────────────────┐
│  Cluster (legion-cluster)                           │
│  iroh QUIC · mDNS LAN bootstrap · iroh-gossip       │
└─────────────────────────────────────────────────────┘
```

## Crate Layout

| Crate | Role |
|---|---|
| `legion-core` | Shared types, traits (`EventStore`, `ToolRegistry`, `AgentLoop`) |
| `legion-store` | EventStore impl: hiqlite + fjall + iroh-blobs |
| `legion-loop` | Agent loop state machine built on rs-ai |
| `legion-namespace` | 9P namespace server (jetstream) |
| `legion-cluster` | iroh endpoint, mDNS discovery, Raft bootstrap |
| `legion-runtime` | WASM (wasmtime/extism) and Bun function executors |
| `legion-deploy` | CAS function deployment CLI and server handler |
| `legion-ecosystem` | Agent tools, supervised child runs, and workflow DAG execution |
| `legion-server` | Top-level binary wiring all crates together |

## Key Dependencies

| Library | Role |
|---|---|
| `rs-ai` | LLM provider abstraction (OpenAI, Anthropic, Gemini, Mistral, Bedrock…). Full credit to Mario Zechner for designing the original version |
| `hiqlite` | Raft-replicated SQLite (openraft + rusqlite). I hit upon this partly by chance and partly because of my love of all things SQLite |
| `fjall` | Pure-Rust LSM store for Raft log. I hope this one pans out |
| `iroh` | QUIC P2P transport, public-key routing, because I wanted to start with Bonjour peer discovery on the LAN and this makes it planetary |
| `iroh-blobs` | Content-addressed blob store, because it saved me the trouble of reinventing S3/R2 |
| `iroh-gossip` | Cluster membership + health |
| `iroh-mdns-address-lookup` | LAN mDNS bootstrap |
| `mdns-sd` | Bonjour/DNS-SD service registration |
| `jetstream` | 9P RPC over QUIC/iroh. This was a very lucky find. |
| `wasmtime` + `extism` | WASM function runtime, because, well, I don't want to have separate Intel and ARM functions, and this combo seemed moderately sane even though I mostly use Go for WASM |
| `openraft` | Consensus engine (used via hiqlite) |

## Status

Early design stage. See [docs/10-roadmap.md](docs/10-roadmap.md) for milestone plan, or 99-world-domination.md (when robots take over running this) for the end state.

## Documentation

- [00 — Overview](docs/00-overview.md)
- [01 — Architecture](docs/01-architecture.md)
- [02 — Agent Loop](docs/02-agent-loop.md)
- [03 — Turn Store](docs/03-turn-store.md)
- [04 — Function Deployment](docs/04-function-deployment.md)
- [05 — Storage](docs/05-storage.md)
- [06 — Networking & Discovery](docs/06-networking.md)
- [07 — 9P Namespace](docs/07-9p-namespace.md)
- [08 — Function Runtime](docs/08-runtime.md)
- [09 — Getting Started](docs/09-getting-started.md)
- [10 — Roadmap](docs/10-roadmap.md)
- [11 — Built-in Agent Tools](docs/11-builtin-tools.md)
- [12 — Backup and Restore](docs/12-backup-restore.md)
- [13 — Load Testing](docs/13-load-testing.md)
- [14 — Agent Ecosystem](docs/14-agent-ecosystem.md)
- [15 — Pure-Go Port Analysis](docs/15-go-port-analysis.md)
