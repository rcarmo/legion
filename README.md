# Legion

> A self-hosted, self-healing durable functions platform with Raft consensus, content-addressed storage, and a 9P namespace — running AI agents and WASM/Bun functions across a LAN-bootstrapped P2P cluster.

Legion is the open, self-hostable equivalent of Cloudflare Agents / Durable Objects, built entirely in Rust.

## What It Is

- **Durable agent loop** — AI agent turns are event-sourced and crash-resumable on any cluster node
- **Distributed by default** — Raft consensus via hiqlite (openraft + SQLite) replicates all state
- **Content-addressed deployment** — Functions are WASM modules or Bun bundles stored by hash via iroh-blobs; deployment is push-a-blob + register
- **Self-healing** — iroh P2P reconnects by public key, not IP; mDNS bootstraps the cluster with zero config on a LAN
- **9P namespace** — Every cluster resource (functions, sessions, peers) is accessible as a file path via jetstream

## Architecture at a Glance

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
│  iroh QUIC · mDNS LAN bootstrap · iroh-gossip      │
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
| `legion-server` | Top-level binary wiring all crates together |

## Key Dependencies

| Library | Role |
|---|---|
| `rs-ai` | LLM provider abstraction (OpenAI, Anthropic, Gemini, Mistral, Bedrock…) |
| `hiqlite` | Raft-replicated SQLite (openraft + rusqlite) |
| `fjall` | Pure-Rust LSM store for Raft log |
| `iroh` | QUIC P2P transport, public-key routing |
| `iroh-blobs` | Content-addressed blob store |
| `iroh-gossip` | Cluster membership + health |
| `iroh-mdns-address-lookup` | LAN mDNS bootstrap |
| `mdns-sd` | Bonjour/DNS-SD service registration |
| `jetstream` | 9P RPC over QUIC/iroh |
| `wasmtime` + `extism` | WASM function runtime |
| `openraft` | Consensus engine (used via hiqlite) |

## Status

Early design stage. See [docs/10-roadmap.md](docs/10-roadmap.md) for milestone plan.

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
