# Legion: Overview

Legion is a self-hosted platform for running durable AI agents and general-purpose functions across a cluster of nodes. It combines:

- **Durable execution** — every agent turn is event-sourced; the cluster survives crashes, restarts, and node loss without losing state
- **Self-forming clusters** — nodes discover each other via mDNS on a LAN and bootstrap a Raft cluster with no manual configuration
- **Content-addressed functions** — WASM modules and Bun bundles are deployed as immutable blobs; the hash *is* the version
- **Unified namespace** — every resource is accessible as a path in a 9P namespace, making the interface consistent for humans, code, and agents

## Motivation

Existing durable functions platforms (Temporal, Durable Objects, AWS Step Functions) are either cloud-locked, operationally heavy, or not suitable for local/edge deployments. Legion targets:

- LAN-first: a cluster of machines (VMs, NUCs, RPis) that should self-heal without a cloud control plane
- Agent workloads: long-running AI agent sessions that outlive individual node lifetimes
- Embedded edge: small binaries (<50MB), minimal runtime dependencies, no JVM or Go runtime required

## Design Decisions

### Why Rust?

- Single static binary per node
- No garbage collector pauses during LLM streaming
- Memory safety without runtime cost
- WASM host via wasmtime; Bun subprocess for JS/TS functions

### Why 9P?

Plan 9's filesystem protocol provides a clean, minimal RPC model where everything is a file operation. This:
- Makes the cluster accessible via any 9P client (shell, editor, script)
- Provides a natural namespace for functions, sessions, and cluster state
- Maps directly to WASI syscalls for WASM functions (no custom RPC protocol needed)
- Enables transparent remote access: `/peers/<node-key>/fn/<name>` routes to any node

### Why Raft over eventually-consistent approaches?

Durable function semantics require strong consistency: you must be able to commit a turn to the log before executing it, and any node must be able to resume a session from the exact same state. Eventual consistency makes this hard. Raft provides:
- Single-leader writes (no split-brain)
- Committed entries are durable across node failures
- Any node with the log can replay and resume execution

### Why iroh instead of traditional networking?

iroh provides public-key-addressed QUIC connections with NAT traversal and relay fallback. Nodes are identified by their keypair, not their IP address. This means:
- Node IP addresses can change (DHCP, mobile, VPN) without breaking cluster connectivity
- mDNS advertises the node's public key; peers reconnect by key after restart
- The same network layer works on LAN, WAN, and through NAT — no separate VPN setup

## Non-Goals

- Not a general-purpose distributed database
- Not a replacement for Kubernetes or Docker
- Not a serverless cold-start latency competitor (sessions warm up; first call may be slow)
- Not cloud-native (no Kubernetes operator, no Helm chart in v1)

## Relationship to Prior Art

| System | How Legion Differs |
|---|---|
| Cloudflare Durable Objects | Self-hosted; Rust; LAN-first; open source |
| Temporal | No JVM/Go dependency; embedded storage; no separate server |
| Salvor | Distributed (not single-node); P2P transport; 9P namespace |
| picoclaw | Rust; durable (not in-memory session); cluster-native |
| AWS Lambda | Stateful by default; no cold-start tax for warm sessions |
