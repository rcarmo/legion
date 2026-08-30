# Architecture

## Layer Diagram

```
┌─────────────────────────────────────────────────────────┐
│                    Client Layer                         │
│   Bun workers · WASM guests · REST API · CLI · picoclaw │
└────────────────────────┬────────────────────────────────┘
                         │ 9P over QUIC (iroh transport)
┌────────────────────────▼────────────────────────────────┐
│               9P Namespace (legion-namespace)           │
│  /fn/<name>              callable functions             │
│  /fn/<name>/versions     CID history                    │
│  /sessions/<id>/turns    append-only turn log           │
│  /sessions/<id>/status   running|parked|complete        │
│  /sessions/<id>/fork     create branch                  │
│  /deploy/blobs/<cid>     push artifact blob             │
│  /deploy/register        publish function version       │
│  /cluster/peers          live membership view           │
│  /cluster/leader         current Raft leader key        │
└────────────────────────┬────────────────────────────────┘
                         │
           ┌─────────────┴──────────────┐
           │                            │
┌──────────▼──────────┐    ┌────────────▼──────────────┐
│  Agent Loop         │    │  Function Executor.       │
│  (legion-loop)      │    │  (legion-runtime)         │
│                     │    │                           │
│  rs-ai EventStream  │    │  wasmtime + extism (WASM) │
│  TurnPhase FSM      │    │  Bun subprocess (JS/TS)   │
│  Tool dispatch      │    │  Fetch blob by CID        │
│  Budget enforcement │    │  Enforce budget           │
│  Park / Resume      │    │  Collect result CID       │
└──────────┬──────────┘    └────────────┬──────────────┘
           │                            │
           └─────────────┬──────────────┘
                         │
┌────────────────────────▼───────────────────────────────┐
│               EventStore (legion-store)                │
│                                                        │
│  hiqlite (openraft + rusqlite)                         │
│  ┌─────────────────────────────────────────────────┐   │
│  │ turns · sessions · functions · cluster_state    │   │
│  │ distributed locks · listen/notify · migrations  │   │
│  └───────────────────────┬─────────────────────────┘   │
│                          │ Raft log entries            │
│  fjall (pure Rust LSM)  ◄┘                             │
│                                                        │
│  iroh-blobs (CAS) ◄── payload CIDs from turns table    │
└────────────────────────┬───────────────────────────────┘
                         │
┌────────────────────────▼────────────────────────────────┐
│               Cluster Layer (legion-cluster)            │
│                                                         │
│  iroh endpoint (public-key identity)                    │
│  iroh-mdns-address-lookup (LAN bootstrap)               │
│  mdns-sd (Bonjour/DNS-SD registration)                  │
│  iroh-gossip (membership + health broadcast)            │
│  openraft (via hiqlite) — leader election               │
└─────────────────────────────────────────────────────────┘
```

## Crate Dependency Graph

```
legion-server
  ├── legion-namespace
  │     └── legion-core (traits only)
  ├── legion-loop
  │     ├── legion-core
  │     └── rs-ai
  ├── legion-store
  │     ├── legion-core
  │     ├── hiqlite
  │     ├── fjall
  │     └── iroh-blobs
  ├── legion-cluster
  │     ├── legion-core
  │     ├── iroh
  │     ├── iroh-mdns-address-lookup
  │     ├── iroh-gossip
  │     └── mdns-sd
  ├── legion-runtime
  │     ├── legion-core
  │     ├── wasmtime
  │     └── extism
  └── legion-deploy
        ├── legion-core
        └── iroh-blobs
```

`legion-core` has **no I/O dependencies** — it defines only traits and pure types. All implementations live in the crates that import it.

## Data Flow: Agent Turn

```
1. Client writes to /sessions/<id>/turns (9P write)
   │
2. legion-namespace receives TurnEvent::UserMessage
   │
3. legion-loop picks up the event
   │
4. EventStore.append(run_id, ModelCallIntent)   ← write-ahead
   │
5. rs-ai.stream(history, tools) → EventStream
   │
6. Loop over Event stream:
   ├── TextDelta → buffer
   ├── ThinkingDelta → buffer (not stored until ThinkingEnd)
   ├── ToolCallEnd → EventStore.append(ToolCallIntent)
   │                 dispatch_tool(name, args)
   │                 EventStore.append(ToolResult)
   └── Done → EventStore.append(AssistantMessage)
              large content → iroh-blobs (CID stored in turn)
   │
7. /sessions/<id>/turns updated; listen/notify wakes watchers
```

## Data Flow: Function Deployment

```
1. Build: bun build --target=bun fn.ts → fn.js  (or compile to WASM)
   │
2. legion-deploy push fn.js
   → iroh-blobs store → CID = sha256(content)
   │
3. legion-deploy register --name my-fn --cid <hash> --runtime bun
   → Raft entry: RegisterFunction { name, cid, runtime, schema }
   │
4. All nodes: resolve "my-fn" → CID → fetch blob on demand → execute
```

## Node Startup Sequence

```
1. Load or generate iroh keypair (persisted to disk)
2. Bind iroh endpoint
3. Start iroh-mdns-address-lookup → advertise on LAN
4. Scan for peers (DiscoveryEvent::Discovered)
5a. If peers found → request Raft cluster join
5b. If no peers → start single-node Raft, become leader
6. Start hiqlite with discovered peer addresses
7. Run hiqlite migrations
8. Start jetstream 9P server
9. Register Bonjour service via mdns-sd
10. Ready
```

## Failure Modes

| Failure | Recovery |
|---|---|
| Node crash mid-turn | On restart: replay log → resume from last committed turn |
| Leader node crash | Raft elects new leader; sessions continue on any node |
| Network partition | iroh relay fallback; Raft pauses until quorum restored |
| Dangling write-ahead | Session status → `PendingReconciliation`; blocks resume until resolved |
| Node rejoins after partition | Raft log catch-up; iroh reconnects by public key automatically |
| Function blob missing | Fetch from any peer that has the CID; iroh-blobs handles routing |
