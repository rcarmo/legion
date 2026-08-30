# Legion Built-in Agent Tools — Design Note

**Date:** 2026-08-30  
**Context:** Milestone 1 complete. Before M2 ships tool dispatch, we need the built-in 
tool surface locked down so the `ToolRegistry` trait isn't under-designed.

## The Problem

The current `ToolRegistry` trait is stateless:

```rust
fn definitions(&self) -> &[ToolDefinition];
async fn dispatch(&self, name: &str, args: serde_json::Value) -> Result<serde_json::Value>;
```

Built-in cluster tools (e.g. `cluster.status`, `sessions.list`) need shared state:
- `Arc<dyn EventStore>` — to list/inspect sessions
- `Arc<ClusterNode>` — to query peer topology
- `Arc<DeployRegistry>` — to list/invoke deployed functions (M3)

The simplest fix: implementors of `ToolRegistry` close over the state they need.
This is already the case — the trait is `Send + Sync`, and implementors are `Arc<dyn ToolRegistry>`.
The test double `EchoToolRegistry` already does this. No trait change needed.

What we need is a **concrete `BuiltinToolRegistry`** in `legion-server` (not `legion-core`)
that takes `Arc<dyn EventStore>` and `Arc<ClusterNode>` in its constructor.

## Built-in Tool Surface (Milestone 1 → 3)

### Cluster tools (`cluster.*`)
| Tool | Effect | Description |
|------|--------|-------------|
| `cluster.status` | Read | Node ID, role (leader/follower/solo), bound addrs, peer count |
| `cluster.peers` | Read | List known iroh peers with short IDs and last-seen |
| `cluster.self` | Read | This node's full identity (endpoint ID, short ID, data dir) |

### Session tools (`sessions.*`)
| Tool | Effect | Description |
|------|--------|-------------|
| `sessions.list` | Read | List sessions with status, turn count, model |
| `sessions.get` | Read | Full metadata + recent log for one session |
| `sessions.fork` | Write | Fork a session at a given seq |
| `sessions.park` | Write | Park a session with a reason (awaits external wakeup) |
| `sessions.resume` | Write | Resume a parked session with an external event |
| `sessions.cancel` | Write | Mark a session Aborted |

### Function tools (`fn.*`) — M3
| Tool | Effect | Description |
|------|--------|-------------|
| `fn.list` | Read | List deployed WASM/JS functions |
| `fn.deploy` | Write | Deploy a function (WASM bytes or JS source) |
| `fn.invoke` | Write | Call a deployed function with JSON args |
| `fn.delete` | Write | Remove a deployed function |

### Namespace tools (`ns.*`) — M2
| Tool | Effect | Description |
|------|--------|-------------|
| `ns.ls` | Read | List a 9P path |
| `ns.read` | Read | Read a namespace entry |
| `ns.write` | Write | Write to the namespace |

## Implementation Plan

1. **M1.5**: Add `BuiltinToolRegistry` to `legion-server` — cluster + session tools only.
   - Implements `ToolRegistry` trait from `legion-core`
   - Takes `Arc<dyn EventStore>` + `Arc<ClusterNode>` + `Arc<BootstrapOutcome>`
   - Replace `EchoToolRegistry` in `main.rs` with `BuiltinToolRegistry`

2. **M2**: Add `ns.*` tools once `legion-namespace` 9P tree is live.

3. **M3**: Add `fn.*` tools once `legion-runtime` WASM runtime is live.

## Key Design Constraints

- Tools are called inside the LLM response loop — **no blocking I/O on sync thread**.
  All tool dispatch is `async`. ✓ Already guaranteed by `async_trait` on `ToolRegistry`.

- `cluster.*` and `sessions.list` are `EffectClass::Read` — safe to replay without 
  re-executing on recovery. They do NOT get write-ahead logged.

- `fn.invoke` is `EffectClass::Write` — write-ahead logged before dispatch. 
  A crash mid-invocation enters `PendingReconciliation`.

- Agent should NOT be able to delete its own running session or kill the leader 
  without a quorum check. Add a `ClusterGuard` check before destructive writes.
