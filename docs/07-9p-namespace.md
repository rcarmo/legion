# 9P Namespace

Legion exposes all cluster resources through a 9P filesystem namespace served by `jetstream` over iroh QUIC. Everything — functions, sessions, deployment, cluster state — is accessible as a file path.

## Why 9P?

- **Minimal protocol**: The entire RPC surface is `open`, `read`, `write`, `stat`, `walk` — standard Unix filesystem semantics
- **Transparent remoting**: `/peers/<key>/fn/<name>` routes to any node without a custom RPC protocol
- **WASI compatibility**: WASM function hosts intercept WASI `fd_write`/`fd_read` and route them through the 9P namespace — functions do not know they are in a cluster
- **Shell-accessible**: Any 9P client (`9p`, `v9fs`, `plan9port`) can browse and operate the cluster
- **Transport-agnostic**: jetstream supports iroh (P2P), QUIC (direct), and WebTransport (browser)

## Namespace Tree

```
/
├── fn/                         Functions
│   ├── <name>                  Call function (write = invoke, read = result)
│   ├── <name>/
│   │   ├── schema              JSON schema for this function
│   │   ├── versions            List of registered CIDs
│   │   └── default             Current default CID
│   └── ...
│
├── sessions/                   Agent sessions
│   ├── <run-id>/
│   │   ├── turns               Append-only turn log (write = add turn)
│   │   ├── status              Session status (read = current, write = control)
│   │   ├── context             Current context window (read-only)
│   │   ├── fork                Fork from current state (write = fork config)
│   │   └── config              RunConfig (read = current, write = update before start)
│   └── new                     Create new session (write = RunConfig, read = run-id)
│
├── deploy/                     Function deployment
│   ├── blobs/
│   │   └── <cid>               Push blob (write) or fetch blob (read) by CID
│   ├── register                Register a function (write = RegisterFunction JSON)
│   ├── route                   Update routing (write = RouteConfig JSON)
│   └── promote                 Promote canary to default (write = PromoteRequest JSON)
│
└── cluster/                    Cluster management
    ├── peers                   List of known peers with iroh keys and roles
    ├── leader                  Current Raft leader key
    ├── health                  Cluster health summary (read-only)
    └── self                    This node's identity and status
```

### Remote peers

Any path can be prefixed with `/peers/<iroh-key>/` to route to a specific remote node:

```
/peers/Ki.../sessions/abc123/turns   ← turn log on node Ki...
/peers/mX.../fn/my-fn                ← invoke function on node mX...
```

The namespace server handles routing transparently via iroh QUIC.

---

## Operations Reference

### Invoke a function

```bash
# Write JSON input → function executes → read JSON output
echo '{"question": "What is 2+2?"}' | 9p write /fn/math-agent
9p read /fn/math-agent          # blocks until result ready
```

For streaming results, open `/fn/<name>` for read after write — chunks arrive as they are produced.

### Create a session

```bash
echo '{"model":"anthropic/claude-opus-4-5","system":"You are helpful"}' \
  | 9p write /sessions/new
# Returns: run-id
RUN=$(9p read /sessions/new)
```

### Send a user message

```bash
echo '{"kind":"UserMessage","content":"Hello!"}' \
  | 9p write /sessions/$RUN/turns
```

### Watch for turns

```bash
# Read blocks until a new turn is appended
9p read /sessions/$RUN/turns    # streams turn events
```

### Check status

```bash
9p read /sessions/$RUN/status   # → running|parked|complete|pending_reconciliation
```

### Fork a session

```bash
echo '{"at_seq": 5}' | 9p write /sessions/$RUN/fork
# Returns: new run-id (shares history up to seq 5)
```

### Deploy a function

```bash
# Push blob
9p write /deploy/blobs/- < dist/my-fn.js
# Returns: CID

# Register
echo '{"name":"my-fn","cid":"bafkrei...","runtime":"bun"}' \
  | 9p write /deploy/register
```

---

## Implementation: jetstream

jetstream is an RPC framework based on the 9P protocol over QUIC, with iroh as a first-class transport.

Key types used in `legion-namespace`:

```rust
// Implement jetstream's FileSystem trait
impl jetstream::FileSystem for LegionNamespace {
    async fn attach(&self, uname: &str, aname: &str) -> Result<Fid>;
    async fn walk(&self, fid: Fid, wnames: &[&str]) -> Result<(Fid, Vec<Qid>)>;
    async fn open(&self, fid: Fid, mode: OpenMode) -> Result<(Qid, u32)>;
    async fn read(&self, fid: Fid, offset: u64, count: u32) -> Result<Bytes>;
    async fn write(&self, fid: Fid, offset: u64, data: Bytes) -> Result<u32>;
    async fn stat(&self, fid: Fid) -> Result<Stat>;
}
```

Each path in the namespace is a `Resource` enum variant:

```rust
enum Resource {
    FunctionDir,
    FunctionFile { name: String },
    FunctionSchema { name: String },
    FunctionVersions { name: String },
    SessionDir { run_id: RunId },
    SessionTurns { run_id: RunId },
    SessionStatus { run_id: RunId },
    SessionFork { run_id: RunId },
    DeployBlobs,
    DeployRegister,
    ClusterPeers,
    ClusterLeader,
    RemoteProxy { peer_key: PublicKey, path: String },
}
```

---

## WASM Integration

For WASM functions, the Legion runtime intercepts WASI filesystem calls and routes them through the 9P namespace:

```
WASM guest calls: fd_write(fd, "Hello") where fd = /sessions/abc/turns
     ↓
wasmtime host interceptor
     ↓
legion-namespace write handler
     ↓
EventStore.append(UserMessage("Hello"))
```

This means WASM functions can interact with the cluster using standard file I/O — no custom SDK required. A WASM function that reads its input from stdin and writes its result to stdout is already compatible.

---

## Bun Integration

For Bun workers, a thin TypeScript adapter provides a Node.js-compatible fs interface backed by 9P:

```typescript
// In a Bun worker (via the legion-bun-client package)
import { createLegionFs } from '@legion/client';

const fs = createLegionFs({ sessionId: process.env.LEGION_SESSION_ID });

// Read context
const context = JSON.parse(await fs.readFile('/sessions/$ID/context', 'utf8'));

// Append a turn
await fs.writeFile('/sessions/$ID/turns', JSON.stringify({
  kind: 'AssistantMessage',
  content: 'Hello!'
}));
```

The adapter communicates with the legion node via WebTransport (jetstream's browser/Bun-compatible transport).
