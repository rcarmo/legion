# Function Runtime

Legion runs functions in two execution environments: WASM (via wasmtime + extism) and Bun (via subprocess). Both runtimes receive input through the 9P namespace and return output the same way.

## Runtime Selection

The runtime is specified at function registration time:

```json
{
  "name": "my-fn",
  "cid": "bafkrei...",
  "runtime": "bun"         // or "wasm" or "wasm-component"
}
```

The `legion-runtime` crate dispatches to the appropriate executor.

---

## WASM Runtime (wasmtime + extism)

### Why extism over raw wasmtime?

Raw wasmtime exposes WASI to guest modules, but doesn't provide typed input/output or a plugin development kit. extism adds:

- **Typed PDK**: Guest functions declare typed inputs/outputs in Rust, TypeScript, Go, Python, etc.
- **Host functions**: The host (Legion) provides functions the guest can call (logging, HTTP, etc.)
- **Memory management**: extism handles the host↔guest ABI for arbitrary-length data

### WASM execution flow

```
1. Resolve function name → CID (from hiqlite function registry)
2. Fetch blob from iroh-blobs (cached locally after first fetch)
3. Create extism Plugin from wasm bytes
4. Set up host functions (9P namespace access, budget tracking)
5. plugin.call("run", input_json) → output_json
6. Enforce timeout + memory limits
7. Store result CID in iroh-blobs if large
8. Return result to caller
```

### Host functions exposed to WASM guests

```rust
// Available to all WASM functions via extism
host_fn!(legion_log(message: String) -> () { /* forward to tracing */ });
host_fn!(legion_read(path: String) -> String { /* read from 9P namespace */ });
host_fn!(legion_write(path: String, data: String) -> () { /* write to 9P namespace */ });
host_fn!(legion_budget_remaining() -> BudgetRemaining { /* check budget */ });
```

### Writing a WASM function (Rust PDK)

```rust
use extism_pdk::*;
use serde::{Deserialize, Serialize};

#[derive(Deserialize)]
struct Input { question: String }

#[derive(Serialize)]
struct Output { answer: String }

#[plugin_fn]
pub fn run(input: Json<Input>) -> FnResult<Json<Output>> {
    Ok(Json(Output {
        answer: format!("You asked: {}", input.question),
    }))
}
```

### Writing a WASM function (TypeScript PDK)

```typescript
import { input, output } from "@extism/pdk";

interface Input { question: string }
interface Output { answer: string }

export function run(): number {
  const inp = input.json<Input>();
  output.setJson<Output>({ answer: `You asked: ${inp.question}` });
  return 0;
}
```

Compile with: `javy compile my-fn.js -o my-fn.wasm` or use `extism-js`

---

## Bun Runtime

Bun functions run as child processes. The parent Legion process communicates with the Bun worker via stdio and optionally via the 9P namespace.

### Bun execution flow

```
1. Resolve function name → CID
2. Fetch blob from iroh-blobs → write to temp path (or memfd)
3. Spawn: bun run /tmp/legion-fn-<cid>.js
4. Write JSON input to worker stdin
5. Read JSON output from worker stdout
6. Enforce timeout via process group kill
7. Capture stderr for logging
8. Return result
```

### Bun function interface

```typescript
// A Bun function reads from stdin, writes to stdout
const input = JSON.parse(await Bun.stdin.text());

// ... do work ...

process.stdout.write(JSON.stringify({
  answer: "42",
  sources: ["wikipedia.org/wiki/..."]
}));
```

### Environment variables

Legion injects these into every Bun worker:

```
LEGION_SESSION_ID    current session run_id
LEGION_NODE_KEY      this node's iroh public key
LEGION_9P_ADDR       iroh endpoint for 9P namespace access
LEGION_BUDGET_JSON   remaining budget (tokens, steps, wall_ms)
```

### Long-running Bun agents

For agents that need to run a full session loop (not just one call), the Bun worker can use the `@legion/client` package to interact with the session directly:

```typescript
import { LegionClient } from '@legion/client';

const client = new LegionClient(process.env.LEGION_9P_ADDR);
const session = client.session(process.env.LEGION_SESSION_ID);

// Stream turns
for await (const turn of session.turns()) {
  console.log(turn);
  // Respond
  await session.appendTurn({ kind: 'AssistantMessage', content: '...' });
}
```

---

## Resource Limits

Enforced per function invocation:

```rust
pub struct RuntimeLimits {
    pub max_memory_bytes: usize,    // WASM: wasmtime memory limit
    pub max_wall_ms:      u64,      // both: wall clock timeout
    pub max_cpu_ms:       Option<u64>, // WASM only: fuel-based CPU limit
    pub max_output_bytes: usize,    // both: stdout/return size limit
}
```

### WASM fuel

wasmtime supports "fuel" — a deterministic instruction counter that causes the WASM module to trap when exhausted. This provides a CPU budget independent of wall clock time (useful for reproducible billing).

```rust
let mut store = Store::new(&engine, ());
store.set_fuel(100_000_000)?;  // ~100M WASM instructions
```

### Process isolation

Bun functions run in a separate process group. On timeout or budget exhaustion, Legion sends `SIGKILL` to the entire group, ensuring no orphaned subprocesses.

---

## Caching

Function blobs are cached locally after the first fetch from iroh-blobs:

```
iroh-blobs local store: /var/lib/legion/blobs/
```

The cache is content-addressed (CID = hash), so stale entries are impossible by construction. Eviction is time-based (LRU, configurable TTL) with a configurable size limit.

---

## Runtime Comparison

| | WASM (extism) | Bun |
|---|---|---|
| Languages | Rust, TS, Python, Go, C, … | TypeScript, JavaScript |
| Startup time | ~5ms | ~50–200ms |
| Memory isolation | Strict (wasmtime sandbox) | Process isolation |
| CPU determinism | Yes (fuel) | No |
| Streaming output | Via host functions | Via stdout chunking |
| SDK required | extism PDK (optional) | None (stdio convention) |
| File system access | Via host functions | Via env + @legion/client |
| Native modules | No | Yes (Bun native) |

Choose WASM for security-sensitive, multi-language, or billing-critical functions. Choose Bun for TypeScript-native agents that benefit from Bun's full API surface.
