# Function Deployment

Functions in Legion are immutable, content-addressed artifacts. Deployment is the act of pushing a blob and registering its hash as a named, versioned function in the Raft-replicated function registry. This makes finding functions a mess for humans, but I will take it for now, there will be readable metadata later.

## Core Principle: The Hash Is the Version

```
bun build --target=bun my-agent.ts > my-agent.js
sha256(my-agent.js) = bafkrei...   ← this IS the version identifier
```

You never say "version 2.1.0" — you say "the function at CID bafkrei...". Semantic version strings are informational labels on top of this.

## Deployment Flow

```
1. Build artifact
   ┌─ WASM: cargo build --target=wasm32-wasi → fn.wasm
   └─ Bun:  bun build --target=bun fn.ts → fn.js

2. Push blob
   legion deploy push fn.js
     → POST /deploy/blobs (via 9P write)
     → iroh-blobs stores it, returns CID
     → CID = content hash (immutable, deduplicated)

3. Register function
   legion deploy register \
     --name my-agent \
     --cid bafkrei... \
     --runtime bun \
     --schema schema.json
     → Raft entry: RegisterFunction { name, cid, runtime, schema, version }
     → Committed → all nodes resolve "my-agent" → CID

4. Route traffic
   By default, the most recently registered CID becomes the default.
   Override with: legion deploy route --name my-agent --cid bafkrei... --weight 100
```

## Artifact Types

| Runtime | Format | Notes |
|---|---|---|
| `bun` | Single-file JS bundle | Built with `bun build --target=bun` |
| `wasm` | `.wasm` module | WASI-compatible; extism PDK recommended for tool authoring |
| `wasm-component` | `.wasm` component | WASI 0.2 Component Model |

## Function Schema

Every registered function has a JSON Schema describing its inputs and outputs:

```json
{
  "name": "my-agent",
  "description": "Answers questions about the codebase",
  "input": {
    "type": "object",
    "properties": {
      "question": { "type": "string" },
      "context":  { "type": "string" }
    },
    "required": ["question"]
  },
  "output": {
    "type": "object",
    "properties": {
      "answer": { "type": "string" },
      "sources": { "type": "array", "items": { "type": "string" } }
    }
  },
  "effects": "read",
  "timeout_ms": 30000
}
```

The `effects` field feeds into the agent loop's effect classification (see [02-agent-loop.md](02-agent-loop.md)).

## Routing

### Default routing

```sql
-- function_routes table
name = "my-agent"
default_cid = "bafkrei..."
routes = NULL   -- single version, full traffic
```

### Canary deployment

```sql
routes = [
  { "cid": "bafkrei_v1...", "weight": 90 },
  { "cid": "bafkrei_v2...", "weight": 10 }
]
```

Traffic is split probabilistically at request time. The weight is stored as a Raft entry; all nodes route identically.

### Atomic promotion

To promote a canary to 100%:

```
legion deploy promote --name my-agent --cid bafkrei_v2...
  → Raft entry: SetDefaultRoute { name, cid }
  → Committed → all nodes switch instantly
```

### Rollback

```
legion deploy rollback --name my-agent --to bafkrei_v1...
  → Same as promote, but to the previous CID
```

## Blob Distribution

Blobs are stored in iroh-blobs, which handles peer-to-peer distribution automatically:

- When a blob is pushed to node A, nodes B and C fetch it on first use
- Content integrity is structurally guaranteed (hash == content)
- A node that has the blob can serve it to any peer that requests it
- Blobs are never modified; the CID is a permanent identifier

The `legion-deploy` crate uses iroh-blobs' native transfer protocol — no HTTP, no registry service.

## CLI Reference

```bash
# Build
bun build --target=bun src/my-fn.ts --outfile dist/my-fn.js

# Deploy
legion deploy push dist/my-fn.js
# → CID: bafkrei...

legion deploy register \
  --name my-fn \
  --cid bafkrei... \
  --runtime bun \
  --schema schemas/my-fn.json

# Inspect
legion deploy list
legion deploy versions my-fn
legion deploy inspect bafkrei...

# Route
legion deploy canary my-fn --cid bafkrei_new... --weight 10
legion deploy promote my-fn --cid bafkrei_new...
legion deploy rollback my-fn

# Via 9P
echo '{"question": "hello"}' | 9p write /fn/my-fn
9p read /fn/my-fn/versions
```

## Dependency Deduplication

Because blobs are content-addressed, shared dependencies between functions are automatically deduplicated:

- Two functions built with the same `bun` stdlib version share the same blob for that stdlib
- No special bundling or tree-shaking required for deduplication — it's structural

In practice, Bun's `--target=bun` single-file bundles do inline all dependencies, so each function blob is self-contained. WASM components via the Component Model can share interface types.
