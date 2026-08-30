# Getting Started

This guide sets up a single-node Legion instance for local development, then extends to a 3-node cluster. Right now this is the best documentation `piclaw` can do, and will eventually be split into better sections.

## Prerequisites

- Rust 1.95+ (stable; legion uses edition 2024)
- Bun 1.4+ (for Bun function authoring and the client CLI)
- A recent Linux or macOS system
- `plan9port` (optional, for native 9P client): `brew install plan9port` or `apt install plan9-tools`

## Clone and Build

```bash
git clone https://github.com/rcarmo/legion
cd legion
make build
```

This produces a single binary: `target/release/legion`. With no command (or with `serve`) it runs the daemon; the other subcommands are REST clients.

## Single-Node Quick Start

```bash
# Generate a node keypair and start
LEGION_DATA_DIR=~/.local/share/legion target/release/legion serve

# In another terminal, check status
target/release/legion health
target/release/legion cluster peers
```

The server starts, generates a keypair, creates a single-node Raft cluster, and advertises itself via mDNS.

## Your First Function (Bun)

```bash
# Create a simple function
cat > hello.ts << 'EOF'
const input = JSON.parse(await Bun.stdin.text());
process.stdout.write(JSON.stringify({ greeting: `Hello, ${input.name}!` }));
EOF

# Deploy it
legion deploy push hello hello.ts --runtime bun

# Invoke it
echo '{"name": "World"}' | legion call hello
# → {"greeting": "Hello, World!"}
```

## Your First Function (WASM)

```bash
# Create a Rust extism plugin
cargo new --lib hello-wasm
cd hello-wasm

# Cargo.toml: add extism-pdk, crate-type = ["cdylib"]
cat >> Cargo.toml << 'EOF'
[dependencies]
extism-pdk = "1"
[lib]
crate-type = ["cdylib"]
EOF

cat > src/lib.rs << 'EOF'
use extism_pdk::*;
use serde::{Deserialize, Serialize};

#[derive(Deserialize)] struct Input { name: String }
#[derive(Serialize)]   struct Output { greeting: String }

#[plugin_fn]
pub fn run(input: Json<Input>) -> FnResult<Json<Output>> {
    Ok(Json(Output { greeting: format!("Hello, {}!", input.name) }))
}
EOF

cargo build --release --target wasm32-wasip1
legion deploy push hello-wasm target/wasm32-wasip1/release/hello_wasm.wasm --runtime wasm
echo '{"name":"World"}' | legion call hello-wasm
```

## Your First Agent Session

```bash
# Set your API key
export ANTHROPIC_API_KEY=sk-ant-...

# Start a session
RUN=$(legion session new \
  --model anthropic/claude-opus-4-5 \
  --system-prompt "You are helpful" | jq -r .id)

# Send a message
legion session send $RUN "What is the capital of Portugal?"

# Or stream a new message as SSE
legion session stream $RUN "What is the capital of Portugal?"

# View full turn history
legion session history $RUN
```

## Connecting a Second Node (3-node Cluster)

On a second machine on the same LAN:

```bash
# Same command — no cluster config needed
legion-server --data-dir ~/.local/share/legion --listen 0.0.0.0:7777
```

The new node will:
1. Advertise itself via mDNS
2. Discover the existing node via mDNS
3. Request to join the Raft cluster
4. Receive the full state machine snapshot
5. Become a Raft follower

Verify:
```bash
9p read /cluster/peers   # shows both nodes
9p read /cluster/leader  # shows leader's iroh key
```

Repeat on a third machine. The cluster is now fault-tolerant: any single node failure maintains quorum.

## Configuration

All configuration is in a TOML file (default: `~/.config/legion/config.toml`):

```toml
[node]
data_dir       = "~/.local/share/legion"
keypair_path   = "~/.config/legion/keypair"

[listen]
nine_p_addr    = "0.0.0.0:7777"
api_addr       = "0.0.0.0:8080"

[raft]
heartbeat_ms         = 200
election_timeout_ms  = 1000

[discovery]
mdns   = true   # LAN discovery (default on)
dht    = false  # BitTorrent DHT (for WAN peers)

[blobs]
cache_dir      = "~/.local/share/legion/blobs"
cache_max_gb   = 10
cache_ttl_days = 30

[runtime]
bun_path       = "/home/agent/.bun/bin/bun"
max_memory_mb  = 512
max_wall_ms    = 30000

[backup]
enabled        = false
# s3_bucket   = "my-legion-backup"
# s3_prefix   = "legion/"
```

## REST API

Legion exposes a REST API on port 8080 for the bundled CLI and other clients:

```
GET    /health
GET    /cluster/peers
GET    /sessions                       # list/filter sessions
POST   /sessions                       # create session
GET    /sessions/{id}                  # session status
POST   /sessions/{id}/messages         # send and resolve
GET    /sessions/{id}/stream           # resolve via SSE
GET    /sessions/{id}/log              # event history
POST   /sessions/{id}/events           # external trigger
GET    /functions                      # list functions
POST   /functions                      # deploy Bun source or base64 WASM
DELETE /functions/{name}
POST   /functions/{name}/invoke
```

## Useful Commands

```bash
legion server start      # start the server
legion server status     # check server status
legion cluster peers     # list cluster peers
legion cluster health    # health summary
legion session list      # list sessions
legion session new       # create session
legion session send      # send user message
legion session history   # view turn history
legion deploy push <name> <path> --runtime bun|wasm
legion deploy list       # list functions
legion deploy delete     # remove a function
legion call <name>       # invoke function (stdin/stdout)
```

## Troubleshooting

**Node doesn't discover peers**: Check that UDP multicast is enabled on your network interface (`ip maddr show`). Some managed switches block mDNS multicast.

**Raft won't form**: Ensure all nodes can reach each other on the configured iroh port. Legion will log the iroh relay it falls back to if direct connections fail.

**Function fails silently**: Check `~/.local/share/legion/logs/` and look for budget exhaustion or WASM trap messages.

**Session stuck in `pending_reconciliation`**: A write-ahead intent was logged but the tool result never arrived (crash mid-execution). Use `legion session reconcile <run-id>` to skip or replay the dangling entry.
