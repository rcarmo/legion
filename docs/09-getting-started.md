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

The daemon reads `./legion.toml` by default. Set `LEGION_CONFIG` to an explicit path; unlike the optional default, an unreadable or invalid explicit file is fatal.

```toml
# Optional API authentication; prefer LEGION_API_KEY in the environment.
# api_key = "replace-me"

# Distributed builds additionally accept top-level raft_peers,
# raft_node_id, raft_secret, and raft_api_secret values here.

[cluster]
data_dir = "/var/lib/legion"
bind_addr = "0.0.0.0:0"
api_port = 8080
mdns = true

[model]
default_model = "anthropic/claude-haiku-3-5"
system_prompt = "You are a Legion cluster agent."

[invocation]
timeout_ms = 30000
max_input_bytes = 1048576
max_output_bytes = 4194304
max_concurrent_per_function = 8
max_requests_per_window = 120
rate_window_ms = 60000

[session_rate_limit]
max_requests_per_window = 30
window_ms = 60000
```

The packaged systemd unit sets `LEGION_CONFIG=/etc/legion/legion.toml` and loads secrets from `/etc/legion/legion.env`.

## systemd Installation

Build and install a dedicated `legion` service account, hardened unit, configuration, and environment template:

```bash
cargo build -p legion-server --release
sudo ENABLE=1 make install
systemctl status legion.service
journalctl -u legion.service -f
```

`make install` preserves existing files under `/etc/legion`. `make uninstall` removes the binary and unit but deliberately preserves `/etc/legion` and `/var/lib/legion`. Use `DESTDIR=/tmp/legion-package make install` to stage a package without touching the host.

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
POST   /sessions/{id}/reconcile        # skip/retry a dangling tool call
GET    /functions                      # list functions
POST   /functions                      # deploy Bun source or base64 WASM
DELETE /functions/{name}
POST   /functions/{name}/invoke
GET    /metrics                        # Prometheus text metrics
```

## Namespace authentication

Set `LEGION_NAMESPACE_CAPABILITY` (or `namespace_capability` in `legion.toml`) to require a bearer capability on every 9P attach. Clients send it in `Tattach.aname` as `cap=<token>`; Legion's peer proxy forwards the configured capability automatically. Keep this token independent from `LEGION_API_KEY`, and prefer the environment or service credential storage over a checked-in config file. When unset, the namespace remains available to authenticated iroh peers for development compatibility.

## Useful Commands

```bash
systemctl start legion   # start an installed daemon
systemctl status legion  # inspect an installed daemon
legion cluster peers     # list cluster peers
legion cluster health    # health summary
legion session list      # list sessions
legion session new       # create session
legion session send      # send user message
legion session history   # view turn history
legion session reconcile <id> --action skip|retry
legion deploy push <name> <path> --runtime bun|wasm
legion deploy list       # list functions
legion deploy delete     # remove a function
legion call <name>       # invoke function (stdin/stdout)
```

### Invocation limits

Bun and WASM functions share the same limits whether called through REST or by an agent. Configure them in `legion.toml`:

```toml
[invocation]
timeout_ms = 30000
max_input_bytes = 1048576
max_output_bytes = 4194304
max_concurrent_per_function = 8
max_requests_per_window = 120
rate_window_ms = 60000
```

The equivalent environment overrides are `LEGION_INVOKE_TIMEOUT_MS`, `LEGION_INVOKE_MAX_INPUT_BYTES`, `LEGION_INVOKE_MAX_OUTPUT_BYTES`, `LEGION_INVOKE_MAX_CONCURRENT_PER_FUNCTION`, `LEGION_INVOKE_MAX_REQUESTS_PER_WINDOW`, and `LEGION_INVOKE_RATE_WINDOW_MS`. Limit errors return HTTP 413 (payload), 429 (rate/concurrency), or 504 (deadline). `/metrics` reports per-function invocation counts and wall time plus replay-derived agent turn and token totals.

Legion exports agent-loop spans and token consumption over OTLP/HTTP when `OTEL_EXPORTER_OTLP_ENDPOINT` or a signal-specific endpoint is set. Standard OpenTelemetry variables configure endpoints, headers, and timeouts. Input, output, cache-read, and cache-write usage are monotonic counters where the provider supplies those values. Attributes are restricted to bounded operational dimensions such as provider, configured model, node, and outcome; session IDs, run IDs, prompts, and user content are forbidden to avoid high-cardinality series and data leakage. The existing `/metrics` endpoint remains available independently of OTLP export. Run `make otel-integration-test` to verify local trace and metric delivery.

Session execution requests (`messages`, `stream`, and external `events`) are limited per session through `[session_rate_limit]`, with `LEGION_SESSION_MAX_REQUESTS_PER_WINDOW` and `LEGION_SESSION_RATE_WINDOW_MS` overrides. Read and reconciliation routes remain available. HTTP 429 responses include `Retry-After`; function and session rejections are exported by `/metrics`.

Session budgets accept `max_turns`, `max_tool_calls`, `max_tokens_in`, `max_tokens_out`, and `max_wall_ms`. Budget halts are stored as `SessionBudgetHalt` events and set the durable session status to `budget_halt`.

## Troubleshooting

**Node doesn't discover peers**: Check that UDP multicast is enabled on your network interface (`ip maddr show`). Some managed switches block mDNS multicast.

**Raft won't form**: Ensure all nodes can reach each other on the configured iroh port. Legion will log the iroh relay it falls back to if direct connections fail.

**Function fails silently**: Check `~/.local/share/legion/logs/` and look for budget exhaustion or WASM trap messages.

**Session stuck in `pending_reconciliation`**: A write-ahead intent was logged but the tool result never arrived (crash mid-execution). Use `legion session reconcile <run-id> --action skip` to record a synthetic skipped result, or `--action retry` to dispatch the stored arguments again. Legacy intents created before arguments were persisted can only be skipped.
