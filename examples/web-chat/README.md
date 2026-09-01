# Web chat

A dependency-free browser chat using HTTP and Server-Sent Events (SSE). The browser retains its Legion run ID in local storage, and the companion keeps an atomic transcript journal keyed by that run ID. Reloading the page or restarting the companion restores the same durable Legion session and visible transcript.

```sh
bun examples/web-chat/server.ts
# open http://127.0.0.1:3001
```

Requires model credentials in the Legion service environment. Set `HOST`, `PORT`, `LEGION_URL`, and `LEGION_MODEL` to override the defaults. Set `STATE_DIRECTORY` or `WEB_CHAT_STATE_DIR` to persist visible transcripts across companion restarts; systemd's `StateDirectory=` is recommended. `MODEL_TIMEOUT_MS` bounds stalled upstream streams and defaults to 120 seconds.

The browser receives incremental text and lifecycle events over SSE. The model-state panel shows the selected model, durable run state, elapsed time, and final input/output token and latency metrics. Thinking events are represented only as a `Reasoning` activity state; hidden reasoning text is never forwarded to the browser.

Clearing site storage starts a new conversation. The reference node serves the systemd-managed example at `http://redshirt:18081/` (or the VM's current LAN address) using `opencode/nemotron-3.5-lightning-free`, with transcripts under `/var/lib/legion-web-chat`.
