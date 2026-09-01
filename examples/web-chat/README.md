# Web chat

A dependency-free browser chat over WebSocket. The browser retains its Legion run ID in local storage, and the companion keeps an atomic transcript journal keyed by that run ID. Reloading the page or restarting the companion restores the same durable Legion session and visible transcript.

```sh
bun examples/web-chat/server.ts
# open http://127.0.0.1:3001
```

Requires model credentials in the Legion service environment. Set `HOST`, `PORT`, `LEGION_URL`, and `LEGION_MODEL` to override the defaults. Set `STATE_DIRECTORY` or `WEB_CHAT_STATE_DIR` to persist visible transcripts across companion restarts; systemd's `StateDirectory=` is recommended.

The UI shows connection state, restored history, server errors, and a message form. Clearing site storage starts a new conversation. The reference node serves the systemd-managed example at `http://192.168.1.176:18081/` using `opencode/nemotron-3.5-lightning-free`, with transcripts under `/var/lib/legion-web-chat`.
