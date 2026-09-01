# Web chat

A dependency-free browser chat over WebSocket. Each browser connection is routed to one durable Legion session through `WebChatAdapter` and `LegionChannelRouter`.

```sh
bun examples/web-chat/server.ts
# open http://127.0.0.1:3001
```

Requires model credentials in the Legion service environment. Set `HOST`, `PORT`, `LEGION_URL`, and `LEGION_MODEL` to override the defaults.

The example UI shows connection state, a per-connection transcript, server errors, and a message form. Each browser connection creates one durable Legion session. The reference node serves the systemd-managed example at `http://192.168.1.176:18081/` using `opencode/nemotron-3.5-lightning-free`.
