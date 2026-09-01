# Web chat

A dependency-free browser chat over WebSocket. Each browser connection is routed to one durable Legion session through `WebChatAdapter` and `LegionChannelRouter`.

```sh
bun examples/web-chat/server.ts
# open http://127.0.0.1:3001
```

Requires model credentials in the Legion service environment. Set `PORT` to change the web server port.
