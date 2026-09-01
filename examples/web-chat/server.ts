import type { ServerWebSocket } from "bun";
import { LegionChannelRouter, WebChatAdapter } from "../../packages/channels/src/index.ts";
import { legion, model } from "../shared/legion.ts";

export async function startWebChat(port = Number(process.env.PORT ?? 3001)) {
  const clients = new Map<string, ServerWebSocket<{ chatId: string }>>();
  const adapter = new WebChatAdapter(({ chatId, text, replyTo }) => {
    clients.get(chatId)?.send(JSON.stringify({ text, replyTo }));
  });
  const router = new LegionChannelRouter({ client: legion, session: { model: model(), system_prompt: "Be concise and helpful." } });
  await adapter.start(message => router.handle(adapter, message));
  const html = await Bun.file(new URL("./index.html", import.meta.url)).text();
  return Bun.serve<{ chatId: string }>({
    port,
    fetch(request, server) {
      if (new URL(request.url).pathname === "/ws") {
        const chatId = crypto.randomUUID();
        return server.upgrade(request, { data: { chatId } }) ? undefined : new Response("upgrade failed", { status: 400 });
      }
      return new Response(html, { headers: { "content-type": "text/html" } });
    },
    websocket: {
      open(ws) { clients.set(ws.data.chatId, ws); },
      close(ws) { clients.delete(ws.data.chatId); },
      message(ws, raw) {
        const value = JSON.parse(String(raw));
        void adapter.receive({ id: crypto.randomUUID(), chatId: ws.data.chatId, senderId: ws.data.chatId, text: String(value.text ?? "") });
      },
    },
  });
}

if (import.meta.main) {
  const server = await startWebChat();
  console.log(`web chat: http://127.0.0.1:${server.port}`);
}
