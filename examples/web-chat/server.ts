import type { ServerWebSocket } from "bun";
import { mkdir, rename } from "node:fs/promises";
import { legion, model } from "../shared/legion.ts";

type ChatEntry = { role: "user" | "legion"; text: string };
type ChatSocket = ServerWebSocket<{ requestedSessionId?: string; sessionId?: string }>;

function validSessionId(value: string) {
  return /^[0-9a-f]{8}-[0-9a-f]{4}-[1-8][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i.test(value);
}

function transcriptStore(stateDir?: string) {
  const memory = new Map<string, ChatEntry[]>();
  const locks = new Map<string, Promise<void>>();
  const pathFor = (sessionId: string) => `${stateDir}/${sessionId}.json`;

  async function load(sessionId: string): Promise<ChatEntry[]> {
    if (!stateDir) return memory.get(sessionId) ?? [];
    if (!validSessionId(sessionId)) return [];
    try {
      const value = await Bun.file(pathFor(sessionId)).json();
      return Array.isArray(value) ? value.filter(entry =>
        (entry?.role === "user" || entry?.role === "legion") && typeof entry?.text === "string"
      ) : [];
    } catch {
      return [];
    }
  }

  async function append(sessionId: string, entry: ChatEntry) {
    const previous = locks.get(sessionId) ?? Promise.resolve();
    const current = previous.then(async () => {
      const history = [...await load(sessionId), entry];
      memory.set(sessionId, history);
      if (!stateDir || !validSessionId(sessionId)) return;
      await mkdir(stateDir, { recursive: true });
      const temporary = `${pathFor(sessionId)}.${crypto.randomUUID()}.tmp`;
      await Bun.write(temporary, JSON.stringify(history));
      await rename(temporary, pathFor(sessionId));
    });
    locks.set(sessionId, current);
    try { await current; } finally { if (locks.get(sessionId) === current) locks.delete(sessionId); }
  }

  return { load, append };
}

export async function startWebChat(
  port = Number(process.env.PORT ?? 3001),
  client = legion,
  stateDir = process.env.STATE_DIRECTORY || process.env.WEB_CHAT_STATE_DIR,
) {
  const html = await Bun.file(new URL("./index.html", import.meta.url)).text();
  const sessionConfig = { model: model(), system_prompt: "Be concise and helpful." };
  const transcripts = transcriptStore(stateDir);

  async function initialize(ws: ChatSocket) {
    let sessionId = ws.data.requestedSessionId;
    if (sessionId) {
      try { await client.getSession(sessionId); } catch { sessionId = undefined; }
    }
    if (!sessionId) sessionId = (await client.createSession(sessionConfig)).id;
    ws.data.sessionId = sessionId;
    ws.send(JSON.stringify({ type: "ready", sessionId, history: await transcripts.load(sessionId) }));
  }

  return Bun.serve<{ requestedSessionId?: string; sessionId?: string }>({
    hostname: process.env.HOST ?? "127.0.0.1",
    port,
    fetch(request, server) {
      const url = new URL(request.url);
      if (url.pathname === "/ws") {
        const requestedSessionId = url.searchParams.get("session")?.trim() || undefined;
        return server.upgrade(request, { data: { requestedSessionId } }) ? undefined : new Response("upgrade failed", { status: 400 });
      }
      return new Response(html, { headers: { "content-type": "text/html" } });
    },
    websocket: {
      open(ws) {
        void initialize(ws).catch(error => ws.send(JSON.stringify({
          type: "error", error: error instanceof Error ? error.message : String(error),
        })));
      },
      message(ws, raw) {
        try {
          const value = JSON.parse(String(raw));
          const text = String(value.text ?? "").trim();
          const sessionId = ws.data.sessionId;
          if (!text) return;
          if (!sessionId) {
            ws.send(JSON.stringify({ type: "error", error: "chat session is not ready" }));
            return;
          }
          void transcripts.append(sessionId, { role: "user", text })
            .then(() => client.sendMessage(sessionId, text))
            .then(async response => {
              await transcripts.append(sessionId, { role: "legion", text: response.response });
              ws.send(JSON.stringify({ type: "message", text: response.response }));
            })
            .catch(error => ws.send(JSON.stringify({ type: "error", error: error instanceof Error ? error.message : String(error) })));
        } catch {
          ws.send(JSON.stringify({ type: "error", error: "invalid message" }));
        }
      },
    },
  });
}

if (import.meta.main) {
  const server = await startWebChat();
  console.log(`web chat: http://${server.hostname}:${server.port}`);
}
