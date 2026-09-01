import { mkdir, rename } from "node:fs/promises";
import { legion, model } from "../shared/legion.ts";

type ChatEntry = { role: "user" | "legion"; text: string };
type UiEvent = { type: string; [key: string]: unknown };
type Emit = (event: UiEvent) => void | Promise<void>;
type StreamTurn = (sessionId: string, text: string, emit: Emit) => Promise<void>;

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
    } catch { return []; }
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

function sseData(event: UiEvent) {
  return `data: ${JSON.stringify(event)}\n\n`;
}

async function defaultStreamTurn(sessionId: string, text: string, emit: Emit) {
  const baseUrl = (process.env.LEGION_URL ?? "http://127.0.0.1:18080").replace(/\/$/, "");
  const headers: Record<string, string> = { accept: "text/event-stream" };
  if (process.env.LEGION_API_KEY) headers.authorization = `Bearer ${process.env.LEGION_API_KEY}`;
  const controller = new AbortController();
  const timeoutMs = Number(process.env.MODEL_TIMEOUT_MS ?? 120_000);
  const timeout = setTimeout(() => controller.abort(), timeoutMs);
  let reader: ReadableStreamDefaultReader<Uint8Array> | undefined;
  try {
    const response = await fetch(`${baseUrl}/sessions/${encodeURIComponent(sessionId)}/stream?message=${encodeURIComponent(text)}`, {
      headers, signal: controller.signal,
    });
    if (!response.ok || !response.body) throw new Error(`Legion stream failed: ${response.status}`);
    reader = response.body.getReader();
    const decoder = new TextDecoder();
    let buffer = "";
    while (true) {
      const { done, value } = await reader.read();
      buffer += decoder.decode(value, { stream: !done });
      const frames = buffer.split(/\r?\n\r?\n/);
      buffer = frames.pop() ?? "";
      for (const frame of frames) {
        for (const line of frame.split(/\r?\n/)) {
          if (!line.startsWith("data:")) continue;
          const event = JSON.parse(line.slice(5).trim()) as UiEvent;
          if (event.type === "thinking_delta") await emit({ type: "state", state: "reasoning" });
          else await emit(event);
          if (["done", "error", "budget_halt"].includes(event.type)) return;
        }
      }
      if (done) return;
    }
  } catch (error) {
    if (controller.signal.aborted) throw new Error(`model stream timed out after ${Math.round(timeoutMs / 1000)}s`);
    throw error;
  } finally {
    clearTimeout(timeout);
    await reader?.cancel().catch(() => undefined);
  }
}

export async function startWebChat(
  port = Number(process.env.PORT ?? 3001),
  client = legion,
  stateDir = process.env.STATE_DIRECTORY || process.env.WEB_CHAT_STATE_DIR,
  streamTurn: StreamTurn = defaultStreamTurn,
) {
  const html = await Bun.file(new URL("./index.html", import.meta.url)).text();
  const selectedModel = model();
  const sessionConfig = { model: selectedModel, system_prompt: "Be concise and helpful." };
  const transcripts = transcriptStore(stateDir);
  const listeners = new Map<string, Set<Emit>>();
  const busy = new Set<string>();
  const emit = (sessionId: string, event: UiEvent) => listeners.get(sessionId)?.forEach(listener => listener(event));

  async function session(requested?: string) {
    let sessionId = requested;
    if (sessionId) {
      try { await client.getSession(sessionId); } catch { sessionId = undefined; }
    }
    if (!sessionId) sessionId = (await client.createSession(sessionConfig)).id;
    return { sessionId, model: selectedModel, history: await transcripts.load(sessionId) };
  }

  return Bun.serve({
    hostname: process.env.HOST ?? "127.0.0.1",
    port,
    async fetch(request) {
      const url = new URL(request.url);
      if (url.pathname === "/api/session" && request.method === "GET") {
        return Response.json(await session(url.searchParams.get("session")?.trim() || undefined));
      }
      if (url.pathname === "/api/events" && request.method === "GET") {
        const sessionId = url.searchParams.get("session")?.trim() ?? "";
        if (!validSessionId(sessionId)) return Response.json({ error: "invalid session" }, { status: 400 });
        let listener: Emit;
        let heartbeat: ReturnType<typeof setInterval>;
        const stream = new ReadableStream({
          start(controller) {
            listener = event => controller.enqueue(sseData(event));
            const set = listeners.get(sessionId) ?? new Set<Emit>();
            set.add(listener); listeners.set(sessionId, set);
            controller.enqueue(sseData({ type: "state", state: busy.has(sessionId) ? "responding" : "idle", model: selectedModel, sessionId }));
            heartbeat = setInterval(() => controller.enqueue(": keep-alive\n\n"), 15_000);
          },
          cancel() {
            clearInterval(heartbeat);
            const set = listeners.get(sessionId); set?.delete(listener);
            if (!set?.size) listeners.delete(sessionId);
          },
        });
        return new Response(stream, { headers: { "content-type": "text/event-stream", "cache-control": "no-cache", connection: "keep-alive" } });
      }
      if (url.pathname === "/api/messages" && request.method === "POST") {
        const body = await request.json().catch(() => ({})) as { sessionId?: string; text?: string };
        const sessionId = body.sessionId?.trim() ?? "";
        const text = body.text?.trim() ?? "";
        if (!validSessionId(sessionId) || !text) return Response.json({ error: "sessionId and text are required" }, { status: 400 });
        if (busy.has(sessionId)) return Response.json({ error: "session is busy" }, { status: 409 });
        busy.add(sessionId);
        await transcripts.append(sessionId, { role: "user", text });
        emit(sessionId, { type: "state", state: "thinking", startedAt: Date.now() });
        void streamTurn(sessionId, text, async event => {
          if (event.type === "done" && typeof event.content === "string") {
            await transcripts.append(sessionId, { role: "legion", text: event.content });
          }
          emit(sessionId, event);
        })
          .catch(error => emit(sessionId, { type: "error", message: error instanceof Error ? error.message : String(error) }))
          .finally(() => { busy.delete(sessionId); emit(sessionId, { type: "state", state: "idle" }); });
        return Response.json({ accepted: true }, { status: 202 });
      }
      return new Response(html, { headers: { "content-type": "text/html" } });
    },
  });
}

if (import.meta.main) {
  const server = await startWebChat();
  console.log(`web chat: http://${server.hostname}:${server.port}`);
}
