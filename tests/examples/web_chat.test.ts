import { expect, test } from "bun:test";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { startWebChat } from "../../examples/web-chat/server.ts";

const sessionId = "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa";
function client(calls: string[]) {
  return {
    createSession: async () => ({ id: sessionId, status: "idle" }),
    getSession: async (id: string) => { calls.push(`get:${id}`); return {}; },
  } as any;
}

async function sseEvents(response: Response, count: number) {
  if (!response.body) throw new Error("missing SSE body");
  const reader = response.body.getReader();
  const decoder = new TextDecoder();
  let buffer = "";
  const events: any[] = [];
  while (events.length < count) {
    const { done, value } = await reader.read();
    if (done) break;
    buffer += decoder.decode(value, { stream: true });
    const frames = buffer.split(/\r?\n\r?\n/);
    buffer = frames.pop() ?? "";
    for (const frame of frames) {
      const line = frame.split(/\r?\n/).find(item => item.startsWith("data:"));
      if (line) events.push(JSON.parse(line.slice(5).trim()));
    }
  }
  await reader.cancel();
  return events;
}

const streamTurn = async (_id: string, _text: string, emit: (event: any) => void | Promise<void>) => {
  await emit({ type: "state", state: "reasoning" });
  await emit({ type: "text_delta", delta: "Durable " });
  await emit({ type: "text_delta", delta: "answer" });
  await emit({ type: "done", content: "Durable answer", seq: 3, tokens_in: 12, tokens_out: 4, wall_ms: 25 });
};

test("web chat serves its UI and initializes a durable session", async () => {
  const server = await startWebChat(0, client([]), undefined, streamTurn);
  try {
    const response = await fetch(`http://127.0.0.1:${server.port}/`);
    expect(response.status).toBe(200);
    expect(await response.text()).toContain("Responses stream over SSE");
    const initialized = await fetch(`http://127.0.0.1:${server.port}/api/session`).then(value => value.json());
    expect(initialized).toEqual({ sessionId, model: "anthropic/claude-haiku-3-5", history: [] });
  } finally { server.stop(true); }
});

test("web chat streams model state and persists across a server restart", async () => {
  const stateDir = await mkdtemp(join(tmpdir(), "legion-web-chat-"));
  const calls: string[] = [];
  let server = await startWebChat(0, client(calls), stateDir, streamTurn);
  const port = server.port;
  try {
    const eventResponse = await fetch(`http://127.0.0.1:${port}/api/events?session=${sessionId}`);
    const reading = sseEvents(eventResponse, 7);
    const accepted = await fetch(`http://127.0.0.1:${port}/api/messages`, {
      method: "POST", headers: { "content-type": "application/json" },
      body: JSON.stringify({ sessionId, text: "Durable question" }),
    });
    expect(accepted.status).toBe(202);
    const events = await reading;
    expect(events.map(event => event.type)).toEqual(["state", "state", "state", "text_delta", "text_delta", "done", "state"]);
    expect(events.find(event => event.type === "done")).toMatchObject({
      content: "Durable answer", tokens_in: 12, tokens_out: 4, wall_ms: 25,
    });
    expect(JSON.stringify(events)).not.toContain("hidden reasoning");
    server.stop(true);

    server = await startWebChat(port, client(calls), stateDir, streamTurn);
    const restored = await fetch(`http://127.0.0.1:${port}/api/session?session=${sessionId}`).then(value => value.json());
    expect(restored.history).toEqual([
      { role: "user", text: "Durable question" },
      { role: "legion", text: "Durable answer" },
    ]);
    expect(calls).toEqual([`get:${sessionId}`]);
  } finally {
    server.stop(true);
    await rm(stateDir, { recursive: true, force: true });
  }
});
