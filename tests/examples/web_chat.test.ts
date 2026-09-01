import { expect, test } from "bun:test";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { startWebChat } from "../../examples/web-chat/server.ts";

function nextMessage(socket: WebSocket) {
  return new Promise<any>((resolve, reject) => {
    socket.onmessage = event => resolve(JSON.parse(String(event.data)));
    socket.onerror = () => reject(new Error("WebSocket failed"));
  });
}

function client(calls: string[]) {
  return {
    createSession: async () => ({ id: "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa", status: "idle" }),
    getSession: async (id: string) => { calls.push(`get:${id}`); return {}; },
    sendMessage: async (id: string, text: string) => {
      calls.push(`send:${id}:${text}`);
      return { response: "Durable answer" };
    },
  } as any;
}

test("web chat serves its browser UI and accepts WebSocket clients", async () => {
  const server = await startWebChat(0, client([]));
  try {
    const response = await fetch(`http://127.0.0.1:${server.port}/`);
    expect(response.status).toBe(200);
    expect(await response.text()).toContain("Legion Web Chat");
    const socket = new WebSocket(`ws://127.0.0.1:${server.port}/ws`);
    expect(await nextMessage(socket)).toEqual({
      type: "ready", sessionId: "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa", history: [],
    });
    socket.close();
  } finally {
    server.stop(true);
  }
});

test("web chat restores transcript and Legion session after a server restart", async () => {
  const stateDir = await mkdtemp(join(tmpdir(), "legion-web-chat-"));
  const sessionId = "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa";
  const calls: string[] = [];
  let server = await startWebChat(0, client(calls), stateDir);
  const port = server.port;
  try {
    const first = new WebSocket(`ws://127.0.0.1:${port}/ws`);
    expect(await nextMessage(first)).toEqual({ type: "ready", sessionId, history: [] });
    first.send(JSON.stringify({ text: "Durable question" }));
    expect(await nextMessage(first)).toEqual({ type: "message", text: "Durable answer" });
    first.close();
    server.stop(true);

    server = await startWebChat(port, client(calls), stateDir);
    const restored = new WebSocket(`ws://127.0.0.1:${port}/ws?session=${sessionId}`);
    expect(await nextMessage(restored)).toEqual({
      type: "ready",
      sessionId,
      history: [
        { role: "user", text: "Durable question" },
        { role: "legion", text: "Durable answer" },
      ],
    });
    expect(calls).toEqual([`send:${sessionId}:Durable question`, `get:${sessionId}`]);
    restored.close();
  } finally {
    server.stop(true);
    await rm(stateDir, { recursive: true, force: true });
  }
});
