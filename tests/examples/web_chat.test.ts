import { expect, test } from "bun:test";
import { startWebChat } from "../../examples/web-chat/server.ts";

test("web chat serves its browser UI and accepts WebSocket clients", async () => {
  const server = await startWebChat(0);
  try {
    const response = await fetch(`http://127.0.0.1:${server.port}/`);
    expect(response.status).toBe(200);
    expect(await response.text()).toContain("Legion Web Chat");
    const opened = new Promise<void>((resolve, reject) => {
      const socket = new WebSocket(`ws://127.0.0.1:${server.port}/ws`);
      socket.onopen = () => { socket.close(); resolve(); };
      socket.onerror = () => reject(new Error("WebSocket failed to open"));
    });
    await opened;
  } finally {
    server.stop(true);
  }
});
