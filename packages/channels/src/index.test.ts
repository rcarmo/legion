import { describe, expect, test } from "bun:test";
import { LegionChannelRouter, WebChatAdapter } from "./index.ts";

class FakeClient {
  creates = 0;
  messages: Array<[string, string]> = [];
  async createSession() { this.creates++; return { id: "session-1", status: "idle" }; }
  async sendMessage(id: string, content: string) {
    this.messages.push([id, content]);
    return { response: `answer:${content}` };
  }
}

describe("picoclaw-compatible channel contract", () => {
  test("web chats retain a durable session and reply correlation", async () => {
    const client = new FakeClient();
    const sent: unknown[] = [];
    const adapter = new WebChatAdapter((message) => { sent.push(message); });
    const router = new LegionChannelRouter({
      client: client as never,
      session: { model: "faux/model" },
    });
    await adapter.start((message) => router.handle(adapter, message));
    await adapter.receive({ id: "m1", chatId: "c1", senderId: "u1", text: "hello" });
    await adapter.receive({ id: "m2", chatId: "c1", senderId: "u1", text: "again" });
    expect(client.creates).toBe(1);
    expect(client.messages).toEqual([["session-1", "hello"], ["session-1", "again"]]);
    expect(sent[0]).toEqual({ chatId: "c1", text: "answer:hello", replyTo: "m1" });
  });
});
