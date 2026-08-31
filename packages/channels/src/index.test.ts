import { describe, expect, test } from "bun:test";
import { LegionChannelRouter, TelegramAdapter, WebChatAdapter } from "./index.ts";

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

  test("telegram polling normalizes updates and sends correlated replies", async () => {
    const calls: Array<{ method: string; body: any }> = [];
    let adapter: TelegramAdapter;
    const fakeFetch = async (input: string | URL | Request, init?: RequestInit) => {
      const method = String(input).split("/").pop()!;
      const body = JSON.parse(String(init?.body));
      calls.push({ method, body });
      if (method === "getUpdates") return Response.json({ result: [{
        update_id: 8,
        message: { message_id: 12, chat: { id: 34 }, from: { id: 56 }, text: "hello" },
      }] });
      return Response.json({ ok: true });
    };
    adapter = new TelegramAdapter("token", "https://telegram.test", fakeFetch as typeof fetch);
    let received: any;
    await adapter.start(async (message) => { received = message; await adapter.stop(); });
    await adapter.send("34", "answer", "12");
    expect(received).toMatchObject({ id: "12", channel: "telegram", chatId: "34", senderId: "56", text: "hello" });
    expect(calls[0]).toEqual({ method: "getUpdates", body: { offset: 0, timeout: 25, allowed_updates: ["message"] } });
    expect(calls[1]).toEqual({ method: "sendMessage", body: { chat_id: "34", text: "answer", reply_parameters: { message_id: 12 } } });
  });
});
