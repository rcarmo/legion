import { LegionClient, type SessionCreate } from "@legion/client";

/** Picoclaw-shaped inbound message normalized across transport adapters. */
export interface ChannelMessage {
  id: string;
  channel: "telegram" | "web";
  chatId: string;
  senderId: string;
  text: string;
  replyTo?: string;
  raw?: unknown;
}

export interface ChannelAdapter {
  readonly name: string;
  start(handler: (message: ChannelMessage) => Promise<void>): Promise<void>;
  stop(): Promise<void>;
  send(chatId: string, text: string, replyTo?: string): Promise<void>;
}

export interface SessionRouterOptions {
  client: LegionClient;
  session: SessionCreate;
  key?: (message: ChannelMessage) => string;
}

/** Routes each channel conversation to one durable Legion session. */
export class LegionChannelRouter {
  private readonly sessions = new Map<string, string>();
  constructor(private readonly options: SessionRouterOptions) {}

  async handle(adapter: ChannelAdapter, message: ChannelMessage): Promise<void> {
    const key = this.options.key?.(message) ?? `${message.channel}:${message.chatId}`;
    let session = this.sessions.get(key);
    if (!session) {
      session = (await this.options.client.createSession(this.options.session)).id;
      this.sessions.set(key, session);
    }
    const response = await this.options.client.sendMessage(session, message.text);
    await adapter.send(message.chatId, response.response, message.id);
  }
}

export class TelegramAdapter implements ChannelAdapter {
  readonly name = "telegram";
  private offset = 0;
  private active = false;
  constructor(
    private readonly token: string,
    private readonly apiBase = "https://api.telegram.org",
    private readonly fetchImpl: typeof fetch = fetch,
  ) {}

  private call(method: string, body: unknown) {
    return this.fetchImpl(`${this.apiBase}/bot${this.token}/${method}`, {
      method: "POST", headers: { "content-type": "application/json" }, body: JSON.stringify(body),
    });
  }

  async start(handler: (message: ChannelMessage) => Promise<void>): Promise<void> {
    this.active = true;
    while (this.active) {
      const response = await this.call("getUpdates", { offset: this.offset, timeout: 25, allowed_updates: ["message"] });
      if (!response.ok) throw new Error(`Telegram getUpdates failed: ${response.status}`);
      const payload = await response.json() as { result: Array<any> };
      for (const update of payload.result) {
        this.offset = Math.max(this.offset, update.update_id + 1);
        const message = update.message;
        if (!message?.text) continue;
        await handler({
          id: String(message.message_id), channel: "telegram", chatId: String(message.chat.id),
          senderId: String(message.from?.id ?? message.chat.id), text: message.text,
          replyTo: message.reply_to_message ? String(message.reply_to_message.message_id) : undefined,
          raw: update,
        });
      }
    }
  }

  async stop() { this.active = false; }
  async send(chatId: string, text: string, replyTo?: string) {
    const response = await this.call("sendMessage", {
      chat_id: chatId, text, ...(replyTo ? { reply_parameters: { message_id: Number(replyTo) } } : {}),
    });
    if (!response.ok) throw new Error(`Telegram sendMessage failed: ${response.status}`);
  }
}

/** Framework-neutral web-chat adapter for WebSocket/EventSource hosts. */
export class WebChatAdapter implements ChannelAdapter {
  readonly name = "web";
  private handler?: (message: ChannelMessage) => Promise<void>;
  constructor(private readonly emit: (message: { chatId: string; text: string; replyTo?: string }) => void | Promise<void>) {}
  async start(handler: (message: ChannelMessage) => Promise<void>) { this.handler = handler; }
  async stop() { this.handler = undefined; }
  async receive(message: Omit<ChannelMessage, "channel">) {
    if (!this.handler) throw new Error("web adapter is not started");
    await this.handler({ ...message, channel: "web" });
  }
  async send(chatId: string, text: string, replyTo?: string) { await this.emit({ chatId, text, replyTo }); }
}
