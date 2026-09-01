import { LegionChannelRouter, TelegramAdapter } from "../../packages/channels/src/index.ts";
import { legion, model, required } from "../shared/legion.ts";

const telegram = new TelegramAdapter(required("TELEGRAM_BOT_TOKEN"));
const router = new LegionChannelRouter({
  client: legion,
  session: { model: model(), system_prompt: "Reply concisely. Plain text only." },
});
process.on("SIGINT", () => void telegram.stop());
console.log("Telegram adapter polling; press Ctrl-C to stop");
await telegram.start(message => router.handle(telegram, message));
