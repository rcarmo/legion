import { legion, model } from "../shared/legion.ts";

const session = await legion.createSession({
  model: model(),
  system_prompt: "Answer concisely. Remember facts stated earlier in this durable session.",
  budget: { max_turns: 8, max_wall_ms: 120000 },
});
const first = await legion.sendMessage(session.id, process.argv[2] ?? "Remember that my project is called Legion.");
const second = await legion.sendMessage(session.id, process.argv[3] ?? "What is my project called?");
const log = await legion.getLog(session.id);
console.log(JSON.stringify({ session: session.id, first, second, log }, null, 2));
