import { legion, model } from "../shared/legion.ts";

await legion.registerAgent({
  name: "fact-checker",
  description: "Check one claim",
  config: { model: model(), system_prompt: "Check the assignment and answer with verdict and rationale.", tools: [] },
});
const parent = await legion.createSession({ model: model(), system_prompt: "Parent supervisor session" });
const log = await legion.getLog(parent.id) as { entries: Array<{ seq: number }> };
const atSeq = log.entries.at(-1)?.seq ?? 0;
const child = await legion.invokeAgent("fact-checker", process.argv.slice(2).join(" ") || "SQLite is an embedded database.", {
  runId: parent.id, atSeq,
});
console.log(JSON.stringify({ parent: parent.id, forkedAt: atSeq, child }, null, 2));
