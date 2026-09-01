import { legion } from "../shared/legion.ts";

const topic = process.argv.slice(2).join(" ") || "Benefits and risks of local-first software";
const result = await legion.runWorkflow([
  { id: "technical", tool: "agent.researcher", args: { prompt: `Research technical aspects of: ${topic}` } },
  { id: "operational", tool: "agent.researcher", args: { prompt: `Research operational aspects of: ${topic}` } },
  { id: "review", tool: "agent.reviewer", args: { prompt: `Synthesize the evidence about: ${topic}` }, depends_on: ["technical", "operational"] },
]);
console.log(JSON.stringify(result, null, 2));
