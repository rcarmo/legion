import { legion, model } from "../shared/legion.ts";

for (const profile of [
  { name: "researcher", description: "Collect concise factual evidence", system: "Research the assignment. Return claims and sources." },
  { name: "reviewer", description: "Review evidence and synthesize it", system: "Review dependency outputs. Identify conflicts and write a concise conclusion." },
]) {
  await legion.registerAgent({
    name: profile.name,
    description: profile.description,
    config: { model: model(), system_prompt: profile.system, tools: [], budget: { max_turns: 4 } },
  });
}
console.log("registered researcher and reviewer");
