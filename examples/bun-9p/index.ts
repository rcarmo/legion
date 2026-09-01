import { createLegionFs } from "../../packages/legion-bun-client/src/index.ts";
import { required } from "../shared/legion.ts";

const fs = createLegionFs({
  hostname: process.env.LEGION_9P_HOST ?? "127.0.0.1",
  port: Number(process.env.LEGION_9P_PORT ?? 5640),
  capability: required("LEGION_NAMESPACE_CAPABILITY"),
});
async function step<T>(name: string, operation: () => Promise<T>): Promise<T> {
  try { return await operation(); }
  catch (error) { throw new Error(`${name}: ${error instanceof Error ? error.message : error}`); }
}
try {
  const cluster = await step("read /cluster/self", () => fs.readJson("/cluster/self"));
  await step("write /sessions/new", () => fs.writeJson("/sessions/new", { model: "faux/bun-9p", budget: {}, tools: [], metadata: { example: "bun-9p" } }));
  const session = await step("read /sessions/new", () => fs.readJson<{ run_id: string }>("/sessions/new"));
  if (!session.run_id) throw new Error(`invalid session response: ${JSON.stringify(session)}`);
  const status = await step(`read /sessions/${session.run_id}/status`, () => fs.readJson(`/sessions/${session.run_id}/status`));
  console.log(JSON.stringify({ cluster, session, status }, null, 2));
} finally {
  fs.close();
}
