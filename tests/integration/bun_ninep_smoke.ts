import { LegionFs } from "../../packages/legion-bun-client/src/index.ts";

const port = Number(process.env.LEGION_TEST_NINEP_PORT ?? 15640);
const capability = process.env.LEGION_TEST_NINEP_CAPABILITY ?? "test-capability";
const fs = new LegionFs({ port, capability });
await fs.writeJson("/sessions/new", { model: "faux/sdk-smoke" });
const created = await fs.readJson<{ run_id: string }>("/sessions/new");
if (!/^[0-9a-f-]{36}$/.test(created.run_id)) throw new Error(`unexpected session result: ${JSON.stringify(created)}`);
fs.close();

const denied = new LegionFs({ port, capability: "wrong" });
try {
  await denied.readJson("/cluster/self");
  throw new Error("wrong capability was accepted");
} catch (error) {
  if (String(error).includes("wrong capability was accepted")) throw error;
} finally {
  denied.close();
}
console.log("Bun 9P roundtrip and capability denial passed");
