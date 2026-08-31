import { afterEach, describe, expect, test } from "bun:test";
import { LegionClient } from "./index.ts";

let server: ReturnType<typeof Bun.serve> | undefined;
afterEach(() => server?.stop(true));

describe("LegionClient", () => {
  test("sends auth and maps agent/workflow contracts", async () => {
    const seen: Array<{ path: string; auth: string | null; body: unknown }> = [];
    server = Bun.serve({ port: 0, async fetch(request) {
      seen.push({
        path: new URL(request.url).pathname,
        auth: request.headers.get("authorization"),
        body: request.method === "POST" ? await request.json() : null,
      });
      return Response.json(new URL(request.url).pathname === "/workflows/run"
        ? { outputs: { a: 1 }, waves: [["a"]] }
        : { output: { content: "done" } });
    }});
    const client = new LegionClient({ baseUrl: server.url.toString(), apiKey: "secret" });
    await client.invokeAgent("researcher", "question", { runId: "parent", atSeq: 4 });
    const workflow = await client.runWorkflow([{ id: "a", tool: "agent.researcher" }]);
    expect(seen[0]).toEqual({
      path: "/agents/researcher/invoke", auth: "Bearer secret",
      body: { prompt: "question", parent_run_id: "parent", at_seq: 4 },
    });
    expect(seen[1].path).toBe("/workflows/run");
    expect(workflow.waves).toEqual([["a"]]);
  });

  test("throws Legion error payloads", async () => {
    server = Bun.serve({ port: 0, fetch: () => Response.json({ error: "bad graph" }, { status: 422 }) });
    const client = new LegionClient({ baseUrl: server.url.toString() });
    expect(client.runWorkflow([])).rejects.toThrow("bad graph");
  });
});
