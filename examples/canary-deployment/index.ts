import { legion, request } from "../shared/legion.ts";

type Deploy = { artifact_cid: string };
const stableCode = await Bun.file(new URL("./stable.ts", import.meta.url)).text();
const canaryCode = await Bun.file(new URL("./canary.ts", import.meta.url)).text();
const stable = await request<Deploy>("/functions", { method: "POST", body: JSON.stringify({ name: "example-stable-source", runtime: "bun", code: stableCode }) });
const canary = await request<Deploy>("/functions", { method: "POST", body: JSON.stringify({ name: "example-canary-source", runtime: "bun", code: canaryCode }) });
await request("/deploy/register", { method: "POST", body: JSON.stringify({ name: "example-canary", artifact_cid: stable.artifact_cid, runtime: "bun" }) });
const weightedRoute = await request("/deploy/route", { method: "POST", body: JSON.stringify({ name: "example-canary", artifact_cid: canary.artifact_cid, weight: 2500 }) });
await request("/deploy/route", { method: "POST", body: JSON.stringify({ name: "example-canary", artifact_cid: canary.artifact_cid, weight: 0 }) });
const before = await legion.invokeFunction("example-canary", { phase: "stable-default" });
await request("/deploy/promote", { method: "POST", body: JSON.stringify({ name: "example-canary", artifact_cid: canary.artifact_cid }) });
const after = await legion.invokeFunction("example-canary", { phase: "promoted" });
console.log(JSON.stringify({ stableCid: stable.artifact_cid, canaryCid: canary.artifact_cid, weightedRoute, before, after }, null, 2));
