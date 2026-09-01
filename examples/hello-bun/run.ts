import { legion, request } from "../shared/legion.ts";

const code = await Bun.file(new URL("./function.ts", import.meta.url)).text();
await request("/functions", {
  method: "POST",
  body: JSON.stringify({
    name: "example-hello-bun",
    runtime: "bun",
    description: "Minimal Bun greeting",
    code,
    idempotent: true,
    parameters: { type: "object", properties: { name: { type: "string" } } },
  }),
});
const result = await legion.invokeFunction("example-hello-bun", { name: process.argv[2] ?? "Legion" });
console.log(JSON.stringify(result, null, 2));
