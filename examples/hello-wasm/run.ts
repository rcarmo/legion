import { legion, request } from "../shared/legion.ts";

const wasm = process.env.LEGION_WASM_FILE ?? new URL("../../target/wasm32-wasip1/release/legion_example_hello_wasm.wasm", import.meta.url).pathname;
if (!(await Bun.file(wasm).exists())) throw new Error("build first: make example-wasm");
const bytes = await Bun.file(wasm).arrayBuffer();
await request("/functions", {
  method: "POST",
  body: JSON.stringify({
    name: "example-hello-wasm", runtime: "wasm", description: "Portable WASM greeting",
    wasm_b64: Buffer.from(bytes).toString("base64"), idempotent: true,
  }),
});
console.log(JSON.stringify(await legion.invokeFunction("example-hello-wasm", { name: process.argv[2] ?? "Legion" }), null, 2));
