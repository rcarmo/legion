import { pathToFileURL } from "node:url";

const script = process.argv[2];
if (!script) throw new Error("worker requires a function script path");
const moduleURL = pathToFileURL(script).href;

for await (const line of console) {
  const request = JSON.parse(line);
  try {
    const imported = await import(`${moduleURL}?call=${request.call_id}`);
    const run = imported.run ?? imported.default;
    if (typeof run !== "function") throw new Error("persistent Bun functions must export run(args, env)");
    const value = await run(request.args, request.env ?? {});
    console.log(JSON.stringify({ call_id: request.call_id, output: value }));
  } catch (error) {
    console.log(JSON.stringify({ call_id: request.call_id, error: error instanceof Error ? error.message : String(error) }));
  }
}
