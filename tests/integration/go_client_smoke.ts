import { LegionClient } from "../../packages/client/src/index";

const client = new LegionClient({baseUrl: process.env.LEGION_TEST_URL!});
function check(ok: unknown, message: string): asserts ok { if (!ok) throw new Error(message); }
const {id} = await client.createSession({model:"faux/dashboard"});
check((await client.getSession(id) as {id:string}).id === id, "session detail contract");
check(Array.isArray((await client.getLog(id) as {entries:unknown[]}).entries), "session log contract");
check((await client.listSessions()).sessions.length > 0, "session listing contract");
await client.registerAgent({name:"reviewer",description:"Review evidence",config:{model:"faux/dashboard"}});
check((await client.listAgents()).agents.some(a=>a.name==="reviewer"), "agent listing contract");
const registration = await (await fetch(`${client.baseUrl}/deploy/register`, {method:"POST",headers:{"content-type":"application/json"},body:JSON.stringify({name:"workflow-echo",runtime:"bun",code:'export default async (args) => ({echo: args});',parameters:{type:"object"}})})).json() as {status:string};
check(registration.status === "success", `function registration: ${JSON.stringify(registration)}`);
check((await client.listFunctions()).functions.length > 0, "function listing contract");
const result = await client.runWorkflow([{id:"first",tool:"fn.workflow-echo",args:{value:42}},{id:"second",tool:"fn.workflow-echo",depends_on:["first"]}]);
check(result.waves.length === 2, "workflow waves");
check((result.outputs.second as any).echo.dependencies.first.echo.value === 42, "function-tool dependency outputs");
console.log("TypeScript client session, agent, function and workflow contracts passed");
