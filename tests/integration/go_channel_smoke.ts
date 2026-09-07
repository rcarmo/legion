import { LegionClient } from "../../packages/client/src/index";
import { LegionChannelRouter, WebChatAdapter, TelegramAdapter } from "../../packages/channels/src/index";

const client=new LegionClient({baseUrl:process.env.LEGION_TEST_URL!,apiKey:process.env.LEGION_TEST_API_KEY});
const config={model:"legion-m5-faux/model"};
function check(ok:unknown,message:string):asserts ok {if(!ok)throw new Error(message);}
const replies:Array<{text:string;replyTo?:string}>=[];
const router=new LegionChannelRouter({client,session:config});
const web=new WebChatAdapter(message=>{replies.push(message)});
await web.start(message=>router.handle(web,message));
await web.receive({chatId:"room",senderId:"user",id:"one",text:"hello"});
await web.receive({chatId:"room",senderId:"user",id:"two",text:"again"});
check(replies.length===2&&replies.every(r=>r.text==="local model reply"),"web response content");
check(replies[1].replyTo==="two","web reply correlation");
const sessions=await client.listSessions();check(sessions.sessions.length===1,"web conversation reused one durable session");
const parent=(sessions.sessions[0] as {run_id:string}).run_id;
await client.registerAgent({name:"reviewer",config});
const fresh=await client.invokeAgent("reviewer","review") as {run_id:string;status:{status:string};output:{content:string}};
check(fresh.status.status==="completed"&&fresh.output.content==="local model reply","agent invocation");
const child=await client.invokeAgent("reviewer","supervised review",{runId:parent,atSeq:0}) as {run_id:string;parent_run_id:string};
check(child.parent_run_id===parent&&child.run_id!==parent,"supervised fork identity");
const results=await client.runWorkflow([{id:"a",tool:"agent.reviewer",args:{prompt:"analyse"}},{id:"b",tool:"agent.reviewer",depends_on:["a"],args:{prompt:"review"}}]);
check(results.waves.length===2,"agent workflow completion");
// Telegram transport is stubbed; router, client, HTTP and inference are real.
let delivered=false;const telegramReplies:any[]=[];let telegram!:TelegramAdapter;
telegram=new TelegramAdapter("local-test","https://telegram.invalid",(async(input,init)=>{
 if(String(input).endsWith("sendMessage")){telegramReplies.push(JSON.parse(String(init?.body)));return Response.json({ok:true,result:{}})}
 if(!delivered){delivered=true;return Response.json({ok:true,result:[{update_id:1,message:{message_id:7,text:"telegram",chat:{id:12},from:{id:34}}}]})}
 await telegram.stop();return Response.json({ok:true,result:[]});
}) as typeof fetch);
await telegram.start(message=>router.handle(telegram,message));
check(telegramReplies[0]?.text==="local model reply"&&telegramReplies[0]?.reply_parameters?.message_id===7,"telegram response and correlation");
await web.stop();
console.log("Live web/Telegram channels, agents, supervised forks and agent DAG passed");
