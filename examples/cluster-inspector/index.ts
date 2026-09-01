import { legion, request } from "../shared/legion.ts";

const [health, cluster, sessions, functions, agents] = await Promise.all([
  legion.health(),
  request<{ self: unknown; peers: unknown[] }>("/cluster/peers"),
  legion.listSessions("?limit=20"),
  legion.listFunctions(),
  legion.listAgents(),
]);
console.log(JSON.stringify({ health, cluster, sessions: sessions.sessions, functions: functions.functions, agents: agents.agents }, null, 2));
