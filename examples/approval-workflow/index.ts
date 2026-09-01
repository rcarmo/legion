import { legion, request } from "../shared/legion.ts";

const session = await legion.createSession({ model: "faux/approval", system_prompt: "Approval example" });
await request(`/sessions/${session.id}/park`, {
  method: "POST",
  body: JSON.stringify({ description: "Approve deployment of the release" }),
});
const before = await legion.getSession(session.id);
await request(`/sessions/${session.id}/events`, {
  method: "POST",
  body: JSON.stringify({ trigger: "approval-granted", payload: { approved_by: process.argv[2] ?? "operator" } }),
});
const after = await legion.getSession(session.id);
console.log(JSON.stringify({ session: session.id, before, after }, null, 2));
