# `@legion/client`

Zero-dependency REST client for Legion on Bun and Node.js 20+.

```ts
import { LegionClient } from "@legion/client";

const legion = new LegionClient({ baseUrl: "http://localhost:8080", apiKey: process.env.LEGION_API_KEY });
const session = await legion.createSession({ model: "anthropic/claude-haiku-3-5" });
console.log(await legion.sendMessage(session.id, "Hello"));
```

The client covers sessions, functions, agent profiles, supervised agent invocation, and workflow graphs.
