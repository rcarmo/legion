# `legion-bun-client`

Native Bun 9P2000.L adapter for Legion functions. It connects to Legion's loopback-only TCP bridge and exposes `readFile`, `writeFile`, `readJson`, `writeJson`, and `invoke` methods.

```ts
import { createLegionFs } from "legion-bun-client";
const fs = createLegionFs({ capability: process.env.LEGION_NAMESPACE_CAPABILITY });
const context = await fs.readJson(`/sessions/${process.env.LEGION_SESSION_ID}/context`);
```

Enable the local bridge explicitly with `ninep_tcp_addr = "127.0.0.1:5640"`. Non-loopback binds are rejected.
