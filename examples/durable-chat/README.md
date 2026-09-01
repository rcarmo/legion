# Durable chat

Creates one persistent model session, sends two related messages, and prints the durable event log.

```sh
export LEGION_MODEL=anthropic/claude-haiku-3-5
export ANTHROPIC_API_KEY=… # available to the Legion service
bun examples/durable-chat/index.ts
```

Provider credentials belong in the Legion service environment, not this example. The session survives client exit and appears in `/dashboard`.
