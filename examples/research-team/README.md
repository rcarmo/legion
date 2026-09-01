# Research team

Registers researcher and reviewer agent profiles, runs two researchers in parallel, and feeds their outputs to a reviewer in the second DAG wave.

```sh
bun examples/research-team/register.ts
bun examples/research-team/run.ts "Should an internal tool be local-first?"
```

Requires model credentials in the Legion service environment. Registered profiles are currently node-local; child session event logs are durable.
