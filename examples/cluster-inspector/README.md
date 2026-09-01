# Cluster inspector

Prints a compact JSON inventory of node health, identity, peers, recent sessions, functions, and registered agents.

```sh
bun examples/cluster-inspector/index.ts
```

This is deliberately scriptable: pipe it to `jq`, a monitoring collector, or another Bun program.
