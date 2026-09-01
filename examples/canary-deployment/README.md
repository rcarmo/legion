# Canary deployment

Deploys stable and canary Bun artifacts, registers the stable default, records a 25% deterministic canary route, verifies the stable path at weight 0, then promotes and verifies the canary as the new default.

```sh
bun examples/canary-deployment/index.ts
```

The output includes both immutable artifact CIDs and responses before and after promotion.
