# Hello Bun

Deploys a small idempotent Bun function and invokes it through Legion.

```sh
bun examples/hello-bun/run.ts Rui
```

Expected output contains `Hello, Rui!`. Remove it with:

```sh
curl -X DELETE "$LEGION_URL/functions/example-hello-bun"
```
