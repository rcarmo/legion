# Hello WASM

Builds an Extism guest, deploys it as a portable Legion function, and invokes it.

```sh
make example-wasm
bun examples/hello-wasm/run.ts Rui
```

Expected output contains `Hello, Rui!` and `"runtime":"wasm"`.
