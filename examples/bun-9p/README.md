# Bun 9P

Uses `legion-bun-client` to negotiate native 9P2000.L, attach with a namespace capability, read cluster identity, create a durable session, and read its status.

Enable the opt-in loopback bridge:

```toml
namespace_capability = "replace-me"
ninep_tcp_addr = "127.0.0.1:5640"
```

Then run:

```sh
export LEGION_NAMESPACE_CAPABILITY=replace-me
bun examples/bun-9p/index.ts
```

Non-loopback bridge addresses and clients are rejected by Legion.
