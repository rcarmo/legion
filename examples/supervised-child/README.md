# Supervised child

Creates a parent session, finds its latest durable sequence, and invokes a `fact-checker` agent as a forked child. The result includes both parent and child run IDs and the child's terminal status.

```sh
bun examples/supervised-child/index.ts "Plan 9 introduced the 9P protocol."
```

Requires model credentials in the Legion service environment.
