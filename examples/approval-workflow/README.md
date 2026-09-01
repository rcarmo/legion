# Approval workflow

Creates a durable session, parks it with an `AwaitingApproval` reason, inspects the parked state, and resumes it with an external approval event.

```sh
bun examples/approval-workflow/index.ts Rui
```

The example does not call a model, so it needs no provider credentials.
