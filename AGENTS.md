# Legion — Agent Instructions

Legion is a Rust workspace implementing a self-hosted durable functions platform.

## Repository Layout

```
projects/legion/
  Cargo.toml          workspace root
  Makefile
  README.md
  AGENTS.md           (this file)
  docs/               all design documentation
  crates/
    legion-core/      shared types and traits (no I/O)
    legion-store/     EventStore over hiqlite + fjall + iroh-blobs
    legion-loop/      agent loop built on rs-ai
    legion-namespace/ 9P namespace (jetstream)
    legion-cluster/   iroh + mDNS + Raft bootstrap
    legion-runtime/   WASM (wasmtime/extism) + Bun executor
    legion-deploy/    CAS function deployment
    legion-server/    binary entry point
```

## Development Principles

- **Use the root `Makefile` for all build and test flows.** It keeps one shared `target/`, runs Cargo sequentially, checks free space, and clears repository-local junk. Prefer `make verify-m3`, `make check`, or a targeted `make test-*` target over direct Cargo commands.
- **Do not create crate-local or fixture-local Cargo target directories.** `CARGO_TARGET_DIR` is centralized at the workspace root. Run `make clean-junk` after interrupted work and `make clean` when build artifacts are no longer needed.
- **Read before editing.** Never edit blind.
- **No I/O in legion-core.** All traits defined there must be pure.
- **legion-store is the only crate that touches disk or network for persistence.** Other crates depend on the trait, not the impl.
- **legion-loop depends only on legion-core + rs-ai.** It must not import hiqlite, iroh, or jetstream directly.
- **All cluster-specific code lives in legion-cluster.**
- **Prefer fjall over rocksdb.** No C++ deps unless unavoidable.
- **Prefer redb over sled** for any B-tree storage needs.
- **All SQL schema changes go through hiqlite migrations**, not raw `CREATE TABLE IF NOT EXISTS`.

## Key Trait Contracts

### EventStore (legion-core)
```rust
#[async_trait]
pub trait EventStore: Send + Sync {
    async fn append(&self, run_id: RunId, event: TurnEvent) -> Result<SeqNum>;
    async fn read_log(&self, run_id: RunId) -> Result<Vec<TurnEnvelope>>;
    async fn read_recent(&self, run_id: RunId, n: usize) -> Result<Vec<TurnEnvelope>>;
    async fn session_status(&self, run_id: RunId) -> Result<SessionStatus>;
    async fn set_status(&self, run_id: RunId, status: SessionStatus) -> Result<()>;
}
```

### AgentLoop (legion-core)
```rust
#[async_trait]
pub trait AgentLoop: Send + Sync {
    async fn start(&self, config: RunConfig) -> Result<RunId>;
    async fn recover(&self, run_id: RunId) -> Result<()>;
    async fn resume(&self, run_id: RunId, event: ExternalEvent) -> Result<()>;
    async fn resolve(&self, run_id: RunId) -> Result<TurnEnvelope>;
}
```

### ToolRegistry (legion-core)
```rust
#[async_trait]
pub trait ToolRegistry: Send + Sync {
    fn definitions(&self) -> Vec<ToolDefinition>;
    async fn dispatch(&self, name: &str, args: serde_json::Value) -> Result<serde_json::Value>;
}
```

## Coding Conventions

- Edition 2024 Rust (matches rs-ai)
- `tokio` full runtime
- `thiserror` for all error types — no `anyhow` in library crates
- `anyhow` is acceptable in the binary (`legion-server`)
- `serde` + `serde_json` for serialization; derive `Serialize + Deserialize` on all public types
- `async-trait` for trait objects
- All SQL lives in `crates/legion-store/src/migrations/` as numbered `.sql` files
- Test doubles live in `legion-core/src/test_doubles/`

## git

- No force push, no rebase of shared branches
- Conventional commits: `feat:`, `fix:`, `docs:`, `refactor:`, `test:`, `chore:`
- Commit as: `rcarmo <rcarmo@users.noreply.github.com>`

## References

Full design rationale in `docs/`. Key external repositories:
- rs-ai: https://github.com/rcarmo/rs-ai (LLM abstraction)
- picoclaw: https://github.com/sipeed/picoclaw (reference agent loop in Go)
- salvor: https://github.com/joseym/salvor (reference durable execution design)
- hiqlite: https://github.com/sebadob/hiqlite (Raft + SQLite)
- iroh: https://github.com/n0-computer/iroh (P2P transport)
- jetstream: https://github.com/sevki/jetstream (9P over QUIC)

## Project cache and temporary-file policy (5 October 2026)

- Reproducible build caches and temporary files MUST use `/workspace/tmp/legion/` locally. NEVER create ad-hoc paths or unscoped temporary files. In CI/CD or when `/workspace/tmp` is absent/unwritable, use a generic temporary base with the same `legion/` suffix.
- Makefile base selection: explicit `WORKSPACE_TMP_BASE` first; otherwise CI uses `RUNNER_TEMP`, inherited `TMPDIR`/`TEMP`, or `/tmp`. Outside CI, use writable `/workspace/tmp` if present, otherwise that generic fallback. CI unset/empty/0/false means local mode. Inherited temporary settings are captured before project overrides. An override selects a base, never replaces the project suffix. No directory is created merely by parsing Make.
- Inspect `make workspace-paths`; `make workspace-prepare` creates only the selected project layout. Explicit overrides must be writable and belong to the intended runner/workspace; keep persistent data elsewhere.
- Use documented `cache/`, `build/`, `tests/`, `logs/` and `runs/` subdirectories. Unique run folders belong inside these directories. Run `make workspace-prepare` to create the declared layout.
- Before running a build/test tool, configure its temporary and cache paths to this layout (for example TMPDIR, Go GOCACHE/GOMODCACHE/GOTMPDIR, Cargo target output, Bun install cache or Gradle user home). TMPDIR alone does not override explicit paths or every cache. The Makefile's WORKSPACE_* variables declare the layout; older targets and scripts must be checked and migrated before use.
- This rule supersedes older scratch/cache examples in these instructions. Preserve published output contracts until their consumers are updated and verified. Keep source, curated checkpoints, credentials, persistent service data and durable deliverables outside disposable storage.
- Do not delete or move existing artifacts during adoption, especially while a process or another worktree uses them. Cleanup must be restricted to this project's disposable paths.

## Profiling and tuning policy

- Profile and tune during pre-release testing, using representative workloads. Routine development tests run without profiling; do not attach CPU, heap, allocation or trace capture to every test run.
- During pre-release analysis, compare equivalent workloads, fix measured bottlenecks, and re-measure without weakening correctness or test coverage.
- After analysis, retain concise findings, commands, source/toolchain identities and measured before/after results. Delete raw profiling captures and their disposable analysis binaries once analysis is complete; do not retain raw captures indefinitely or treat them as permanent checkpoints.
- Keep profiling data in the project's documented disposable workspace paths, with the existing portable CI/absent-workspace fallback. Restrict cleanup to owned files and wait for processes using them to finish. Preserve source, user data and non-profiling evidence.
- This policy supersedes older instructions requiring profiling on every development test or indefinite retention of raw profiling data. If existing Make targets or harnesses still force profiling for ordinary tests, correct that tooling before using those paths for routine development; this documentation change alone does not change runtime behaviour.
