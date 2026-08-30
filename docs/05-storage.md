# Storage

Legion uses three complementary storage layers, each optimised for a different access pattern. Which is a nice way to say I couldn't stuff everything directly into SQLite this time. Yet.

## Layer Summary

| Layer | Crate | Type | Role |
|---|---|---|---|
| **Structured state** | hiqlite | Raft-replicated SQLite | Turn log, function registry, session state, cluster config |
| **Raft log** | fjall | Pure-Rust LSM | Append-only Raft log entries, compacted automatically |
| **Large payloads** | iroh-blobs | CAS | Turn content, function bundles, Raft snapshots |

## hiqlite: Raft-Replicated SQLite

hiqlite wraps `rusqlite` with `openraft` to provide a strongly-consistent, highly-available SQL database that can be embedded directly in the Legion process.

### Why SQLite?

- Zero external dependencies — the database is part of the binary
- Full SQL for migrations, joins, and structured queries
- Excellent write-ahead log performance for append-heavy workloads
- Widely understood schema and query language
- hiqlite adds Raft replication on top with no API change for the application

### Consistency model

- **Writes**: Always routed to the Raft leader. The `execute()` call blocks until the entry is committed across a quorum.
- **Reads (default)**: Served from the local SQLite replica. May be slightly stale.
- **Reads (consistent)**: `query_consistent()` forces a round-trip to the leader. Used for write-ahead log reads and session status checks.

### hiqlite features used

| Feature | Use in Legion |
|---|---|
| `sqlite` | Core turn and session storage |
| `auto-heal` | Automatic log catch-up after node rejoin |
| `backup` | Encrypted S3 backup of the SQLite state machine snapshot |
| `dlock` | Distributed lock for leader-only operations (e.g. function promotion) |
| `listen_notify_local` | Wake parked sessions on turn completion |
| `migrations` | Schema evolution via numbered SQL files |

### Configuration

```toml
[hiqlite]
node_id         = 1
data_dir        = "/var/lib/legion/hiqlite"
listen_addr     = "0.0.0.0:8100"
peers           = []   # populated by legion-cluster from mDNS discovery
raft_heartbeat_ms = 200
raft_election_timeout_ms = 1000
```

### Schema files

Located in `crates/legion-store/src/migrations/`:

```
0001_initial.sql           -- turns, sessions
0002_functions.sql         -- functions, function_routes
0003_cluster_state.sql     -- peers, leader_cache
```

---

## fjall: Raft Log Store

fjall provides the `RaftLogStorage` implementation that openraft (via hiqlite) uses to persist log entries.

### Why fjall over RocksDB?

| | fjall | RocksDB |
|---|---|---|
| Language | 100% safe Rust | C++ (binding) |
| C deps | None | Yes |
| Build time | Fast | Slow |
| LSM | Yes | Yes |
| Cross-keyspace atomics | Yes | No |
| MSRV | 1.90 | N/A |

For a Raft log, the access pattern is:
- **Writes**: Sequential append (new log entries)
- **Reads**: Range scan during log catch-up, random access for specific indices
- **Compaction**: Entries before a snapshot can be discarded

LSM trees are naturally suited to append-dominant workloads. fjall's cross-keyspace atomic writes let us update the log entry and the log metadata (last index, term) in a single atomic operation — critical for Raft correctness.

### fjall keyspaces

```
legion_raft_log/          ← log entries (key = u64 index, value = CBOR entry)
legion_raft_meta/         ← hard state (vote, current term)
legion_raft_snapshots/    ← snapshot metadata
```

---

## iroh-blobs: Content-Addressed Storage

iroh-blobs stores arbitrary binary content by its SHA-256 hash (CID). Content is immutable once stored.

### What goes in iroh-blobs

| Content | Why |
|---|---|
| Turn payloads > 8KB | Tool outputs, images, long documents |
| Function bundles | WASM modules, Bun JS bundles |
| Raft snapshot files | Offloads large snapshots from SQLite |
| Email/attachment content | Referenced from session turns |

### CID as a reference

All references to iroh-blobs content are stored as CID strings in SQLite:

```sql
-- In turns table
payload_cid TEXT   -- 'bafkrei...' or NULL if payload is inline

-- In functions table
cid TEXT NOT NULL  -- always a CID; functions are always stored as blobs
```

### Blob distribution

iroh-blobs transfers content peer-to-peer over iroh QUIC connections. When a node fetches a CID it does not have locally:

1. It queries the iroh DHT or gossip layer for peers that have the content
2. It opens a direct QUIC connection to one of those peers
3. It downloads and verifies the content (hash check)
4. It caches locally for future requests

This means: push a function blob to any one node → all nodes can serve it to any caller.

---

## Dev vs. Production

| Setting | Dev (single node) | Production (3-node cluster) |
|---|---|---|
| hiqlite peers | Empty (single-node mode) | 3 addresses from mDNS |
| fjall | Local directory | Local directory (each node) |
| iroh-blobs | Local store | Distributed across all peers |
| S3 backup | Disabled | Optional (hiqlite backup feature) |

The application code is identical — only configuration changes.

---

## Storage Budget

Rough estimates for a typical deployment:

| Data | Size per item | Notes |
|---|---|---|
| One agent turn (text) | 1–10 KB | Inline in SQLite |
| One agent turn (with tool output) | 10 KB–10 MB | Payload in iroh-blobs |
| Function bundle (Bun) | 50–500 KB | iroh-blobs |
| Function bundle (WASM) | 200 KB–5 MB | iroh-blobs |
| Raft log entry | 100–500 bytes | fjall |
| SQLite WAL checkpoint | ~4 KB | Per write batch |

A typical agent session (20 turns, mixed tool use) occupies ~5–50 MB total across all layers.
