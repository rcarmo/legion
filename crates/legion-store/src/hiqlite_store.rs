//! HiqliteStore — distributed EventStore backed by hiqlite (Raft-replicated SQLite).

use std::path::Path;
use std::sync::Arc;

use async_trait::async_trait;
use hiqlite::{macros::params, Client, Node, NodeConfig};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use uuid::Uuid;

use legion_core::{
    error::{LegionError, Result},
    traits::EventStore,
    types::{
        RunConfig, RunId, SeqNum, SessionFilter, SessionStatus, SessionSummary,
        TurnEnvelope, TurnEvent, TurnEventKind,
    },
};

// ── Row types ─────────────────────────────────────────────────────────────────

#[derive(Debug, Serialize, Deserialize)]
struct TurnRow {
    seq:        i64,
    prev_hash:  Vec<u8>,
    kind:        String,
    payload:     Option<String>,
    payload_cid: Option<String>,
    model:       Option<String>,
    tokens_in:  Option<i64>,
    tokens_out: Option<i64>,
    wall_ms:    Option<i64>,
    created_at: i64,
}

impl From<&mut hiqlite::Row<'_>> for TurnRow {
    fn from(row: &mut hiqlite::Row<'_>) -> Self {
        Self {
            seq:        row.get("seq"),
            prev_hash:  row.get("prev_hash"),
            kind:        row.get("kind"),
            payload:     row.get("payload"),
            payload_cid: row.get("payload_cid"),
            model:       row.get("model"),
            tokens_in:  row.get("tokens_in"),
            tokens_out: row.get("tokens_out"),
            wall_ms:    row.get("wall_ms"),
            created_at: row.get("created_at"),
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
struct SessionRow {
    run_id:     String,
    status:     String,
    config:     String,
    created_at: i64,
    updated_at: i64,
    turns:      i64,
}

impl From<&mut hiqlite::Row<'_>> for SessionRow {
    fn from(row: &mut hiqlite::Row<'_>) -> Self {
        Self {
            run_id:     row.get("run_id"),
            status:     row.get("status"),
            config:     row.get("config"),
            created_at: row.get("created_at"),
            updated_at: row.get("updated_at"),
            turns:      row.get("turns"),
        }
    }
}

// ── HiqliteStore ─────────────────────────────────────────────────────────────

/// Distributed event store backed by hiqlite (Raft + SQLite).
#[derive(Clone)]
pub struct HiqliteStore {
    client: Arc<Client>,
}

impl HiqliteStore {
    /// Start a single-node hiqlite instance (development / test mode).
    pub async fn start_single(data_dir: &Path) -> Result<Self> {
        let data_str = data_dir.to_string_lossy().to_string();
        std::fs::create_dir_all(data_dir)
            .map_err(|e| LegionError::Store(format!("create hiqlite data dir: {e}")))?;

        let config = NodeConfig {
            node_id: 1,
            nodes: vec![Node {
                id:        1,
                addr_raft: "127.0.0.1:17001".into(),
                addr_api:  "127.0.0.1:17002".into(),
            }],
            data_dir:    data_str.into(),
            secret_raft: "legion-dev-raft".into(),
            secret_api:  "legion-dev-api".into(),
            ..Default::default()
        };

        Self::connect(config).await
    }

    /// Start this node and connect it to the configured hiqlite cluster.
    pub async fn connect(config: NodeConfig) -> Result<Self> {
        let client = hiqlite::start_node(config).await
            .map_err(|e| LegionError::Store(format!("start hiqlite node: {e}")))?;
        client.wait_until_healthy_db().await;
        apply_schema(&client).await?;
        Ok(Self { client: Arc::new(client) })
    }

    /// Wrap an already-started client. The caller is responsible for schema setup.
    pub fn from_client(client: Client) -> Self {
        Self { client: Arc::new(client) }
    }
}

async fn apply_schema(client: &Client) -> Result<()> {
    for migration in [
        include_str!("migrations/0001_initial.sql"),
        include_str!("migrations/0002_functions.sql"),
    ] {
        // The shared migrations predate distributed mode. Make their DDL
        // idempotent so every Raft node may safely initialize an existing DB.
        let sql = migration
            .replace("CREATE TABLE ", "CREATE TABLE IF NOT EXISTS ")
            .replace("CREATE INDEX ", "CREATE INDEX IF NOT EXISTS ");
        let results = client.batch(sql).await
            .map_err(|e| LegionError::Store(format!("apply hiqlite schema: {e}")))?;
        for result in results {
            result.map_err(|e| LegionError::Store(format!("apply hiqlite schema: {e}")))?;
        }
    }
    Ok(())
}

// ── Internal helpers ──────────────────────────────────────────────────────────

fn hash_envelope(envelope: &TurnEnvelope) -> [u8; 32] {
    let content = serde_json::json!({
        "seq":        envelope.seq,
        "prev_hash":  envelope.prev_hash,
        "event":      envelope.event,
        "created_at": envelope.created_at,
    });
    let mut hasher = Sha256::new();
    hasher.update(serde_json::to_vec(&content).unwrap_or_default());
    hasher.finalize().into()
}

fn verify_chain(log: &[TurnEnvelope], run_id: RunId) -> Result<()> {
    for (index, envelope) in log.iter().enumerate() {
        let expected = if index == 0 {
            [0u8; 32]
        } else {
            hash_envelope(&log[index - 1])
        };
        if envelope.prev_hash != expected {
            return Err(LegionError::TamperEvident(run_id, envelope.seq));
        }
    }
    Ok(())
}

fn now_ms() -> i64 { chrono::Utc::now().timestamp_millis() }

fn turn_row_to_envelope(run_id: RunId, r: TurnRow) -> TurnEnvelope {
    let prev: [u8; 32] = r.prev_hash.as_slice().try_into().unwrap_or([0u8; 32]);
    let payload_val = r.payload.as_deref()
        .and_then(|s| serde_json::from_str(s).ok());
    let event = TurnEvent {
        kind:        serde_json::from_str(&r.kind).unwrap_or(TurnEventKind::SessionStarted),
        payload:     payload_val,
        payload_cid: r.payload_cid,
        model:       r.model,
        tokens_in:   r.tokens_in.map(|v| v as u32),
        tokens_out:  r.tokens_out.map(|v| v as u32),
        wall_ms:     r.wall_ms.map(|v| v as u64),
    };
    TurnEnvelope { run_id, seq: r.seq as u64, prev_hash: prev, event, created_at: r.created_at }
}

// ── EventStore impl ───────────────────────────────────────────────────────────

#[async_trait]
impl EventStore for HiqliteStore {
    async fn create_session(&self, run_id: RunId, config: &RunConfig) -> Result<()> {
        let config_json = serde_json::to_string(config)
            .map_err(|e| LegionError::Serialization(e))?;
        let idle = serde_json::to_string(&SessionStatus::Idle)
            .map_err(|e| LegionError::Store(e.to_string()))?;
        let now = now_ms();

        self.client.execute(
            "INSERT INTO sessions (run_id, parent_run, fork_seq, status, config, created_at, updated_at)
             VALUES ($1, NULL, NULL, $2, $3, $4, $4)",
            params!(run_id.to_string(), idle, config_json, now),
        ).await.map_err(|e| LegionError::Store(e.to_string()))?;
        Ok(())
    }

    async fn append(&self, run_id: RunId, event: TurnEvent) -> Result<SeqNum> {
        let now = now_ms();
        let run_str = run_id.to_string();

        // Get the current tail. Hashes are derived from envelope content, just
        // like SqliteStore, so no extra schema column is required.
        let last: Vec<TurnRow> = self.client.query_as(
            "SELECT seq, prev_hash, kind, payload, payload_cid, model, tokens_in, tokens_out, wall_ms, created_at
             FROM turns WHERE run_id = $1 ORDER BY seq DESC LIMIT 1",
            params!(run_str.clone()),
        ).await.map_err(|e| LegionError::Store(e.to_string()))?;

        let (next_seq, prev_hash) = if let Some(row) = last.into_iter().next() {
            let seq = row.seq as u64 + 1;
            let envelope = turn_row_to_envelope(run_id, row);
            (seq, hash_envelope(&envelope))
        } else {
            (0u64, [0u8; 32])
        };
        let kind_tag = serde_json::to_string(&event.kind)
            .unwrap_or_else(|_| "\"SessionStarted\"".into());
        let payload_str = event.payload.as_ref().map(|v| v.to_string());

        self.client.execute(
            "INSERT INTO turns (run_id, seq, prev_hash, kind, payload, payload_cid, model, tokens_in, tokens_out, wall_ms, created_at)
             VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)",
            params!(
                run_str.clone(),
                next_seq as i64,
                prev_hash.to_vec(),
                kind_tag,
                payload_str,
                event.payload_cid,
                event.model,
                event.tokens_in.map(|v| v as i64),
                event.tokens_out.map(|v| v as i64),
                event.wall_ms.map(|v| v as i64),
                now
            ),
        ).await.map_err(|e| LegionError::Store(e.to_string()))?;

        self.client.execute(
            "UPDATE sessions SET updated_at = $1 WHERE run_id = $2",
            params!(now, run_str),
        ).await.map_err(|e| LegionError::Store(e.to_string()))?;

        Ok(next_seq)
    }

    async fn read_log(&self, run_id: RunId) -> Result<Vec<TurnEnvelope>> {
        let rows: Vec<TurnRow> = self.client.query_as(
            "SELECT seq, prev_hash, kind, payload, payload_cid, model, tokens_in, tokens_out, wall_ms, created_at
             FROM turns WHERE run_id = $1 ORDER BY seq ASC",
            params!(run_id.to_string()),
        ).await.map_err(|e| LegionError::Store(e.to_string()))?;

        let log: Vec<_> = rows.into_iter()
            .map(|row| turn_row_to_envelope(run_id, row))
            .collect();
        verify_chain(&log, run_id)?;
        Ok(log)
    }

    async fn read_recent(&self, run_id: RunId, n: usize) -> Result<Vec<TurnEnvelope>> {
        let rows: Vec<TurnRow> = self.client.query_as(
            "SELECT seq, prev_hash, kind, payload, payload_cid, model, tokens_in, tokens_out, wall_ms, created_at
             FROM turns WHERE run_id = $1 ORDER BY seq DESC LIMIT $2",
            params!(run_id.to_string(), n as i64),
        ).await.map_err(|e| LegionError::Store(e.to_string()))?;

        let mut envs: Vec<TurnEnvelope> = rows.into_iter()
            .map(|r| turn_row_to_envelope(run_id, r)).collect();
        envs.reverse();
        Ok(envs)
    }

    async fn session_status(&self, run_id: RunId) -> Result<SessionStatus> {
        #[derive(Deserialize)]
        struct Stat { status: String }
        impl From<&mut hiqlite::Row<'_>> for Stat {
            fn from(row: &mut hiqlite::Row<'_>) -> Self { Stat { status: row.get("status") } }
        }

        let rows: Vec<Stat> = self.client.query_as(
            "SELECT status FROM sessions WHERE run_id = $1",
            params!(run_id.to_string()),
        ).await.map_err(|e| LegionError::Store(e.to_string()))?;

        let s = rows.into_iter().next()
            .map(|r| r.status)
            .ok_or_else(|| LegionError::SessionNotFound(run_id))?;

        serde_json::from_str(&s)
            .map_err(|e| LegionError::Store(format!("deserialize status: {e}")))
    }

    async fn set_status(&self, run_id: RunId, status: SessionStatus) -> Result<()> {
        let s = serde_json::to_string(&status)
            .map_err(|e| LegionError::Store(e.to_string()))?;
        let now = now_ms();
        self.client.execute(
            "UPDATE sessions SET status = $1, updated_at = $2 WHERE run_id = $3",
            params!(s, now, run_id.to_string()),
        ).await.map_err(|e| LegionError::Store(e.to_string()))?;
        Ok(())
    }

    async fn fork(&self, run_id: RunId, at_seq: SeqNum) -> Result<RunId> {
        let new_id  = Uuid::new_v4();
        let now     = now_ms();
        let run_str = run_id.to_string();
        let new_str = new_id.to_string();

        #[derive(Deserialize)]
        struct Ses { status: String, config: String }
        impl From<&mut hiqlite::Row<'_>> for Ses {
            fn from(row: &mut hiqlite::Row<'_>) -> Self {
                Ses { status: row.get("status"), config: row.get("config") }
            }
        }

        let rows: Vec<Ses> = self.client.query_as(
            "SELECT status, config FROM sessions WHERE run_id = $1",
            params!(run_str.clone()),
        ).await.map_err(|e| LegionError::Store(e.to_string()))?;
        let ses = rows.into_iter().next()
            .ok_or_else(|| LegionError::SessionNotFound(run_id))?;

        self.client.execute(
            "INSERT INTO sessions (run_id, parent_run, fork_seq, status, config, created_at, updated_at)
             VALUES ($1, $2, $3, $4, $5, $6, $6)",
            params!(new_str.clone(), run_str.clone(), at_seq as i64, ses.status, ses.config, now),
        ).await.map_err(|e| LegionError::Store(e.to_string()))?;

        self.client.execute(
            "INSERT INTO turns (run_id, seq, prev_hash, kind, payload, payload_cid, model, tokens_in, tokens_out, wall_ms, created_at)
             SELECT $1, seq, prev_hash, kind, payload, payload_cid, model, tokens_in, tokens_out, wall_ms, $2
             FROM turns WHERE run_id = $3 AND seq <= $4",
            params!(new_str, now, run_str, at_seq as i64),
        ).await.map_err(|e| LegionError::Store(e.to_string()))?;

        Ok(new_id)
    }

    async fn list_sessions(&self, filter: SessionFilter) -> Result<Vec<SessionSummary>> {
        let limit  = filter.limit.unwrap_or(100) as i64;
        let offset = filter.offset.unwrap_or(0) as i64;
        let status = filter.status;

        let rows: Vec<SessionRow> = self.client.query_as(
            "SELECT s.run_id, s.status, s.config, s.created_at, s.updated_at,
                    (SELECT COUNT(*) FROM turns t WHERE t.run_id = s.run_id) as turns
             FROM sessions s
             WHERE $3 IS NULL
                OR (json_valid(s.status) AND json_extract(s.status, '$.status') = $3)
                OR s.status = $3
             ORDER BY s.created_at DESC LIMIT $1 OFFSET $2",
            params!(limit, offset, status),
        ).await.map_err(|e| LegionError::Store(e.to_string()))?;

        rows.into_iter().map(|r| {
            let run_id = Uuid::parse_str(&r.run_id)
                .map_err(|e| LegionError::Store(e.to_string()))?;
            let status: SessionStatus = serde_json::from_str(&r.status)
                .map_err(|e| LegionError::Store(e.to_string()))?;
            let config: RunConfig = serde_json::from_str(&r.config)
                .map_err(|e| LegionError::Store(e.to_string()))?;
            Ok(SessionSummary {
                run_id,
                status,
                model:      config.model,
                turns:      r.turns as u64,
                created_at: r.created_at,
                updated_at: r.updated_at,
            })
        }).collect()
    }
}
