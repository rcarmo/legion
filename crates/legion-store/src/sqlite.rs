//! SQLite-backed `EventStore` (single-node, WAL mode).
//!
//! All writes use `rusqlite` directly with WAL journaling.
//! This is the single-node implementation; the distributed version
//! (via hiqlite) will implement the same `EventStore` trait.

use std::path::Path;
use std::sync::Arc;
use tokio::sync::Mutex;
use async_trait::async_trait;
use rusqlite::{Connection, params};
use uuid::Uuid;

use legion_core::{
    error::{LegionError, Result},
    traits::EventStore,
    types::{
        RunConfig, RunId, SeqNum, SessionFilter, SessionStatus, SessionSummary,
        TurnEnvelope, TurnEvent, TurnEventKind,
    },
};

use crate::migrations;

// ── SqliteStore ───────────────────────────────────────────────────────────────

/// Single-node SQLite-backed `EventStore`.
///
/// All operations are synchronous SQLite calls wrapped in a Tokio Mutex to
/// allow use from async code. In production, replace with the hiqlite-backed
/// implementation for Raft replication.
#[derive(Clone)]
pub struct SqliteStore {
    conn: Arc<Mutex<Connection>>,
}

impl SqliteStore {
    /// Open (or create) a store at the given path.
    pub fn open(path: impl AsRef<Path>) -> Result<Self> {
        let conn = Connection::open(path)
            .map_err(|e| LegionError::Store(e.to_string()))?;
        migrations::apply(&conn)
            .map_err(|e| LegionError::Store(format!("migration: {e}")))?;
        Ok(Self { conn: Arc::new(Mutex::new(conn)) })
    }

    /// Open an in-memory store (for testing).
    pub fn open_in_memory() -> Result<Self> {
        let conn = Connection::open_in_memory()
            .map_err(|e| LegionError::Store(e.to_string()))?;
        migrations::apply(&conn)
            .map_err(|e| LegionError::Store(format!("migration: {e}")))?;
        Ok(Self { conn: Arc::new(Mutex::new(conn)) })
    }
}

// ── EventStore impl ───────────────────────────────────────────────────────────

#[async_trait]
impl EventStore for SqliteStore {
    async fn create_session(&self, run_id: RunId, config: &RunConfig) -> Result<()> {
        let conn = self.conn.lock().await;
        let now  = chrono::Utc::now().timestamp_millis();
        let cfg  = serde_json::to_string(config)?;
        let idle = serde_json::to_string(&SessionStatus::Idle)?;
        conn.execute(
            "INSERT INTO sessions (run_id, status, config, created_at, updated_at)
             VALUES (?1, ?2, ?3, ?4, ?4)",
            params![run_id.to_string(), idle, cfg, now],
        ).map_err(|e| LegionError::Store(e.to_string()))?;
        Ok(())
    }

    async fn append(&self, run_id: RunId, event: TurnEvent) -> Result<SeqNum> {
        let conn = self.conn.lock().await;
        let now  = chrono::Utc::now().timestamp_millis();

        // Get next seq and prev_hash in one transaction
        let run_str = run_id.to_string();

        // Check session exists
        let exists: bool = conn
            .query_row(
                "SELECT 1 FROM sessions WHERE run_id = ?1",
                params![run_str],
                |_| Ok(true),
            )
            .unwrap_or(false);
        if !exists {
            return Err(LegionError::SessionNotFound(run_id));
        }

        // Get current log tail
        let (seq, prev_hash): (SeqNum, Vec<u8>) = conn
            .query_row(
                "SELECT COALESCE(MAX(seq) + 1, 0), '' FROM turns WHERE run_id = ?1",
                params![run_str],
                |row| Ok((row.get::<_, i64>(0)? as SeqNum, vec![])),
            )
            .map_err(|e| LegionError::Store(e.to_string()))?;

        // Compute prev_hash from last envelope if seq > 0
        let prev_hash: [u8; 32] = if seq == 0 {
            [0u8; 32]
        } else {
            let last = load_turn(&conn, &run_str, seq - 1)?;
            hash_envelope(&last)
        };

        let kind_tag = event_kind_tag(&event.kind);
        let payload  = event.payload.as_ref()
            .map(|v| serde_json::to_string(v))
            .transpose()?;

        conn.execute(
            "INSERT INTO turns
             (run_id, seq, prev_hash, kind, payload, payload_cid, model,
              tokens_in, tokens_out, wall_ms, created_at)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11)",
            params![
                run_str,
                seq as i64,
                prev_hash.as_slice(),
                kind_tag,
                payload,
                event.payload_cid,
                event.model,
                event.tokens_in.map(|v| v as i64),
                event.tokens_out.map(|v| v as i64),
                event.wall_ms.map(|v| v as i64),
                now,
            ],
        ).map_err(|e| LegionError::Store(e.to_string()))?;

        // Touch session updated_at
        conn.execute(
            "UPDATE sessions SET updated_at = ?1 WHERE run_id = ?2",
            params![now, run_str],
        ).map_err(|e| LegionError::Store(e.to_string()))?;

        Ok(seq)
    }

    async fn read_log(&self, run_id: RunId) -> Result<Vec<TurnEnvelope>> {
        let conn = self.conn.lock().await;
        let run_str = run_id.to_string();
        let turns = load_all_turns(&conn, &run_str, run_id)?;
        verify_chain(&turns, run_id)?;
        Ok(turns)
    }

    async fn read_recent(&self, run_id: RunId, n: usize) -> Result<Vec<TurnEnvelope>> {
        let conn = self.conn.lock().await;
        let run_str = run_id.to_string();

        // Count total turns first for offset calculation
        let total: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM turns WHERE run_id = ?1",
                params![run_str],
                |row| row.get(0),
            )
            .map_err(|e| LegionError::Store(e.to_string()))?;

        let offset = (total as usize).saturating_sub(n);
        let mut stmt = conn.prepare(
            "SELECT seq, prev_hash, kind, payload, payload_cid, model,
                    tokens_in, tokens_out, wall_ms, created_at
             FROM turns WHERE run_id = ?1 ORDER BY seq ASC LIMIT ?2 OFFSET ?3",
        ).map_err(|e| LegionError::Store(e.to_string()))?;

        let turns = stmt
            .query_map(params![run_str, n as i64, offset as i64], |row| {
                row_to_envelope(row, run_id)
            })
            .map_err(|e| LegionError::Store(e.to_string()))?
            .collect::<std::result::Result<Vec<_>, _>>()
            .map_err(|e| LegionError::Store(e.to_string()))?;

        Ok(turns)
    }

    async fn session_status(&self, run_id: RunId) -> Result<SessionStatus> {
        let conn = self.conn.lock().await;
        let run_str = run_id.to_string();
        let status_json: String = conn
            .query_row(
                "SELECT status FROM sessions WHERE run_id = ?1",
                params![run_str],
                |row| row.get(0),
            )
            .map_err(|_| LegionError::SessionNotFound(run_id))?;
        parse_status(&status_json, run_id)
    }

    async fn set_status(&self, run_id: RunId, status: SessionStatus) -> Result<()> {
        let conn = self.conn.lock().await;
        let run_str    = run_id.to_string();
        let status_json = serde_json::to_string(&status)?;
        let now = chrono::Utc::now().timestamp_millis();
        let affected = conn.execute(
            "UPDATE sessions SET status = ?1, updated_at = ?2 WHERE run_id = ?3",
            params![status_json, now, run_str],
        ).map_err(|e| LegionError::Store(e.to_string()))?;
        if affected == 0 {
            return Err(LegionError::SessionNotFound(run_id));
        }
        Ok(())
    }

    async fn fork(&self, run_id: RunId, at_seq: SeqNum) -> Result<RunId> {
        let conn = self.conn.lock().await;
        let run_str = run_id.to_string();
        let now = chrono::Utc::now().timestamp_millis();

        // Load parent config
        let (config_json, status_json): (String, String) = conn
            .query_row(
                "SELECT config, status FROM sessions WHERE run_id = ?1",
                params![run_str],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .map_err(|_| LegionError::SessionNotFound(run_id))?;

        let _ = status_json; // parent status not needed

        let new_id  = Uuid::new_v4();
        let new_str = new_id.to_string();

        let idle = serde_json::to_string(&SessionStatus::Idle)
            .map_err(|e| LegionError::Store(e.to_string()))?;
        conn.execute(
            "INSERT INTO sessions
             (run_id, parent_run, fork_seq, status, config, created_at, updated_at)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?6)",
            params![new_str, run_str, at_seq as i64, idle, config_json, now],
        ).map_err(|e| LegionError::Store(e.to_string()))?;

        // Copy turns up to at_seq
        conn.execute(
            "INSERT INTO turns
             (run_id, seq, prev_hash, kind, payload, payload_cid, model,
              tokens_in, tokens_out, wall_ms, created_at)
             SELECT ?1, seq, prev_hash, kind, payload, payload_cid, model,
                    tokens_in, tokens_out, wall_ms, created_at
             FROM turns WHERE run_id = ?2 AND seq <= ?3",
            params![new_str, run_str, at_seq as i64],
        ).map_err(|e| LegionError::Store(e.to_string()))?;

        Ok(new_id)
    }

    async fn list_sessions(&self, filter: SessionFilter) -> Result<Vec<SessionSummary>> {
        let conn    = self.conn.lock().await;
        let limit   = filter.limit.unwrap_or(50) as i64;
        let offset  = filter.offset.unwrap_or(0) as i64;

        let mut stmt = conn.prepare(
            "SELECT s.run_id, s.status, s.config, s.created_at, s.updated_at,
                    (SELECT COUNT(*) FROM turns t WHERE t.run_id = s.run_id) AS turns
             FROM sessions s
             ORDER BY s.created_at DESC
             LIMIT ?1 OFFSET ?2",
        ).map_err(|e| LegionError::Store(e.to_string()))?;

        let rows = stmt.query_map(params![limit, offset], |row| {
            let run_id_str: String = row.get(0)?;
            let status_json: String = row.get(1)?;
            let config_json: String = row.get(2)?;
            let created_at: i64     = row.get(3)?;
            let updated_at: i64     = row.get(4)?;
            let turns: i64          = row.get(5)?;
            Ok((run_id_str, status_json, config_json, created_at, updated_at, turns))
        }).map_err(|e| LegionError::Store(e.to_string()))?;

        let mut summaries = vec![];
        for row in rows {
            let (rid, status_json, config_json, created_at, updated_at, turns) =
                row.map_err(|e| LegionError::Store(e.to_string()))?;
            let run_id = Uuid::parse_str(&rid)
                .map_err(|e| LegionError::Store(e.to_string()))?;
            let status = parse_status(&status_json, run_id)?;
            let config: RunConfig = serde_json::from_str(&config_json)?;
            summaries.push(SessionSummary {
                run_id,
                status,
                model: config.model,
                turns: turns as u64,
                created_at,
                updated_at,
            });
        }
        Ok(summaries)
    }
}

// ── Internal helpers ──────────────────────────────────────────────────────────

fn event_kind_tag(kind: &TurnEventKind) -> String {
    // Store the entire kind as JSON so we can reconstruct it
    serde_json::to_string(kind).unwrap_or_else(|_| "\"unknown\"".into())
}

fn load_turn(conn: &Connection, run_str: &str, seq: SeqNum) -> Result<TurnEnvelope> {
    let run_id = Uuid::parse_str(run_str)
        .map_err(|e| LegionError::Store(e.to_string()))?;
    conn.query_row(
        "SELECT seq, prev_hash, kind, payload, payload_cid, model,
                tokens_in, tokens_out, wall_ms, created_at
         FROM turns WHERE run_id = ?1 AND seq = ?2",
        params![run_str, seq as i64],
        |row| row_to_envelope(row, run_id),
    ).map_err(|e| LegionError::Store(e.to_string()))
}

fn load_all_turns(conn: &Connection, run_str: &str, run_id: RunId) -> Result<Vec<TurnEnvelope>> {
    let mut stmt = conn.prepare(
        "SELECT seq, prev_hash, kind, payload, payload_cid, model,
                tokens_in, tokens_out, wall_ms, created_at
         FROM turns WHERE run_id = ?1 ORDER BY seq ASC",
    ).map_err(|e| LegionError::Store(e.to_string()))?;

    let turns = stmt
        .query_map(params![run_str], |row| row_to_envelope(row, run_id))
        .map_err(|e| LegionError::Store(e.to_string()))?
        .collect::<std::result::Result<Vec<_>, _>>()
        .map_err(|e| LegionError::Store(e.to_string()))?;
    Ok(turns)
}

fn row_to_envelope(row: &rusqlite::Row, run_id: RunId) -> rusqlite::Result<TurnEnvelope> {
    let seq: i64           = row.get(0)?;
    let prev_hash_bytes: Vec<u8> = row.get(1)?;
    let kind_json: String  = row.get(2)?;
    let payload_str: Option<String> = row.get(3)?;
    let payload_cid: Option<String> = row.get(4)?;
    let model: Option<String>       = row.get(5)?;
    let tokens_in: Option<i64>      = row.get(6)?;
    let tokens_out: Option<i64>     = row.get(7)?;
    let wall_ms: Option<i64>        = row.get(8)?;
    let created_at: i64             = row.get(9)?;

    let mut prev_hash = [0u8; 32];
    let len = prev_hash_bytes.len().min(32);
    prev_hash[..len].copy_from_slice(&prev_hash_bytes[..len]);

    let kind: TurnEventKind = serde_json::from_str(&kind_json)
        .unwrap_or(TurnEventKind::UserMessage);
    let payload = payload_str
        .as_deref()
        .and_then(|s| serde_json::from_str(s).ok());

    Ok(TurnEnvelope {
        run_id,
        seq: seq as SeqNum,
        prev_hash,
        event: TurnEvent {
            kind,
            payload,
            payload_cid,
            model,
            tokens_in:  tokens_in.map(|v| v as u32),
            tokens_out: tokens_out.map(|v| v as u32),
            wall_ms:    wall_ms.map(|v| v as u64),
        },
        created_at,
    })
}

fn hash_envelope(env: &TurnEnvelope) -> [u8; 32] {
    use sha2::{Sha256, Digest};
    // Hash only content fields (not run_id) so forked chains remain valid.
    let content = serde_json::json!({
        "seq":        env.seq,
        "prev_hash":  env.prev_hash,
        "event":      env.event,
        "created_at": env.created_at,
    });
    let json = serde_json::to_vec(&content).unwrap_or_default();
    let mut hasher = Sha256::new();
    hasher.update(&json);
    hasher.finalize().into()
}

fn verify_chain(log: &[TurnEnvelope], run_id: RunId) -> Result<()> {
    for (i, env) in log.iter().enumerate() {
        let expected = if i == 0 {
            [0u8; 32]
        } else {
            hash_envelope(&log[i - 1])
        };
        if env.prev_hash != expected {
            return Err(LegionError::TamperEvident(run_id, env.seq));
        }
    }
    Ok(())
}

fn parse_status(json: &str, run_id: RunId) -> Result<SessionStatus> {
    // Legacy plain-string statuses (e.g. 'idle') stored before full JSON
    if !json.starts_with('{') && !json.starts_with('"') {
        let s = format!("\"{}\"", json.trim());
        return serde_json::from_str::<SessionStatus>(&s)
            .map_err(|_| LegionError::Store(format!("unknown status '{json}' for {run_id}")));
    }
    serde_json::from_str(json)
        .map_err(|e| LegionError::Store(format!("status parse error for {run_id}: {e}")))
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use legion_core::types::{Budget, TurnEventKind};

    fn config() -> RunConfig {
        RunConfig {
            system_prompt: Some("test".into()),
            model:         "faux/test".into(),
            budget:        Budget::default(),
            tools:         vec![],
            metadata:      None,
        }
    }

    #[tokio::test]
    async fn sqlite_store_basic_append_and_read() {
        let store = SqliteStore::open_in_memory().unwrap();
        let run_id = Uuid::new_v4();
        store.create_session(run_id, &config()).await.unwrap();

        let s0 = store.append(run_id, TurnEvent::user_message("hello")).await.unwrap();
        let s1 = store.append(run_id, TurnEvent::model_call_intent()).await.unwrap();
        assert_eq!(s0, 0);
        assert_eq!(s1, 1);

        let log = store.read_log(run_id).await.unwrap();
        assert_eq!(log.len(), 2);
        assert!(matches!(log[0].event.kind, TurnEventKind::UserMessage));
        assert!(matches!(log[1].event.kind, TurnEventKind::ModelCallIntent));
    }

    #[tokio::test]
    async fn sqlite_store_hash_chain_valid() {
        let store = SqliteStore::open_in_memory().unwrap();
        let run_id = Uuid::new_v4();
        store.create_session(run_id, &config()).await.unwrap();
        for i in 0..5u32 {
            store.append(run_id, TurnEvent::user_message(format!("msg {i}"))).await.unwrap();
        }
        store.read_log(run_id).await.unwrap(); // verifies chain
    }

    #[tokio::test]
    async fn sqlite_store_status_transitions() {
        let store = SqliteStore::open_in_memory().unwrap();
        let run_id = Uuid::new_v4();
        store.create_session(run_id, &config()).await.unwrap();
        assert!(matches!(store.session_status(run_id).await.unwrap(), SessionStatus::Idle));

        store.set_status(run_id, SessionStatus::Running).await.unwrap();
        assert!(matches!(store.session_status(run_id).await.unwrap(), SessionStatus::Running));

        store.set_status(run_id, SessionStatus::Completed).await.unwrap();
        assert!(store.session_status(run_id).await.unwrap().is_terminal());
    }

    #[tokio::test]
    async fn sqlite_store_read_recent() {
        let store = SqliteStore::open_in_memory().unwrap();
        let run_id = Uuid::new_v4();
        store.create_session(run_id, &config()).await.unwrap();
        for i in 0..10u32 {
            store.append(run_id, TurnEvent::user_message(format!("msg {i}"))).await.unwrap();
        }
        let recent = store.read_recent(run_id, 3).await.unwrap();
        assert_eq!(recent.len(), 3);
        assert_eq!(recent[0].seq, 7);
        assert_eq!(recent[2].seq, 9);
    }

    #[tokio::test]
    async fn sqlite_store_fork() {
        let store = SqliteStore::open_in_memory().unwrap();
        let run_id = Uuid::new_v4();
        store.create_session(run_id, &config()).await.unwrap();
        store.append(run_id, TurnEvent::user_message("a")).await.unwrap();
        store.append(run_id, TurnEvent::user_message("b")).await.unwrap();
        store.append(run_id, TurnEvent::user_message("c")).await.unwrap();

        let fork_id = store.fork(run_id, 1).await.unwrap();
        let fork_log = store.read_log(fork_id).await.unwrap();
        assert_eq!(fork_log.len(), 2); // seq 0 and 1
    }

    #[tokio::test]
    async fn sqlite_store_session_not_found() {
        let store = SqliteStore::open_in_memory().unwrap();
        let bad_id = Uuid::new_v4();
        let err = store.append(bad_id, TurnEvent::user_message("x")).await;
        assert!(matches!(err, Err(LegionError::SessionNotFound(_))));
    }

    #[tokio::test]
    async fn sqlite_store_list_sessions() {
        let store = SqliteStore::open_in_memory().unwrap();
        for _ in 0..3 {
            let id = Uuid::new_v4();
            store.create_session(id, &config()).await.unwrap();
        }
        let sessions = store.list_sessions(SessionFilter::default()).await.unwrap();
        assert_eq!(sessions.len(), 3);
    }
}
