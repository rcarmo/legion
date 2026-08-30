/// SQL migrations applied in order on every startup.
/// Each entry is (version, sql).
pub const MIGRATIONS: &[(u32, &str)] = &[
    (1, include_str!("migrations/0001_initial.sql")),
    (2, include_str!("migrations/0002_functions.sql")),
];

pub fn apply(conn: &rusqlite::Connection) -> Result<(), rusqlite::Error> {
    // Create migrations tracking table
    conn.execute_batch("
        CREATE TABLE IF NOT EXISTS _migrations (
            version    INTEGER PRIMARY KEY,
            applied_at INTEGER NOT NULL
        );
        PRAGMA journal_mode = WAL;
        PRAGMA foreign_keys = ON;
        PRAGMA synchronous = NORMAL;
    ")?;

    let current: u32 = conn
        .query_row(
            "SELECT COALESCE(MAX(version), 0) FROM _migrations",
            [],
            |row| row.get(0),
        )
        .unwrap_or(0);

    for (version, sql) in MIGRATIONS {
        if *version > current {
            conn.execute_batch(sql)?;
            conn.execute(
                "INSERT INTO _migrations (version, applied_at) VALUES (?1, ?2)",
                rusqlite::params![version, chrono::Utc::now().timestamp_millis()],
            )?;
        }
    }
    Ok(())
}
