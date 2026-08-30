-- Legion initial schema: sessions and turns

CREATE TABLE sessions (
    run_id      TEXT    PRIMARY KEY,
    parent_run  TEXT,
    fork_seq    INTEGER,
    status      TEXT    NOT NULL DEFAULT 'idle',
    config      TEXT    NOT NULL,               -- JSON RunConfig
    created_at  INTEGER NOT NULL,
    updated_at  INTEGER NOT NULL
);

CREATE TABLE turns (
    run_id      TEXT    NOT NULL,
    seq         INTEGER NOT NULL,
    prev_hash   BLOB    NOT NULL,               -- 32-byte SHA-256
    kind        TEXT    NOT NULL,               -- TurnEventKind tag
    payload     TEXT,                           -- JSON, NULL if using CID
    payload_cid TEXT,                           -- iroh-blobs CID (future)
    model       TEXT,
    tokens_in   INTEGER,
    tokens_out  INTEGER,
    wall_ms     INTEGER,
    created_at  INTEGER NOT NULL,
    PRIMARY KEY (run_id, seq),
    FOREIGN KEY (run_id) REFERENCES sessions(run_id)
);

CREATE INDEX idx_turns_run_kind ON turns(run_id, kind);
CREATE INDEX idx_sessions_status ON sessions(status);
