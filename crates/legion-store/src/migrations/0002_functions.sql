-- Function registry

CREATE TABLE functions (
    name        TEXT    NOT NULL,
    cid         TEXT    NOT NULL,
    runtime     TEXT    NOT NULL DEFAULT 'bun',  -- 'wasm' | 'bun'
    schema_json TEXT,
    version     TEXT,
    created_at  INTEGER NOT NULL,
    PRIMARY KEY (name, cid)
);

CREATE TABLE function_routes (
    name        TEXT    PRIMARY KEY,
    default_cid TEXT    NOT NULL,
    routes_json TEXT                              -- JSON canary config
);
