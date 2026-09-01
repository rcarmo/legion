CREATE TABLE functions (
    name TEXT NOT NULL,
    version TEXT NOT NULL,
    runtime TEXT NOT NULL,
    artifact_cid TEXT NOT NULL,
    manifest TEXT NOT NULL,
    created_at INTEGER NOT NULL,
    PRIMARY KEY (name, version)
);
CREATE TABLE schema_migrations (
    version INTEGER PRIMARY KEY,
    applied_at INTEGER NOT NULL
);
INSERT INTO schema_migrations(version, applied_at) VALUES (1, unixepoch('subsec') * 1000), (2, unixepoch('subsec') * 1000);
