DDL = """
CREATE TABLE IF NOT EXISTS lake_format (
    version INTEGER PRIMARY KEY, created_at TEXT NOT NULL
);
INSERT OR IGNORE INTO lake_format VALUES (1, strftime('%Y-%m-%dT%H:%M:%fZ','now'));
CREATE TABLE IF NOT EXISTS lookup_indexes (
    table_name TEXT PRIMARY KEY REFERENCES lake_tables(name),
    snapshot_id INTEGER NOT NULL,
    key_columns TEXT NOT NULL,
    entry_count INTEGER NOT NULL,
    built_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS lookup_entries (
    table_name TEXT NOT NULL REFERENCES lookup_indexes(table_name) ON DELETE CASCADE,
    row_key TEXT NOT NULL,
    file_json TEXT NOT NULL,
    ordinal INTEGER NOT NULL,
    PRIMARY KEY(table_name, row_key)
);
CREATE TABLE IF NOT EXISTS transform_jobs (
    name TEXT PRIMARY KEY,
    source_table TEXT NOT NULL REFERENCES lake_tables(name),
    target_table TEXT NOT NULL REFERENCES lake_tables(name),
    document TEXT NOT NULL,
    created_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS transform_runs (
    id TEXT PRIMARY KEY,
    job_name TEXT NOT NULL REFERENCES transform_jobs(name),
    input_snapshot INTEGER NOT NULL,
    output_snapshot INTEGER NOT NULL,
    input_rows INTEGER NOT NULL,
    output_rows INTEGER NOT NULL,
    created_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS dataset_exports (
    id TEXT PRIMARY KEY,
    table_name TEXT NOT NULL,
    snapshot_id INTEGER NOT NULL,
    format TEXT NOT NULL,
    path TEXT NOT NULL,
    sha256 TEXT NOT NULL,
    row_count INTEGER NOT NULL,
    created_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS quality_runs (
    id TEXT PRIMARY KEY,
    table_name TEXT NOT NULL,
    snapshot_id INTEGER NOT NULL,
    specification TEXT NOT NULL,
    outcome TEXT NOT NULL,
    created_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS import_receipts (
    table_name TEXT NOT NULL,
    import_id TEXT NOT NULL,
    file_sha256 TEXT NOT NULL,
    format TEXT NOT NULL,
    snapshot_id INTEGER NOT NULL,
    created_at TEXT NOT NULL,
    PRIMARY KEY(table_name, import_id)
);
CREATE TABLE IF NOT EXISTS stream_imports (
    table_name TEXT NOT NULL,
    import_id TEXT NOT NULL,
    input_sha256 TEXT NOT NULL,
    chunk_rows INTEGER NOT NULL,
    invalid_policy TEXT NOT NULL,
    chunks INTEGER NOT NULL,
    accepted INTEGER NOT NULL,
    rejected INTEGER NOT NULL,
    status TEXT NOT NULL CHECK(status IN ('running','failed','completed')),
    updated_at TEXT NOT NULL,
    PRIMARY KEY(table_name, import_id)
);
CREATE TABLE IF NOT EXISTS aggregate_jobs (
    name TEXT PRIMARY KEY,
    source_table TEXT NOT NULL REFERENCES lake_tables(name),
    target_table TEXT NOT NULL REFERENCES lake_tables(name),
    document TEXT NOT NULL,
    created_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS aggregate_runs (
    id TEXT PRIMARY KEY,
    job_name TEXT NOT NULL REFERENCES aggregate_jobs(name),
    input_snapshot INTEGER NOT NULL,
    output_snapshot INTEGER NOT NULL,
    input_rows INTEGER NOT NULL,
    output_rows INTEGER NOT NULL,
    created_at TEXT NOT NULL
);
"""
