from contextlib import contextmanager
from pathlib import Path
import json
import sqlite3

from .types import Conflict, Missing, Schema, canonical, name, timestamp
from .metadata import DDL as EXTENSION_DDL


DDL = """
CREATE TABLE IF NOT EXISTS lake_tables (
    name TEXT PRIMARY KEY,
    schema_version INTEGER NOT NULL,
    head INTEGER NOT NULL DEFAULT 0,
    created_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS schemas (
    table_name TEXT NOT NULL REFERENCES lake_tables(name),
    version INTEGER NOT NULL,
    document TEXT NOT NULL,
    fingerprint TEXT NOT NULL,
    created_at TEXT NOT NULL,
    PRIMARY KEY(table_name, version)
);
CREATE TABLE IF NOT EXISTS batches (
    table_name TEXT NOT NULL REFERENCES lake_tables(name),
    batch_id TEXT NOT NULL,
    input_sha256 TEXT NOT NULL,
    schema_version INTEGER NOT NULL,
    snapshot_id INTEGER NOT NULL,
    row_count INTEGER NOT NULL,
    rejected INTEGER NOT NULL,
    committed_at TEXT NOT NULL,
    PRIMARY KEY(table_name, batch_id)
);
CREATE TABLE IF NOT EXISTS snapshots (
    table_name TEXT NOT NULL REFERENCES lake_tables(name),
    id INTEGER NOT NULL,
    parent_id INTEGER,
    operation TEXT NOT NULL,
    manifest_path TEXT NOT NULL,
    manifest_sha256 TEXT NOT NULL,
    row_count INTEGER NOT NULL,
    created_at TEXT NOT NULL,
    PRIMARY KEY(table_name, id)
);
CREATE TABLE IF NOT EXISTS data_files (
    table_name TEXT NOT NULL REFERENCES lake_tables(name),
    id TEXT NOT NULL,
    schema_version INTEGER NOT NULL,
    partition_json TEXT NOT NULL,
    relative_path TEXT NOT NULL,
    sha256 TEXT NOT NULL,
    row_count INTEGER NOT NULL,
    byte_count INTEGER NOT NULL,
    statistics TEXT NOT NULL,
    added_snapshot INTEGER NOT NULL,
    PRIMARY KEY(table_name, id)
);
CREATE TABLE IF NOT EXISTS row_keys (
    table_name TEXT NOT NULL REFERENCES lake_tables(name),
    row_key TEXT NOT NULL,
    batch_id TEXT NOT NULL,
    PRIMARY KEY(table_name, row_key)
);
CREATE TABLE IF NOT EXISTS rejected_rows (
    table_name TEXT NOT NULL,
    batch_id TEXT NOT NULL,
    ordinal INTEGER NOT NULL,
    reason TEXT NOT NULL,
    raw_json TEXT NOT NULL,
    PRIMARY KEY(table_name, batch_id, ordinal)
);
CREATE TABLE IF NOT EXISTS history (
    sequence INTEGER PRIMARY KEY AUTOINCREMENT,
    table_name TEXT NOT NULL,
    action TEXT NOT NULL,
    detail TEXT NOT NULL,
    created_at TEXT NOT NULL
);
"""


class Catalog:
    def __init__(self, root):
        self.root = Path(root).resolve()
        self.root.mkdir(parents=True, exist_ok=True)
        self.path = self.root / "catalog.sqlite"
        connection = self.connect()
        try:
            connection.execute("PRAGMA journal_mode=WAL")
            connection.executescript(DDL)
            connection.executescript(EXTENSION_DDL)
        finally:
            connection.close()

    def connect(self):
        connection = sqlite3.connect(self.path, timeout=20, isolation_level=None)
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA foreign_keys=ON")
        return connection

    @contextmanager
    def transaction(self, write=False):
        connection = self.connect()
        try:
            connection.execute("BEGIN IMMEDIATE" if write else "BEGIN")
            yield connection
            connection.commit()
        except BaseException:
            connection.rollback()
            raise
        finally:
            connection.close()

    def create_table(self, table_name, schema):
        table_name = name(table_name)
        schema = Schema.parse(schema)
        with self.transaction(True) as connection:
            existing = connection.execute(
                "SELECT fingerprint FROM schemas WHERE table_name=? AND version=1", (table_name,)
            ).fetchone()
            if existing:
                if existing[0] != schema.fingerprint():
                    raise Conflict("table already exists with a different schema")
            else:
                connection.execute(
                    "INSERT INTO lake_tables VALUES (?, 1, 0, ?)", (table_name, timestamp())
                )
                connection.execute(
                    "INSERT INTO schemas VALUES (?, 1, ?, ?, ?)",
                    (table_name, canonical(schema.encode()), schema.fingerprint(), timestamp()),
                )
                self.event(connection, table_name, "table_created", {"schema": schema.fingerprint()})
        return self.table(table_name)

    def table(self, table_name, connection=None):
        if connection is None:
            with self.transaction() as connection:
                return self.table(table_name, connection)
        row = connection.execute("SELECT * FROM lake_tables WHERE name=?", (table_name,)).fetchone()
        if row is None:
            raise Missing(f"unknown table: {table_name}")
        return dict(row)

    def schema(self, table_name, version=None, connection=None):
        if connection is None:
            with self.transaction() as connection:
                return self.schema(table_name, version, connection)
        table = self.table(table_name, connection)
        version = table["schema_version"] if version is None else version
        row = connection.execute(
            "SELECT document FROM schemas WHERE table_name=? AND version=?", (table_name, version)
        ).fetchone()
        if row is None:
            raise Missing("schema version does not exist")
        return Schema.parse(json.loads(row[0]))

    def tables(self):
        with self.transaction() as connection:
            return [dict(row) for row in connection.execute("SELECT * FROM lake_tables ORDER BY name")]

    def event(self, connection, table_name, action, detail):
        connection.execute(
            "INSERT INTO history(table_name,action,detail,created_at) VALUES (?,?,?,?)",
            (table_name, action, canonical(detail), timestamp()),
        )

    def snapshots(self, table_name):
        self.table(table_name)
        with self.transaction() as connection:
            return [dict(row) for row in connection.execute(
                "SELECT * FROM snapshots WHERE table_name=? ORDER BY id DESC", (table_name,)
            )]

    def batch(self, table_name, batch_id, connection=None):
        if connection is None:
            with self.transaction() as connection:
                return self.batch(table_name, batch_id, connection)
        row = connection.execute(
            "SELECT * FROM batches WHERE table_name=? AND batch_id=?", (table_name, batch_id)
        ).fetchone()
        return dict(row) if row else None

    def rejects(self, table_name, batch_id):
        with self.transaction() as connection:
            return [dict(row) for row in connection.execute(
                "SELECT * FROM rejected_rows WHERE table_name=? AND batch_id=? ORDER BY ordinal",
                (table_name, batch_id),
            )]
