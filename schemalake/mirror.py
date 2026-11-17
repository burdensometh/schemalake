from pathlib import Path
import json
import sqlite3

from .types import Conflict, canonical, timestamp


DDL = """
CREATE TABLE IF NOT EXISTS heads (
    table_name TEXT PRIMARY KEY, snapshot_id INTEGER NOT NULL,
    schema_document TEXT NOT NULL, imported_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS rows (
    table_name TEXT NOT NULL, ordinal INTEGER NOT NULL, payload TEXT NOT NULL,
    PRIMARY KEY(table_name, ordinal)
);
CREATE TABLE IF NOT EXISTS receipts (
    table_name TEXT NOT NULL, snapshot_id INTEGER NOT NULL, rows INTEGER NOT NULL,
    imported_at TEXT NOT NULL, PRIMARY KEY(table_name, snapshot_id)
);
"""


class Mirror:
    def __init__(self, path):
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        with self.connect() as connection:
            connection.executescript(DDL)

    def connect(self):
        connection = sqlite3.connect(self.path, timeout=20, isolation_level=None)
        connection.row_factory = sqlite3.Row
        return connection

    def apply(self, scanner, table_name, snapshot_id=None):
        plan = scanner.plan(table_name, snapshot_id)
        schema = scanner.catalog.schema(table_name, plan['schema_version'])
        connection = self.connect()
        try:
            connection.execute('BEGIN IMMEDIATE')
            old = connection.execute('SELECT * FROM receipts WHERE table_name=? AND snapshot_id=?',
                                     (table_name, plan['snapshot_id'])).fetchone()
            if old:
                connection.rollback()
                return {**dict(old), 'replayed': True}
            head = connection.execute('SELECT snapshot_id FROM heads WHERE table_name=?', (table_name,)).fetchone()
            if head and head[0] > plan['snapshot_id']:
                raise Conflict('cannot apply an older unseen snapshot')
            connection.execute('DELETE FROM rows WHERE table_name=?', (table_name,))
            count = 0
            for ordinal, row in enumerate(scanner.rows(table_name, plan['snapshot_id'])):
                connection.execute('INSERT INTO rows VALUES (?,?,?)', (table_name, ordinal, canonical(row)))
                count += 1
            connection.execute('INSERT INTO heads VALUES (?,?,?,?) ON CONFLICT(table_name) DO UPDATE SET '
                               'snapshot_id=excluded.snapshot_id,schema_document=excluded.schema_document,imported_at=excluded.imported_at',
                               (table_name, plan['snapshot_id'], canonical(schema.encode()), timestamp()))
            connection.execute('INSERT INTO receipts VALUES (?,?,?,?)',
                               (table_name, plan['snapshot_id'], count, timestamp()))
            connection.commit()
            return {'table_name': table_name, 'snapshot_id': plan['snapshot_id'], 'rows': count, 'replayed': False}
        except BaseException:
            connection.rollback()
            raise
        finally:
            connection.close()

    def rows(self, table_name):
        connection = self.connect()
        try:
            return [json.loads(row[0]) for row in connection.execute(
                'SELECT payload FROM rows WHERE table_name=? ORDER BY ordinal', (table_name,),
            )]
        finally:
            connection.close()

    def heads(self):
        connection = self.connect()
        try:
            return [dict(row) for row in connection.execute('SELECT * FROM heads ORDER BY table_name')]
        finally:
            connection.close()
