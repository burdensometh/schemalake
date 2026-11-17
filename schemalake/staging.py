import json

from .commit import SnapshotCommitter
from .types import Conflict, Invalid, Missing, canonical, sha, timestamp
from .writer import DataWriter


DDL = """
CREATE TABLE IF NOT EXISTS prepared_batches (
    table_name TEXT NOT NULL REFERENCES lake_tables(name),
    batch_id TEXT NOT NULL,
    input_sha256 TEXT NOT NULL,
    expected_head INTEGER NOT NULL,
    schema_version INTEGER NOT NULL,
    status TEXT NOT NULL CHECK(status IN ('preparing','prepared','committed','aborted')),
    files TEXT NOT NULL,
    keys TEXT NOT NULL,
    row_count INTEGER NOT NULL,
    snapshot_id INTEGER,
    created_at TEXT NOT NULL,
    PRIMARY KEY(table_name,batch_id)
);
"""


class StagedIngestor:
    def __init__(self, catalog, objects, ingestor):
        self.catalog = catalog
        self.objects = objects
        self.ingestor = ingestor
        self.committer = SnapshotCommitter(catalog, objects)
        with catalog.transaction(True) as connection:
            connection.executescript(DDL)

    def get(self, table_name, batch_id):
        with self.catalog.transaction() as connection:
            row = connection.execute('SELECT * FROM prepared_batches WHERE table_name=? AND batch_id=?',
                                     (table_name, batch_id)).fetchone()
            if row is None:
                raise Missing('prepared batch does not exist')
            result = dict(row)
            result['files'] = json.loads(result['files'])
            result['keys'] = json.loads(result['keys'])
            return result

    def prepare(self, table_name, batch_id, rows, file_rows=1000):
        self.ingestor.validate_batch_id(batch_id)
        if not isinstance(rows, list) or len(rows) > 100000:
            raise Invalid('staging requires at most 100000 input rows')
        input_hash = sha(rows)
        with self.catalog.transaction(True) as connection:
            old = connection.execute('SELECT * FROM prepared_batches WHERE table_name=? AND batch_id=?',
                                     (table_name, batch_id)).fetchone()
            if old:
                if old['input_sha256'] != input_hash:
                    raise Conflict('prepared batch reused for different input')
                if old['status'] == 'aborted':
                    raise Conflict('aborted preparation cannot be restarted')
                return self.get(table_name, batch_id)
            table = self.catalog.table(table_name, connection)
            schema = self.catalog.schema(table_name, connection=connection)
            normalized = [schema.normalize(row) for row in rows]
            keys = [schema.key(row) for row in normalized if schema.key(row) is not None]
            if len(keys) != len(set(keys)):
                raise Invalid('duplicate key in prepared input')
            connection.execute('INSERT INTO prepared_batches VALUES (?,?,?,?,?,?,?,?,?,?,?)',
                               (table_name, batch_id, input_hash, table['head'], table['schema_version'],
                                'preparing', '[]', canonical(keys), len(rows), None, timestamp()))
        files = DataWriter(self.objects, file_rows).write(table_name, table['schema_version'], schema, normalized)
        with self.catalog.transaction(True) as connection:
            state = connection.execute('SELECT status FROM prepared_batches WHERE table_name=? AND batch_id=?',
                                       (table_name, batch_id)).fetchone()[0]
            if state != 'preparing':
                raise Conflict('preparation status changed while objects were written')
            connection.execute("UPDATE prepared_batches SET files=?,status='prepared' WHERE table_name=? AND batch_id=?",
                               (canonical(files), table_name, batch_id))
        return self.get(table_name, batch_id)

    def commit(self, table_name, batch_id):
        with self.catalog.transaction(True) as connection:
            row = connection.execute('SELECT * FROM prepared_batches WHERE table_name=? AND batch_id=?',
                                     (table_name, batch_id)).fetchone()
            if row is None:
                raise Missing('prepared batch does not exist')
            state = dict(row)
            if state['status'] == 'committed':
                return {'table': table_name, 'snapshot_id': state['snapshot_id'], 'replayed': True}
            if state['status'] != 'prepared':
                raise Conflict('batch is not ready to commit')
            table = self.catalog.table(table_name, connection)
            if (table['head'], table['schema_version']) != (state['expected_head'], state['schema_version']):
                raise Conflict('table changed after preparation')
            keys = json.loads(state['keys'])
            for key in keys:
                if connection.execute('SELECT 1 FROM row_keys WHERE table_name=? AND row_key=?', (table_name, key)).fetchone():
                    raise Conflict('prepared key already exists')
            added = json.loads(state['files'])
            for file in added:
                list(self.objects.read_rows(file))
            old = self.committer.current(connection, table_name)
            snapshot = self.committer.commit(connection, table_name, 'staged_append', old + added,
                                             state['expected_head'], {'batch_id': batch_id})
            connection.executemany('INSERT INTO row_keys VALUES (?,?,?)', ((table_name, key, batch_id) for key in keys))
            connection.execute("UPDATE prepared_batches SET status='committed',snapshot_id=? WHERE table_name=? AND batch_id=?",
                               (snapshot, table_name, batch_id))
        return {'table': table_name, 'snapshot_id': snapshot, 'replayed': False}

    def abort(self, table_name, batch_id):
        with self.catalog.transaction(True) as connection:
            row = connection.execute('SELECT status FROM prepared_batches WHERE table_name=? AND batch_id=?',
                                     (table_name, batch_id)).fetchone()
            if row is None:
                raise Missing('prepared batch does not exist')
            if row[0] == 'committed':
                raise Conflict('committed batch cannot be aborted')
            connection.execute("UPDATE prepared_batches SET status='aborted' WHERE table_name=? AND batch_id=?", (table_name, batch_id))
        return self.get(table_name, batch_id)
