from .manifests import build_document
from .types import Conflict, canonical, timestamp


class SnapshotCommitter:
    def __init__(self, catalog, objects):
        self.catalog = catalog
        self.objects = objects
        if getattr(objects, 'catalog', None) is None and hasattr(objects, 'bind'):
            objects.bind(catalog)

    def previous(self, connection, table_name):
        """Return (record, files) for the current head, or (None, []) when empty."""
        table = self.catalog.table(table_name, connection)
        if not table['head']:
            return None, []
        record = dict(connection.execute(
            'SELECT * FROM snapshots WHERE table_name=? AND id=?',
            (table_name, table['head']),
        ).fetchone())
        return record, self.objects.manifest(record)['files']

    def current(self, connection, table_name):
        return self.previous(connection, table_name)[1]

    def commit(self, connection, table_name, operation, files, expected_head=None, detail=None, more=None):
        table = self.catalog.table(table_name, connection)
        if expected_head is not None and table['head'] != expected_head:
            raise Conflict('snapshot head changed')
        previous_record, previous_files = self.previous(connection, table_name)
        snapshot_id = table['head'] + 1
        document = build_document(
            table_name, snapshot_id, table['schema_version'], table['head'] or None,
            operation, files, previous_record, previous_files, more,
        )
        path, checksum = self.objects.write_manifest(table_name, document)
        count = sum(file['row_count'] for file in files)
        connection.execute(
            'INSERT INTO snapshots VALUES (?,?,?,?,?,?,?,?)',
            (table_name, snapshot_id, table['head'] or None, operation, path, checksum, count, timestamp()),
        )
        for file in files:
            connection.execute(
                'INSERT OR IGNORE INTO data_files VALUES (?,?,?,?,?,?,?,?,?,?)',
                (table_name, file['id'], file['schema_version'], canonical(file['partition']),
                 file['path'], file['sha256'], file['row_count'], file['byte_count'],
                 canonical(file['statistics']), snapshot_id),
            )
        connection.execute('UPDATE lake_tables SET head=? WHERE name=?', (snapshot_id, table_name))
        self.catalog.event(connection, table_name, operation, {
            'snapshot_id': snapshot_id, 'parent': table['head'], 'rows': count, **(detail or {}),
        })
        return snapshot_id

    def rebuild_keys(self, connection, table_name, files, batch_id):
        schema = self.catalog.schema(table_name, connection=connection)
        connection.execute('DELETE FROM row_keys WHERE table_name=?', (table_name,))
        keys = set()
        for file in files:
            for raw in self.objects.read_rows(file):
                row = schema.normalize(raw)
                key = schema.key(row)
                if key is not None:
                    if key in keys:
                        raise Conflict('rewrite creates duplicate primary key')
                    keys.add(key)
        connection.executemany('INSERT INTO row_keys VALUES (?,?,?)',
                               ((table_name, key, batch_id) for key in keys))
