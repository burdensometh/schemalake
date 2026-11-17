from .projection import project_row
from .types import Conflict, canonical, timestamp


class SnapshotCommitter:
    def __init__(self, catalog, objects):
        self.catalog = catalog
        self.objects = objects

    def current(self, connection, table_name):
        table = self.catalog.table(table_name, connection)
        if not table['head']:
            return []
        record = dict(connection.execute(
            'SELECT * FROM snapshots WHERE table_name=? AND id=?',
            (table_name, table['head']),
        ).fetchone())
        return self.objects.manifest(record)['files']

    def commit(self, connection, table_name, operation, files, expected_head=None, detail=None):
        table = self.catalog.table(table_name, connection)
        if expected_head is not None and table['head'] != expected_head:
            raise Conflict('snapshot head changed')
        snapshot_id = table['head'] + 1
        document = {
            'format_version': 1, 'table': table_name, 'snapshot_id': snapshot_id,
            'schema_version': table['schema_version'], 'parent_id': table['head'] or None,
            'operation': operation, 'files': files,
        }
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
        table = self.catalog.table(table_name, connection)
        current_version = table['schema_version']
        schema = self.catalog.schema(table_name, current_version, connection)
        versions = {file['schema_version'] for file in files}
        file_schemas = {version: self.catalog.schema(table_name, version, connection) for version in versions}
        connection.execute('DELETE FROM row_keys WHERE table_name=?', (table_name,))
        keys = set()
        for file in files:
            source_schema = file_schemas[file['schema_version']]
            for raw in self.objects.read_rows(file):
                if file['schema_version'] == current_version:
                    row = schema.normalize(raw)
                else:
                    row = project_row(raw, source_schema, schema)
                key = schema.key(row)
                if key is not None:
                    if key in keys:
                        raise Conflict('rewrite creates duplicate primary key')
                    keys.add(key)
        connection.executemany('INSERT INTO row_keys VALUES (?,?,?)',
                               ((table_name, key, batch_id) for key in keys))
