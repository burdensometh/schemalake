import json

from .manifests import FORMAT_V1, FORMAT_V2, ManifestValidator, build_document
from .types import Conflict, Invalid, Missing, timestamp


DDL = """
CREATE TABLE IF NOT EXISTS manifest_migrations (
    table_name TEXT NOT NULL,
    snapshot_id INTEGER NOT NULL,
    source_format INTEGER NOT NULL,
    target_format INTEGER NOT NULL,
    source_manifest_sha256 TEXT NOT NULL,
    target_manifest_sha256 TEXT NOT NULL,
    migrated_at TEXT NOT NULL,
    PRIMARY KEY(table_name, snapshot_id)
);
"""


class ManifestMigration:
    """Convert persisted v1 snapshot manifests to composable v2 deltas.

    Each snapshot is rewritten in its own write transaction: its resolved file
    list never changes, only the manifest identity and the way it is stored.
    Work is idempotent and resumable because every step re-reads the persisted
    format before acting, and content-addressed manifest objects make an
    interrupted attempt safe to rewrite.
    """

    def __init__(self, catalog, objects):
        self.catalog = catalog
        self.objects = objects
        if getattr(objects, 'catalog', None) is None and hasattr(objects, 'bind'):
            objects.bind(catalog)
        with catalog.transaction(True) as connection:
            connection.executescript(DDL)

    def _scan(self, connection, table_name):
        validator = ManifestValidator()
        rows = []
        for row in connection.execute(
            'SELECT * FROM snapshots WHERE table_name=? ORDER BY id', (table_name,)
        ):
            record = dict(row)
            document = json.loads(self.objects.read(
                record['manifest_path'], record['manifest_sha256']))
            rows.append((record, document, validator.document(document, record)))
        return rows

    def status(self):
        tables = {}
        complete = True
        with self.catalog.transaction() as connection:
            for table in self.catalog.tables():
                name = table['name']
                rows = self._scan(connection, name)
                v1 = sum(1 for _, _, version in rows if version == FORMAT_V1)
                v2 = len(rows) - v1
                complete = complete and v1 == 0
                tables[name] = {'snapshots': len(rows), 'v1': v1, 'v2': v2}
            version = connection.execute('SELECT max(version) FROM lake_format').fetchone()[0]
        return {'format_version': version or FORMAT_V1,
                'target_format_version': FORMAT_V2,
                'complete': complete, 'tables': tables}

    def _next_v1(self, connection, table_name, after=0):
        validator = ManifestValidator()
        for row in connection.execute(
            'SELECT * FROM snapshots WHERE table_name=? AND id>? ORDER BY id', (table_name, after)
        ):
            record = dict(row)
            document = json.loads(self.objects.read(
                record['manifest_path'], record['manifest_sha256']))
            if validator.document(document, record) == FORMAT_V1:
                return record
        return None

    def migrate(self, table_name=None, limit=None):
        if limit is not None and (type(limit) is not int or limit < 0):
            raise Invalid('migration limit must be a nonnegative integer')
        if table_name is not None:
            self.catalog.table(table_name)
            tables = [table_name]
        else:
            tables = [table['name'] for table in self.catalog.tables()]
        migrated = 0
        for name in tables:
            after = 0
            while limit is None or migrated < limit:
                with self.catalog.transaction(True) as connection:
                    record = self._next_v1(connection, name, after)
                    if record is None:
                        break
                    changed = self._migrate_one(connection, record)
                after = record['id']
                if changed:
                    migrated += 1
        if self.status()['complete']:
            with self.catalog.transaction(True) as connection:
                connection.execute('INSERT OR IGNORE INTO lake_format VALUES (?,?)',
                                   (FORMAT_V2, timestamp()))
        return {'migrated': migrated, 'tables': tables, 'status': self.status()}

    def _migrate_one(self, connection, record):
        validator = ManifestValidator()
        document = json.loads(self.objects.read(
            record['manifest_path'], record['manifest_sha256']))
        if validator.document(document, record) != FORMAT_V1:
            return None
        current_files = self.objects.manifest(record)['files']
        parent_record = None
        previous_files = None
        if record['parent_id'] is not None:
            parent = connection.execute(
                'SELECT * FROM snapshots WHERE table_name=? AND id=?',
                (record['table_name'], record['parent_id']),
            ).fetchone()
            if parent is None:
                raise Missing('snapshot parent is missing from the catalog')
            parent_record = dict(parent)
            previous_files = self.objects.manifest(parent_record)['files']
        extra = {key: value for key, value in document.items()
                 if key not in {'format_version', 'table', 'snapshot_id', 'schema_version',
                                'parent_id', 'operation', 'files'}}
        migrated = build_document(
            record['table_name'], record['id'], document['schema_version'],
            record['parent_id'], record['operation'], current_files,
            parent_record, previous_files, extra,
        )
        path, checksum = self.objects.write_manifest(record['table_name'], migrated)
        updated = connection.execute(
            'UPDATE snapshots SET manifest_path=?,manifest_sha256=? '
            'WHERE table_name=? AND id=? AND manifest_sha256=?',
            (path, checksum, record['table_name'], record['id'], record['manifest_sha256']),
        ).rowcount
        if updated != 1:
            raise Conflict('snapshot manifest changed during migration')
        connection.execute(
            'INSERT OR REPLACE INTO manifest_migrations VALUES (?,?,?,?,?,?,?)',
            (record['table_name'], record['id'], FORMAT_V1, FORMAT_V2,
             record['manifest_sha256'], checksum, timestamp()),
        )
        return path, checksum
