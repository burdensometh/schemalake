from .commit import SnapshotCommitter
from .types import Conflict, Invalid, convert
from .writer import DataWriter


class Maintenance:
    def __init__(self, catalog, objects):
        self.catalog = catalog
        self.objects = objects
        self.committer = SnapshotCommitter(catalog, objects)

    def partition_filter(self, schema, partition):
        fields = {field.name: field for field in schema.fields}
        if not isinstance(partition, dict) or set(partition) != set(schema.partition_by):
            raise Invalid('replacement requires all partition columns exactly')
        return {key: convert(value, fields[key].type) for key, value in partition.items()}

    def replace(self, table_name, rows, partition=None, expected_head=None, file_rows=1000):
        if not isinstance(rows, list):
            raise Invalid('replacement rows must be an array')
        with self.catalog.transaction(True) as connection:
            table = self.catalog.table(table_name, connection)
            if expected_head is not None and expected_head != table['head']:
                raise Conflict('replacement head changed')
            schema = self.catalog.schema(table_name, connection=connection)
            selected = None if partition is None else self.partition_filter(schema, partition)
            normalized = [schema.normalize(row) for row in rows]
            if selected is not None and any(schema.partition(row) != selected for row in normalized):
                raise Invalid('replacement row is outside selected partition')
            old_files = self.committer.current(connection, table_name)
            retained = [] if selected is None else [f for f in old_files if f['partition'] != selected]
            added = DataWriter(self.objects, file_rows).write(table_name, table['schema_version'], schema, normalized)
            files = retained + added
            self.committer.rebuild_keys(connection, table_name, files, 'replacement')
            snapshot = self.committer.commit(connection, table_name, 'replace', files, table['head'],
                                             {'partition': selected})
        return {'table': table_name, 'snapshot_id': snapshot, 'rows': sum(f['row_count'] for f in files)}

    def compact(self, table_name, partition=None, target_rows=10000, expected_head=None):
        writer = DataWriter(self.objects, target_rows)
        with self.catalog.transaction(True) as connection:
            table = self.catalog.table(table_name, connection)
            if expected_head is not None and expected_head != table['head']:
                raise Conflict('compaction head changed')
            schema = self.catalog.schema(table_name, connection=connection)
            selected = None if partition is None else self.partition_filter(schema, partition)
            files = self.committer.current(connection, table_name)
            chosen = [file for file in files if selected is None or file['partition'] == selected]
            retained = [file for file in files if file not in chosen]
            rows = []
            for file in chosen:
                rows.extend(self.objects.read_rows(file))
            added = writer.write(table_name, table['schema_version'], schema, rows)
            snapshot = self.committer.commit(connection, table_name, 'compact', retained + added,
                                             table['head'], {'replaced_files': len(chosen)})
        return {'table': table_name, 'snapshot_id': snapshot, 'files_before': len(chosen),
                'files_after': len(added), 'rows': len(rows)}

    def drop_partition(self, table_name, partition, expected_head=None):
        return self.replace(table_name, [], partition, expected_head)

    def partitions(self, table_name, snapshot_id=None):
        from .scan import Scanner
        plan = Scanner(self.catalog, self.objects).plan(table_name, snapshot_id)
        result = {}
        for file in plan['files']:
            key = tuple(sorted(file['partition'].items()))
            item = result.setdefault(key, {'partition': file['partition'], 'rows': 0, 'files': 0, 'bytes': 0})
            item['rows'] += file['row_count']
            item['files'] += 1
            item['bytes'] += file['byte_count']
        return list(result.values())
