import json

from .types import Conflict, Invalid, Missing


class Provenance:
    def __init__(self, catalog, objects, retention=None):
        self.catalog = catalog
        self.objects = objects
        self.retention = retention

    def lineage(self, table_name, snapshot_id=None):
        result = []
        with self.catalog.transaction() as connection:
            table = self.catalog.table(table_name, connection)
            current = table['head'] if snapshot_id is None else snapshot_id
            if type(current) is not int or current < 0:
                raise Invalid('snapshot id must be a nonnegative integer')
            if current and self.retention is not None:
                self.retention.require_readable(table_name, current, connection)
            visited = set()
            while current:
                if current in visited:
                    raise Conflict('snapshot parent chain contains a cycle')
                visited.add(current)
                row = connection.execute('SELECT * FROM snapshots WHERE table_name=? AND id=?', (table_name, current)).fetchone()
                if row is None:
                    raise Missing('snapshot lineage points to an absent snapshot')
                item = dict(row)
                if self.retention is not None:
                    item['expired'] = self.retention.is_expired(table_name, current, connection)
                result.append(item)
                current = item['parent_id']
        return result

    def snapshot(self, table_name, snapshot_id=None):
        chain = self.lineage(table_name, snapshot_id)
        if not chain:
            return {'table': table_name, 'snapshot_id': 0, 'parents': [], 'files': [], 'input_receipts': []}
        record = chain[0]
        document = self.objects.manifest(record)
        with self.catalog.transaction() as connection:
            batches = [dict(row) for row in connection.execute(
                'SELECT * FROM batches WHERE table_name=? AND snapshot_id=?', (table_name, record['id']),
            )]
            staged = [dict(row) for row in connection.execute(
                'SELECT table_name,batch_id,input_sha256,snapshot_id FROM prepared_batches '
                'WHERE table_name=? AND snapshot_id=? AND status=?', (table_name, record['id'], 'committed'),
            )]
            derived = []
            for run_table, job_table in [('transform_runs', 'transform_jobs'), ('aggregate_runs', 'aggregate_jobs')]:
                rows = connection.execute(
                    f'SELECT r.*,j.source_table FROM {run_table} r JOIN {job_table} j ON r.job_name=j.name '
                    'WHERE j.target_table=? AND r.output_snapshot=?', (table_name, record['id']),
                )
                derived.extend(dict(row) for row in rows)
        return {'table': table_name, 'snapshot_id': record['id'], 'parents': [item['id'] for item in chain[1:]],
                'operation': record['operation'], 'manifest_path': record['manifest_path'],
                'manifest_sha256': record['manifest_sha256'], 'files': document['files'],
                'input_receipts': batches + staged, 'derived_inputs': derived}

    def object_references(self, relative_path):
        references = []
        for table in self.catalog.tables():
            for snapshot in self.catalog.snapshots(table['name']):
                if self.retention is not None and self.retention.is_expired(table['name'], snapshot['id']):
                    continue
                document = self.objects.manifest(snapshot)
                count = sum(file['path'] == relative_path for file in document['files'])
                if count:
                    references.append({'table': table['name'], 'snapshot_id': snapshot['id'], 'occurrences': count})
        return {'path': relative_path, 'references': references,
                'retained': bool(references), 'physical_exists': self.objects.path(relative_path).is_file()}

    def batch(self, table_name, batch_id):
        receipt = self.catalog.batch(table_name, batch_id)
        if receipt is None:
            raise Missing('batch receipt does not exist')
        with self.catalog.transaction() as connection:
            history = []
            for row in connection.execute('SELECT * FROM history WHERE table_name=? ORDER BY sequence', (table_name,)):
                value = dict(row)
                value['detail'] = json.loads(value['detail'])
                if value['detail'].get('batch_id') == batch_id:
                    history.append(value)
        return {'receipt': receipt, 'rejected_rows': self.catalog.rejects(table_name, batch_id), 'history': history}
