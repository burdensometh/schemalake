from collections import Counter
import hashlib
import json

from .types import Conflict, canonical


class Reconcile:
    def __init__(self, catalog, scanner):
        self.catalog = catalog
        self.scanner = scanner

    def fingerprint(self, rows):
        values = Counter(canonical(row) for row in rows)
        result = hashlib.sha256()
        for payload, count in sorted(values.items()):
            encoded = payload.encode('utf-8')
            result.update(str(len(encoded)).encode() + b':' + encoded + b':' + str(count).encode() + b'\n')
        return {'sha256': result.hexdigest(), 'rows': sum(values.values()), 'distinct_rows': len(values)}

    def against_mirror(self, mirror, table_name):
        connection = mirror.connect()
        try:
            connection.execute('BEGIN')
            head = connection.execute('SELECT * FROM heads WHERE table_name=?', (table_name,)).fetchone()
            if head is None:
                raise Conflict('mirror does not contain this table')
            rows = [json.loads(row[0]) for row in connection.execute(
                'SELECT payload FROM rows WHERE table_name=? ORDER BY ordinal', (table_name,),
            )]
            connection.commit()
        finally:
            connection.close()
        expected = self.fingerprint(self.scanner.rows(table_name, head['snapshot_id']))
        actual = self.fingerprint(rows)
        return {'table': table_name, 'snapshot_id': head['snapshot_id'], 'ok': expected == actual,
                'expected': expected, 'actual': actual}

    def against_jsonl(self, table_name, snapshot_id, path):
        with open(path, encoding='utf-8') as stream:
            rows = [json.loads(line) for line in stream if line.strip()]
        schema = self.catalog.schema(table_name, self.scanner.plan(table_name, snapshot_id)['schema_version'])
        actual = self.fingerprint(schema.normalize(row) for row in rows)
        expected = self.fingerprint(self.scanner.rows(table_name, snapshot_id))
        return {'table': table_name, 'snapshot_id': snapshot_id, 'ok': expected == actual,
                'expected': expected, 'actual': actual}

    def between_snapshots(self, table_name, first, second):
        left = self.fingerprint(self.scanner.rows(table_name, first))
        right = self.fingerprint(self.scanner.rows(table_name, second))
        return {'table': table_name, 'first': first, 'second': second, 'equivalent': left == right,
                'left': left, 'right': right}

    def committed_counts(self, table_name):
        plan = self.scanner.plan(table_name)
        with self.catalog.transaction() as connection:
            table = self.catalog.table(table_name, connection)
            receipts = [dict(row) for row in connection.execute('SELECT * FROM batches WHERE table_name=?', (table_name,))]
            rejected = connection.execute('SELECT count(*) FROM rejected_rows WHERE table_name=?', (table_name,)).fetchone()[0]
        return {'table': table_name, 'observed_head': plan['snapshot_id'], 'catalog_head': table['head'],
                'stable_observation': table['head'] == plan['snapshot_id'],
                'current_rows': plan['rows'], 'batch_receipts': len(receipts),
                'accepted_over_history': sum(row['row_count'] for row in receipts),
                'quarantined_rows': rejected}
