from .expressions import Expression
from .types import Conflict, Invalid, canonical, timestamp
import uuid


DDL = """
CREATE TABLE IF NOT EXISTS enrichment_runs (
    run_id TEXT PRIMARY KEY,
    target_table TEXT NOT NULL,
    source_snapshots TEXT NOT NULL,
    definition TEXT NOT NULL,
    output_snapshot INTEGER NOT NULL,
    row_count INTEGER NOT NULL,
    created_at TEXT NOT NULL
);
"""


class Enrichment:
    def __init__(self, catalog, joins, maintenance):
        self.catalog = catalog
        self.joins = joins
        self.maintenance = maintenance
        with self.catalog.transaction(True) as connection:
            connection.executescript(DDL)

    def run(self, left_table, right_table, target_table, on, select, how='inner', snapshots=None, expected_target=None):
        if target_table in {left_table, right_table}:
            raise Invalid('enrichment target must differ from its sources')
        left = self.catalog.schema(left_table)
        right = self.catalog.schema(right_table)
        target = self.catalog.schema(target_table)
        columns = ['left_' + field.name for field in left.fields] + ['right_' + field.name for field in right.fields]
        if not isinstance(select, dict) or set(select) != {field.name for field in target.fields}:
            raise Invalid('enrichment select must match target schema')
        expressions = {column: Expression(spec, columns) for column, spec in select.items()}
        joined = self.joins.join(left_table, right_table, on, how, snapshots)
        rows = []
        for pair in joined['rows']:
            context = {'left_' + key: value for key, value in pair['left'].items()}
            context.update({'right_' + field.name: pair['right'][field.name] if pair['right'] else None for field in right.fields})
            rows.append(target.normalize({column: expression.evaluate(context) for column, expression in expressions.items()}))
        result = self.maintenance.replace(target_table, rows, expected_head=expected_target)
        run_id = uuid.uuid4().hex
        definition = {'on': on, 'select': select, 'how': how}
        with self.catalog.transaction(True) as connection:
            connection.execute('INSERT INTO enrichment_runs VALUES (?,?,?,?,?,?,?)',
                               (run_id, target_table, canonical(joined['snapshots']), canonical(definition),
                                result['snapshot_id'], len(rows), timestamp()))
        return {'run_id': run_id, 'source_snapshots': joined['snapshots'], 'target_snapshot': result['snapshot_id'],
                'rows': len(rows)}

    def history(self, target_table):
        import json
        self.catalog.table(target_table)
        with self.catalog.transaction() as connection:
            result = []
            for row in connection.execute('SELECT * FROM enrichment_runs WHERE target_table=? ORDER BY created_at', (target_table,)):
                item = dict(row)
                item['source_snapshots'] = json.loads(item['source_snapshots'])
                item['definition'] = json.loads(item['definition'])
                result.append(item)
            return result
