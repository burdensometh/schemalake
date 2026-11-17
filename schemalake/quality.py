import uuid

from .predicates import Predicate
from .types import Invalid, canonical, timestamp


class Quality:
    def __init__(self, catalog, scanner):
        self.catalog = catalog
        self.scanner = scanner

    def evaluate(self, table_name, rules, snapshot_id=None):
        plan = self.scanner.plan(table_name, snapshot_id)
        schema = self.catalog.schema(table_name, plan['schema_version'])
        fields = {field.name for field in schema.fields}
        if not isinstance(rules, list) or not rules:
            raise Invalid('quality rules must be a nonempty array')
        compiled = []
        for ordinal, rule in enumerate(rules):
            if not isinstance(rule, dict):
                raise Invalid('quality rule must be an object')
            kind = rule.get('kind')
            if kind == 'unique':
                columns = rule.get('columns')
                if not isinstance(columns, list) or not columns or set(columns) - fields:
                    raise Invalid('unique rule needs known columns')
                compiled.append((ordinal, kind, tuple(columns), set()))
            elif kind == 'check':
                compiled.append((ordinal, kind, Predicate(schema, rule.get('where')), None))
            else:
                raise Invalid('quality kind must be unique or check')
        violations = [0] * len(compiled)
        samples = [[] for _ in compiled]
        count = 0
        for row_index, row in enumerate(self.scanner.rows(table_name, plan['snapshot_id'])):
            count += 1
            for ordinal, kind, check, seen in compiled:
                if kind == 'unique':
                    key = canonical([row[column] for column in check])
                    invalid = key in seen
                    seen.add(key)
                else:
                    invalid = not check.matches(row)
                if invalid:
                    violations[ordinal] += 1
                    if len(samples[ordinal]) < 5:
                        samples[ordinal].append(row_index)
        outcome = {'table': table_name, 'snapshot_id': plan['snapshot_id'], 'rows': count,
                   'ok': not any(violations),
                   'rules': [{'ordinal': i, 'violations': value, 'sample_row_ordinals': samples[i]}
                             for i, value in enumerate(violations)]}
        run_id = uuid.uuid4().hex
        with self.catalog.transaction(True) as connection:
            connection.execute('INSERT INTO quality_runs VALUES (?,?,?,?,?,?)',
                               (run_id, table_name, plan['snapshot_id'], canonical(rules), canonical(outcome), timestamp()))
        return {**outcome, 'run_id': run_id}

    def history(self, table_name):
        import json
        self.catalog.table(table_name)
        with self.catalog.transaction() as connection:
            rows = connection.execute('SELECT * FROM quality_runs WHERE table_name=? ORDER BY created_at', (table_name,))
            result = []
            for row in rows:
                value = dict(row)
                value['specification'] = json.loads(value['specification'])
                value['outcome'] = json.loads(value['outcome'])
                result.append(value)
            return result
