from decimal import Decimal, localcontext
import json
import uuid

from .types import Conflict, Invalid, Missing, canonical, timestamp


class GroupPlan:
    def __init__(self, schema, document):
        if not isinstance(document, dict) or set(document) != {'group_by', 'aggregates'}:
            raise Invalid('aggregate plan requires group_by and aggregates')
        self.fields = {field.name: field for field in schema.fields}
        self.keys = document['group_by']
        if not isinstance(self.keys, list) or len(set(self.keys)) != len(self.keys) or set(self.keys) - set(self.fields):
            raise Invalid('invalid aggregate grouping columns')
        self.aggregates = document['aggregates']
        if not isinstance(self.aggregates, dict) or not self.aggregates or set(self.aggregates) & set(self.keys):
            raise Invalid('invalid aggregate output names')
        for output, rule in self.aggregates.items():
            if not isinstance(rule, dict) or rule.get('op') not in {'count', 'sum', 'mean', 'min', 'max'}:
                raise Invalid('unsupported aggregate operation')
            field = rule.get('field')
            if field not in self.fields:
                raise Invalid('aggregate references unknown field')
            if rule['op'] in {'sum', 'mean'} and self.fields[field].type not in {'int64', 'decimal'}:
                raise Invalid('numeric aggregation requires numeric field')

    def zero(self):
        return {output: {'count': 0, 'sum': Decimal(0), 'min': None, 'max': None}
                for output in self.aggregates}

    def add(self, state, row):
        for output, rule in self.aggregates.items():
            value = row[rule['field']]
            if value is None:
                continue
            item = state[output]
            item['count'] += 1
            type_name = self.fields[rule['field']].type
            comparison = Decimal(value) if type_name == 'decimal' else value
            if rule['op'] in {'sum', 'mean'}:
                with localcontext() as context:
                    context.prec = 80
                    item['sum'] += Decimal(value)
            if item['min'] is None or comparison < item['min']:
                item['min'] = comparison
            if item['max'] is None or comparison > item['max']:
                item['max'] = comparison

    def finish(self, state):
        result = {}
        for output, rule in self.aggregates.items():
            item = state[output]
            op = rule['op']
            if op == 'count':
                value = item['count']
            elif op == 'sum':
                value = item['sum']
                if self.fields[rule['field']].type == 'int64':
                    value = int(value)
            elif op == 'mean':
                with localcontext() as context:
                    context.prec = 38
                    value = item['sum'] / item['count'] if item['count'] else None
            else:
                value = item[op]
            result[output] = format(value, 'f') if isinstance(value, Decimal) else value
        return result

    def run(self, rows):
        groups = {}
        count = 0
        if not self.keys:
            groups['[]'] = ({}, self.zero())
        for row in rows:
            count += 1
            key = canonical([row[column] for column in self.keys])
            if key not in groups:
                groups[key] = ({column: row[column] for column in self.keys}, self.zero())
            self.add(groups[key][1], row)
        result = [{**keys, **self.finish(state)} for key, (keys, state) in sorted(groups.items())]
        return count, result


class AggregateJobs:
    def __init__(self, catalog, scanner, maintenance):
        self.catalog = catalog
        self.scanner = scanner
        self.maintenance = maintenance

    def register(self, job_name, source_table, target_table, document):
        from .types import name
        job_name = name(job_name)
        if source_table == target_table:
            raise Invalid('aggregate source and target must differ')
        source = self.catalog.schema(source_table)
        target = self.catalog.schema(target_table)
        plan = GroupPlan(source, document)
        if set(plan.keys) | set(plan.aggregates) != {field.name for field in target.fields}:
            raise Invalid('aggregate output does not match target schema')
        with self.catalog.transaction(True) as connection:
            old = connection.execute('SELECT * FROM aggregate_jobs WHERE name=?', (job_name,)).fetchone()
            if old:
                if (old['source_table'], old['target_table'], old['document']) != (source_table, target_table, canonical(document)):
                    raise Conflict('aggregate name reused for another plan')
            else:
                connection.execute('INSERT INTO aggregate_jobs VALUES (?,?,?,?,?)',
                                   (job_name, source_table, target_table, canonical(document), timestamp()))
        return self.get(job_name)

    def get(self, job_name):
        with self.catalog.transaction() as connection:
            row = connection.execute('SELECT * FROM aggregate_jobs WHERE name=?', (job_name,)).fetchone()
            if row is None:
                raise Missing('aggregate job does not exist')
            result = dict(row)
            result['document'] = json.loads(result['document'])
            return result

    def preview(self, job_name, snapshot_id=None):
        job = self.get(job_name)
        snapshot = self.scanner.plan(job['source_table'], snapshot_id)
        schema = self.catalog.schema(job['source_table'], snapshot['schema_version'])
        count, rows = GroupPlan(schema, job['document']).run(self.scanner.rows(job['source_table'], snapshot_id))
        target = self.catalog.schema(job['target_table'])
        return {'job': job, 'input_snapshot': snapshot['snapshot_id'], 'input_rows': count,
                'rows': [target.normalize(row) for row in rows]}

    def refresh(self, job_name, snapshot_id=None, expected_target=None):
        staged = self.preview(job_name, snapshot_id)
        result = self.maintenance.replace(staged['job']['target_table'], staged['rows'], expected_head=expected_target)
        run_id = uuid.uuid4().hex
        with self.catalog.transaction(True) as connection:
            connection.execute('INSERT INTO aggregate_runs VALUES (?,?,?,?,?,?,?)',
                               (run_id, job_name, staged['input_snapshot'], result['snapshot_id'], staged['input_rows'],
                                len(staged['rows']), timestamp()))
        return {'run_id': run_id, 'input_snapshot': staged['input_snapshot'], 'output_snapshot': result['snapshot_id'],
                'input_rows': staged['input_rows'], 'output_rows': len(staged['rows'])}
