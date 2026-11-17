from decimal import Decimal, localcontext
import json
import uuid

from .types import Conflict, Invalid, Missing, canonical, timestamp
from .writer import DataWriter


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

    def merge(self, state, incoming):
        for output in self.aggregates:
            item = state[output]
            other = incoming[output]
            item['count'] += other['count']
            with localcontext() as context:
                context.prec = 80
                item['sum'] += other['sum']
            for bound in ('min', 'max'):
                candidate = other[bound]
                if candidate is None:
                    continue
                if item[bound] is None or (candidate < item[bound] if bound == 'min' else candidate > item[bound]):
                    item[bound] = candidate

    def encode_state(self, state):
        document = {}
        for output, rule in self.aggregates.items():
            item = state[output]
            decimal = self.fields[rule['field']].type == 'decimal'
            document[output] = {
                'count': item['count'],
                'sum': format(item['sum'], 'f'),
                'min': format(item['min'], 'f') if decimal and item['min'] is not None else item['min'],
                'max': format(item['max'], 'f') if decimal and item['max'] is not None else item['max'],
            }
        return document

    def decode_state(self, document):
        state = {}
        for output, rule in self.aggregates.items():
            item = document[output]
            decimal = self.fields[rule['field']].type == 'decimal'
            state[output] = {
                'count': item['count'],
                'sum': Decimal(item['sum']),
                'min': Decimal(item['min']) if decimal and item['min'] is not None else item['min'],
                'max': Decimal(item['max']) if decimal and item['max'] is not None else item['max'],
            }
        return state

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
        count, rows = GroupPlan(schema, job['document']).run(self.scanner.rows(job['source_table'], snapshot['snapshot_id']))
        target = self.catalog.schema(job['target_table'])
        return {'job': job, 'input_snapshot': snapshot['snapshot_id'], 'input_rows': count,
                'rows': [target.normalize(row) for row in rows]}

    def snapshot_document(self, connection, table_name, snapshot_id, schema_version):
        if snapshot_id == 0:
            return {'schema_version': schema_version, 'files': []}
        record = connection.execute('SELECT * FROM snapshots WHERE table_name=? AND id=?',
                                    (table_name, snapshot_id)).fetchone()
        if record is None:
            raise Missing('snapshot does not exist')
        return self.scanner.objects.manifest(dict(record))

    @staticmethod
    def partition_files(files):
        result = {}
        for file in files:
            result.setdefault(canonical(file['partition']), []).append(file)
        return result

    def refresh(self, job_name, snapshot_id=None, expected_target=None):
        job = self.get(job_name)
        source_schema = self.catalog.schema(job['source_table'])
        plan = GroupPlan(source_schema, job['document'])
        target_schema = self.catalog.schema(job['target_table'])
        objects = self.scanner.objects
        with self.catalog.transaction(True) as connection:
            target = self.catalog.table(job['target_table'], connection)
            if expected_target is not None and expected_target != target['head']:
                raise Conflict('aggregate target head changed')
            source = self.catalog.table(job['source_table'], connection)
            input_snapshot = source['head'] if snapshot_id is None else snapshot_id
            if type(input_snapshot) is not int or input_snapshot < 0:
                raise Invalid('snapshot id must be a nonnegative integer')
            watermark = connection.execute(
                'SELECT * FROM aggregate_watermarks WHERE job_name=?', (job_name,)).fetchone()
            processed = watermark['source_snapshot'] if watermark else 0
            if input_snapshot < processed:
                raise Conflict('aggregate input snapshot is older than the processed watermark')
            if watermark and input_snapshot == processed:
                groups = connection.execute(
                    'SELECT count(DISTINCT group_json) FROM aggregate_contributions WHERE job_name=?',
                    (job_name,)).fetchone()[0]
                return {'run_id': None, 'job_name': job_name, 'input_snapshot': input_snapshot,
                        'output_snapshot': target['head'], 'input_rows': 0, 'output_rows': groups,
                        'partitions_changed': 0, 'replayed': True}
            before = self.partition_files(
                self.snapshot_document(connection, job['source_table'], processed, source['schema_version'])['files'])
            current = self.snapshot_document(connection, job['source_table'], input_snapshot, source['schema_version'])
            after = self.partition_files(current['files'])
            changed = sorted(key for key in set(before) | set(after)
                             if {file['id'] for file in before.get(key, [])}
                             != {file['id'] for file in after.get(key, [])})
            input_rows = 0
            for key in changed:
                connection.execute('DELETE FROM aggregate_contributions WHERE job_name=? AND partition_json=?',
                                   (job_name, key))
                states = {}
                for file in after.get(key, []):
                    if file['schema_version'] != current['schema_version']:
                        raise Conflict('baseline snapshots require one schema version')
                    for raw in objects.read_rows(file):
                        row = source_schema.normalize(raw)
                        if source_schema.partition(row) != file['partition']:
                            raise Conflict('row does not belong to its declared partition')
                        input_rows += 1
                        group = canonical([row[column] for column in plan.keys])
                        if group not in states:
                            states[group] = plan.zero()
                        plan.add(states[group], row)
                for group, state in states.items():
                    connection.execute('INSERT INTO aggregate_contributions VALUES (?,?,?,?)',
                                       (job_name, key, group, canonical(plan.encode_state(state))))
            merged = {}
            for record in connection.execute(
                    'SELECT group_json, state_json FROM aggregate_contributions WHERE job_name=?', (job_name,)):
                group = record['group_json']
                if group not in merged:
                    merged[group] = plan.zero()
                plan.merge(merged[group], plan.decode_state(json.loads(record['state_json'])))
            if not plan.keys and not merged:
                merged['[]'] = plan.zero()
            rows = [target_schema.normalize({**dict(zip(plan.keys, json.loads(group))), **plan.finish(state)})
                    for group, state in sorted(merged.items())]
            added = DataWriter(objects).write(job['target_table'], target['schema_version'], target_schema, rows)
            committer = self.maintenance.committer
            committer.rebuild_keys(connection, job['target_table'], added, f'aggregate:{job_name}')
            output_snapshot = committer.commit(
                connection, job['target_table'], 'replace', added, target['head'],
                {'job': job_name, 'input_snapshot': input_snapshot, 'partitions_changed': len(changed)})
            run_id = uuid.uuid4().hex
            connection.execute('INSERT INTO aggregate_runs VALUES (?,?,?,?,?,?,?)',
                               (run_id, job_name, input_snapshot, output_snapshot, input_rows, len(rows), timestamp()))
            connection.execute(
                'INSERT INTO aggregate_watermarks VALUES (?,?,?,?) ON CONFLICT(job_name) DO UPDATE SET '
                'source_snapshot=excluded.source_snapshot, target_snapshot=excluded.target_snapshot, '
                'updated_at=excluded.updated_at',
                (job_name, input_snapshot, output_snapshot, timestamp()))
            return {'run_id': run_id, 'job_name': job_name, 'input_snapshot': input_snapshot,
                    'output_snapshot': output_snapshot, 'input_rows': input_rows, 'output_rows': len(rows),
                    'partitions_changed': len(changed), 'replayed': False}

    def status(self, job_name):
        job = self.get(job_name)
        with self.catalog.transaction() as connection:
            watermark = connection.execute(
                'SELECT * FROM aggregate_watermarks WHERE job_name=?', (job_name,)).fetchone()
            counts = connection.execute(
                'SELECT count(*) AS states, count(DISTINCT partition_json) AS partitions, '
                'count(DISTINCT group_json) AS groups FROM aggregate_contributions WHERE job_name=?',
                (job_name,)).fetchone()
            source = self.catalog.table(job['source_table'], connection)
            target = self.catalog.table(job['target_table'], connection)
        processed = watermark['source_snapshot'] if watermark else 0
        return {'job': job['name'], 'source_table': job['source_table'], 'target_table': job['target_table'],
                'processed_snapshot': processed, 'source_head': source['head'], 'pending': source['head'] > processed,
                'target_snapshot': watermark['target_snapshot'] if watermark else 0, 'target_head': target['head'],
                'partitions': counts['partitions'], 'groups': counts['groups'], 'states': counts['states']}

    def history(self, job_name):
        self.get(job_name)
        with self.catalog.transaction() as connection:
            return [dict(row) for row in connection.execute(
                'SELECT * FROM aggregate_runs WHERE job_name=? ORDER BY created_at', (job_name,))]
