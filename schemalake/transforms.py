import json
import uuid

from .expressions import Expression
from .predicates import Predicate
from .types import Conflict, Invalid, Missing, canonical, name, timestamp


class Transforms:
    def __init__(self, catalog, scanner, maintenance):
        self.catalog = catalog
        self.scanner = scanner
        self.maintenance = maintenance

    def compile(self, source, target, document):
        if not isinstance(document, dict) or set(document) - {'select', 'where'}:
            raise Invalid('transform accepts select and optional where')
        fields = [field.name for field in source.fields]
        output = document.get('select')
        if not isinstance(output, dict) or set(output) != {f.name for f in target.fields}:
            raise Invalid('transform select must define all target columns')
        expressions = {column: Expression(expression, fields) for column, expression in output.items()}
        predicate = Predicate(source, document['where']) if 'where' in document else None
        return expressions, predicate

    def register(self, job_name, source_table, target_table, document):
        job_name = name(job_name)
        if source_table == target_table:
            raise Invalid('transform source and target must differ')
        source = self.catalog.schema(source_table)
        target = self.catalog.schema(target_table)
        self.compile(source, target, document)
        with self.catalog.transaction(True) as connection:
            existing = connection.execute('SELECT * FROM transform_jobs WHERE name=?', (job_name,)).fetchone()
            if existing:
                if (existing['source_table'], existing['target_table'], existing['document']) != (
                    source_table, target_table, canonical(document),
                ):
                    raise Conflict('transform name reused with different definition')
            else:
                connection.execute('INSERT INTO transform_jobs VALUES (?,?,?,?,?)',
                                   (job_name, source_table, target_table, canonical(document), timestamp()))
        return self.get(job_name)

    def get(self, job_name):
        with self.catalog.transaction() as connection:
            row = connection.execute('SELECT * FROM transform_jobs WHERE name=?', (job_name,)).fetchone()
            if row is None:
                raise Missing('transform job does not exist')
            result = dict(row)
            result['document'] = json.loads(result['document'])
            return result

    def preview(self, job_name, snapshot_id=None):
        job = self.get(job_name)
        plan = self.scanner.plan(job['source_table'], snapshot_id)
        source = self.catalog.schema(job['source_table'], plan['schema_version'])
        target = self.catalog.schema(job['target_table'])
        expressions, predicate = self.compile(source, target, job['document'])
        output = []
        input_rows = 0
        for row in self.scanner.rows(job['source_table'], snapshot_id):
            input_rows += 1
            if predicate is None or predicate.matches(row):
                values = {column: expression.evaluate(row) for column, expression in expressions.items()}
                output.append(target.normalize(values))
        return {'job': job, 'input_snapshot': plan['snapshot_id'], 'input_rows': input_rows, 'rows': output}

    def refresh(self, job_name, snapshot_id=None, expected_target=None):
        staged = self.preview(job_name, snapshot_id)
        job = staged['job']
        result = self.maintenance.replace(job['target_table'], staged['rows'], expected_head=expected_target)
        run_id = uuid.uuid4().hex
        with self.catalog.transaction(True) as connection:
            connection.execute('INSERT INTO transform_runs VALUES (?,?,?,?,?,?,?)',
                               (run_id, job_name, staged['input_snapshot'], result['snapshot_id'],
                                staged['input_rows'], len(staged['rows']), timestamp()))
        return {'run_id': run_id, 'job_name': job_name, 'input_snapshot': staged['input_snapshot'],
                'output_snapshot': result['snapshot_id'], 'input_rows': staged['input_rows'],
                'output_rows': len(staged['rows'])}

    def history(self, job_name):
        self.get(job_name)
        with self.catalog.transaction() as connection:
            return [dict(row) for row in connection.execute(
                'SELECT * FROM transform_runs WHERE job_name=? ORDER BY created_at', (job_name,),
            )]
