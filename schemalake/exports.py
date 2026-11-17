from pathlib import Path
import csv
import hashlib
import json
import os
import tempfile
import uuid

from .types import Conflict, Invalid, Missing, canonical, timestamp


class Exports:
    def __init__(self, catalog, scanner, root):
        self.catalog = catalog
        self.scanner = scanner
        self.root = Path(root).resolve()
        self.root.mkdir(parents=True, exist_ok=True)

    def csv_value(self, value):
        if value is None:
            return r'\N'
        if type(value) is bool:
            return str(value).lower()
        if isinstance(value, str) and value.startswith('\\'):
            return '\\' + value
        return value

    def create(self, table_name, snapshot_id=None, format='jsonl', partition=None, expression=None):
        if format not in {'jsonl', 'csv'}:
            raise Invalid('unsupported export format')
        plan = self.scanner.plan(table_name, snapshot_id, partition)
        schema = self.catalog.schema(table_name, plan['schema_version'])
        export_id = uuid.uuid4().hex
        destination = self.root / (export_id + '.' + format)
        descriptor, temporary = tempfile.mkstemp(prefix='.export-', dir=self.root)
        count = 0
        try:
            with os.fdopen(descriptor, 'w', encoding='utf-8', newline='') as stream:
                writer = csv.DictWriter(stream, fieldnames=[field.name for field in schema.fields]) if format == 'csv' else None
                if writer:
                    writer.writeheader()
                if expression is None:
                    rows = self.scanner.rows(table_name, plan['snapshot_id'], partition)
                else:
                    rows = self.scanner.where(table_name, expression, plan['snapshot_id'], partition)
                for row in rows:
                    if writer:
                        writer.writerow({key: self.csv_value(value) for key, value in row.items()})
                    else:
                        stream.write(canonical(row) + '\n')
                    count += 1
                stream.flush()
                os.fsync(stream.fileno())
            checksum = hashlib.sha256(Path(temporary).read_bytes()).hexdigest()
            os.replace(temporary, destination)
            with self.catalog.transaction(True) as connection:
                connection.execute('INSERT INTO dataset_exports VALUES (?,?,?,?,?,?,?,?)',
                                   (export_id, table_name, plan['snapshot_id'], format, destination.name,
                                    checksum, count, timestamp()))
            return self.get(export_id)
        finally:
            if os.path.exists(temporary):
                os.unlink(temporary)

    def get(self, export_id):
        with self.catalog.transaction() as connection:
            row = connection.execute('SELECT * FROM dataset_exports WHERE id=?', (export_id,)).fetchone()
            if row is None:
                raise Missing('export does not exist')
            return dict(row)

    def artifact(self, export_id):
        item = self.get(export_id)
        path = (self.root / item['path']).resolve()
        if not path.is_relative_to(self.root) or not path.is_file():
            raise Missing('export object is missing')
        if hashlib.sha256(path.read_bytes()).hexdigest() != item['sha256']:
            raise Conflict('export checksum mismatch')
        return path

    def list(self, table_name):
        self.catalog.table(table_name)
        with self.catalog.transaction() as connection:
            return [dict(row) for row in connection.execute(
                'SELECT * FROM dataset_exports WHERE table_name=? ORDER BY created_at DESC', (table_name,),
            )]
