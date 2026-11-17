from pathlib import Path
import csv
import hashlib
import io
import json

from .types import Conflict, Invalid, convert, timestamp


class FileImports:
    def __init__(self, catalog, ingestor):
        self.catalog = catalog
        self.ingestor = ingestor

    def csv_value(self, value, field):
        if value == r'\N':
            if not field.nullable:
                raise Invalid('null in nonnullable CSV column')
            return None
        if field.type == 'string' and value.startswith('\\\\'):
            return value[1:]
        if field.type == 'int64':
            try:
                value = int(value)
            except ValueError:
                raise Invalid('invalid CSV integer') from None
        elif field.type == 'boolean':
            if value not in {'true', 'false'}:
                raise Invalid('CSV boolean must be true or false')
            value = value == 'true'
        return convert(value, field.type)

    def parse(self, table_name, raw, format):
        try:
            text = raw.decode('utf-8-sig')
        except UnicodeDecodeError:
            raise Invalid('input file must use UTF-8') from None
        if format == 'jsonl':
            rows = []
            for ordinal, line in enumerate(text.splitlines(), 1):
                if not line.strip():
                    continue
                try:
                    rows.append(json.loads(line))
                except json.JSONDecodeError:
                    raise Invalid(f'invalid JSON at input line {ordinal}') from None
            return rows
        if format != 'csv':
            raise Invalid('import format must be jsonl or csv')
        schema = self.catalog.schema(table_name)
        fields = {field.name: field for field in schema.fields}
        reader = csv.DictReader(io.StringIO(text, newline=''))
        if reader.fieldnames is None or len(reader.fieldnames) != len(set(reader.fieldnames)):
            raise Invalid('CSV needs unique column names')
        if set(reader.fieldnames) != set(fields):
            raise Invalid('CSV columns must match table schema')
        rows = []
        for ordinal, row in enumerate(reader, 2):
            if None in row or any(value is None for value in row.values()):
                raise Invalid(f'CSV width mismatch at record {ordinal}')
            try:
                rows.append({column: self.csv_value(value, fields[column]) for column, value in row.items()})
            except Invalid as error:
                raise Invalid(f'CSV record {ordinal}: {error}') from error
        return rows

    def load(self, table_name, import_id, path, format='jsonl', invalid='reject'):
        self.ingestor.validate_batch_id(import_id)
        path = Path(path)
        if not path.is_file() or path.stat().st_size > 64 * 1024 * 1024:
            raise Invalid('import requires a file of at most 64 MiB')
        raw = path.read_bytes()
        checksum = hashlib.sha256(raw).hexdigest()
        with self.catalog.transaction() as connection:
            previous = connection.execute('SELECT * FROM import_receipts WHERE table_name=? AND import_id=?',
                                          (table_name, import_id)).fetchone()
        if previous:
            if previous['file_sha256'] != checksum or previous['format'] != format:
                raise Conflict('import id reused for different file')
            return {**dict(previous), 'replayed': True}
        rows = self.parse(table_name, raw, format)
        result = self.ingestor.ingest(table_name, import_id, rows, invalid)
        with self.catalog.transaction(True) as connection:
            connection.execute('INSERT OR IGNORE INTO import_receipts VALUES (?,?,?,?,?,?)',
                               (table_name, import_id, checksum, format, result['snapshot_id'], timestamp()))
        return {**result, 'import_id': import_id, 'file_sha256': checksum}
