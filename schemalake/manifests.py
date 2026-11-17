from decimal import Decimal
from pathlib import PurePosixPath
import re

from .types import Conflict, Invalid, convert, name


class ManifestValidator:
    def integer(self, value, label, minimum=0):
        if type(value) is not int or value < minimum:
            raise Conflict(f'{label} must be an integer >= {minimum}')
        return value

    def checksum(self, value, label):
        if not isinstance(value, str) or not re.fullmatch('[0-9a-f]{64}', value):
            raise Conflict(f'{label} must be a lowercase SHA256 digest')
        return value

    def object_path(self, value, table_name, checksum, encoding):
        if not isinstance(value, str):
            raise Conflict('object path must be text')
        path = PurePosixPath(value)
        if path.is_absolute() or '..' in path.parts or '\\' in value:
            raise Conflict('object path must be a safe relative POSIX path')
        if len(path.parts) != 5 or path.parts[:3] != ('tables', table_name, 'data'):
            raise Conflict('object is outside its table data prefix')
        if not re.fullmatch('[0-9a-f]{24}', path.parts[3]):
            raise Conflict('invalid partition directory identity')
        suffix = '.jsonl.gz' if encoding == 'gzip-jsonl' else '.jsonl'
        if path.parts[4] != checksum + suffix:
            raise Conflict('object filename does not match content identity or encoding')
        return value

    def statistics(self, value, row_count):
        if not isinstance(value, dict) or not value:
            raise Conflict('file statistics must contain field entries')
        for column, item in value.items():
            try:
                name(column)
            except Invalid:
                raise Conflict('statistics contain invalid field name') from None
            if not isinstance(item, dict) or set(item) != {'null_count', 'min', 'max'}:
                raise Conflict('invalid field statistics shape')
            nulls = self.integer(item['null_count'], 'null_count')
            if nulls > row_count:
                raise Conflict('null_count exceeds object row count')
            if (item['min'] is None) != (item['max'] is None):
                raise Conflict('min and max must both be null or nonnull')
            if nulls == row_count and item['min'] is not None:
                raise Conflict('all-null field cannot have nonnull bounds')
            if nulls < row_count and item['min'] is None:
                raise Conflict('nonnull field values require statistics bounds')
        return value

    def file(self, table_name, value):
        if not isinstance(value, dict):
            raise Conflict('file descriptor must be an object')
        required = {'id', 'schema_version', 'partition', 'path', 'sha256', 'row_count', 'byte_count', 'statistics'}
        if not required <= set(value):
            raise Conflict('file descriptor is missing required fields')
        checksum = self.checksum(value['sha256'], 'file checksum')
        if value['id'] != checksum:
            raise Conflict('file id differs from checksum')
        self.integer(value['schema_version'], 'file schema version', 1)
        count = self.integer(value['row_count'], 'file row count', 1)
        self.integer(value['byte_count'], 'file byte count', 1)
        partition = value['partition']
        if not isinstance(partition, dict):
            raise Conflict('file partition must be an object')
        for column, item in partition.items():
            try:
                name(column)
            except Invalid:
                raise Conflict('invalid partition column name') from None
            if item is None or type(item) not in {str, int, bool}:
                raise Conflict('partition values must be nonnull typed scalar values')
        encoding = value.get('encoding', 'jsonl')
        if encoding not in {'jsonl', 'gzip-jsonl'}:
            raise Conflict('unknown file encoding')
        if encoding == 'gzip-jsonl':
            self.integer(value.get('decoded_byte_count'), 'decoded byte count', 1)
        self.object_path(value['path'], table_name, checksum, encoding)
        self.statistics(value['statistics'], count)
        return value

    def document(self, value, record):
        if not isinstance(value, dict):
            raise Conflict('snapshot manifest must be an object')
        required = {'table', 'snapshot_id', 'schema_version', 'parent_id', 'operation', 'files'}
        if not required <= set(value):
            raise Conflict('snapshot manifest is missing required fields')
        if value.get('format_version', 1) != 1:
            raise Conflict('unsupported snapshot manifest format')
        table_name = value['table']
        try:
            name(table_name)
        except Invalid:
            raise Conflict('invalid manifest table name') from None
        if table_name != record['table_name']:
            raise Conflict('manifest belongs to a different table')
        snapshot = self.integer(value['snapshot_id'], 'snapshot id', 1)
        if snapshot != record['id']:
            raise Conflict('manifest snapshot identity differs from catalog')
        parent = value['parent_id']
        if parent is not None:
            self.integer(parent, 'parent snapshot id', 1)
            if parent >= snapshot:
                raise Conflict('manifest parent must precede its child')
        if parent != record['parent_id']:
            raise Conflict('manifest parent differs from catalog')
        self.integer(value['schema_version'], 'snapshot schema version', 1)
        if value['operation'] != record['operation']:
            raise Conflict('manifest operation differs from catalog')
        if not isinstance(value['files'], list):
            raise Conflict('manifest files must be an array')
        for item in value['files']:
            self.file(table_name, item)
        total = sum(file['row_count'] for file in value['files'])
        if total != record['row_count']:
            raise Conflict('manifest row total differs from catalog')
        return value

    def schema_binding(self, schema, file, version):
        if file['schema_version'] != version:
            raise Conflict('baseline file and snapshot schema versions differ')
        fields = {field.name: field for field in schema.fields}
        if set(file['partition']) != set(schema.partition_by):
            raise Conflict('partition descriptor does not match schema')
        if set(file['statistics']) != set(fields):
            raise Conflict('statistics field set does not match schema')
        for column, value in file['partition'].items():
            try:
                normalized = convert(value, fields[column].type)
            except Invalid:
                raise Conflict('partition value violates schema') from None
            if normalized != value:
                raise Conflict('partition value is not canonical')
        for column, item in file['statistics'].items():
            if item['min'] is None:
                continue
            field = fields[column]
            try:
                low = convert(item['min'], field.type)
                high = convert(item['max'], field.type)
            except Invalid:
                raise Conflict('statistics bounds violate field type') from None
            if field.type == 'decimal':
                low, high = Decimal(low), Decimal(high)
            if low > high:
                raise Conflict('statistics lower bound exceeds upper bound')
        return file
