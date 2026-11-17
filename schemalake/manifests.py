from bisect import bisect_left
from decimal import Decimal
from pathlib import PurePosixPath
import re

from .types import Conflict, Invalid, canonical, convert, name


FORMAT_V1 = 1
FORMAT_V2 = 2
SUPPORTED = {FORMAT_V1, FORMAT_V2}
CORE_KEYS = {'format_version', 'table', 'snapshot_id', 'schema_version', 'parent_id', 'operation'}
DELTA_KEYS = {'files', 'base', 'removed'}


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

    def manifest_path(self, value, table_name):
        if not isinstance(value, str):
            raise Conflict('manifest path must be text')
        path = PurePosixPath(value)
        if path.is_absolute() or '..' in path.parts or '\\' in value:
            raise Conflict('manifest path must be a safe relative POSIX path')
        if len(path.parts) != 4 or path.parts[:2] != ('tables', table_name) or path.parts[2] != 'manifests':
            raise Conflict('manifest is outside its table manifest prefix')
        if not re.fullmatch('[0-9a-f]{64}\\.json', path.parts[3]):
            raise Conflict('manifest filename must be a content identity')
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

    def format_version(self, value):
        if not isinstance(value, dict):
            raise Conflict('snapshot manifest must be an object')
        version = value.get('format_version', FORMAT_V1)
        if version not in SUPPORTED:
            raise Conflict('unsupported snapshot manifest format')
        return version

    def common(self, value, record):
        version = self.format_version(value)
        required = {'table', 'snapshot_id', 'schema_version', 'parent_id', 'operation', 'files'}
        if not required <= set(value):
            raise Conflict('snapshot manifest is missing required fields')
        table_name = value['table']
        try:
            name(table_name)
        except Invalid:
            raise Conflict('invalid manifest table name') from None
        if table_name != record['table_name']:
            raise Conflict('manifest belongs to a different table')
        if self.integer(value['snapshot_id'], 'snapshot id', 1) != record['id']:
            raise Conflict('manifest snapshot identity differs from catalog')
        parent = value['parent_id']
        if parent is not None:
            self.integer(parent, 'parent snapshot id', 1)
            if parent >= value['snapshot_id']:
                raise Conflict('manifest parent must precede its child')
        if parent != record['parent_id']:
            raise Conflict('manifest parent differs from catalog')
        self.integer(value['schema_version'], 'snapshot schema version', 1)
        if value['operation'] != record['operation']:
            raise Conflict('manifest operation differs from catalog')
        return version, table_name

    def base(self, value, table_name, snapshot, parent):
        base = value.get('base')
        if base is None:
            return None
        if not isinstance(base, dict) or set(base) != {'snapshot_id', 'manifest_path', 'manifest_sha256'}:
            raise Conflict('manifest base reference has an invalid shape')
        base_id = self.integer(base['snapshot_id'], 'base snapshot id', 1)
        if parent is None or base_id != parent:
            raise Conflict('manifest base must be its parent snapshot')
        self.manifest_path(base['manifest_path'], table_name)
        checksum = self.checksum(base['manifest_sha256'], 'base manifest checksum')
        if PurePosixPath(base['manifest_path']).parts[3] != checksum + '.json':
            raise Conflict('base manifest path does not match its content identity')
        return base

    def removals(self, value):
        removed = value.get('removed')
        if removed is None:
            return {}
        if not isinstance(removed, dict):
            raise Conflict('manifest removed files must be an object')
        for checksum, count in removed.items():
            self.checksum(checksum, 'removed file id')
            self.integer(count, 'removed file count', 1)
        return removed

    def fragment(self, value, table_name, snapshot_id):
        """Validate a manifest reached through a base reference.

        A base reference names an immutable document; it is not required to be
        the manifest the catalog currently associates with its snapshot, so
        only the document's own identity and structure are checked here.
        """
        version = self.format_version(value)
        required = {'table', 'snapshot_id', 'schema_version', 'parent_id', 'operation', 'files'}
        if not required <= set(value):
            raise Conflict('snapshot manifest is missing required fields')
        if value['table'] != table_name:
            raise Conflict('manifest base belongs to a different table')
        if self.integer(value['snapshot_id'], 'base snapshot id', 1) != snapshot_id:
            raise Conflict('manifest base reference resolves to another snapshot')
        parent = value['parent_id']
        if parent is not None:
            self.integer(parent, 'parent snapshot id', 1)
            if parent >= snapshot_id:
                raise Conflict('manifest parent must precede its child')
        self.integer(value['schema_version'], 'snapshot schema version', 1)
        if not isinstance(value['operation'], str) or not value['operation']:
            raise Conflict('manifest operation must be a nonempty string')
        if version == FORMAT_V2:
            self.base(value, table_name, snapshot_id, parent)
            self.removals(value)
        self.file_list(value, table_name)
        return version

    def file_list(self, value, table_name):
        if not isinstance(value['files'], list):
            raise Conflict('manifest files must be an array')
        for item in value['files']:
            self.file(table_name, item)
        return value['files']

    def document(self, value, record):
        version, table_name = self.common(value, record)
        if version == FORMAT_V1:
            files = self.file_list(value, table_name)
            if sum(file['row_count'] for file in files) != record['row_count']:
                raise Conflict('manifest row total differs from catalog')
            return version
        self.base(value, table_name, value['snapshot_id'], value['parent_id'])
        self.removals(value)
        self.file_list(value, table_name)
        return version

    def resolved(self, value, record):
        version, table_name = self.common(value, record)
        files = self.file_list(value, table_name)
        if sum(file['row_count'] for file in files) != record['row_count']:
            raise Conflict('manifest row total differs from catalog')
        return version

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


def reference(record):
    return {'snapshot_id': record['id'],
            'manifest_path': record['manifest_path'],
            'manifest_sha256': record['manifest_sha256']}


def derive_delta(previous, current):
    """Return (removed multiset, added suffix) or ``None`` when not expressible.

    Real commits present a new file list as the surviving prefix of the
    previous list followed by new descriptors.  The longest such prefix is
    matched as an ordered subsequence; every unmatched previous descriptor is
    removed by file id and the remaining current suffix is added.  Repeated
    references to one object are preserved.  A pair that cannot be expressed
    this way falls back to a self-contained list instead of guessing.
    """
    positions = {}
    for index, file in enumerate(previous):
        positions.setdefault(canonical(file), []).append(index)
    used = []
    cursor = 0
    matched = 0
    for file in current:
        bucket = positions.get(canonical(file), [])
        offset = bisect_left(bucket, cursor)
        if offset == len(bucket):
            break
        cursor = bucket[offset] + 1
        used.append(bucket[offset])
        matched += 1
    used_set = set(used)
    removed = {}
    for index, file in enumerate(previous):
        if index not in used_set:
            removed[file['id']] = removed.get(file['id'], 0) + 1
    added = list(current[matched:])
    try:
        replay = apply_delta(previous, removed, added)
    except Conflict:
        return None
    if len(replay) != len(current) or any(
        canonical(left) != canonical(right) for left, right in zip(replay, current)
    ):
        return None
    return removed, added


def apply_delta(previous, removed, added):
    """Remove the trailing occurrences named by ``removed`` and append ``added``.

    ``derive_delta`` retains the earliest occurrence of a repeated descriptor,
    so the removed copies are the trailing ones; deleting from the end leaves
    the order of surviving descriptors unchanged.
    """
    remaining = dict(removed or {})
    survivors = []
    for file in reversed(previous):
        count = remaining.get(file['id'], 0)
        if count:
            remaining[file['id']] = count - 1
        else:
            survivors.append(file)
    if any(remaining.values()):
        raise Conflict('manifest removes more file copies than its base holds')
    survivors.reverse()
    survivors.extend(added)
    return survivors


def build_document(table_name, snapshot_id, schema_version, parent_id, operation,
                   current_files, previous_record=None, previous_files=None, more=None):
    document = {
        'format_version': FORMAT_V2,
        'table': table_name,
        'snapshot_id': snapshot_id,
        'schema_version': schema_version,
        'parent_id': parent_id,
        'operation': operation,
    }
    for key, value in (more or {}).items():
        if key not in CORE_KEYS and key not in DELTA_KEYS:
            document[key] = value
    delta = None
    if previous_record is not None and previous_files is not None:
        delta = derive_delta(previous_files, list(current_files))
    if delta is None:
        document['base'] = None
        document['removed'] = {}
        document['files'] = list(current_files)
    else:
        document['base'] = reference(previous_record)
        document['removed'], document['files'] = delta
    return document
