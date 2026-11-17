import json

from .types import Conflict, Invalid, canonical, convert, timestamp


class KeyIndexes:
    def __init__(self, catalog, objects, scanner):
        self.catalog = catalog
        self.objects = objects
        self.scanner = scanner

    def build(self, table_name, snapshot_id=None):
        plan = self.scanner.plan(table_name, snapshot_id)
        schema = self.catalog.schema(table_name, plan['schema_version'])
        if not schema.primary_key:
            raise Invalid('lookup index requires a primary key')
        entries = {}
        for file in plan['files']:
            for ordinal, raw in enumerate(self.objects.read_rows(file)):
                row = schema.normalize(raw)
                key = schema.key(row)
                if key in entries:
                    raise Conflict('snapshot contains duplicate key')
                entries[key] = (canonical(file), ordinal)
        with self.catalog.transaction(True) as connection:
            head = self.catalog.table(table_name, connection)['head']
            connection.execute('DELETE FROM lookup_indexes WHERE table_name=?', (table_name,))
            connection.execute('INSERT INTO lookup_indexes VALUES (?,?,?,?,?)',
                               (table_name, plan['snapshot_id'], canonical(list(schema.primary_key)), len(entries), timestamp()))
            connection.commit()
            connection.executemany('INSERT INTO lookup_entries VALUES (?,?,?,?)',
                                   ((table_name, key, file, ordinal) for key, (file, ordinal) in entries.items()))
            self.catalog.event(connection, table_name, 'index_built', {'snapshot_id': plan['snapshot_id']})
        return {'table': table_name, 'snapshot_id': plan['snapshot_id'], 'entries': len(entries)}

    def normalized_key(self, schema, values):
        if not isinstance(values, dict) or set(values) != set(schema.primary_key):
            raise Invalid('lookup needs every primary-key column exactly')
        fields = {field.name: field for field in schema.fields}
        return canonical([convert(values[key], fields[key].type) for key in schema.primary_key])

    def lookup(self, table_name, values, snapshot_id=None):
        plan = self.scanner.plan(table_name, snapshot_id)
        schema = self.catalog.schema(table_name, plan['schema_version'])
        if not schema.primary_key:
            raise Invalid('lookup requires a primary key')
        key = self.normalized_key(schema, values)
        with self.catalog.transaction() as connection:
            index = connection.execute('SELECT * FROM lookup_indexes WHERE table_name=?', (table_name,)).fetchone()
            if index:
                entry = connection.execute('SELECT * FROM lookup_entries WHERE table_name=? AND row_key=?',
                                           (table_name, key)).fetchone()
                if entry is None:
                    return {'snapshot_id': plan['snapshot_id'], 'used_index': True, 'rows': []}
                file = json.loads(entry['file_json'])
                ordinal = entry['ordinal']
            else:
                file = None
        if file is None:
            rows = [row for row in self.scanner.rows(table_name, plan['snapshot_id']) if schema.key(row) == key]
            return {'snapshot_id': plan['snapshot_id'], 'used_index': False, 'rows': rows}
        rows = list(self.objects.read_rows(file))
        if not 0 <= ordinal < len(rows):
            raise Conflict('lookup index position is outside object')
        row = schema.normalize(rows[ordinal])
        if schema.key(row) != key:
            raise Conflict('lookup index points to a different key')
        return {'snapshot_id': plan['snapshot_id'], 'used_index': True, 'rows': [row]}

    def status(self, table_name):
        with self.catalog.transaction() as connection:
            table = self.catalog.table(table_name, connection)
            row = connection.execute('SELECT * FROM lookup_indexes WHERE table_name=?', (table_name,)).fetchone()
            return {'table': table_name, 'head': table['head'], 'index': dict(row) if row else None,
                    'usable': bool(row and row['snapshot_id'] == table['head'])}

    def range(self, table_name, lower=None, upper=None, snapshot_id=None, include_upper=False):
        from decimal import Decimal
        if type(include_upper) is not bool:
            raise Invalid('include_upper must be boolean')
        plan = self.scanner.plan(table_name, snapshot_id)
        schema = self.catalog.schema(table_name, plan['schema_version'])
        if not schema.primary_key:
            raise Invalid('range lookup requires primary key')
        fields = {field.name: field for field in schema.fields}
        def typed(key):
            return tuple(Decimal(value) if fields[column].type == 'decimal' else value
                         for column, value in zip(schema.primary_key, json.loads(key)))
        low = typed(self.normalized_key(schema, lower)) if lower is not None else None
        high = typed(self.normalized_key(schema, upper)) if upper is not None else None
        if low is not None and high is not None and low > high:
            raise Invalid('range lower bound exceeds upper bound')
        def accepts(key):
            return (low is None or key >= low) and (high is None or key < high or (include_upper and key == high))
        with self.catalog.transaction() as connection:
            index = connection.execute('SELECT snapshot_id FROM lookup_indexes WHERE table_name=?', (table_name,)).fetchone()
            usable = bool(index)
            entries = list(connection.execute('SELECT * FROM lookup_entries WHERE table_name=?', (table_name,))) if usable else []
        rows = []
        if usable:
            file_cache = {}
            for entry in entries:
                key = typed(entry['row_key'])
                if not accepts(key):
                    continue
                file = json.loads(entry['file_json'])
                identity = (file['path'], file['sha256'])
                if identity not in file_cache:
                    file_cache[identity] = list(self.objects.read_rows(file))
                values = file_cache[identity]
                if not 0 <= entry['ordinal'] < len(values):
                    raise Conflict('range index position is invalid')
                row = schema.normalize(values[entry['ordinal']])
                if schema.key(row) != entry['row_key']:
                    raise Conflict('range index key is invalid')
                rows.append((key, row))
        else:
            for row in self.scanner.rows(table_name, plan['snapshot_id']):
                key = typed(schema.key(row))
                if accepts(key):
                    rows.append((key, row))
        return {'snapshot_id': plan['snapshot_id'], 'used_index': usable,
                'rows': [row for key, row in sorted(rows, key=lambda item: item[0])]}

    def drop(self, table_name):
        with self.catalog.transaction(True) as connection:
            self.catalog.table(table_name, connection)
            connection.execute('DELETE FROM lookup_indexes WHERE table_name=?', (table_name,))
