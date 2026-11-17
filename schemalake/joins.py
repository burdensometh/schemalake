from .types import Conflict, Invalid


class SnapshotJoin:
    def __init__(self, catalog, scanner):
        self.catalog = catalog
        self.scanner = scanner

    def join(self, left_table, right_table, on, how='inner', snapshots=None):
        if how not in {'inner', 'left'}:
            raise Invalid('join supports inner or left')
        if not isinstance(on, dict) or not on:
            raise Invalid('join requires left-to-right key mapping')
        snapshots = snapshots or {}
        with self.catalog.transaction() as connection:
            left_id = snapshots.get(left_table, self.catalog.table(left_table, connection)['head'])
            right_id = snapshots.get(right_table, self.catalog.table(right_table, connection)['head'])
        left_schema = self.catalog.schema(left_table)
        right_schema = self.catalog.schema(right_table)
        left_fields = {f.name: f.type for f in left_schema.fields}
        right_fields = {f.name: f.type for f in right_schema.fields}
        for left, right in on.items():
            if left not in left_fields or right not in right_fields or left_fields[left] != right_fields[right]:
                raise Invalid('join keys must be known columns with matching types')
        lookup = {}
        for row in self.scanner.rows(right_table, right_id):
            key = tuple(row[right] for right in on.values())
            if key in lookup:
                raise Conflict('right join key is not unique')
            lookup[key] = row
        output = []
        for row in self.scanner.rows(left_table, left_id):
            key = tuple(row[left] for left in on)
            match = lookup.get(key)
            if match is not None or how == 'left':
                output.append({'left': row, 'right': match})
        return {'snapshots': {left_table: left_id, right_table: right_id}, 'rows': output}
