from collections import Counter, defaultdict

from .types import Invalid, canonical


class Plans:
    def __init__(self, catalog, scanner):
        self.catalog = catalog
        self.scanner = scanner

    def file_identity(self, file):
        return file['path'], file['sha256']

    def diff(self, table_name, before, after):
        old = self.scanner.plan(table_name, before)
        new = self.scanner.plan(table_name, after)
        old_counts = Counter(self.file_identity(file) for file in old['files'])
        new_counts = Counter(self.file_identity(file) for file in new['files'])
        old_items = {self.file_identity(file): file for file in old['files']}
        new_items = {self.file_identity(file): file for file in new['files']}
        removed = [old_items[key] for key, n in (old_counts - new_counts).items() for _ in range(n)]
        added = [new_items[key] for key, n in (new_counts - old_counts).items() for _ in range(n)]
        partitions = {}
        for file in removed + added:
            partitions[canonical(file['partition'])] = file['partition']
        return {'table': table_name, 'before': before, 'after': after,
                'added': added, 'removed': removed, 'changed_partitions': list(partitions.values()),
                'row_count_delta': new['rows'] - old['rows']}

    def preview_ingest(self, table_name, rows):
        if not isinstance(rows, list):
            raise Invalid('rows must be an array')
        schema = self.catalog.schema(table_name)
        groups = defaultdict(int)
        issues = []
        keys = set()
        with self.catalog.transaction() as connection:
            table = self.catalog.table(table_name, connection)
            for ordinal, raw in enumerate(rows):
                try:
                    row = schema.normalize(raw)
                    key = schema.key(row)
                    if key is not None:
                        old = connection.execute('SELECT 1 FROM row_keys WHERE table_name=? AND row_key=?',
                                                 (table_name, key)).fetchone()
                        if old or key in keys:
                            raise Invalid('duplicate primary key')
                        keys.add(key)
                    groups[canonical(schema.partition(row))] += 1
                except Invalid as error:
                    issues.append({'ordinal': ordinal, 'reason': str(error)})
        import json
        return {'table': table_name, 'expected_head': table['head'], 'schema_version': table['schema_version'],
                'accepted': sum(groups.values()), 'rejected': len(issues), 'issues': issues,
                'partitions': [{'partition': json.loads(key), 'rows': value} for key, value in sorted(groups.items())]}
