from collections import Counter
from pathlib import Path
from tempfile import TemporaryDirectory
import json
import sqlite3

from .types import canonical


class RowDiff:
    def __init__(self, catalog, scanner):
        self.catalog = catalog
        self.scanner = scanner

    def compare(self, table_name, before, after):
        old = self.scanner.plan(table_name, before)
        new = self.scanner.plan(table_name, after)
        # 两个快照按各自的 schema 版本读取：rows() 已把旧文件投影到该快照
        # 解释所用的结构，主键列不允许演进，因此身份仍可按主键对齐。
        schema = self.catalog.schema(table_name, new['schema_version'])
        with TemporaryDirectory(prefix='lake-diff-') as temporary:
            database = sqlite3.connect(Path(temporary) / 'diff.sqlite')
            try:
                database.execute('CREATE TABLE items (side INTEGER, identity TEXT, payload TEXT, n INTEGER, '
                                 'PRIMARY KEY(side,identity,payload))')
                for side, snapshot in enumerate((before, after)):
                    for row in self.scanner.rows(table_name, snapshot):
                        payload = canonical(row)
                        key = schema.key(row) if schema.primary_key else payload
                        database.execute('INSERT INTO items VALUES (?,?,?,1) '
                                         'ON CONFLICT(side,identity,payload) DO UPDATE SET n=n+1',
                                         (side, key, payload))
                database.commit()
                identities = database.execute('SELECT DISTINCT identity FROM items ORDER BY identity')
                for (identity,) in identities:
                    values = database.execute('SELECT side,payload,n FROM items WHERE identity=?', (identity,)).fetchall()
                    sides = [Counter(), Counter()]
                    for side, payload, count in values:
                        sides[side][payload] = count
                    if schema.primary_key:
                        left = next(iter(sides[0]), None)
                        right = next(iter(sides[1]), None)
                        if left != right:
                            kind = 'added' if left is None else ('removed' if right is None else 'changed')
                            yield {'kind': kind, 'key': json.loads(identity),
                                   'before': json.loads(left) if left is not None else None,
                                   'after': json.loads(right) if right is not None else None}
                    else:
                        for payload, count in (sides[0] - sides[1]).items():
                            yield {'kind': 'removed', 'row': json.loads(payload), 'count': count}
                        for payload, count in (sides[1] - sides[0]).items():
                            yield {'kind': 'added', 'row': json.loads(payload), 'count': count}
            finally:
                database.close()

    def summary(self, table_name, before, after):
        totals = {'added': 0, 'removed': 0, 'changed': 0}
        for change in self.compare(table_name, before, after):
            totals[change['kind']] += change.get('count', 1)
        return {'table': table_name, 'before': before, 'after': after, **totals}
