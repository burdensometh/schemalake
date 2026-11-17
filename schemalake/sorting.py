from contextlib import ExitStack
from decimal import Decimal
from pathlib import Path
from tempfile import TemporaryDirectory
import heapq
import json

from .types import Invalid, canonical


class Descending:
    def __init__(self, value):
        self.value = value

    def __lt__(self, other):
        return self.value > other.value

    def __eq__(self, other):
        return self.value == other.value


class SortedRows:
    def __init__(self, catalog, scanner):
        self.catalog = catalog
        self.scanner = scanner

    def specification(self, schema, order):
        fields = {field.name: field for field in schema.fields}
        if not isinstance(order, list) or not order:
            raise Invalid('sort order must be a nonempty array')
        result = []
        seen = set()
        for item in order:
            if not isinstance(item, dict) or set(item) - {'field', 'descending', 'nulls_first'}:
                raise Invalid('invalid sort rule')
            field = item.get('field')
            if field not in fields or field in seen:
                raise Invalid('sort columns must be known and distinct')
            reverse = item.get('descending', False)
            nulls_first = item.get('nulls_first', False)
            if type(reverse) is not bool or type(nulls_first) is not bool:
                raise Invalid('sort flags must be boolean')
            result.append((field, fields[field].type, reverse, nulls_first))
            seen.add(field)
        return result

    def key(self, item, rules):
        row, position = item['row'], item['position']
        values = []
        for field, type_name, reverse, nulls_first in rules:
            value = row[field]
            if value is None:
                values.append((0 if nulls_first else 1, 0))
            else:
                value = Decimal(value) if type_name == 'decimal' else value
                values.append((1 if nulls_first else 0, Descending(value) if reverse else value))
        return tuple(values), position

    def write_run(self, path, records):
        with path.open('w', encoding='utf-8') as stream:
            for item in records:
                stream.write(canonical(item) + '\n')

    def merge(self, paths, rules):
        with ExitStack() as stack:
            streams = [stack.enter_context(path.open(encoding='utf-8')) for path in paths]
            queue = []
            for ordinal, stream in enumerate(streams):
                line = stream.readline()
                if line:
                    item = json.loads(line)
                    heapq.heappush(queue, (self.key(item, rules), ordinal, item))
            while queue:
                _, ordinal, item = heapq.heappop(queue)
                yield item
                line = streams[ordinal].readline()
                if line:
                    following = json.loads(line)
                    heapq.heappush(queue, (self.key(following, rules), ordinal, following))

    def rows(self, table_name, order, snapshot_id=None, memory_rows=10000, fan_in=32):
        if type(memory_rows) is not int or memory_rows < 1:
            raise Invalid('sort memory_rows must be positive')
        if type(fan_in) is not int or not 2 <= fan_in <= 128:
            raise Invalid('sort fan_in must be between 2 and 128')
        plan = self.scanner.plan(table_name, snapshot_id)
        schema = self.catalog.schema(table_name, plan['schema_version'])
        rules = self.specification(schema, order)
        with TemporaryDirectory(prefix='lake-sort-') as temporary:
            root = Path(temporary)
            runs = []
            buffer = []
            sequence = 0
            for position, row in enumerate(self.scanner.rows(table_name, snapshot_id)):
                buffer.append({'row': row, 'position': position})
                if len(buffer) == memory_rows:
                    path = root / f'run-{sequence}.jsonl'
                    self.write_run(path, sorted(buffer, key=lambda item: self.key(item, rules)))
                    sequence += 1
                    runs.append(path)
                    buffer = []
            if buffer:
                path = root / f'run-{sequence}.jsonl'
                self.write_run(path, sorted(buffer, key=lambda item: self.key(item, rules)))
                sequence += 1
                runs.append(path)
            while len(runs) > fan_in:
                next_runs = []
                for start in range(0, len(runs), fan_in):
                    group = runs[start:start + fan_in]
                    path = root / f'run-{sequence}.jsonl'
                    sequence += 1
                    self.write_run(path, self.merge(group, rules))
                    next_runs.append(path)
                    for old in group:
                        old.unlink()
                runs = next_runs
            for item in self.merge(runs, rules):
                yield item['row']
