from collections import defaultdict
import json

from .types import Invalid, canonical


class DataWriter:
    def __init__(self, objects, file_rows=1000):
        if type(file_rows) is not int or not 1 <= file_rows <= 100000:
            raise Invalid('file_rows must be between 1 and 100000')
        self.objects = objects
        self.file_rows = file_rows

    def write(self, table_name, version, schema, rows):
        groups = defaultdict(list)
        for raw in rows:
            row = schema.normalize(raw)
            groups[canonical(schema.partition(row))].append(row)
        files = []
        for partition, records in sorted(groups.items()):
            for offset in range(0, len(records), self.file_rows):
                files.append(self.objects.write_rows(table_name, version, schema,
                                                     json.loads(partition), records[offset:offset + self.file_rows]))
        return files
