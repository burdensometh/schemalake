import gzip
import hashlib
import json

from .storage import Objects
from .types import Conflict, Invalid, canonical


class EncodedObjects(Objects):
    def __init__(self, root, encoding='jsonl'):
        super().__init__(root)
        if encoding not in {'jsonl', 'gzip-jsonl'}:
            raise Invalid('unsupported data object encoding')
        self.encoding = encoding

    def write_rows(self, table_name, schema_version, schema, partition, rows):
        if self.encoding == 'jsonl':
            return super().write_rows(table_name, schema_version, schema, partition, rows)
        raw = ('\n'.join(canonical(row) for row in rows) + '\n').encode('utf-8')
        encoded = gzip.compress(raw, mtime=0)
        partition_id = hashlib.sha256(canonical(partition).encode()).hexdigest()[:24]
        path, checksum = self.put(f'tables/{table_name}/data/{partition_id}', encoded, 'jsonl.gz')
        return {'id': checksum, 'schema_version': schema_version, 'partition': partition, 'path': path,
                'encoding': 'gzip-jsonl', 'sha256': checksum, 'row_count': len(rows),
                'byte_count': len(encoded), 'decoded_byte_count': len(raw), 'statistics': self.statistics(schema, rows)}

    def read_rows(self, file):
        if file.get('encoding', 'jsonl') == 'jsonl':
            yield from super().read_rows(file)
            return
        if file['encoding'] != 'gzip-jsonl':
            raise Invalid('unknown data object encoding')
        encoded = self.read(file['path'], file['sha256'])
        if len(encoded) != file['byte_count']:
            raise Conflict('encoded object size mismatch')
        try:
            raw = gzip.decompress(encoded)
        except (ValueError, OSError, EOFError):
            raise Conflict('invalid gzip object') from None
        if len(raw) != file['decoded_byte_count']:
            raise Conflict('decoded object size mismatch')
        lines = raw.decode('utf-8').splitlines()
        if len(lines) != file['row_count']:
            raise Conflict('decoded object row count mismatch')
        for line in lines:
            row = json.loads(line)
            if not isinstance(row, dict):
                raise Invalid('data object row must be an object')
            yield row

    def inventory(self):
        return [{'path': str(path.relative_to(self.root)), 'bytes': path.stat().st_size}
                for path in sorted(self.root.rglob('*'))
                if path.is_file() and path.suffix in {'.json', '.jsonl', '.gz'}]
