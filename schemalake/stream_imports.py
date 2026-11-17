from pathlib import Path
import hashlib
import json

from .types import Conflict, Invalid, timestamp


class StreamImports:
    def __init__(self, catalog, ingestor):
        self.catalog = catalog
        self.ingestor = ingestor

    def file_hash(self, path):
        result = hashlib.sha256()
        with path.open('rb') as stream:
            for block in iter(lambda: stream.read(1024 * 1024), b''):
                result.update(block)
        return result.hexdigest()

    def get(self, table_name, import_id):
        with self.catalog.transaction() as connection:
            row = connection.execute('SELECT * FROM stream_imports WHERE table_name=? AND import_id=?',
                                     (table_name, import_id)).fetchone()
            return dict(row) if row else None

    def chunks(self, path, chunk_rows):
        rows = []
        with path.open('rb') as stream:
            for line_number, line in enumerate(stream, 1):
                if len(line) > 1024 * 1024:
                    raise Invalid(f'JSONL line {line_number} exceeds 1 MiB')
                if not line.strip():
                    continue
                try:
                    value = json.loads(line)
                except (ValueError, UnicodeDecodeError):
                    raise Invalid(f'invalid JSON at line {line_number}') from None
                rows.append(value)
                if len(rows) == chunk_rows:
                    yield rows
                    rows = []
        if rows:
            yield rows

    def load(self, table_name, import_id, path, chunk_rows=1000, invalid='reject'):
        self.ingestor.validate_batch_id(import_id)
        if type(chunk_rows) is not int or not 1 <= chunk_rows <= 100000:
            raise Invalid('chunk_rows must be between 1 and 100000')
        if invalid not in {'reject', 'quarantine'}:
            raise Invalid('invalid stream row policy')
        path = Path(path)
        if not path.is_file():
            raise Invalid('stream import input must be a regular file')
        before = path.stat()
        checksum = self.file_hash(path)
        with self.catalog.transaction(True) as connection:
            self.catalog.table(table_name, connection)
            old = connection.execute('SELECT * FROM stream_imports WHERE table_name=? AND import_id=?',
                                     (table_name, import_id)).fetchone()
            if old:
                if (old['input_sha256'], old['chunk_rows'], old['invalid_policy']) != (checksum, chunk_rows, invalid):
                    raise Conflict('stream import identity or policy changed')
                if old['status'] == 'completed':
                    return {**dict(old), 'replayed': True}
                connection.execute("UPDATE stream_imports SET status='running',updated_at=? WHERE table_name=? AND import_id=?",
                                   (timestamp(), table_name, import_id))
            else:
                connection.execute('INSERT INTO stream_imports VALUES (?,?,?,?,?,0,0,0,?,?)',
                                   (table_name, import_id, checksum, chunk_rows, invalid, 'running', timestamp()))
        try:
            for ordinal, rows in enumerate(self.chunks(path, chunk_rows)):
                child_id = 'stream.' + hashlib.sha256((import_id + ':' + str(ordinal)).encode()).hexdigest()
                result = self.ingestor.ingest(table_name, child_id, rows, invalid)
                with self.catalog.transaction(True) as connection:
                    state = connection.execute('SELECT * FROM stream_imports WHERE table_name=? AND import_id=?',
                                               (table_name, import_id)).fetchone()
                    if state['chunks'] == ordinal:
                        connection.execute('UPDATE stream_imports SET chunks=chunks+1,accepted=accepted+?,rejected=rejected+?,updated_at=? '
                                           'WHERE table_name=? AND import_id=?',
                                           (result['row_count'], result['rejected'], timestamp(), table_name, import_id))
                    elif state['chunks'] < ordinal:
                        raise Conflict('stream import progress contains a gap')
            after = path.stat()
            if (before.st_ino, before.st_size, before.st_mtime_ns) != (after.st_ino, after.st_size, after.st_mtime_ns):
                raise Conflict('input file changed during stream import')
            with self.catalog.transaction(True) as connection:
                connection.execute("UPDATE stream_imports SET status='completed',updated_at=? WHERE table_name=? AND import_id=?",
                                   (timestamp(), table_name, import_id))
            return {**self.get(table_name, import_id), 'replayed': False}
        except BaseException:
            with self.catalog.transaction(True) as connection:
                connection.execute("UPDATE stream_imports SET status='failed',updated_at=? WHERE table_name=? AND import_id=?",
                                   (timestamp(), table_name, import_id))
            raise
