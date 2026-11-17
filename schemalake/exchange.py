from pathlib import Path
import hashlib
import json
import zipfile

from .types import Conflict, Invalid, canonical


class Exchange:
    def __init__(self, catalog, objects, scanner, ingestor):
        self.catalog = catalog
        self.objects = objects
        self.scanner = scanner
        self.ingestor = ingestor

    def export(self, table_name, path, snapshot_id=None):
        destination = Path(path)
        if destination.exists():
            raise Conflict('exchange destination already exists')
        plan = self.scanner.plan(table_name, snapshot_id)
        schema = self.catalog.schema(table_name, plan['schema_version'])
        manifest = {'format': 'schemalake-exchange-v1', 'table': table_name,
                    'snapshot_id': plan['snapshot_id'], 'schema': schema.encode(), 'files': plan['files']}
        entries = {}
        for file in plan['files']:
            raw = self.objects.read(file['path'], file['sha256'])
            entries['objects/' + file['sha256']] = raw
        destination.parent.mkdir(parents=True, exist_ok=True)
        with destination.open('xb') as handle:
            with zipfile.ZipFile(handle, 'w', zipfile.ZIP_DEFLATED) as archive:
                archive.writestr('manifest.json', canonical(manifest))
                for name, raw in sorted(entries.items()):
                    archive.writestr(name, raw)
        return {'path': str(destination), 'sha256': hashlib.sha256(destination.read_bytes()).hexdigest(),
                'snapshot_id': plan['snapshot_id'], 'objects': len(entries), 'rows': plan['rows']}

    def inspect(self, path):
        with zipfile.ZipFile(path) as archive:
            names = archive.namelist()
            if len(names) != len(set(names)) or 'manifest.json' not in names:
                raise Invalid('exchange has repeated names or no manifest')
            for info in archive.infolist():
                if info.filename.startswith('/') or '..' in Path(info.filename).parts:
                    raise Invalid('unsafe exchange member name')
                if info.file_size > 128 * 1024 * 1024:
                    raise Invalid('exchange member exceeds supported object size')
            if sum(item.file_size for item in archive.infolist()) > 512 * 1024 * 1024:
                raise Invalid('exchange exceeds supported decoded size')
            try:
                manifest = json.loads(archive.read('manifest.json'))
            except (ValueError, UnicodeDecodeError):
                raise Invalid('exchange manifest is invalid') from None
            if manifest.get('format') != 'schemalake-exchange-v1':
                raise Invalid('unknown exchange format')
            from .types import Schema
            schema = Schema.parse(manifest['schema'])
            expected = {'manifest.json'} | {'objects/' + file['sha256'] for file in manifest['files']}
            if set(names) != expected:
                raise Invalid('exchange members do not match manifest')
            count = 0
            for file in manifest['files']:
                raw = archive.read('objects/' + file['sha256'])
                if hashlib.sha256(raw).hexdigest() != file['sha256'] or len(raw) != file['byte_count']:
                    raise Conflict('exchange object checksum or size mismatch')
                if file.get('encoding', 'jsonl') == 'gzip-jsonl':
                    import gzip
                    raw = gzip.decompress(raw)
                    if len(raw) != file['decoded_byte_count']:
                        raise Conflict('exchange decoded size mismatch')
                elif file.get('encoding', 'jsonl') != 'jsonl':
                    raise Invalid('unsupported exchange object codec')
                lines = raw.decode('utf-8').splitlines()
                if len(lines) != file['row_count']:
                    raise Conflict('exchange row count mismatch')
                for line in lines:
                    row = schema.normalize(json.loads(line))
                    if schema.partition(row) != file['partition']:
                        raise Conflict('exchange row partition mismatch')
                    count += 1
            return {'manifest': manifest, 'rows': count, 'files': len(manifest['files'])}

    def import_snapshot(self, table_name, batch_id, path):
        inspection = self.inspect(path)
        if inspection['rows'] > 100000:
            raise Invalid('atomic exchange import supports at most 100000 rows')
        manifest = inspection['manifest']
        self.catalog.create_table(table_name, manifest['schema'])
        rows = []
        with zipfile.ZipFile(path) as archive:
            for file in manifest['files']:
                raw = archive.read('objects/' + file['sha256'])
                if file.get('encoding', 'jsonl') == 'gzip-jsonl':
                    import gzip
                    raw = gzip.decompress(raw)
                rows.extend(json.loads(line) for line in raw.decode('utf-8').splitlines())
        return self.ingestor.ingest(table_name, batch_id, rows)
