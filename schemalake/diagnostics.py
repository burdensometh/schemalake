from .types import Conflict, Invalid, Missing, canonical


class Diagnostics:
    def __init__(self, catalog, objects, scanner):
        self.catalog = catalog
        self.objects = objects
        self.scanner = scanner

    def inventory(self):
        live = set()
        for table in self.catalog.tables():
            for snapshot in self.catalog.snapshots(table['name']):
                live.add(snapshot['manifest_path'])
                document = self.objects.manifest(snapshot)
                live.update(file['path'] for file in document['files'])
        inventory = self.objects.inventory()
        referenced = {item['path'] for item in inventory if item['path'] in live}
        return {'objects': len(inventory), 'referenced_objects': len(referenced),
                'unreferenced': [item for item in inventory if item['path'] not in live],
                'missing_paths': sorted(live - referenced)}

    def check(self, table_name, verify_statistics=True):
        failures = []
        checked = set()
        snapshots = self.catalog.snapshots(table_name)
        for record in snapshots:
            try:
                document = self.objects.manifest(record)
                if document.get('table') != table_name:
                    raise Conflict('manifest belongs to a different table')
                if document['parent_id'] != record['parent_id']:
                    raise Conflict('manifest parent differs from catalog')
                schema = self.catalog.schema(table_name, document['schema_version'])
                keys = set()
                for file in document['files']:
                    rows = [schema.normalize(row) for row in self.objects.read_rows(file)]
                    if any(schema.partition(row) != file['partition'] for row in rows):
                        raise Conflict('object row partition differs from manifest')
                    if verify_statistics and self.objects.statistics(schema, rows) != file['statistics']:
                        raise Conflict('object statistics differ from manifest')
                    for row in rows:
                        key = schema.key(row)
                        if key is not None:
                            if key in keys:
                                raise Conflict('duplicate primary key in snapshot')
                            keys.add(key)
                    checked.add(file['path'])
            except (Conflict, Invalid, Missing, ValueError, KeyError) as error:
                failures.append({'snapshot': record['id'], 'error': str(error)})
        with self.catalog.transaction() as connection:
            integrity = connection.execute('PRAGMA integrity_check').fetchone()[0]
            foreign_keys = [tuple(row) for row in connection.execute('PRAGMA foreign_key_check')]
        return {'table': table_name, 'ok': not failures and integrity == 'ok' and not foreign_keys,
                'snapshots': len(snapshots), 'files': len(checked), 'failures': failures,
                'sqlite_integrity': integrity, 'foreign_key_failures': foreign_keys}

    def history(self, table_name, limit=100):
        import json
        if type(limit) is not int or not 1 <= limit <= 10000:
            raise Invalid('history limit must be between 1 and 10000')
        self.catalog.table(table_name)
        with self.catalog.transaction() as connection:
            rows = connection.execute('SELECT * FROM history WHERE table_name=? ORDER BY sequence DESC LIMIT ?',
                                      (table_name, limit))
            result = []
            for row in rows:
                value = dict(row)
                value['detail'] = json.loads(value['detail'])
                result.append(value)
            return result
