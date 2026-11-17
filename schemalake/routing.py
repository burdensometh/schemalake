from .predicates import Predicate
from .types import Invalid, canonical, sha


class RoutedIngestor:
    def __init__(self, catalog, ingestor):
        self.catalog = catalog
        self.ingestor = ingestor

    def route(self, source_schema, batch_id, rows, routes, invalid='reject'):
        from .types import Schema
        schema = Schema.parse(source_schema)
        self.ingestor.validate_batch_id(batch_id)
        if not isinstance(rows, list) or not isinstance(routes, list) or not routes:
            raise Invalid('routing requires row and nonempty route arrays')
        if len({route.get('table') for route in routes}) != len(routes):
            raise Invalid('route target tables must be distinct')
        compiled = []
        for route in routes:
            if not isinstance(route, dict) or set(route) - {'table', 'where'}:
                raise Invalid('route accepts table and optional where')
            target = route.get('table')
            self.catalog.table(target)
            predicate = Predicate(schema, route['where']) if 'where' in route else None
            compiled.append((target, predicate))
        normalized = [schema.normalize(row) for row in rows]
        results = []
        definition = sha({'source_schema': schema.encode(), 'routes': routes, 'rows': rows})
        for target, predicate in compiled:
            selected = [row for row in normalized if predicate is None or predicate.matches(row)]
            child_id = batch_id[:60] + '.' + definition[:24]
            results.append(self.ingestor.ingest(target, child_id, selected, invalid))
        return {'batch_id': batch_id, 'definition_sha256': definition, 'tables': results}

    def status(self, batch_id, routes, definition_sha256):
        child_id = batch_id[:60] + '.' + definition_sha256[:24]
        result = []
        for route in routes:
            table = route['table']
            receipt = self.catalog.batch(table, child_id)
            result.append({'table': table, 'committed': receipt is not None, 'receipt': receipt})
        return result
