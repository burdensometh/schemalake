from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, urlsplit
import itertools
import json

from .types import Conflict, Invalid, LakeError, Missing, canonical


def handler_for(lake):
    class Handler(BaseHTTPRequestHandler):
        server_version = 'SchemaLake/0.1'

        def log_message(self, *args):
            pass

        def send_json(self, status, value):
            raw = canonical(value).encode('utf-8')
            self.send_response(status)
            self.send_header('Content-Type', 'application/json; charset=utf-8')
            self.send_header('Content-Length', str(len(raw)))
            self.end_headers()
            self.wfile.write(raw)

        def body(self):
            try:
                length = int(self.headers.get('Content-Length', '0'))
            except ValueError:
                raise Invalid('invalid content length') from None
            if not 1 <= length <= 8 * 1024 * 1024:
                raise Invalid('JSON request body must be between 1 byte and 8 MiB')
            if self.headers.get('Content-Type', '').split(';')[0] != 'application/json':
                raise Invalid('request requires application/json')
            try:
                value = json.loads(self.rfile.read(length))
            except (ValueError, UnicodeDecodeError):
                raise Invalid('invalid UTF-8 JSON request') from None
            if not isinstance(value, dict):
                raise Invalid('JSON body must be an object')
            return value

        def query(self):
            split = urlsplit(self.path)
            params = parse_qs(split.query, keep_blank_values=True)
            if any(len(values) != 1 for values in params.values()):
                raise Invalid('query parameters must not repeat')
            return split.path.strip('/').split('/'), {key: values[0] for key, values in params.items()}

        def integer(self, params, key, default=None):
            if key not in params:
                return default
            try:
                value = int(params[key])
            except ValueError:
                raise Invalid(f'{key} must be an integer') from None
            if value < 0:
                raise Invalid(f'{key} must be nonnegative')
            return value

        def guarded(self, function):
            try:
                function()
            except Missing as error:
                self.send_json(404, {'error': str(error)})
            except Conflict as error:
                self.send_json(409, {'error': str(error)})
            except (Invalid, TypeError, KeyError) as error:
                self.send_json(400, {'error': str(error)})
            except LakeError as error:
                self.send_json(422, {'error': str(error)})
            except Exception:
                self.send_json(500, {'error': 'internal storage failure'})

        def do_GET(self):
            self.guarded(self.get)

        def get(self):
            parts, params = self.query()
            if parts == ['health']:
                return self.send_json(200, {'status': 'ok', 'format_version': 1})
            if parts == ['tables']:
                return self.send_json(200, lake.catalog.tables())
            if len(parts) == 2 and parts[0] == 'exports':
                return self.send_json(200, lake.exports.get(parts[1]))
            if len(parts) != 3 or parts[0] != 'tables':
                raise Missing('endpoint does not exist')
            table, action = parts[1:]
            snapshot = self.integer(params, 'snapshot')
            if action == 'schema':
                value = lake.catalog.schema(table, self.integer(params, 'version')).encode()
            elif action == 'snapshots':
                value = lake.catalog.snapshots(table)
            elif action == 'plan':
                value = lake.scan.plan(table, snapshot)
            elif action == 'partitions':
                value = lake.maintenance.partitions(table, snapshot)
            elif action == 'statistics':
                value = lake.scan.statistics(table, snapshot)
            elif action == 'index':
                value = lake.indexes.status(table)
            elif action == 'provenance':
                value = lake.provenance.snapshot(table, snapshot)
            elif action == 'history':
                value = lake.diagnostics.history(table, self.integer(params, 'limit', 100))
            elif action == 'changes':
                value = lake.changes.progress(table)
            elif action == 'check':
                value = lake.diagnostics.check(table)
            elif action == 'exports':
                value = lake.exports.list(table)
            elif action == 'rows':
                offset = self.integer(params, 'offset', 0)
                limit = self.integer(params, 'limit', 100)
                if not 1 <= limit <= 10000:
                    raise Invalid('limit must be between 1 and 10000')
                columns = params['columns'].split(',') if 'columns' in params else None
                plan = lake.scan.plan(table, snapshot)
                iterator = lake.scan.rows(table, plan['snapshot_id'], columns=columns)
                value = {'table': table, 'snapshot_id': plan['snapshot_id'], 'total': plan['rows'],
                         'rows': list(itertools.islice(iterator, offset, offset + limit))}
            else:
                raise Missing('endpoint does not exist')
            self.send_json(200, value)

        def do_POST(self):
            self.guarded(self.post)

        def post(self):
            parts, params = self.query()
            body = self.body()
            if params:
                raise Invalid('POST operations do not take query parameters')
            if parts == ['tables']:
                value = lake.catalog.create_table(body['name'], body['schema'])
            elif parts == ['joins']:
                value = lake.joins.join(**body)
            elif parts == ['route']:
                value = lake.routing.route(**body)
            elif parts == ['transforms']:
                value = lake.transforms.register(**body)
            elif parts == ['aggregates']:
                value = lake.aggregates.register(**body)
            elif parts == ['enrich']:
                value = lake.enrichment.run(**body)
            elif len(parts) == 3 and parts[0] == 'aggregates':
                if parts[2] == 'refresh':
                    value = lake.aggregates.refresh(parts[1], **body)
                elif parts[2] == 'preview':
                    value = lake.aggregates.preview(parts[1], **body)
                else:
                    raise Missing('endpoint does not exist')
            elif len(parts) == 3 and parts[0] == 'transforms':
                if parts[2] == 'refresh':
                    value = lake.transforms.refresh(parts[1], **body)
                elif parts[2] == 'preview':
                    value = lake.transforms.preview(parts[1], **body)
                else:
                    raise Missing('endpoint does not exist')
            elif len(parts) == 3 and parts[0] == 'tables':
                table, action = parts[1:]
                operations = {
                    'ingest': lake.ingest.ingest, 'changes': lake.changes.ingest,
                    'replace': lake.maintenance.replace,
                    'compact': lake.maintenance.compact, 'lookup': lake.indexes.lookup,
                    'index': lake.indexes.build, 'preview': lake.plans.preview_ingest,
                    'export': lake.exports.create, 'quality': lake.quality.evaluate,
                    'diff': lake.plans.diff,
                    'range': lake.indexes.range,
                    'prepare': lake.staged.prepare, 'commit': lake.staged.commit, 'abort': lake.staged.abort,
                }
                if action == 'query':
                    value = list(lake.scan.where(table, **body))
                elif action == 'sort':
                    value = list(lake.sort.rows(table, **body))
                elif action == 'rowdiff':
                    value = list(lake.rowdiff.compare(table, **body))
                elif action in operations:
                    value = operations[action](table, **body)
                else:
                    raise Missing('endpoint does not exist')
            else:
                raise Missing('endpoint does not exist')
            self.send_json(200, value)
    return Handler


def server(lake, host='127.0.0.1', port=8762):
    return ThreadingHTTPServer((host, port), handler_for(lake))
