from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import patch
from urllib.request import Request, urlopen
from urllib.error import HTTPError
import json
import threading
import unittest

from schemalake.app import Lake
from schemalake.api import server
from schemalake.mirror import Mirror
from schemalake.types import Conflict, Invalid
from test_core import SCHEMA, row


class ServicesTest(unittest.TestCase):
    def setUp(self):
        self.temporary = TemporaryDirectory()
        self.root = Path(self.temporary.name)
        self.lake = Lake(self.root / 'lake', file_rows=1)
        self.lake.catalog.create_table('readings', SCHEMA)

    def tearDown(self):
        self.temporary.cleanup()

    def append(self, batch='b', rows=None):
        return self.lake.ingest.ingest('readings', batch, [row()] if rows is None else rows)

    def test_jsonl_import_and_identity(self):
        path = self.root / 'input.jsonl'
        path.write_text(json.dumps(row()) + '\n', encoding='utf-8')
        first = self.lake.imports.load('readings', 'file1', path)
        self.assertEqual(first['row_count'], 1)
        self.assertTrue(self.lake.imports.load('readings', 'file1', path)['replayed'])
        path.write_text(json.dumps(row(2)) + '\n', encoding='utf-8')
        with self.assertRaises(Conflict):
            self.lake.imports.load('readings', 'file1', path)

    def test_csv_roundtrip_null_and_backslash(self):
        self.append(rows=[row(1, site=r'\N', reading=None), row(2, site='', reading='1.2300')])
        item = self.lake.exports.create('readings', format='csv')
        self.lake.catalog.create_table('copy', SCHEMA)
        self.lake.imports.load('copy', 'imported', self.lake.exports.artifact(item['id']), 'csv')
        self.assertEqual(list(self.lake.scan.rows('copy')), list(self.lake.scan.rows('readings')))

    def test_export_immutable_and_checksum_verified(self):
        self.append()
        item = self.lake.exports.create('readings')
        path = self.lake.exports.artifact(item['id'])
        old = path.read_bytes()
        self.append('later', [row(2)])
        self.assertEqual(path.read_bytes(), old)
        path.write_bytes(old + b'\n')
        with self.assertRaises(Conflict):
            self.lake.exports.artifact(item['id'])

    def test_mirror_idempotence_and_old_snapshot(self):
        self.append()
        self.append('b2', [row(2)])
        mirror = Mirror(self.root / 'mirror.sqlite')
        mirror.apply(self.lake.scan, 'readings', 2)
        self.assertTrue(mirror.apply(self.lake.scan, 'readings', 2)['replayed'])
        with self.assertRaises(Conflict):
            mirror.apply(self.lake.scan, 'readings', 1)
        self.assertTrue(self.lake.reconcile.against_mirror(mirror, 'readings')['ok'])

    def test_mirror_failure_keeps_data_and_receipt(self):
        self.append()
        mirror = Mirror(self.root / 'mirror.sqlite')
        mirror.apply(self.lake.scan, 'readings')
        self.append('b2', [row(2)])
        original = self.lake.scan.rows
        def fail(*args, **kwargs):
            yield next(original(*args, **kwargs))
            raise OSError('author read fault')
        with patch.object(self.lake.scan, 'rows', fail):
            with self.assertRaises(OSError):
                mirror.apply(self.lake.scan, 'readings')
        self.assertEqual(mirror.heads()[0]['snapshot_id'], 1)
        self.assertEqual(len(mirror.rows('readings')), 1)

    def test_expression_transform_and_full_refresh(self):
        self.lake.catalog.create_table('clean', SCHEMA)
        select = {field['name']: {'field': field['name']} for field in SCHEMA['fields']}
        select['reading'] = {'op': 'multiply', 'args': [{'field': 'reading'}, {'literal': '2'}]}
        self.lake.transforms.register('normalize', 'readings', 'clean', {'select': select})
        self.append()
        first = self.lake.transforms.refresh('normalize')
        self.assertEqual(list(self.lake.scan.rows('clean'))[0]['reading'], '25')
        self.append('b2', [row(2, reading='3')])
        second = self.lake.transforms.refresh('normalize')
        self.assertEqual((first['input_rows'], second['input_rows']), (1, 2))
        self.assertEqual(len(self.lake.transforms.history('normalize')), 2)

    def test_group_aggregate_materializes_typed_table(self):
        target = {'fields': [{'id': 1, 'name': 'site', 'type': 'string'},
                             {'id': 2, 'name': 'total', 'type': 'decimal'},
                             {'id': 3, 'name': 'n', 'type': 'int64'}], 'primary_key': ['site']}
        self.lake.catalog.create_table('totals', target)
        self.append(rows=[row(1, reading='1.25'), row(2, reading='2.75'), row(3, site='b', reading=None)])
        plan = {'group_by': ['site'], 'aggregates': {'total': {'op': 'sum', 'field': 'reading'},
                                                   'n': {'op': 'count', 'field': 'reading'}}}
        self.lake.aggregates.register('by_site', 'readings', 'totals', plan)
        result = self.lake.aggregates.refresh('by_site')
        self.assertEqual(result['input_rows'], 3)
        self.assertEqual(sorted(self.lake.scan.rows('totals'), key=lambda r: r['site']),
                         [{'site': 'a', 'total': '4', 'n': 2}, {'site': 'b', 'total': '0', 'n': 0}])

    def test_routing_baseline_is_per_target_commit(self):
        self.lake.catalog.create_table('target_a', SCHEMA)
        self.lake.catalog.create_table('target_b', SCHEMA)
        self.lake.ingest.ingest('target_b', 'existing', [row()])
        with self.assertRaises(Invalid):
            self.lake.routing.route(SCHEMA, 'route1', [row()], [{'table': 'target_a'}, {'table': 'target_b'}])
        self.assertEqual(len(list(self.lake.scan.rows('target_a'))), 1)
        self.assertEqual(len(list(self.lake.scan.rows('target_b'))), 1)

    def test_staged_commit_and_cas(self):
        prepared = self.lake.staged.prepare('readings', 'staged', [row()])
        self.assertEqual(prepared['status'], 'prepared')
        self.assertEqual(self.lake.catalog.table('readings')['head'], 0)
        self.lake.staged.commit('readings', 'staged')
        self.assertTrue(self.lake.staged.commit('readings', 'staged')['replayed'])
        self.lake.staged.prepare('readings', 'staged2', [row(2)])
        self.append('concurrent', [row(3)])
        with self.assertRaises(Conflict):
            self.lake.staged.commit('readings', 'staged2')
        self.assertEqual(self.lake.staged.abort('readings', 'staged2')['status'], 'aborted')

    def test_stream_import_retries_only_unreceipted_chunks(self):
        path = self.root / 'stream.jsonl'
        path.write_text('\n'.join(json.dumps(row(i)) for i in range(1, 6)) + '\n', encoding='utf-8')
        original = self.lake.ingest.ingest
        calls = 0
        def interrupted(*args, **kwargs):
            nonlocal calls
            calls += 1
            if calls == 2:
                raise OSError('author interruption')
            return original(*args, **kwargs)
        with patch.object(self.lake.ingest, 'ingest', interrupted):
            with self.assertRaises(OSError):
                self.lake.stream_imports.load('readings', 'stream1', path, 2)
        self.assertEqual(len(list(self.lake.scan.rows('readings'))), 2)
        result = self.lake.stream_imports.load('readings', 'stream1', path, 2)
        self.assertEqual((result['accepted'], result['chunks']), (5, 3))
        self.assertEqual(len(list(self.lake.scan.rows('readings'))), 5)

    def test_gzip_and_plain_objects_can_share_snapshots(self):
        self.append()
        compressed = Lake(self.lake.home, file_rows=1, encoding='gzip-jsonl')
        compressed.ingest.ingest('readings', 'compressed', [row(2)])
        self.assertEqual(len(list(compressed.scan.rows('readings'))), 2)
        self.assertTrue(compressed.diagnostics.check('readings')['ok'])

    def test_exchange_import_preserves_snapshot_values(self):
        self.append(rows=[row(), row(2)])
        path = self.root / 'snapshot.zip'
        self.lake.exchange.export('readings', path)
        self.assertEqual(self.lake.exchange.inspect(path)['rows'], 2)
        self.lake.exchange.import_snapshot('imported', 'exchange1', path)
        self.assertEqual(list(self.lake.scan.rows('imported')), list(self.lake.scan.rows('readings')))

    def test_sort_external_merge_stability_and_nulls(self):
        self.append(rows=[row(1, reading='10'), row(2, reading='2'), row(3, reading='2'), row(4, reading=None)])
        result = list(self.lake.sort.rows('readings', [{'field': 'reading', 'nulls_first': True}], memory_rows=1, fan_in=2))
        self.assertEqual([r['id'] for r in result], [4, 2, 3, 1])

    def test_row_diff_primary_key_and_compaction(self):
        self.append(rows=[row(), row(2)])
        self.lake.maintenance.replace('readings', [row(1, reading='9'), row(3)])
        result = self.lake.rowdiff.summary('readings', 1, 2)
        self.assertEqual((result['added'], result['removed'], result['changed']), (1, 1, 1))
        self.lake.maintenance.compact('readings')
        self.assertEqual(list(self.lake.rowdiff.compare('readings', 2, 3)), [])

    def test_quality_checks_use_pinned_data(self):
        self.append(rows=[row(1, reading='1'), row(2, reading='2')])
        result = self.lake.quality.evaluate('readings', [{'kind': 'unique', 'columns': ['site']}])
        self.assertFalse(result['ok'])
        self.assertEqual(result['rules'][0]['violations'], 1)

    def test_lookup_enrichment_creates_derived_data(self):
        dims = {'fields': [{'id': 1, 'name': 'site', 'type': 'string'}, {'id': 2, 'name': 'offset', 'type': 'decimal'}], 'primary_key': ['site']}
        self.lake.catalog.create_table('calibration', dims)
        self.lake.catalog.create_table('corrected', SCHEMA)
        self.append()
        self.lake.ingest.ingest('calibration', 'dimension', [{'site': 'a', 'offset': '0.5'}])
        select = {field['name']: {'field': 'left_' + field['name']} for field in SCHEMA['fields']}
        select['reading'] = {'op': 'add', 'args': [{'field': 'left_reading'}, {'field': 'right_offset'}]}
        result = self.lake.enrichment.run('readings', 'calibration', 'corrected', {'site': 'site'}, select)
        self.assertEqual(result['source_snapshots'], {'readings': 1, 'calibration': 1})
        self.assertEqual(list(self.lake.scan.rows('corrected'))[0]['reading'], '13')

    def test_http_two_phase_compaction_endpoints(self):
        http = server(self.lake, port=0)
        thread = threading.Thread(target=http.serve_forever, daemon=True)
        thread.start()
        url = f'http://127.0.0.1:{http.server_port}'
        try:
            for batch_id, ident in [('b1', 1), ('b2', 2)]:
                request = Request(url + '/tables/readings/ingest',
                                  data=json.dumps({'batch_id': batch_id, 'rows': [row(ident)]}).encode(),
                                  headers={'Content-Type': 'application/json'})
                with urlopen(request) as response:
                    json.load(response)
            request = Request(url + '/tables/readings/compact-prepare',
                              data=json.dumps({'target_rows': 100}).encode(),
                              headers={'Content-Type': 'application/json'})
            with urlopen(request) as response:
                job = json.load(response)
            self.assertEqual(job['status'], 'prepared')
            request = Request(url + '/tables/readings/compact-jobs', data=b'{}',
                              headers={'Content-Type': 'application/json'})
            with urlopen(request) as response:
                self.assertEqual(len(json.load(response)), 1)
            request = Request(url + '/tables/readings/compact-commit',
                              data=json.dumps({'job_id': job['job_id']}).encode(),
                              headers={'Content-Type': 'application/json'})
            with urlopen(request) as response:
                published = json.load(response)
            self.assertEqual(published['rows'], 2)
            with urlopen(url + '/tables/readings/rows') as response:
                self.assertEqual(len(json.load(response)['rows']), 2)
        finally:
            http.shutdown()
            http.server_close()
            thread.join(5)

    def test_http_uses_persisted_core(self):
        http = server(self.lake, port=0)
        thread = threading.Thread(target=http.serve_forever, daemon=True)
        thread.start()
        url = f'http://127.0.0.1:{http.server_port}'
        try:
            request = Request(url + '/tables/readings/ingest', data=json.dumps({'batch_id': 'http', 'rows': [row()]}).encode(),
                              headers={'Content-Type': 'application/json'})
            with urlopen(request) as response:
                self.assertEqual(json.load(response)['row_count'], 1)
            with urlopen(url + '/tables/readings/rows') as response:
                self.assertEqual(json.load(response)['rows'][0]['id'], 1)
            with self.assertRaises(HTTPError) as failure:
                urlopen(url + '/tables/readings/rows?limit=-1')
            self.assertEqual(failure.exception.code, 400)
        finally:
            http.shutdown()
            http.server_close()
            thread.join(5)


if __name__ == '__main__':
    unittest.main()
