from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import patch
from urllib.request import urlopen
import json
import threading
import unittest

from schemalake.app import Lake
from schemalake.api import server
from schemalake.types import Conflict
from test_core import SCHEMA, row


TARGET = {'fields': [
    {'id': 1, 'name': 'site', 'type': 'string'},
    {'id': 2, 'name': 'n', 'type': 'int64'},
    {'id': 3, 'name': 'total', 'type': 'decimal'},
    {'id': 4, 'name': 'avg', 'type': 'decimal', 'nullable': True},
    {'id': 5, 'name': 'lo', 'type': 'decimal', 'nullable': True},
    {'id': 6, 'name': 'hi', 'type': 'decimal', 'nullable': True},
], 'primary_key': ['site']}

PLAN = {'group_by': ['site'], 'aggregates': {
    'n': {'op': 'count', 'field': 'reading'},
    'total': {'op': 'sum', 'field': 'reading'},
    'avg': {'op': 'mean', 'field': 'reading'},
    'lo': {'op': 'min', 'field': 'reading'},
    'hi': {'op': 'max', 'field': 'reading'},
}}


class IncrementalAggregateTest(unittest.TestCase):
    def setUp(self):
        self.temporary = TemporaryDirectory()
        self.root = Path(self.temporary.name)
        self.lake = Lake(self.root / 'lake', file_rows=1)
        self.lake.catalog.create_table('readings', SCHEMA)
        self.lake.catalog.create_table('site_stats', TARGET)
        self.lake.aggregates.register('stats', 'readings', 'site_stats', PLAN)

    def tearDown(self):
        self.temporary.cleanup()

    def append(self, batch, rows):
        return self.lake.ingest.ingest('readings', batch, rows)

    def expected(self):
        return sorted(self.lake.aggregates.preview('stats')['rows'], key=lambda r: r['site'])

    def actual(self):
        return sorted(self.lake.scan.rows('site_stats'), key=lambda r: r['site'])

    def source_reads(self, function):
        reads = []
        original = self.lake.objects.read_rows
        def tracking(file):
            if file['path'].startswith('tables/readings/'):
                reads.append(file['path'])
            yield from original(file)
        with patch.object(self.lake.objects, 'read_rows', tracking):
            result = function()
        return result, reads

    def test_incremental_matches_full_recompute(self):
        self.append('d1', [row(1, '2026-11-01', 'a', '10'), row(2, '2026-11-01', 'b', '100')])
        self.append('d2', [row(3, '2026-11-02', 'a', '2'), row(4, '2026-11-02', 'a', '3')])
        first = self.lake.aggregates.refresh('stats')
        self.assertEqual((first['input_rows'], first['partitions_changed']), (4, 2))
        self.assertFalse(first['replayed'])
        self.assertEqual(self.actual(), self.expected())
        self.lake.maintenance.replace('readings', [row(5, '2026-11-01', 'a', '1')], {'day': '2026-11-01'})
        second = self.lake.aggregates.refresh('stats')
        self.assertEqual((second['input_rows'], second['partitions_changed']), (1, 1))
        self.assertEqual(self.actual(), self.expected())
        stats = {r['site']: r for r in self.actual()}
        self.assertNotIn('b', stats)
        self.assertEqual(stats['a']['hi'], '3')
        self.assertEqual(stats['a']['lo'], '1')
        self.assertEqual(stats['a']['total'], '6')
        self.assertEqual(stats['a']['n'], 3)
        self.assertEqual(stats['a']['avg'], '2')
        runs = self.lake.aggregates.history('stats')
        self.assertEqual([r['input_snapshot'] for r in runs], [2, 3])

    def test_refresh_reads_only_changed_partitions(self):
        self.append('d1', [row(1, '2026-11-01', 'a', '1'), row(2, '2026-11-01', 'b', '2')])
        self.append('d2', [row(3, '2026-11-02', 'a', '4')])
        self.lake.aggregates.refresh('stats')
        day2_paths = {f['path'] for f in self.lake.scan.plan('readings')['files']
                      if f['partition'] == {'day': '2026-11-02'}}
        self.assertTrue(day2_paths)
        self.lake.maintenance.replace('readings', [row(4, '2026-11-01', 'b', '9')], {'day': '2026-11-01'})
        result, reads = self.source_reads(lambda: self.lake.aggregates.refresh('stats'))
        self.assertTrue(reads)
        self.assertFalse(set(reads) & day2_paths)
        self.assertEqual(result['input_rows'], 1)
        self.assertEqual(self.actual(), self.expected())

    def test_append_to_same_partition_recomputes_only_it(self):
        self.append('d1', [row(1, '2026-11-01', 'a', '1')])
        self.append('d2', [row(2, '2026-11-02', 'a', '2')])
        self.lake.aggregates.refresh('stats')
        self.append('d1more', [row(3, '2026-11-01', 'a', '5'), row(4, '2026-11-01', 'c', '7')])
        result, reads = self.source_reads(lambda: self.lake.aggregates.refresh('stats'))
        self.assertEqual(result['partitions_changed'], 1)
        self.assertEqual(result['input_rows'], 3)
        self.assertEqual(len(set(reads)), 3)
        self.assertEqual(self.actual(), self.expected())

    def test_compact_invalidates_only_its_partition(self):
        self.append('d1', [row(1, '2026-11-01', 'a', '1'), row(2, '2026-11-01', 'b', '2')])
        self.append('d2', [row(3, '2026-11-02', 'a', '3')])
        self.lake.aggregates.refresh('stats')
        self.lake.maintenance.compact('readings', partition={'day': '2026-11-01'}, target_rows=100)
        result = self.lake.aggregates.refresh('stats')
        self.assertEqual((result['partitions_changed'], result['input_rows']), (1, 2))
        self.assertEqual(self.actual(), self.expected())

    def test_drop_partition_retracts_its_contributions(self):
        self.append('d1', [row(1, '2026-11-01', 'a', '1'), row(2, '2026-11-01', 'b', '5')])
        self.append('d2', [row(3, '2026-11-02', 'a', '3')])
        self.lake.aggregates.refresh('stats')
        self.lake.maintenance.drop_partition('readings', {'day': '2026-11-01'})
        result = self.lake.aggregates.refresh('stats')
        self.assertEqual(result['input_rows'], 0)
        stats = {r['site']: r for r in self.actual()}
        self.assertNotIn('b', stats)
        self.assertEqual((stats['a']['total'], stats['a']['n']), ('3', 1))
        self.assertEqual(self.actual(), self.expected())

    def test_nulls_and_all_null_group(self):
        self.append('d1', [row(1, '2026-11-01', 'a', None), row(2, '2026-11-01', 'a', '2'),
                           row(3, '2026-11-01', 'b', None)])
        self.lake.aggregates.refresh('stats')
        stats = {r['site']: r for r in self.actual()}
        self.assertEqual((stats['a']['n'], stats['a']['total']), (1, '2'))
        self.assertEqual((stats['b']['n'], stats['b']['total']), (0, '0'))
        self.assertIsNone(stats['b']['avg'])
        self.assertIsNone(stats['b']['lo'])
        self.assertIsNone(stats['b']['hi'])
        self.assertEqual(self.actual(), self.expected())

    def test_mean_and_sum_are_exact_across_partitions(self):
        self.append('d1', [row(1, '2026-11-01', 'a', '0.1')])
        self.append('d2', [row(2, '2026-11-02', 'a', '0.2')])
        self.append('d3', [row(3, '2026-11-03', 'a', '0.3')])
        self.lake.aggregates.refresh('stats')
        stats = {r['site']: r for r in self.actual()}
        self.assertEqual((stats['a']['total'], stats['a']['avg']), ('0.6', '0.2'))

    def test_failed_refresh_rolls_back_and_retry_is_exact(self):
        self.append('d1', [row(1, '2026-11-01', 'a', '1')])
        self.lake.aggregates.refresh('stats')
        self.append('d2', [row(2, '2026-11-02', 'a', '2'), row(3, '2026-11-02', 'b', '4')])
        before = self.lake.aggregates.status('stats')
        with patch.object(self.lake.objects, 'write_manifest', side_effect=OSError('author fault')):
            with self.assertRaises(OSError):
                self.lake.aggregates.refresh('stats')
        self.assertEqual(self.lake.aggregates.status('stats'), before)
        self.assertEqual(self.lake.catalog.table('site_stats')['head'], before['target_head'])
        self.assertEqual(len(self.lake.aggregates.history('stats')), 1)
        result = self.lake.aggregates.refresh('stats')
        self.assertEqual(result['input_rows'], 2)
        stats = {r['site']: r for r in self.actual()}
        self.assertEqual((stats['a']['total'], stats['a']['n']), ('3', 2))
        self.assertEqual((stats['b']['total'], stats['b']['n']), ('4', 1))
        self.assertEqual(self.actual(), self.expected())

    def test_older_snapshot_cannot_overwrite_newer_watermark(self):
        self.append('d1', [row(1, '2026-11-01', 'a', '1')])
        self.lake.aggregates.refresh('stats')
        self.append('d2', [row(2, '2026-11-02', 'a', '2')])
        self.lake.aggregates.refresh('stats')
        with self.assertRaises(Conflict):
            self.lake.aggregates.refresh('stats', snapshot_id=1)
        stats = {r['site']: r for r in self.actual()}
        self.assertEqual(stats['a']['total'], '3')
        self.assertEqual(self.lake.aggregates.status('stats')['processed_snapshot'], 2)

    def test_concurrent_refresh_never_lets_older_win(self):
        self.append('d1', [row(1, '2026-11-01', 'a', '1')])
        self.append('d2', [row(2, '2026-11-02', 'a', '2')])
        outcomes = {}
        def run(name, snapshot):
            try:
                outcomes[name] = ('ok', self.lake.aggregates.refresh('stats', snapshot_id=snapshot))
            except Conflict:
                outcomes[name] = ('conflict', None)
        threads = [threading.Thread(target=run, args=('newer', 2)),
                   threading.Thread(target=run, args=('older', 1))]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()
        self.assertEqual(outcomes['newer'][0], 'ok')
        self.assertEqual(self.lake.aggregates.status('stats')['processed_snapshot'], 2)
        stats = {r['site']: r for r in self.actual()}
        self.assertEqual((stats['a']['total'], stats['a']['n']), ('3', 2))
        self.assertEqual(self.actual(), self.expected())

    def test_replay_when_watermark_current(self):
        self.append('d1', [row(1, '2026-11-01', 'a', '1')])
        first = self.lake.aggregates.refresh('stats')
        second = self.lake.aggregates.refresh('stats')
        self.assertTrue(second['replayed'])
        self.assertEqual(second['output_snapshot'], first['output_snapshot'])
        self.assertEqual(self.lake.catalog.table('site_stats')['head'], first['output_snapshot'])
        self.assertEqual(len(self.lake.aggregates.history('stats')), 1)
        self.assertTrue(self.lake.aggregates.refresh('stats', snapshot_id=1)['replayed'])

    def test_refresh_on_empty_source_establishes_watermark(self):
        first = self.lake.aggregates.refresh('stats')
        self.assertFalse(first['replayed'])
        self.assertEqual((first['input_rows'], first['output_rows']), (0, 0))
        self.assertEqual(self.actual(), [])
        second = self.lake.aggregates.refresh('stats')
        self.assertTrue(second['replayed'])
        self.assertEqual(second['output_snapshot'], first['output_snapshot'])
        self.assertEqual(len(self.lake.aggregates.history('stats')), 1)

    def test_grand_total_matches_full_recompute(self):
        grand = {'fields': [{'id': 1, 'name': 'n', 'type': 'int64'},
                            {'id': 2, 'name': 'total', 'type': 'decimal'},
                            {'id': 3, 'name': 'hi', 'type': 'decimal', 'nullable': True}]}
        self.lake.catalog.create_table('grand', grand)
        self.lake.aggregates.register('grand', 'readings', 'grand',
                                      {'group_by': [], 'aggregates': {
                                          'n': {'op': 'count', 'field': 'reading'},
                                          'total': {'op': 'sum', 'field': 'reading'},
                                          'hi': {'op': 'max', 'field': 'reading'}}})
        first = self.lake.aggregates.refresh('grand')
        self.assertEqual(first['output_rows'], 1)
        self.assertEqual(list(self.lake.scan.rows('grand')),
                         [{'n': 0, 'total': '0', 'hi': None}])
        self.append('d1', [row(1, '2026-11-01', 'a', '5'), row(2, '2026-11-01', 'b', '7')])
        self.lake.aggregates.refresh('grand')
        self.assertEqual(list(self.lake.scan.rows('grand')),
                         [{'n': 2, 'total': '12', 'hi': '7'}])
        self.lake.maintenance.drop_partition('readings', {'day': '2026-11-01'})
        self.lake.aggregates.refresh('grand')
        self.assertEqual(list(self.lake.scan.rows('grand')),
                         [{'n': 0, 'total': '0', 'hi': None}])
        self.assertEqual(self.lake.aggregates.preview('grand')['rows'],
                         list(self.lake.scan.rows('grand')))

    def test_expected_target_guard(self):
        self.append('d1', [row(1, '2026-11-01', 'a', '1')])
        with self.assertRaises(Conflict):
            self.lake.aggregates.refresh('stats', expected_target=7)
        result = self.lake.aggregates.refresh('stats', expected_target=0)
        self.assertEqual(result['output_snapshot'], 1)

    def test_status_tracks_watermark_and_pending(self):
        self.append('d1', [row(1, '2026-11-01', 'a', '1'), row(2, '2026-11-01', 'b', '2')])
        status = self.lake.aggregates.status('stats')
        self.assertEqual((status['processed_snapshot'], status['pending']), (0, True))
        self.lake.aggregates.refresh('stats')
        status = self.lake.aggregates.status('stats')
        self.assertEqual((status['processed_snapshot'], status['pending']), (1, False))
        self.assertEqual((status['partitions'], status['groups']), (1, 2))
        self.assertEqual(status['target_snapshot'], self.lake.catalog.table('site_stats')['head'])

    def test_catch_up_snapshot_by_snapshot(self):
        self.append('d1', [row(1, '2026-11-01', 'a', '1')])
        self.append('d2', [row(2, '2026-11-02', 'a', '2')])
        first = self.lake.aggregates.refresh('stats', snapshot_id=1)
        self.assertEqual(first['input_snapshot'], 1)
        stats = {r['site']: r for r in self.actual()}
        self.assertEqual(stats['a']['total'], '1')
        second = self.lake.aggregates.refresh('stats')
        self.assertEqual(second['input_snapshot'], 2)
        stats = {r['site']: r for r in self.actual()}
        self.assertEqual(stats['a']['total'], '3')
        self.assertEqual(self.actual(), self.expected())

    def test_sequence_of_operations_stays_consistent(self):
        steps = [
            ('append', 's1', [row(1, '2026-11-01', 'a', '1'), row(2, '2026-11-01', 'b', '10')]),
            ('append', 's2', [row(3, '2026-11-02', 'a', '2'), row(4, '2026-11-02', 'c', None)]),
            ('replace', {'day': '2026-11-01'}, [row(5, '2026-11-01', 'b', '20')]),
            ('append', 's3', [row(6, '2026-11-03', 'a', '3'), row(7, '2026-11-03', 'b', '30'),
                              row(8, '2026-11-03', 'c', '4')]),
            ('drop', {'day': '2026-11-02'}, None),
            ('replace', {'day': '2026-11-03'}, [row(9, '2026-11-03', 'c', '5')]),
        ]
        for step in steps:
            if step[0] == 'append':
                self.append(step[1], step[2])
            elif step[0] == 'replace':
                self.lake.maintenance.replace('readings', step[2], step[1])
            else:
                self.lake.maintenance.drop_partition('readings', step[1])
            self.lake.aggregates.refresh('stats')
            self.assertEqual(self.actual(), self.expected())
        stats = {r['site']: r for r in self.actual()}
        self.assertNotIn('a', stats)
        self.assertEqual((stats['b']['total'], stats['b']['n']), ('20', 1))
        self.assertEqual((stats['c']['total'], stats['c']['n']), ('5', 1))
        self.assertTrue(self.lake.diagnostics.check('site_stats')['ok'])

    def test_multi_column_and_int_group_keys(self):
        daily = {'fields': [{'id': 1, 'name': 'day', 'type': 'date'},
                            {'id': 2, 'name': 'site', 'type': 'string'},
                            {'id': 3, 'name': 'n', 'type': 'int64'},
                            {'id': 4, 'name': 'total', 'type': 'decimal'}],
                 'primary_key': ['day', 'site']}
        self.lake.catalog.create_table('daily_stats', daily)
        self.lake.aggregates.register('daily', 'readings', 'daily_stats',
                                      {'group_by': ['day', 'site'],
                                       'aggregates': {'n': {'op': 'count', 'field': 'reading'},
                                                      'total': {'op': 'sum', 'field': 'reading'}}})
        by_id = {'fields': [{'id': 1, 'name': 'id', 'type': 'int64'},
                            {'id': 2, 'name': 'n', 'type': 'int64'}], 'primary_key': ['id']}
        self.lake.catalog.create_table('by_id', by_id)
        self.lake.aggregates.register('per_id', 'readings', 'by_id',
                                      {'group_by': ['id'], 'aggregates': {'n': {'op': 'count', 'field': 'reading'}}})
        self.append('d1', [row(1, '2026-11-01', 'a', '1'), row(2, '2026-11-01', 'b', '2')])
        self.append('d2', [row(3, '2026-11-02', 'a', '3')])
        self.lake.aggregates.refresh('daily')
        self.lake.aggregates.refresh('per_id')
        self.lake.maintenance.replace('readings', [row(4, '2026-11-01', 'a', '9')], {'day': '2026-11-01'})
        self.assertTrue(self.lake.aggregates.status('per_id')['pending'])
        result = self.lake.aggregates.refresh('daily')
        self.assertEqual(result['partitions_changed'], 1)
        daily_rows = sorted(self.lake.scan.rows('daily_stats'), key=lambda r: (r['day'], r['site']))
        self.assertEqual(daily_rows, [
            {'day': '2026-11-01', 'site': 'a', 'n': 1, 'total': '9'},
            {'day': '2026-11-02', 'site': 'a', 'n': 1, 'total': '3'},
        ])
        self.lake.aggregates.refresh('per_id')
        self.assertEqual(sorted(r['id'] for r in self.lake.scan.rows('by_id')), [3, 4])
        self.assertFalse(self.lake.aggregates.status('per_id')['pending'])

    def test_unpartitioned_source_stays_consistent(self):
        plain = {'fields': [{'id': 1, 'name': 'id', 'type': 'int64'},
                            {'id': 2, 'name': 'site', 'type': 'string'},
                            {'id': 3, 'name': 'reading', 'type': 'decimal', 'nullable': True}],
                 'primary_key': ['id']}
        self.lake.catalog.create_table('plain_readings', plain)
        self.lake.catalog.create_table('plain_stats', TARGET)
        self.lake.aggregates.register('plain', 'plain_readings', 'plain_stats', PLAN)
        self.lake.ingest.ingest('plain_readings', 'p1', [{'id': 1, 'site': 'a', 'reading': '1'}])
        self.lake.aggregates.refresh('plain')
        self.lake.ingest.ingest('plain_readings', 'p2', [{'id': 2, 'site': 'a', 'reading': '2'}])
        result = self.lake.aggregates.refresh('plain')
        self.assertEqual(result['input_rows'], 2)
        stats = {r['site']: r for r in self.lake.scan.rows('plain_stats')}
        self.assertEqual(stats['a']['total'], '3')
        self.assertEqual(self.lake.aggregates.status('plain')['processed_snapshot'], 2)

    def test_unchanged_partition_objects_are_never_touched(self):
        self.append('d1', [row(1, '2026-11-01', 'a', '1')])
        self.append('d2', [row(2, '2026-11-02', 'b', '2')])
        self.lake.aggregates.refresh('stats')
        self.lake.maintenance.replace('readings', [row(3, '2026-11-01', 'a', '5')], {'day': '2026-11-01'})
        day2 = [f for f in self.lake.scan.plan('readings')['files'] if f['partition'] == {'day': '2026-11-02'}]
        self.assertEqual(len(day2), 1)
        self.lake.objects.path(day2[0]['path']).unlink()
        result = self.lake.aggregates.refresh('stats')
        self.assertEqual((result['partitions_changed'], result['input_rows']), (1, 1))
        stats = {r['site']: r for r in self.actual()}
        self.assertEqual((stats['a']['total'], stats['b']['total']), ('5', '2'))
        self.assertEqual((stats['a']['n'], stats['b']['n']), (1, 1))

    def test_incremental_refresh_over_mixed_encodings(self):
        self.append('d1', [row(1, '2026-11-01', 'a', '1')])
        compressed = Lake(self.lake.home, file_rows=1, encoding='gzip-jsonl')
        compressed.ingest.ingest('readings', 'd2', [row(2, '2026-11-02', 'a', '2')])
        self.lake.aggregates.refresh('stats')
        stats = {r['site']: r for r in self.actual()}
        self.assertEqual(stats['a']['total'], '3')
        self.lake.maintenance.replace('readings', [row(3, '2026-11-02', 'a', '5')], {'day': '2026-11-02'})
        result = self.lake.aggregates.refresh('stats')
        self.assertEqual(result['input_rows'], 1)
        stats = {r['site']: r for r in self.actual()}
        self.assertEqual(stats['a']['total'], '6')

    def test_http_aggregate_status_and_history(self):
        http = server(self.lake, port=0)
        thread = threading.Thread(target=http.serve_forever, daemon=True)
        thread.start()
        url = f'http://127.0.0.1:{http.server_port}'
        try:
            self.append('d1', [row(1, '2026-11-01', 'a', '1')])
            self.lake.aggregates.refresh('stats')
            with urlopen(url + '/aggregates/stats/status') as response:
                self.assertEqual(json.load(response)['processed_snapshot'], 1)
            with urlopen(url + '/aggregates/stats/history') as response:
                self.assertEqual(len(json.load(response)), 1)
        finally:
            http.shutdown()
            http.server_close()
            thread.join(5)


if __name__ == '__main__':
    unittest.main()
