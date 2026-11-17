from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import patch
import json
import unittest

from schemalake.app import Lake
from schemalake.predicates import Predicate
from test_core import SCHEMA, row


class PruningTest(unittest.TestCase):
    def setUp(self):
        self.temporary = TemporaryDirectory()
        self.root = Path(self.temporary.name)
        self.lake = Lake(self.root / 'lake', file_rows=1)
        self.lake.catalog.create_table('readings', SCHEMA)

    def tearDown(self):
        self.temporary.cleanup()

    def ingest(self, batch, rows, lake=None):
        return (lake or self.lake).ingest.ingest('readings', batch, rows)

    def run_where(self, expression, snapshot_id=None, lake=None):
        lake = lake or self.lake
        original = lake.objects.read_rows
        calls = []

        def counting(file):
            calls.append(file['path'])
            yield from original(file)

        with patch.object(lake.objects, 'read_rows', counting):
            result = list(lake.scan.where('readings', expression, snapshot_id))
        return result, calls

    def test_condition_matching_last_file_reads_only_that_file(self):
        self.ingest('bulk', [row(i, reading=str(i)) for i in range(1, 41)])
        self.assertEqual(len(self.lake.scan.plan('readings')['files']), 40)
        result, calls = self.run_where({'op': 'eq', 'field': 'id', 'value': 40})
        self.assertEqual([r['id'] for r in result], [40])
        self.assertEqual(len(calls), 1)
        result, calls = self.run_where({'op': 'gt', 'field': 'id', 'value': 38})
        self.assertEqual([r['id'] for r in result], [39, 40])
        self.assertEqual(len(calls), 2)

    def test_pruned_results_match_row_level_filter(self):
        rows = []
        for i in range(1, 21):
            reading = None if i % 5 == 0 else ('12.500' if i == 3 else str(i))
            rows.append(row(i, day='2026-11-%02d' % (1 + i % 2), site='abc'[i % 3], reading=reading))
        self.ingest('mixed', rows)
        schema = self.lake.catalog.schema('readings')
        battery = [
            {'op': 'eq', 'field': 'reading', 'value': '12.5'},
            {'op': 'eq', 'field': 'reading', 'value': None},
            {'op': 'ne', 'field': 'reading', 'value': '12.5'},
            {'op': 'lt', 'field': 'reading', 'value': '3'},
            {'op': 'le', 'field': 'id', 'value': 7},
            {'op': 'gt', 'field': 'id', 'value': 18},
            {'op': 'ge', 'field': 'reading', 'value': '19'},
            {'op': 'in', 'field': 'id', 'values': [3, 7, None]},
            {'op': 'in', 'field': 'reading', 'values': ['12.50', '2.5', None]},
            {'op': 'in', 'field': 'id', 'values': []},
            {'op': 'is_null', 'field': 'reading'},
            {'op': 'not_null', 'field': 'reading'},
            {'op': 'not', 'arg': {'op': 'eq', 'field': 'site', 'value': 'a'}},
            {'op': 'not', 'arg': {'op': 'is_null', 'field': 'reading'}},
            {'op': 'not', 'arg': {'op': 'not', 'arg': {'op': 'in', 'field': 'id', 'values': [1, 2]}}},
            {'op': 'and', 'args': [{'op': 'ge', 'field': 'id', 'value': 5},
                                   {'op': 'lt', 'field': 'id', 'value': 8},
                                   {'op': 'not_null', 'field': 'reading'}]},
            {'op': 'or', 'args': [{'op': 'eq', 'field': 'site', 'value': 'z'},
                                  {'op': 'is_null', 'field': 'reading'}]},
            {'op': 'and', 'args': [{'op': 'eq', 'field': 'reading', 'value': None}]},
            {'op': 'or', 'args': [{'op': 'and', 'args': [{'op': 'gt', 'field': 'reading', 'value': '9'},
                                                         {'op': 'not', 'arg': {'op': 'eq', 'field': 'site', 'value': 'a'}}]},
                                  {'op': 'eq', 'field': 'id', 'value': 1}]},
        ]
        total_reads = 0
        for expression in battery:
            reference = Predicate(schema, expression)
            expected = [r for r in self.lake.scan.rows('readings') if reference.matches(r)]
            actual, calls = self.run_where(expression)
            total_reads += len(calls)
            self.assertEqual(actual, expected, json.dumps(expression))
            self.assertLessEqual(len(calls), 20, json.dumps(expression))
        self.assertLess(total_reads, 20 * len(battery))

    def test_all_null_column_file_pruning(self):
        self.ingest('nulls', [row(1, reading=None), row(2, reading=None)])
        self.ingest('values', [row(3, reading='5')])
        result, calls = self.run_where({'op': 'is_null', 'field': 'reading'})
        self.assertEqual(([r['id'] for r in result], len(calls)), ([1, 2], 2))
        result, calls = self.run_where({'op': 'eq', 'field': 'reading', 'value': '5'})
        self.assertEqual(([r['id'] for r in result], len(calls)), ([3], 1))
        result, calls = self.run_where({'op': 'not', 'arg': {'op': 'eq', 'field': 'reading', 'value': '4'}})
        self.assertEqual(([r['id'] for r in result], len(calls)), ([3], 1))
        result, calls = self.run_where({'op': 'not', 'arg': {'op': 'eq', 'field': 'reading', 'value': '5'}})
        self.assertEqual((result, len(calls)), ([], 0))
        result, calls = self.run_where({'op': 'in', 'field': 'reading', 'values': [None, '4']})
        self.assertEqual((result, len(calls)), ([], 0))

    def test_decimal_bounds_compare_exactly(self):
        self.ingest('b1', [row(1, reading='1.10')])
        self.ingest('b2', [row(2, reading='1.20')])
        result, calls = self.run_where({'op': 'eq', 'field': 'reading', 'value': '1.1'})
        self.assertEqual(([r['id'] for r in result], len(calls)), ([1], 1))
        result, calls = self.run_where({'op': 'lt', 'field': 'reading', 'value': '1.10'})
        self.assertEqual((result, len(calls)), ([], 0))
        result, calls = self.run_where({'op': 'le', 'field': 'reading', 'value': '1.1'})
        self.assertEqual(([r['id'] for r in result], len(calls)), ([1], 1))
        result, calls = self.run_where({'op': 'in', 'field': 'reading', 'values': ['1.200']})
        self.assertEqual(([r['id'] for r in result], len(calls)), ([2], 1))

    def test_historical_snapshot_pruning(self):
        self.ingest('b1', [row(i) for i in range(1, 21)])
        self.ingest('b2', [row(i) for i in range(21, 41)])
        result, calls = self.run_where({'op': 'eq', 'field': 'id', 'value': 20}, snapshot_id=1)
        self.assertEqual(([r['id'] for r in result], len(calls)), ([20], 1))
        result, calls = self.run_where({'op': 'eq', 'field': 'id', 'value': 20})
        self.assertEqual(([r['id'] for r in result], len(calls)), ([20], 1))
        result, calls = self.run_where({'op': 'eq', 'field': 'id', 'value': 40}, snapshot_id=1)
        self.assertEqual((result, len(calls)), ([], 0))

    def test_inconclusive_statistics_still_read(self):
        lake = Lake(self.root / 'wide', file_rows=1000)
        lake.catalog.create_table('readings', SCHEMA)
        self.ingest('b', [row(10), row(20)], lake)
        result, calls = self.run_where({'op': 'eq', 'field': 'id', 'value': 15}, lake=lake)
        self.assertEqual((result, len(calls)), ([], 1))
        result, calls = self.run_where({'op': 'in', 'field': 'id', 'values': [12, 15]}, lake=lake)
        self.assertEqual((result, len(calls)), ([], 1))

    def test_filtered_export_matches_scan_and_prunes(self):
        self.ingest('b1', [row(i, reading=str(i)) for i in range(1, 21)])
        self.ingest('b2', [row(i, reading=str(i)) for i in range(21, 41)])
        expression = {'op': 'ge', 'field': 'id', 'value': 38}
        expected = list(self.lake.scan.where('readings', expression))
        original = self.lake.objects.read_rows
        calls = []

        def counting(file):
            calls.append(file['path'])
            yield from original(file)

        with patch.object(self.lake.objects, 'read_rows', counting):
            item = self.lake.exports.create('readings', expression=expression)
        self.assertEqual((item['row_count'], len(calls)), (3, 3))
        path = self.lake.exports.artifact(item['id'])
        self.assertEqual([json.loads(line) for line in path.read_text(encoding='utf-8').splitlines()], expected)
        item = self.lake.exports.create('readings', format='csv', expression=expression)
        lines = self.lake.exports.artifact(item['id']).read_text(encoding='utf-8').splitlines()
        self.assertEqual((item['row_count'], len(lines)), (3, 4))
        with patch.object(self.lake.objects, 'read_rows', counting):
            historical = self.lake.exports.create('readings', snapshot_id=1, expression={'op': 'le', 'field': 'id', 'value': 2})
        self.assertEqual((historical['row_count'], len(calls)), (2, 5))
        self.assertEqual(historical['snapshot_id'], 1)


if __name__ == '__main__':
    unittest.main()
