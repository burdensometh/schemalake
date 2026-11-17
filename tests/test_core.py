from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import patch
import json
import unittest

from schemalake.app import Lake
from schemalake.types import Conflict, Invalid, Missing, Schema, convert


SCHEMA = {'fields': [
    {'id': 1, 'name': 'id', 'type': 'int64'},
    {'id': 2, 'name': 'day', 'type': 'date'},
    {'id': 3, 'name': 'site', 'type': 'string'},
    {'id': 4, 'name': 'reading', 'type': 'decimal', 'nullable': True},
], 'primary_key': ['id'], 'partition_by': ['day']}


def row(id=1, day='2026-11-01', site='a', reading='12.50'):
    return {'id': id, 'day': day, 'site': site, 'reading': reading}


class CoreTest(unittest.TestCase):
    def setUp(self):
        self.temporary = TemporaryDirectory()
        self.root = Path(self.temporary.name)
        self.lake = Lake(self.root / 'lake', file_rows=1)
        self.lake.catalog.create_table('readings', SCHEMA)

    def tearDown(self):
        self.temporary.cleanup()

    def append(self, batch='b1', rows=None):
        return self.lake.ingest.ingest('readings', batch, [row()] if rows is None else rows)

    def test_type_precision_and_identity(self):
        self.assertEqual(convert('1.2300', 'decimal'), '1.23')
        self.assertEqual(convert('-0.000', 'decimal'), '0')
        self.assertEqual(convert('12345678901234567890123456789012345678', 'decimal'), '12345678901234567890123456789012345678')
        with self.assertRaises(Invalid):
            convert(True, 'int64')
        with self.assertRaises(Invalid):
            convert(2**64, 'int64')
        self.assertEqual(convert('2026-11-01T01:00:00+01:00', 'timestamp'), '2026-11-01T00:00:00.000000+00:00')

    def test_schema_defaults_and_unknown_columns(self):
        schema = Schema.parse({'fields': [{'id': 1, 'name': 'n', 'type': 'int64', 'default': 7}]})
        self.assertEqual(schema.normalize({}), {'n': 7})
        with self.assertRaises(Invalid):
            schema.normalize({'other': 1})
        with self.assertRaises(Invalid):
            Schema.parse({'fields': [{'id': 1, 'name': 'x', 'type': 'string', 'nullable': True}], 'primary_key': ['x']})

    def test_append_idempotent_and_conflicting_identity(self):
        first = self.append()
        self.assertEqual(first['snapshot_id'], 1)
        self.assertTrue(self.append()['replayed'])
        with self.assertRaises(Conflict):
            self.append(rows=[row(2)])
        self.assertEqual(len(self.lake.catalog.snapshots('readings')), 1)

    def test_quarantine_preserves_good_rows(self):
        result = self.lake.ingest.ingest('readings', 'b', [row(), row(2, reading='bad')], 'quarantine')
        self.assertEqual((result['row_count'], result['rejected']), (1, 1))
        self.assertEqual(self.lake.catalog.rejects('readings', 'b')[0]['ordinal'], 1)
        self.assertEqual(len(list(self.lake.scan.rows('readings'))), 1)

    def test_reject_policy_rolls_back_whole_batch(self):
        with self.assertRaises(Invalid):
            self.append(rows=[row(), row(2, reading='bad')])
        self.assertEqual(self.lake.catalog.table('readings')['head'], 0)
        self.assertIsNone(self.lake.catalog.batch('readings', 'b1'))

    def test_duplicate_keys_and_expected_head(self):
        self.append()
        with self.assertRaises(Invalid):
            self.append('b2')
        with self.assertRaises(Conflict):
            self.lake.ingest.ingest('readings', 'b3', [row(2)], expected_head=0)
        self.assertEqual(self.lake.catalog.table('readings')['head'], 1)

    def test_partition_projection_and_snapshot_reads(self):
        self.append(rows=[row(), row(2, '2026-11-02', reading=None)])
        self.append('b2', [row(3)])
        self.assertEqual(list(self.lake.scan.rows('readings', 1, {'day': '2026-11-02'}, ['id'])), [{'id': 2}])
        self.assertEqual(self.lake.scan.plan('readings', partition={'day': '2026-11-02'})['files_skipped'], 2)
        self.assertEqual(len(list(self.lake.scan.rows('readings', 1))), 2)

    def test_typed_predicates_and_null_logic(self):
        self.append(rows=[row(1, reading='2'), row(2, reading='10'), row(3, reading=None)])
        self.assertEqual([r['id'] for r in self.lake.scan.where('readings', {'op': 'gt', 'field': 'reading', 'value': '9'})], [2])
        expression = {'op': 'not', 'arg': {'op': 'eq', 'field': 'reading', 'value': '2'}}
        self.assertEqual([r['id'] for r in self.lake.scan.where('readings', expression)], [2])
        self.assertEqual([r['id'] for r in self.lake.scan.where('readings', {'op': 'is_null', 'field': 'reading'})], [3])

    def test_compact_keeps_old_snapshot_and_multiset(self):
        self.append(rows=[row(), row(2)])
        before = list(self.lake.scan.rows('readings'))
        result = self.lake.maintenance.compact('readings', target_rows=100)
        self.assertEqual((result['files_before'], result['files_after']), (2, 1))
        self.assertEqual(list(self.lake.scan.rows('readings', 1)), before)
        self.assertTrue(self.lake.reconcile.between_snapshots('readings', 1, 2)['equivalent'])

    def test_replace_partition_rebuilds_current_keys(self):
        self.append(rows=[row(), row(2, '2026-11-02')])
        self.lake.maintenance.replace('readings', [row(3)], {'day': '2026-11-01'})
        self.append('b2', [row(1)])
        self.assertEqual(sorted(r['id'] for r in self.lake.scan.rows('readings')), [1, 2, 3])
        self.assertEqual(sorted(r['id'] for r in self.lake.scan.rows('readings', 1)), [1, 2])

    def test_replace_conflict_and_bad_partition_rollback(self):
        self.append(rows=[row(), row(2, '2026-11-02')])
        with self.assertRaises(Conflict):
            self.lake.maintenance.replace('readings', [row(2)], {'day': '2026-11-01'})
        with self.assertRaises(Invalid):
            self.lake.maintenance.replace('readings', [row(3, '2026-11-02')], {'day': '2026-11-01'})
        self.assertEqual(self.lake.catalog.table('readings')['head'], 1)

    def test_index_current_and_historical_generation(self):
        self.append()
        self.lake.indexes.build('readings')
        self.assertTrue(self.lake.indexes.lookup('readings', {'id': 1})['used_index'])
        self.append('b2', [row(2)])
        current = self.lake.indexes.lookup('readings', {'id': 2})
        self.assertFalse(current['used_index'])
        self.assertEqual(current['rows'][0]['id'], 2)
        self.assertEqual(self.lake.indexes.lookup('readings', {'id': 1}, 1)['rows'][0]['id'], 1)

    def test_index_build_rejects_racing_head_change(self):
        self.append()
        original = self.lake.objects.read_rows
        triggered = False
        def intercept(file):
            nonlocal triggered
            if not triggered:
                triggered = True
                self.append('during', [row(2)])
            yield from original(file)
        with patch.object(self.lake.objects, 'read_rows', intercept):
            with self.assertRaises(Conflict):
                self.lake.indexes.build('readings')
        self.assertFalse(self.lake.indexes.status('readings')['usable'])

    def test_index_range_uses_typed_order(self):
        self.append(rows=[row(10), row(2), row(3)])
        self.lake.indexes.build('readings')
        result = self.lake.indexes.range('readings', {'id': 2}, {'id': 10})
        self.assertTrue(result['used_index'])
        self.assertEqual([r['id'] for r in result['rows']], [2, 3])

    def test_manifest_failure_never_publishes_partial_snapshot(self):
        self.append()
        with patch.object(self.lake.objects, 'write_manifest', side_effect=OSError('author fault')):
            with self.assertRaises(OSError):
                self.append('failed', [row(2)])
        self.assertEqual(self.lake.catalog.table('readings')['head'], 1)
        self.assertIsNone(self.lake.catalog.batch('readings', 'failed'))
        self.assertEqual([r['id'] for r in self.lake.scan.rows('readings')], [1])
        self.assertGreater(len(self.lake.diagnostics.inventory()['unreferenced']), 0)
        self.assertTrue(self.lake.diagnostics.check('readings')['ok'])

    def test_preview_is_read_only(self):
        value = self.lake.plans.preview_ingest('readings', [row(), row(1)])
        self.assertEqual((value['accepted'], value['rejected']), (1, 1))
        self.assertEqual(self.lake.catalog.table('readings')['head'], 0)


if __name__ == '__main__':
    unittest.main()
