from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import patch
from urllib.request import Request, urlopen
import json
import threading
import unittest

from schemalake.app import Lake
from schemalake.api import server
from schemalake.mirror import Mirror
from schemalake.types import Conflict, Invalid, Missing
from test_core import SCHEMA, row


def upsert(key, version, partition=0, position=0, **fields):
    return {'op': 'upsert', 'row': row(key, **fields), 'version': version,
            'partition': partition, 'position': position}


def delete(key, version, partition=0, position=0):
    return {'op': 'delete', 'key': {'id': key}, 'version': version,
            'partition': partition, 'position': position}


class ChangesTest(unittest.TestCase):
    def setUp(self):
        self.temporary = TemporaryDirectory()
        self.root = Path(self.temporary.name)
        self.lake = Lake(self.root / 'lake', file_rows=10)
        self.lake.catalog.create_table('readings', SCHEMA)

    def tearDown(self):
        self.temporary.cleanup()

    def ingest(self, batch, events, **kwargs):
        return self.lake.changes.ingest('readings', batch, events, **kwargs)

    def ids(self, snapshot=None):
        return sorted(item['id'] for item in self.lake.scan.rows('readings', snapshot))

    def test_upsert_delete_flow_and_old_snapshots(self):
        first = self.ingest('c1', [upsert(1, 1, position=0), upsert(2, 1, position=1)])
        self.assertEqual((first['snapshot_id'], first['applied']), (1, 2))
        self.assertEqual(self.ids(), [1, 2])
        second = self.ingest('c2', [upsert(1, 2, position=2, reading='99'), delete(2, 2, position=3)])
        self.assertEqual((second['snapshot_id'], second['applied']), (2, 2))
        self.assertEqual(self.ids(), [1])
        self.assertEqual(list(self.lake.scan.rows('readings'))[0]['reading'], '99')
        self.assertEqual(self.ids(1), [1, 2])
        self.assertTrue(self.lake.diagnostics.check('readings')['ok'])

    def test_update_and_delete_reach_appended_rows(self):
        self.lake.ingest.ingest('readings', 'seed', [row(1), row(2)])
        result = self.ingest('c1', [upsert(1, 1, position=0, reading='7'), delete(2, 1, position=1)])
        self.assertEqual(result['applied'], 2)
        self.assertEqual([(r['id'], r['reading']) for r in self.lake.scan.rows('readings')], [(1, '7')])
        self.lake.ingest.ingest('readings', 'reseed', [row(2)])
        self.assertEqual(self.ids(), [1, 2])
        with self.assertRaises(Invalid):
            self.lake.ingest.ingest('readings', 'dupe', [row(1)])

    def test_batch_replay_and_conflicting_identity(self):
        first = self.ingest('c1', [upsert(1, 1)])
        self.assertFalse(first['replayed'])
        replay = self.ingest('c1', [upsert(1, 1)])
        self.assertTrue(replay['replayed'])
        self.assertEqual(replay['snapshot_id'], first['snapshot_id'])
        with self.assertRaises(Conflict):
            self.ingest('c1', [upsert(2, 1)])
        self.assertEqual(len(self.lake.catalog.snapshots('readings')), 1)

    def test_refragmented_redelivery_has_no_extra_effect(self):
        self.ingest('c1', [upsert(1, 1, position=0), upsert(2, 1, position=1)])
        result = self.ingest('c2', [upsert(1, 1, position=0), upsert(2, 1, position=1), upsert(3, 1, position=2)])
        self.assertEqual((result['applied'], result['skipped']), (1, 2))
        self.assertEqual(self.ids(), [1, 2, 3])
        head = self.lake.catalog.table('readings')['head']
        again = self.ingest('c3', [upsert(1, 1, position=0)])
        self.assertEqual((again['applied'], again['skipped'], again['snapshot_created']), (0, 1, False))
        self.assertEqual(self.lake.catalog.table('readings')['head'], head)
        self.assertEqual(self.lake.changes.progress('readings')['committed_positions'], 3)

    def test_redelivery_with_different_content_conflicts(self):
        self.ingest('c1', [upsert(1, 1, position=0)])
        with self.assertRaises(Conflict):
            self.ingest('c2', [upsert(1, 1, position=0, reading='99')])
        with self.assertRaises(Conflict):
            self.ingest('c3', [upsert(1, 1, position=1, reading='99')])
        result = self.ingest('c4', [upsert(1, 1, position=1)])
        self.assertEqual((result['applied'], result['skipped']), (0, 1))
        self.assertEqual(self.lake.changes.progress('readings')['committed_positions'], 2)

    def test_late_update_after_delete_does_not_resurrect(self):
        self.ingest('c1', [upsert(1, 5, position=0)])
        self.ingest('c2', [delete(1, 6, position=1)])
        self.assertEqual(self.ids(), [])
        late = self.ingest('c3', [upsert(1, 4, position=2)])
        self.assertEqual((late['applied'], late['skipped']), (0, 1))
        self.assertEqual(self.ids(), [])
        newer = self.ingest('c4', [upsert(1, 7, position=3)])
        self.assertEqual(newer['applied'], 1)
        self.assertEqual(self.ids(), [1])

    def test_delete_of_unknown_key_blocks_older_upsert(self):
        result = self.ingest('c1', [delete(9, 3, position=0)])
        self.assertEqual((result['applied'], result['snapshot_created']), (1, False))
        late = self.ingest('c2', [upsert(9, 2, position=1)])
        self.assertEqual(late['applied'], 0)
        self.assertEqual(self.ids(), [])

    def test_position_gap_and_partition_progress(self):
        with self.assertRaises(Conflict):
            self.ingest('c1', [upsert(1, 1, position=1)])
        self.assertEqual(self.lake.catalog.table('readings')['head'], 0)
        self.assertEqual(self.lake.changes.progress('readings')['committed_positions'], 0)
        self.ingest('c2', [upsert(1, 1, partition=0, position=0), upsert(2, 1, partition=1, position=0),
                           upsert(3, 2, partition=0, position=1)])
        progress = self.lake.changes.progress('readings')
        self.assertEqual({p['partition']: p['next_position'] for p in progress['partitions']}, {0: 2, 1: 1})
        self.assertEqual(progress['committed_positions'], 3)

    def test_key_is_fixed_to_one_source_partition(self):
        self.ingest('c1', [upsert(1, 1, partition=0, position=0)])
        with self.assertRaises(Conflict):
            self.ingest('c2', [upsert(1, 2, partition=1, position=0)])
        self.ingest('c3', [upsert(2, 1, partition=1, position=0)])
        self.assertEqual(self.ids(), [1, 2])

    def test_version_arbitration_inside_one_batch(self):
        result = self.ingest('c1', [upsert(1, 2, position=0, reading='2'), upsert(1, 1, position=1, reading='1')])
        self.assertEqual((result['applied'], result['skipped']), (1, 1))
        self.assertEqual(list(self.lake.scan.rows('readings'))[0]['reading'], '2')
        with self.assertRaises(Conflict):
            self.ingest('c2', [upsert(2, 1, position=2, reading='1'), upsert(2, 1, position=3, reading='2')])
        collapsed = self.ingest('c3', [upsert(3, 1, position=2), upsert(3, 1, position=2)])
        self.assertEqual((collapsed['applied'], collapsed['skipped']), (1, 1))
        self.assertEqual(self.ids(), [1, 3])

    def test_upsert_moves_row_between_partitions(self):
        self.ingest('c1', [upsert(1, 1, position=0)])
        self.ingest('c2', [upsert(1, 2, position=1, day='2026-11-02')])
        rows = list(self.lake.scan.rows('readings'))
        self.assertEqual([(r['id'], r['day']) for r in rows], [(1, '2026-11-02')])
        partitions = self.lake.maintenance.partitions('readings')
        self.assertEqual([p['partition'] for p in partitions], [{'day': '2026-11-02'}])
        self.assertTrue(self.lake.diagnostics.check('readings')['ok'])

    def test_failed_commit_leaves_no_partial_state(self):
        self.ingest('c1', [upsert(1, 1, position=0), upsert(2, 1, position=1)])
        with patch.object(self.lake.objects, 'write_manifest', side_effect=OSError('author fault')):
            with self.assertRaises(OSError):
                self.ingest('c2', [upsert(1, 2, position=2, reading='99')])
        self.assertEqual(self.lake.catalog.table('readings')['head'], 1)
        self.assertEqual(self.lake.changes.progress('readings')['committed_positions'], 2)
        with self.assertRaises(Missing):
            self.lake.changes.receipt('readings', 'c2')
        self.assertEqual(list(self.lake.scan.rows('readings'))[0]['reading'], '12.5')
        retry = self.ingest('c2', [upsert(1, 2, position=2, reading='99')])
        self.assertEqual(retry['snapshot_id'], 2)
        self.assertEqual(list(self.lake.scan.rows('readings'))[0]['reading'], '99')
        self.assertTrue(self.lake.diagnostics.check('readings')['ok'])

    def test_replay_determinism_across_lakes_and_mirror(self):
        other = Lake(self.root / 'other', file_rows=10)
        other.catalog.create_table('readings', SCHEMA)
        for lake in (self.lake, other):
            lake.changes.ingest('readings', 'c1', [upsert(1, 1, position=0), upsert(2, 1, position=1)])
            lake.changes.ingest('readings', 'c2', [delete(1, 2, position=2), upsert(3, 1, position=3)])
        heads = [lake.catalog.snapshots('readings')[0] for lake in (self.lake, other)]
        self.assertEqual(heads[0]['manifest_sha256'], heads[1]['manifest_sha256'])
        mirror = Mirror(self.root / 'mirror.sqlite')
        mirror.apply(self.lake.scan, 'readings')
        self.assertTrue(mirror.apply(other.scan, 'readings')['replayed'])
        self.assertEqual(mirror.rows('readings'), list(self.lake.scan.rows('readings')))
        self.assertTrue(self.lake.reconcile.against_mirror(mirror, 'readings')['ok'])

    def test_crash_replay_keeps_downstream_materialization(self):
        self.ingest('c1', [upsert(1, 1, position=0), upsert(2, 1, position=1)])
        mirror = Mirror(self.root / 'mirror.sqlite')
        mirror.apply(self.lake.scan, 'readings')
        replay = self.ingest('c2', [upsert(1, 1, position=0), upsert(2, 1, position=1), upsert(3, 1, position=2)])
        self.assertEqual((replay['applied'], replay['skipped']), (1, 2))
        mirror.apply(self.lake.scan, 'readings')
        self.assertEqual([r['id'] for r in mirror.rows('readings')], [1, 2, 3])
        fresh = Mirror(self.root / 'fresh.sqlite')
        fresh.apply(self.lake.scan, 'readings')
        self.assertEqual(fresh.rows('readings'), mirror.rows('readings'))
        self.assertTrue(self.lake.reconcile.against_mirror(mirror, 'readings')['ok'])

    def test_compact_and_replace_still_work_after_changes(self):
        self.ingest('c1', [upsert(1, 1, position=0), upsert(2, 1, position=1, day='2026-11-02')])
        self.lake.maintenance.compact('readings')
        self.assertEqual(self.ids(), [1, 2])
        self.ingest('c2', [delete(1, 2, position=2)])
        self.assertEqual(self.ids(), [2])
        self.assertTrue(self.lake.diagnostics.check('readings')['ok'])

    def test_invalid_events_and_missing_primary_key(self):
        bad = [
            {'op': 'noop', 'version': 1, 'partition': 0, 'position': 0},
            {'op': 'upsert', 'row': row(1), 'partition': 0, 'position': 0},
            {'op': 'upsert', 'row': row(1), 'version': True, 'partition': 0, 'position': 0},
            {'op': 'upsert', 'row': row(1), 'version': 1, 'partition': 0, 'position': -1},
            {'op': 'upsert', 'version': 1, 'partition': 0, 'position': 0},
            {'op': 'upsert', 'row': row(1), 'key': {'id': 2}, 'version': 1, 'partition': 0, 'position': 0},
            {'op': 'delete', 'row': row(1), 'key': {'id': 1}, 'version': 1, 'partition': 0, 'position': 0},
            {'op': 'delete', 'key': {'id': 1, 'site': 'a'}, 'version': 1, 'partition': 0, 'position': 0},
            {'op': 'upsert', 'row': row(1), 'version': 1, 'partition': 0, 'position': 0, 'extra': 1},
        ]
        for ordinal, event in enumerate(bad):
            with self.assertRaises(Invalid, msg=f'event {ordinal}'):
                self.ingest(f'bad{ordinal}', [event])
        self.assertEqual(self.lake.catalog.table('readings')['head'], 0)
        self.lake.catalog.create_table('plain', {'fields': [{'id': 1, 'name': 'x', 'type': 'int64'}]})
        with self.assertRaises(Invalid):
            self.lake.changes.ingest('plain', 'c1', [
                {'op': 'upsert', 'row': {'x': 1}, 'version': 1, 'partition': 0, 'position': 0}])

    def test_expected_head_and_empty_batch(self):
        self.ingest('c1', [upsert(1, 1, position=0)])
        with self.assertRaises(Conflict):
            self.ingest('c2', [upsert(2, 1, position=1)], expected_head=0)
        self.ingest('c3', [upsert(2, 1, position=1)], expected_head=1)
        self.assertEqual(self.ids(), [1, 2])
        empty = self.ingest('c4', [])
        self.assertEqual((empty['applied'], empty['snapshot_created']), (0, False))
        self.assertTrue(self.ingest('c4', [])['replayed'])

    def test_http_change_ingest_and_progress(self):
        http = server(self.lake, port=0)
        thread = threading.Thread(target=http.serve_forever, daemon=True)
        thread.start()
        url = f'http://127.0.0.1:{http.server_port}'
        try:
            request = Request(url + '/tables/readings/changes',
                              data=json.dumps({'batch_id': 'http', 'events': [upsert(1, 1)]}).encode(),
                              headers={'Content-Type': 'application/json'})
            with urlopen(request) as response:
                self.assertEqual(json.load(response)['applied'], 1)
            with urlopen(url + '/tables/readings/changes') as response:
                self.assertEqual(json.load(response)['committed_positions'], 1)
        finally:
            http.shutdown()
            http.server_close()
            thread.join(5)


if __name__ == '__main__':
    unittest.main()
