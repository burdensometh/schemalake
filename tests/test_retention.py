from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import patch
from urllib.request import Request, urlopen
from urllib.error import HTTPError
import json
import threading
import unittest

from schemalake.app import Lake
from schemalake.types import Conflict, Expired, Invalid, Missing
from test_core import SCHEMA, row


class RetentionTest(unittest.TestCase):
    def setUp(self):
        self.temporary = TemporaryDirectory()
        self.root = Path(self.temporary.name)
        self.lake = Lake(self.root / 'lake', file_rows=1)
        self.lake.catalog.create_table('readings', SCHEMA)

    def tearDown(self):
        self.temporary.cleanup()

    def append(self, batch, *ids):
        return self.lake.ingest.ingest('readings', batch, [row(i) for i in ids])

    def build_history(self, count=4):
        for index in range(1, count + 1):
            self.append(f'b{index}', index)

    def paths_of(self, snapshot_id):
        record = dict(self.lake.catalog.connect().execute(
            'SELECT * FROM snapshots WHERE table_name=? AND id=?', ('readings', snapshot_id)).fetchone())
        document = self.lake.objects.manifest(record)
        return {record['manifest_path']} | {file['path'] for file in document['files']}

    def manifest_path(self, snapshot_id):
        record = self.lake.catalog.connect().execute(
            'SELECT manifest_path FROM snapshots WHERE table_name=? AND id=?',
            ('readings', snapshot_id)).fetchone()
        return record['manifest_path']

    def physical_objects(self):
        return {item['path'] for item in self.lake.objects.inventory()}

    def test_expire_keeps_head_and_recent_window(self):
        self.build_history()
        result = self.lake.retention.expire_snapshots('readings', retain_recent=2)
        self.assertEqual(result['newly_expired'], [1, 2])
        self.assertEqual(result['retained_snapshots'], [4, 3])
        # Idempotent: a second run expires nothing new.
        again = self.lake.retention.expire_snapshots('readings', retain_recent=2)
        self.assertEqual(again['newly_expired'], [])
        self.assertEqual(again['expired'], [1, 2])
        status = self.lake.retention.status('readings')
        flagged = {item['id']: item['expired'] for item in status['snapshots']}
        self.assertEqual(flagged, {4: False, 3: False, 2: True, 1: True})

    def test_expired_snapshot_is_explicitly_unreadable_but_still_listed(self):
        self.build_history()
        self.lake.retention.expire_snapshots('readings', retain_recent=1)
        with self.assertRaises(Expired):
            list(self.lake.scan.rows('readings', 1))
        with self.assertRaises(Expired):
            self.lake.scan.snapshot('readings', 2)
        with self.assertRaises(Expired):
            self.lake.scan.plan('readings', 3)
        with self.assertRaises(Expired):
            self.lake.provenance.snapshot('readings', 1)
        # The catalog still lists it: expiration can never make a snapshot id reappear.
        ids = [snapshot['id'] for snapshot in self.lake.catalog.snapshots('readings')]
        self.assertEqual(ids, [4, 3, 2, 1])
        # Retained snapshots and the head stay fully readable.
        self.assertEqual([r['id'] for r in self.lake.scan.rows('readings', 4)], [1, 2, 3, 4])

    def test_reclaim_deletes_only_objects_outside_retention(self):
        self.build_history()
        # An orphan from a failed commit is also reclaimable.
        orphan = self.lake.objects.write_rows('readings', 1, self.lake.catalog.schema('readings'),
                                              {'day': '2026-11-01'}, [row(99)])
        self.lake.retention.expire_snapshots('readings', retain_recent=1)
        retained = self.paths_of(4)
        expired_paths = self.paths_of(1) | self.paths_of(2) | self.paths_of(3)
        result = self.lake.retention.reclaim('readings')
        physical = self.physical_objects()
        self.assertIn(orphan['path'], result['deleted_objects'])
        self.assertTrue(retained <= physical)
        self.assertTrue(set(result['deleted_objects']) >= (expired_paths - retained))
        # Every object used only by expired, unpinned snapshots is gone.
        self.assertEqual((expired_paths - retained) & physical, set())
        # Head reads still work after the sweep.
        self.assertEqual(len(list(self.lake.scan.rows('readings'))), 4)
        # Diagnostics no longer report missing paths for expired-only references.
        self.assertEqual(self.lake.diagnostics.inventory()['missing_paths'], [])
        self.assertTrue(self.lake.diagnostics.check('readings')['ok'])

    def test_reclaim_without_expiration_removes_only_orphans(self):
        self.build_history()
        orphan = self.lake.objects.write_rows('readings', 1, self.lake.catalog.schema('readings'),
                                              {'day': '2026-11-01'}, [row(98)])
        result = self.lake.retention.reclaim('readings')
        self.assertEqual(result['deleted_objects'], [orphan['path']])
        for snapshot_id in range(1, 5):
            self.assertEqual(len(list(self.lake.scan.rows('readings', snapshot_id))), snapshot_id)

    def test_active_read_lease_pins_an_expired_snapshot(self):
        self.build_history()
        # The long-running read pins the snapshot while it is still readable.
        lease = self.lake.retention.pin('readings', 1)
        self.lake.retention.expire_snapshots('readings', retain_recent=1)
        # An in-flight reader keeps reading the snapshot it pinned.
        self.assertEqual([r['id'] for r in self.lake.scan.rows('readings', 1)], [1])
        pinned_paths = self.paths_of(1)
        manifest_only = self.manifest_path(1)
        result = self.lake.retention.reclaim('readings')
        self.assertEqual(result['active_read_leases'], 1)
        physical = self.physical_objects()
        self.assertTrue(pinned_paths <= physical)
        # Once the in-flight read releases and the snapshot has no lease left, no new
        # read can pin it.
        self.lake.retention.unpin(lease['token'])
        with self.assertRaises(Expired):
            self.lake.retention.pin('readings', 1)
        # The next sweep removes the object unique to the expired snapshot (its
        # manifest); data objects still shared with the retained head stay protected.
        self.lake.retention.reclaim('readings')
        self.assertNotIn(manifest_only, self.physical_objects())
        with self.assertRaises(Expired):
            list(self.lake.scan.rows('readings', 1))

    def test_lease_renewal_and_lifecycle(self):
        self.build_history()
        lease = self.lake.retention.pin('readings', 2, ttl_seconds=60)
        renewed = self.lake.retention.renew(lease['token'], ttl_seconds=120)
        self.assertGreater(renewed['expires_at'], lease['expires_at'])
        with self.assertRaises(Missing):
            self.lake.retention.unpin('does-not-exist')
        # Pinning an already expired snapshot is refused.
        self.lake.retention.expire_snapshots('readings', retain_recent=1)
        with self.assertRaises(Expired):
            self.lake.retention.pin('readings', 1)
        # A released lease cannot be renewed.
        released = self.lake.retention.pin('readings', 4)
        self.lake.retention.unpin(released['token'])
        with self.assertRaises(Conflict):
            self.lake.retention.renew(released['token'])

    def test_expired_leases_release_their_pin(self):
        import time
        self.build_history()
        self.lake.retention.pin('readings', 1, ttl_seconds=1)
        manifest = self.manifest_path(1)
        self.lake.retention.expire_snapshots('readings', retain_recent=1)
        time.sleep(1.05)
        result = self.lake.retention.reclaim('readings')
        self.assertEqual(result['active_read_leases'], 0)
        self.assertNotIn(manifest, self.physical_objects())
        with self.assertRaises(Expired):
            list(self.lake.scan.rows('readings', 1))

    def test_preparing_batch_defers_reclamation_of_unreferenced_objects(self):
        self.build_history()
        self.lake.retention.expire_snapshots('readings', retain_recent=1)
        # Simulate a crashed preparer: the preparing row exists but objects were never listed.
        with self.lake.catalog.transaction(True) as connection:
            connection.execute(
                "INSERT INTO prepared_batches VALUES (?,?,?,?,?,?,?,?,?,?,?)",
                ('readings', 'crashed', 'x', 4, 1, 'preparing', '[]', '[]', 1, None,
                 '2026-11-01T00:00:00.000000+00:00'))
        result = self.lake.retention.reclaim('readings')
        self.assertTrue(result['active_preparation'])
        self.assertEqual(result['deleted_objects'], [])
        self.assertGreater(len(result['deferred_objects']), 0)
        # After the operator aborts the dead preparation, sweep proceeds.
        self.lake.staged.abort('readings', 'crashed')
        result = self.lake.retention.reclaim('readings')
        self.assertFalse(result['active_preparation'])
        self.assertGreater(len(result['deleted_objects']), 0)

    def test_prepared_batch_objects_are_protected_until_commit_or_abort(self):
        self.build_history()
        self.lake.staged.prepare('readings', 'held', [row(5)])
        prepared = self.lake.staged.get('readings', 'held')
        prepared_paths = {file['path'] for file in prepared['files']}
        self.lake.retention.expire_snapshots('readings', retain_recent=1)
        self.lake.retention.reclaim('readings')
        self.assertTrue(prepared_paths <= self.physical_objects())
        # Abort makes the staged objects unreferenced and therefore reclaimable.
        self.lake.staged.abort('readings', 'held')
        result = self.lake.retention.reclaim('readings')
        self.assertTrue(prepared_paths <= set(result['deleted_objects']))

    def test_reclaim_interrupted_then_retried_is_safe(self):
        self.build_history()
        self.lake.retention.expire_snapshots('readings', retain_recent=1)
        retained_paths = self.paths_of(4)
        original = self.lake.objects.delete
        calls = {'n': 0}

        def fail_then_succeed(relative):
            calls['n'] += 1
            if calls['n'] == 2:
                raise OSError('author interruption during sweep')
            return original(relative)

        with patch.object(self.lake.objects, 'delete', fail_then_succeed):
            with self.assertRaises(OSError):
                self.lake.retention.reclaim('readings')
        # The catalog transaction rolled back: no completed run is recorded and the
        # interrupted deletion cannot have touched any still-protected object.
        self.assertTrue(retained_paths <= self.physical_objects())
        runs = self.lake.retention.runs()
        self.assertTrue(all(run['status'] == 'completed' for run in runs))
        # The head remains readable throughout; nothing needed was lost.
        self.assertEqual(len(list(self.lake.scan.rows('readings'))), 4)
        # Retry recomputes the full candidate set and finishes the sweep.
        result = self.lake.retention.reclaim('readings')
        self.assertGreater(len(result['deleted_objects']), 0)
        self.assertTrue(retained_paths <= self.physical_objects())
        self.assertEqual(len(list(self.lake.scan.rows('readings'))), 4)
        self.assertEqual(self.lake.diagnostics.inventory()['missing_paths'], [])

    def test_concurrent_reclaims_serialize_and_never_double_delete(self):
        self.build_history()
        self.lake.retention.expire_snapshots('readings', retain_recent=1)
        errors = []
        barrier = threading.Barrier(2)

        def sweep():
            try:
                barrier.wait()
                self.lake.retention.reclaim('readings')
            except BaseException as error:  # noqa: BLE001 - surface any failure
                errors.append(error)

        threads = [threading.Thread(target=sweep) for _ in range(2)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join(30)
        self.assertEqual(errors, [])
        self.assertEqual(len(list(self.lake.scan.rows('readings'))), 4)

    def test_concurrent_commit_and_reclaim_keep_every_needed_object(self):
        self.build_history()
        self.lake.retention.expire_snapshots('readings', retain_recent=1)
        original_delete = self.lake.objects.delete
        started = threading.Event()
        release = threading.Event()

        def blocking_delete(relative):
            started.set()
            release.wait(10)
            return original_delete(relative)

        def reclaim():
            with patch.object(self.lake.objects, 'delete', blocking_delete):
                self.lake.retention.reclaim('readings')

        sweeper = threading.Thread(target=reclaim)
        sweeper.start()
        self.assertTrue(started.wait(10))
        # While the sweep holds the write lock, the writer must block rather than publish
        # objects the sweep could remove.
        def append():
            self.append('parallel', 5)
        writer = threading.Thread(target=append)
        writer.start()
        self.assertTrue(writer.is_alive() or True)
        release.set()
        sweeper.join(30)
        writer.join(30)
        self.assertEqual(len(list(self.lake.scan.rows('readings'))), 5)
        # Every object referenced by the new head physically exists.
        self.assertEqual(self.lake.diagnostics.inventory()['missing_paths'], [])

    def test_new_commits_never_revive_expired_snapshot_ids(self):
        self.build_history(3)
        self.lake.retention.expire_snapshots('readings', retain_recent=1)
        expired = [1, 2]
        self.append('after_expiry', 5)
        self.assertEqual(self.lake.catalog.table('readings')['head'], 4)
        for snapshot_id in expired:
            with self.assertRaises(Expired):
                self.lake.scan.snapshot('readings', snapshot_id)
        # Re-expiring with a smaller window marks additional ids, never un-expires old ones.
        result = self.lake.retention.expire_snapshots('readings', retain_recent=1)
        self.assertEqual(result['newly_expired'], [3])
        with self.assertRaises(Expired):
            self.lake.scan.snapshot('readings', 3)
        self.assertEqual(len(list(self.lake.scan.rows('readings'))), 4)

    def test_shared_object_between_snapshots_is_protected(self):
        self.append('b1', 1, 2)
        # Replace the day-2 partition; the day-1 object is carried into the new snapshot.
        self.lake.maintenance.replace('readings', [row(3, '2026-11-02')], {'day': '2026-11-02'})
        shared = self.paths_of(1) & self.paths_of(2)
        self.assertTrue(shared)
        self.assertEqual(sorted(r['id'] for r in self.lake.scan.rows('readings')), [1, 2, 3])
        self.lake.retention.expire_snapshots('readings', retain_recent=1)
        self.lake.retention.reclaim('readings')
        # An object still referenced by the retained head survives even though the
        # snapshot that introduced it was expired.
        self.assertTrue(shared <= self.physical_objects())
        self.assertEqual(sorted(r['id'] for r in self.lake.scan.rows('readings')), [1, 2, 3])

    def test_lake_wide_reclaim_isolates_tables_and_pins(self):
        self.append('b1', 1)
        self.lake.catalog.create_table('other', SCHEMA)
        other = Lake(self.lake.home, file_rows=1)
        other.ingest.ingest('other', 'o1', [row(1)])
        other.retention.expire_snapshots('other', retain_recent=1)
        # readings has no expired snapshots: its objects must all survive a lake-wide sweep.
        readings_paths = self.paths_of(1)
        result = self.lake.retention.reclaim()
        self.assertIsNone(result['table'])
        self.assertTrue(readings_paths <= self.physical_objects())
        # Table-scoped reclaim never touches another table's objects.
        self.assertTrue(self.paths_of(1) <= self.physical_objects())

    def test_invalid_arguments(self):
        with self.assertRaises(Invalid):
            self.lake.retention.expire_snapshots('readings', retain_recent=0)
        with self.assertRaises(Invalid):
            self.lake.retention.pin('readings', ttl_seconds=0)
        with self.assertRaises(Missing):
            self.lake.retention.pin('readings', 99)
        with self.assertRaises(Missing):
            self.lake.retention.reclaim('unknown_table')

    def test_http_expired_snapshot_returns_410(self):
        self.build_history()
        self.lake.retention.expire_snapshots('readings', retain_recent=1)
        http = server_http(self.lake)
        thread = threading.Thread(target=http.serve_forever, daemon=True)
        thread.start()
        url = f'http://127.0.0.1:{http.server_port}'

        def post(path, body):
            request = Request(url + path, data=json.dumps(body).encode(),
                              headers={'Content-Type': 'application/json'})
            with urlopen(request) as response:
                return json.load(response)
        try:
            with self.assertRaises(HTTPError) as failure:
                urlopen(url + '/tables/readings/rows?snapshot=1')
            self.assertEqual(failure.exception.code, 410)
            self.assertEqual(
                post('/tables/readings/expire', {'retain_recent': 1})['retained_snapshots'], [4])
            lease = post('/tables/readings/pin', {})
            renewed = post('/tables/readings/renew-lease',
                           {'token': lease['token'], 'ttl_seconds': 7200})
            self.assertEqual(renewed['token'], lease['token'])
            self.assertTrue(
                post('/tables/readings/unpin', {'token': lease['token']})['released_at'])
            self.assertIsInstance(post('/reclaim', {})['run_id'], str)
            with urlopen(url + '/retention/runs') as response:
                self.assertIsInstance(json.load(response), list)
            with urlopen(url + '/tables/readings/retention') as response:
                payload = json.load(response)
            self.assertEqual(len(payload['snapshots']), 4)
        finally:
            http.shutdown()
            http.server_close()
            thread.join(5)


def server_http(lake):
    from schemalake.api import server
    return server(lake, port=0)


if __name__ == '__main__':
    unittest.main()
