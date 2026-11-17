"""Non-blocking two-phase compaction tests.

The headline scenarios use a controllable slow object store: the compaction
preparation thread is paused inside its first object read or inside its
object write. While paused, ingestion must still acquire the catalog write
lock and commit -- the proof is a real concurrent commit, not a shorter log
line. The gate is released explicitly so timing is deterministic and the
suite never relies on wall-clock sleeps.
"""
import sqlite3
import threading
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import patch

from schemalake.app import Lake
from schemalake.types import Conflict, Invalid
from test_core import SCHEMA, row


class SlowIoGate:
    """Block chosen object read/write calls inside a marked thread until opened.

    The read and write gates are independent: a write-gate scenario lets reads
    flow so the worker actually reaches the object write. The gate is created
    inside the worker but published through a holder, so the test can wait on
    its events directly instead of polling for a thread-local instance.
    """

    def __init__(self, objects, worker_ident, gate_reads=False, gate_writes=False):
        self.objects = objects
        self.worker_ident = worker_ident
        self.read_open = threading.Event()
        self.write_open = threading.Event()
        self.read_entered = threading.Event()
        self.write_entered = threading.Event()
        self.gate_reads = gate_reads
        self.gate_writes = gate_writes
        self._original_read = objects.read
        self._original_write_rows = objects.write_rows

    def _gated_read(self, relative, checksum):
        # Only data object reads are gated. The manifest read that pins the
        # base snapshot happens inside a short read transaction before the
        # job is registered and must never be blocked.
        is_data_object = '/data/' in relative
        if self.gate_reads and threading.get_ident() == self.worker_ident \
                and is_data_object and not self.read_entered.is_set():
            self.read_entered.set()
            self.read_open.wait(timeout=30)
        return self._original_read(relative, checksum)

    def _gated_write_rows(self, *args, **kwargs):
        if threading.get_ident() == self.worker_ident and not self.write_entered.is_set():
            self.write_entered.set()
            if self.gate_writes:
                self.write_open.wait(timeout=30)
        return self._original_write_rows(*args, **kwargs)

    def install(self):
        patch.object(self.objects, 'read', self._gated_read).start()
        patch.object(self.objects, 'write_rows', self._gated_write_rows).start()
        self.read_open.set()
        self.write_open.set()

    def arm_read(self):
        self.read_open.clear()

    def arm_write(self):
        self.write_open.clear()

    def wait_for_read(self):
        if not self.read_entered.wait(timeout=10):
            raise RuntimeError('compaction never entered the object read')

    def wait_for_write(self):
        if not self.write_entered.wait(timeout=10):
            raise RuntimeError('compaction never entered the object write')

    def release_read(self):
        self.read_open.set()

    def release_write(self):
        self.write_open.set()

    def release(self):
        self.read_open.set()
        self.write_open.set()


class CompactionTest(unittest.TestCase):
    def setUp(self):
        self.temporary = TemporaryDirectory()
        self.root = Path(self.temporary.name)
        self.lake = Lake(self.root / 'lake', file_rows=1)
        self.lake.catalog.create_table('readings', SCHEMA)

    def tearDown(self):
        self.temporary.cleanup()

    def append(self, batch, rows):
        return self.lake.ingest.ingest('readings', batch, rows)

    def fingerprint(self, snapshot_id=None):
        return self.lake.reconcile.fingerprint(self.lake.scan.rows('readings', snapshot_id))

    def start_prepare(self, gate_reads=False, gate_writes=False, partition=None, target_rows=10000):
        holder = {}
        outcome = {}

        def prepare():
            gate = SlowIoGate(self.lake.objects, threading.get_ident(), gate_reads, gate_writes)
            gate.install()
            if gate_reads:
                gate.arm_read()
            if gate_writes:
                gate.arm_write()
            self.addCleanup(gate.release)
            holder['gate'] = gate
            try:
                outcome['job'] = self.lake.compaction.prepare(
                    'readings', partition, target_rows)
            except BaseException as error:  # surfaced by the main thread
                outcome['error'] = error

        thread = threading.Thread(target=prepare, name='compaction-prepare')
        thread.start()
        for _ in range(200):
            if 'gate' in holder:
                break
            thread.join(0.02)
        self.assertIn('gate', holder, 'worker never installed its I/O gate')
        return thread, outcome, holder['gate']

    def join_prepare(self, thread, outcome):
        thread.join(timeout=15)
        self.assertFalse(thread.is_alive(), 'compaction preparation did not finish')
        self.assertNotIn('error', outcome, outcome.get('error'))
        return outcome['job']

    def test_preparation_never_holds_the_write_lock_and_ingestion_commits_first(self):
        self.append('b1', [row(1), row(2)])
        before = self.fingerprint()
        thread, outcome, gate = self.start_prepare(gate_reads=True, target_rows=100)
        gate.wait_for_read()
        # The worker is parked inside a slow object read. A fresh connection
        # must get the catalog write lock immediately; timeout=0 fails instead
        # of waiting, so this directly observes lock availability.
        probe = sqlite3.connect(self.lake.catalog.path, timeout=0, isolation_level=None)
        try:
            probe.execute('BEGIN IMMEDIATE')
            probe.execute('SELECT 1 FROM lake_tables')
            probe.rollback()
        except sqlite3.OperationalError:
            self.fail('compaction preparation is holding the catalog write lock')
        finally:
            probe.close()
        # A full ingestion batch commits while object I/O is still blocked:
        # its snapshot becomes visible before the compaction even finishes
        # preparing. This is the real non-blocking guarantee.
        head_before = self.lake.catalog.table('readings')['head']
        self.append('during', [row(3), row(4)])
        head_after = self.lake.catalog.table('readings')['head']
        self.assertEqual(head_after, head_before + 1)
        self.assertEqual(len(list(self.lake.scan.rows('readings'))), 4)
        gate.release_read()
        job = self.join_prepare(thread, outcome)
        self.assertEqual(job['status'], 'prepared')
        # Commit keeps the rows appended during preparation.
        published = self.lake.compaction.commit(job['job_id'])
        self.assertEqual(published['snapshot_id'], head_after + 1)
        self.assertEqual(self.fingerprint()['rows'], 4)
        plan = self.lake.scan.plan('readings')
        # One compacted object for the two base files, plus the two files of
        # the batch that committed during preparation (file_rows=1). The
        # concurrent append must not have been swallowed or duplicated.
        self.assertEqual(len(plan['files']), 3)
        self.assertEqual(sum(file['row_count'] for file in plan['files']), 4)
        # Old snapshot stays readable and the row multiset is preserved.
        self.assertEqual(self.fingerprint(1), before)
        self.assertFalse(
            self.lake.reconcile.between_snapshots('readings', 1, published['snapshot_id'])
            ['equivalent'])
        self.assertTrue(
            self.lake.reconcile.between_snapshots('readings', head_after,
                                                  published['snapshot_id'])['equivalent'])
        self.assertTrue(self.lake.diagnostics.check('readings')['ok'])

    def test_slow_object_write_also_runs_without_blocking_ingestion(self):
        self.append('b1', [row(1), row(2)])
        thread, outcome, gate = self.start_prepare(gate_writes=True, target_rows=100)
        gate.wait_for_write()  # reads already completed; parked inside the write
        head_before = self.lake.catalog.table('readings')['head']
        self.append('during-write', [row(5)])
        self.assertEqual(self.lake.catalog.table('readings')['head'], head_before + 1)
        gate.release_write()
        job = self.join_prepare(thread, outcome)
        published = self.lake.compaction.commit(job['job_id'])
        self.assertEqual(sorted(r['id'] for r in self.lake.scan.rows('readings')), [1, 2, 5])
        plan = self.lake.scan.plan('readings')
        # One compacted object (base two files) plus the concurrent append file.
        self.assertEqual(len(plan['files']), 2)
        self.assertEqual(sum(file['row_count'] for file in plan['files']), 3)
        self.assertTrue(
            self.lake.reconcile.between_snapshots('readings', self.lake.catalog.table('readings')
                                                  ['head'] - 1, published['snapshot_id'])
            ['equivalent'])

    def test_partition_replaced_during_preparation_conflicts_and_retries_safely(self):
        self.append('b1', [row(1), row(2, day='2026-11-02')])
        thread, outcome, gate = self.start_prepare(gate_reads=True, target_rows=100)
        gate.wait_for_read()  # base files are pinned; preparation is parked in I/O
        # Replace the same partition while preparation is parked. The commit
        # phase must detect that a base file instance disappeared rather than
        # silently bringing the old data back.
        self.lake.maintenance.replace('readings', [row(1, reading='99')],
                                      {'day': '2026-11-01'})
        self.assertEqual(self.lake.catalog.table('readings')['head'], 2)
        gate.release_read()
        job = self.join_prepare(thread, outcome)
        with self.assertRaises(Conflict) as failure:
            self.lake.compaction.commit(job['job_id'])
        self.assertIn('retry', str(failure.exception))
        self.assertEqual(self.lake.compaction.get(job['job_id'])['status'], 'conflicted')
        # The replacement result is untouched: the old row must not come back.
        current = list(self.lake.scan.rows('readings'))
        self.assertEqual(sorted(r['id'] for r in current), [1, 2])
        self.assertEqual(next(r for r in current if r['id'] == 1)['reading'], '99')
        self.assertEqual(self.fingerprint()['rows'], 2)
        # Retry against the current snapshot succeeds and keeps current data.
        retry = self.lake.compaction.prepare('readings', target_rows=100)
        published = self.lake.compaction.commit(retry['job_id'])
        self.assertEqual(sorted(r['id'] for r in self.lake.scan.rows('readings')), [1, 2])
        self.assertEqual(next(r for r in self.lake.scan.rows('readings') if r['id'] == 1)
                         ['reading'], '99')
        # The two current files live in different partitions, so the retry
        # keeps one object per partition; what matters is the exact multiset
        # and that the conflicting plan never resurrected replaced data.
        self.assertEqual(
            sorted(file['partition']['day'] for file in self.lake.scan.plan('readings')['files']),
            ['2026-11-01', '2026-11-02'])
        self.assertTrue(self.lake.diagnostics.check('readings')['ok'])
        self.assertTrue(
            self.lake.reconcile.between_snapshots('readings', self.lake.catalog.table('readings')
                                                  ['head'] - 1, published['snapshot_id'])
            ['equivalent'])
        with self.assertRaises(Conflict):
            self.lake.compaction.abort(job['job_id'])

    def test_preparation_failure_leaves_no_snapshot_and_retries_keep_every_row(self):
        self.append('b1', [row(1), row(2)])
        original_write_rows = self.lake.objects.write_rows

        def failing_write(*args, **kwargs):
            failing_write.calls += 1
            if failing_write.calls == 1:
                raise OSError('simulated slow object write fault')
            return original_write_rows(*args, **kwargs)

        failing_write.calls = 0
        with patch.object(self.lake.objects, 'write_rows', failing_write):
            with self.assertRaises(OSError):
                self.lake.compaction.prepare('readings', target_rows=100)
        self.assertEqual(self.lake.catalog.table('readings')['head'], 1)
        self.assertEqual(len(list(self.lake.scan.rows('readings'))), 2)
        jobs = self.lake.compaction.jobs('readings')
        self.assertEqual(jobs[0]['status'], 'preparing')
        self.lake.compaction.abort(jobs[0]['job_id'])
        retry = self.lake.compaction.prepare('readings', target_rows=100)
        published = self.lake.compaction.commit(retry['job_id'])
        self.assertEqual(published['rows'], 2)
        self.assertEqual(sorted(r['id'] for r in self.lake.scan.rows('readings')), [1, 2])
        self.assertTrue(
            self.lake.reconcile.between_snapshots('readings', 1, published['snapshot_id'])
            ['equivalent'])

    def test_commit_replays_idempotently_and_retry_after_storage_failure(self):
        self.append('b1', [row(1), row(2)])
        job = self.lake.compaction.prepare('readings', target_rows=100)
        original_commit = self.lake.compaction.committer.commit
        calls = {'n': 0}

        def flaky_commit(*args, **kwargs):
            calls['n'] += 1
            if calls['n'] == 1:
                raise OSError('catalog publish fault')
            return original_commit(*args, **kwargs)

        with patch.object(self.lake.compaction.committer, 'commit', flaky_commit):
            with self.assertRaises(OSError):
                self.lake.compaction.commit(job['job_id'])
        # No snapshot was published; the prepared job remains committable.
        self.assertEqual(self.lake.catalog.table('readings')['head'], 1)
        self.assertEqual(self.lake.compaction.get(job['job_id'])['status'], 'prepared')
        first = self.lake.compaction.commit(job['job_id'])
        replay = self.lake.compaction.commit(job['job_id'])
        self.assertTrue(replay['replayed'])
        self.assertEqual(replay['snapshot_id'], first['snapshot_id'])
        self.assertEqual(self.lake.catalog.table('readings')['head'], first['snapshot_id'])

    def test_same_physical_object_referenced_twice_is_counted_twice(self):
        # A table without a primary key allows identical rows, and an append
        # reusing the same content produces a second manifest descriptor whose
        # file id (physical object) is identical.
        schema = {'fields': [
            {'id': 1, 'name': 'day', 'type': 'date'},
            {'id': 2, 'name': 'site', 'type': 'string'},
            {'id': 3, 'name': 'reading', 'type': 'decimal'},
        ], 'partition_by': ['day']}
        self.lake.catalog.create_table('events', schema)
        payload = [{'day': '2026-11-01', 'site': 'a', 'reading': '1.5'},
                   {'day': '2026-11-01', 'site': 'a', 'reading': '1.5'}]
        self.lake.ingest.ingest('events', 'e1', payload)
        manifest = self.lake.scan.snapshot('events', 1)
        ids = [file['id'] for file in manifest['files']]
        self.assertEqual(len(ids), 2)
        self.assertEqual(len(set(ids)), 1)  # one physical object, two references
        before = self.lake.reconcile.fingerprint(self.lake.scan.rows('events'))
        result = self.lake.maintenance.compact('events', target_rows=100)
        self.assertEqual(result['files_before'], 2)
        self.assertEqual(result['rows'], 2)
        after = self.lake.reconcile.fingerprint(self.lake.scan.rows('events'))
        self.assertEqual(after, before)
        self.assertEqual(after['rows'], 2)
        self.assertEqual(len(list(self.lake.scan.rows('events'))), 2)
        self.assertTrue(self.lake.diagnostics.check('events')['ok'])

    def test_exact_statistics_and_typed_values_are_preserved(self):
        self.append('b1', [row(1, reading='2.50'), row(2, reading='10.00'),
                           row(3, reading=None)])
        stats_before = self.lake.scan.statistics('readings')
        job = self.lake.compaction.prepare('readings', target_rows=100)
        self.assertEqual(job['plan']['statistics'], stats_before['fields'])
        published = self.lake.compaction.commit(job['job_id'])
        self.assertEqual(self.lake.scan.statistics('readings')['fields'],
                         stats_before['fields'])
        rows = list(self.lake.scan.rows('readings'))
        self.assertEqual({r['reading'] for r in rows}, {'2.5', '10', None})
        self.assertTrue(
            self.lake.reconcile.between_snapshots('readings', 1, published['snapshot_id'])
            ['equivalent'])

    def test_partition_scoped_compaction_keeps_concurrent_other_partition_appends(self):
        self.append('b1', [row(1), row(2, day='2026-11-02')])
        thread, outcome, gate = self.start_prepare(
            gate_reads=True, partition={'day': '2026-11-01'}, target_rows=100)
        gate.wait_for_read()
        # Append to a different partition while preparation is parked.
        self.append('other', [row(3, day='2026-11-02')])
        gate.release_read()
        job = self.join_prepare(thread, outcome)
        published = self.lake.compaction.commit(job['job_id'])
        self.assertEqual(sorted(r['id'] for r in self.lake.scan.rows('readings')), [1, 2, 3])
        self.assertTrue(
            self.lake.reconcile.between_snapshots('readings', self.lake.catalog.table('readings')
                                                  ['head'] - 1, published['snapshot_id'])
            ['equivalent'])
        day_two = self.lake.scan.plan('readings', partition={'day': '2026-11-02'})
        self.assertEqual(day_two['files_skipped'], 1)
        self.assertEqual(day_two['rows'], 2)

    def test_expected_head_conflict_and_empty_selection(self):
        self.append('b1', [row(1)])
        with self.assertRaises(Conflict):
            self.lake.compaction.prepare('readings', target_rows=100, expected_head=99)
        self.lake.catalog.create_table('empty', SCHEMA)
        with self.assertRaises(Invalid):
            self.lake.compaction.prepare('empty', target_rows=100)

    def test_wrapper_contract_matches_baseline(self):
        self.append('b1', [row(1), row(2)])
        result = self.lake.maintenance.compact('readings', target_rows=100)
        self.assertEqual((result['files_before'], result['files_after']), (2, 1))
        self.assertEqual(result['rows'], 2)
        self.assertEqual(sorted(r['id'] for r in self.lake.scan.rows('readings')), [1, 2])


if __name__ == '__main__':
    unittest.main()
