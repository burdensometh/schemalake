from collections import Counter
import json

from .commit import SnapshotCommitter
from .reconcile import Reconcile
from .scan import Scanner
from .types import Conflict, Invalid, Missing, canonical, convert, sha, timestamp
from .writer import DataWriter


DDL = """
CREATE TABLE IF NOT EXISTS compaction_jobs (
    job_id TEXT PRIMARY KEY,
    table_name TEXT NOT NULL REFERENCES lake_tables(name),
    base_head INTEGER NOT NULL,
    schema_version INTEGER NOT NULL,
    partition_json TEXT NOT NULL,
    target_rows INTEGER NOT NULL,
    status TEXT NOT NULL CHECK(status IN ('preparing','prepared','committed','aborted','conflicted')),
    plan_json TEXT NOT NULL,
    row_count INTEGER NOT NULL,
    snapshot_id INTEGER,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);
"""


class CompactionPlanner:
    """Two-phase small-file compaction.

    The preparation phase performs all slow object reads and writes outside of
    any catalog write transaction, so normal ingestion keeps committing. Only
    the short publish phase takes a write lock, and it reconciles against the
    current head before atomically publishing the compacted snapshot.
    """

    def __init__(self, catalog, objects, scanner=None):
        self.catalog = catalog
        self.objects = objects
        self.scanner = scanner or Scanner(catalog, objects)
        self.committer = SnapshotCommitter(catalog, objects)
        self.reconcile = Reconcile(catalog, self.scanner)
        with catalog.transaction(True) as connection:
            connection.executescript(DDL)

    def _partition(self, schema, partition):
        fields = {field.name: field for field in schema.fields}
        if not isinstance(partition, dict) or set(partition) != set(schema.partition_by):
            raise Invalid('compaction requires all partition columns exactly')
        return {key: convert(value, fields[key].type) for key, value in partition.items()}

    def get(self, job_id):
        with self.catalog.transaction() as connection:
            row = connection.execute(
                'SELECT * FROM compaction_jobs WHERE job_id=?', (job_id,)
            ).fetchone()
            if row is None:
                raise Missing('compaction job does not exist')
            result = dict(row)
            result['plan'] = json.loads(result.pop('plan_json'))
            return result

    def jobs(self, table_name):
        self.catalog.table(table_name)
        with self.catalog.transaction() as connection:
            rows = connection.execute(
                'SELECT job_id,base_head,status,row_count,snapshot_id,created_at,updated_at '
                'FROM compaction_jobs WHERE table_name=? ORDER BY created_at', (table_name,)
            )
            return [dict(row) for row in rows]

    def prepare(self, table_name, partition=None, target_rows=10000, expected_head=None):
        if type(target_rows) is not int or not 1 <= target_rows <= 100000:
            raise Invalid('target_rows must be between 1 and 100000')
        # Read-only phase: pin the base snapshot. In WAL mode a read lock does
        # not block concurrent writers, and the transaction is closed before
        # the slow object reads begin.
        with self.catalog.transaction() as connection:
            table = self.catalog.table(table_name, connection)
            if expected_head is not None and expected_head != table['head']:
                raise Conflict('compaction head changed')
            schema = self.catalog.schema(table_name, connection=connection)
            selected = None if partition is None else self._partition(schema, partition)
            base_files = self.committer.current(connection, table_name)
            base_head = table['head']
            schema_version = table['schema_version']
        chosen = [file for file in base_files
                  if selected is None or file['partition'] == selected]
        if not chosen:
            raise Invalid('no files selected for compaction')
        job_id = sha([table_name, base_head, canonical(selected),
                      target_rows, timestamp()])[:24]
        # Short write transaction: CAS the head and register the job. No object
        # reads happen while this lock is held.
        with self.catalog.transaction(True) as connection:
            table = self.catalog.table(table_name, connection)
            if table['head'] != base_head or table['schema_version'] != schema_version:
                raise Conflict('table changed while compaction was registering')
            connection.execute(
                'INSERT INTO compaction_jobs VALUES (?,?,?,?,?,?,?,?,?,?,?,?)',
                (job_id, table_name, base_head, schema_version, canonical(selected),
                 target_rows, 'preparing', '[]', 0, None, timestamp(), timestamp()),
            )
        # Slow object reads and writes happen here, without holding any catalog
        # transaction: concurrent ingestion batches can commit freely.
        rows = []
        for descriptor in chosen:
            rows.extend(schema.normalize(raw) for raw in self.objects.read_rows(descriptor))
        added = DataWriter(self.objects, target_rows).write(table_name, schema_version, schema, rows)
        written_rows = []
        for descriptor in added:
            part_rows = [schema.normalize(raw) for raw in self.objects.read_rows(descriptor)]
            if self.objects.statistics(schema, part_rows) != descriptor['statistics']:
                raise Conflict('compacted object statistics do not match its descriptor')
            written_rows.extend(part_rows)
        if len(written_rows) != len(rows):
            raise Conflict('compaction changed the number of rows')
        input_fingerprint = self.reconcile.fingerprint(rows)
        output_fingerprint = self.reconcile.fingerprint(written_rows)
        if input_fingerprint != output_fingerprint:
            raise Conflict('compaction changed the row multiset')
        input_stats = self.objects.statistics(schema, rows)
        if self.objects.statistics(schema, written_rows) != input_stats:
            raise Conflict('compaction changed exact field statistics')
        plan = {
            'base_head': base_head,
            'selected': selected,
            'base_files': base_files,
            'chosen': chosen,
            'added': added,
            'rows': len(rows),
            'fingerprint': input_fingerprint,
            'statistics': input_stats,
        }
        with self.catalog.transaction(True) as connection:
            state = connection.execute(
                'SELECT status FROM compaction_jobs WHERE job_id=?', (job_id,)
            ).fetchone()[0]
            if state != 'preparing':
                raise Conflict('compaction job status changed while objects were written')
            connection.execute(
                "UPDATE compaction_jobs SET plan_json=?, row_count=?, status='prepared', updated_at=? "
                'WHERE job_id=?',
                (canonical(plan), len(rows), timestamp(), job_id),
            )
        return self.get(job_id)

    def _reconcile(self, plan, current_files):
        """Merge the prepared plan with files committed concurrently.

        File descriptors are content-addressed, so the file id is the physical
        object identity. Multiplicity matters: the same object referenced twice
        must be counted twice. Within the scoped partitions, every base file
        instance must still exist; a missing instance means the partition was
        replaced or dropped, which is a conflict rather than a silent merge.
        """
        base_counts = Counter(file['id'] for file in plan['chosen'])
        scoped = {canonical(file['partition']) for file in plan['chosen']}
        seen = Counter()
        final = []
        for descriptor in current_files:
            if canonical(descriptor['partition']) in scoped:
                seen[descriptor['id']] += 1
                if seen[descriptor['id']] <= base_counts[descriptor['id']]:
                    continue  # base instance: replaced by the compacted objects
                final.append(descriptor)  # appended after preparation: keep it
            else:
                final.append(descriptor)  # outside the compaction scope: keep it
        missing = {ident: count - seen[ident] for ident, count in base_counts.items()
                   if seen[ident] < count}
        if missing:
            raise Conflict('partition changed during compaction preparation; '
                           'retry against the current snapshot')
        concurrent = sum(seen[ident] - base_counts[ident] for ident in seen)
        return final + plan['added'], concurrent

    def _mark(self, job_id, status):
        with self.catalog.transaction(True) as connection:
            connection.execute(
                'UPDATE compaction_jobs SET status=?, updated_at=? WHERE job_id=?',
                (status, timestamp(), job_id),
            )

    def commit(self, job_id):
        job = self.get(job_id)
        if job['status'] == 'committed':
            return {**self._summary(job), 'replayed': True}
        if job['status'] != 'prepared':
            raise Conflict(f"compaction job is not ready to commit: {job['status']}")
        plan = job['plan']
        table_name = job['table_name']
        schema = self.catalog.schema(table_name, job['schema_version'])
        # Re-verify the compacted objects before taking the write lock. The
        # object store is immutable and content-addressed, so this stays cheap
        # and the publish transaction itself only touches catalog state.
        written_rows = []
        for descriptor in plan['added']:
            part_rows = [schema.normalize(raw) for raw in self.objects.read_rows(descriptor)]
            if self.objects.statistics(schema, part_rows) != descriptor['statistics']:
                raise Conflict('compacted object failed verification before commit')
            written_rows.extend(part_rows)
        if len(written_rows) != plan['rows'] or \
                self.reconcile.fingerprint(written_rows) != plan['fingerprint']:
            raise Conflict('compacted objects no longer match the prepared plan')
        with self.catalog.transaction() as connection:
            current_files = self.committer.current(connection, table_name)
        try:
            self._reconcile(plan, current_files)
        except Conflict:
            self._mark(job_id, 'conflicted')
            raise
        # The only write lock of the whole compaction, held for catalog state.
        with self.catalog.transaction(True) as connection:
            table = self.catalog.table(table_name, connection)
            if table['schema_version'] != job['schema_version']:
                raise Conflict('schema changed after compaction preparation')
            current_files = self.committer.current(connection, table_name)
            final_files, concurrent = self._reconcile(plan, current_files)
            if sum(file['row_count'] for file in final_files) != \
                    sum(file['row_count'] for file in current_files):
                raise Conflict('reconciled compaction would change the table row count')
            current_head = table['head']
            snapshot_id = self.committer.commit(
                connection, table_name, 'compact', final_files, current_head,
                {'compaction_job': job_id, 'base_head': plan['base_head'],
                 'replaced_files': len(plan['chosen']), 'concurrent_files_kept': concurrent},
            )
            connection.execute(
                "UPDATE compaction_jobs SET status='committed', snapshot_id=?, updated_at=? "
                'WHERE job_id=?',
                (snapshot_id, timestamp(), job_id),
            )
        return self._summary(self.get(job_id))

    def abort(self, job_id):
        job = self.get(job_id)
        if job['status'] == 'committed':
            raise Conflict('committed compaction cannot be aborted')
        if job['status'] == 'conflicted':
            raise Conflict('conflicted compaction is terminal; prepare a new job instead')
        self._mark(job_id, 'aborted')
        return self.get(job_id)

    def _summary(self, job):
        plan = job['plan']
        return {
            'table': job['table_name'],
            'job_id': job['job_id'],
            'snapshot_id': job['snapshot_id'],
            'files_before': len(plan['chosen']),
            'files_after': len(plan['added']),
            'rows': job['row_count'],
        }
