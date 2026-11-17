import json
import sqlite3
import uuid
from datetime import datetime, timedelta

from .types import Conflict, Expired, Invalid, Missing, timestamp


DDL = """
CREATE TABLE IF NOT EXISTS snapshot_expirations (
    table_name TEXT NOT NULL,
    snapshot_id INTEGER NOT NULL,
    expired_at TEXT NOT NULL,
    PRIMARY KEY(table_name, snapshot_id)
);
CREATE TABLE IF NOT EXISTS read_leases (
    token TEXT PRIMARY KEY,
    table_name TEXT NOT NULL,
    snapshot_id INTEGER NOT NULL,
    created_at TEXT NOT NULL,
    expires_at TEXT NOT NULL,
    released_at TEXT
);
CREATE TABLE IF NOT EXISTS gc_runs (
    id TEXT PRIMARY KEY,
    status TEXT NOT NULL CHECK(status IN ('running','completed')),
    table_name TEXT,
    protected_objects INTEGER NOT NULL,
    deleted_objects INTEGER NOT NULL,
    deferred_objects INTEGER NOT NULL,
    active_preparation INTEGER NOT NULL,
    started_at TEXT NOT NULL,
    finished_at TEXT
);
"""


class Retention:
    """Snapshot expiration, read pinning and object-space reclamation."""

    def __init__(self, catalog, objects, lease_ttl_seconds=3600):
        self.catalog = catalog
        self.objects = objects
        if type(lease_ttl_seconds) is not int or lease_ttl_seconds < 1:
            raise Invalid('lease ttl must be a positive integer of seconds')
        self.lease_ttl_seconds = lease_ttl_seconds
        with catalog.transaction(True) as connection:
            connection.executescript(DDL)

    def is_expired(self, table_name, snapshot_id, connection=None):
        if snapshot_id == 0:
            return False
        if connection is None:
            with self.catalog.transaction() as connection:
                return self.is_expired(table_name, snapshot_id, connection)
        row = connection.execute(
            'SELECT 1 FROM snapshot_expirations WHERE table_name=? AND snapshot_id=?',
            (table_name, snapshot_id),
        ).fetchone()
        return row is not None

    def _has_active_lease(self, connection, table_name, snapshot_id):
        now = timestamp()
        row = connection.execute(
            'SELECT 1 FROM read_leases WHERE table_name=? AND snapshot_id=? '
            'AND released_at IS NULL AND expires_at>? LIMIT 1',
            (table_name, snapshot_id, now),
        ).fetchone()
        return row is not None

    def require_readable(self, table_name, snapshot_id, connection=None):
        """Raise Expired unless the snapshot is retained or an in-flight read lease covers it.

        Leases can only be acquired while a snapshot is still readable, so an expired,
        leased snapshot represents a read that started before expiration and must finish.
        """
        if connection is None:
            with self.catalog.transaction() as connection:
                return self.require_readable(table_name, snapshot_id, connection)
        if not self.is_expired(table_name, snapshot_id, connection):
            return
        if self._has_active_lease(connection, table_name, snapshot_id):
            return
        raise Expired(f'snapshot {snapshot_id} of table {table_name} has expired and is no longer readable')

    def expire_snapshots(self, table_name, retain_recent=1):
        """Expire every snapshot older than the current head minus the retention window.

        retain_recent is the number of snapshots to keep: the current head plus the
        retain_recent-1 most recent ancestors. Expiration is durable: expired rows stay
        in the catalog (so the snapshot can never reappear) but are unreadable.
        """
        if type(retain_recent) is not int or retain_recent < 1:
            raise Invalid('retain_recent must be a positive integer')
        with self.catalog.transaction(True) as connection:
            table = self.catalog.table(table_name, connection)
            head = table['head']
            keep_from = head - retain_recent + 1
            already = {
                row[0] for row in connection.execute(
                    'SELECT snapshot_id FROM snapshot_expirations WHERE table_name=?', (table_name,)
                )
            }
            candidates = [
                row[0] for row in connection.execute(
                    'SELECT id FROM snapshots WHERE table_name=? AND id<? ORDER BY id', (table_name, keep_from)
                )
            ]
            new_expirations = [snapshot_id for snapshot_id in candidates if snapshot_id not in already]
            now = timestamp()
            connection.executemany(
                'INSERT INTO snapshot_expirations VALUES (?,?,?)',
                ((table_name, snapshot_id, now) for snapshot_id in new_expirations),
            )
            for snapshot_id in new_expirations:
                self.catalog.event(connection, table_name, 'snapshot_expired',
                                   {'snapshot_id': snapshot_id, 'retain_recent': retain_recent})
            expired = sorted(already.intersection(candidates)) + new_expirations
            retained = [
                row[0] for row in connection.execute(
                    'SELECT id FROM snapshots WHERE table_name=? AND id>=? ORDER BY id DESC',
                    (table_name, keep_from),
                )
            ]
        return {'table': table_name, 'head': head, 'retain_recent': retain_recent,
                'expired': sorted(expired), 'retained_snapshots': retained,
                'newly_expired': sorted(new_expirations)}

    def pin(self, table_name, snapshot_id=None, ttl_seconds=None):
        if ttl_seconds is None:
            ttl_seconds = self.lease_ttl_seconds
        if type(ttl_seconds) is not int or ttl_seconds < 1:
            raise Invalid('lease ttl must be a positive integer of seconds')
        with self.catalog.transaction(True) as connection:
            table = self.catalog.table(table_name, connection)
            if snapshot_id is None:
                snapshot_id = table['head']
            if type(snapshot_id) is not int or snapshot_id < 0:
                raise Invalid('snapshot id must be a nonnegative integer')
            if snapshot_id:
                record = connection.execute(
                    'SELECT 1 FROM snapshots WHERE table_name=? AND id=?', (table_name, snapshot_id)
                ).fetchone()
                if record is None:
                    raise Missing('snapshot does not exist')
                self.require_readable(table_name, snapshot_id, connection)
            now = timestamp()
            token = uuid.uuid4().hex
            connection.execute(
                'INSERT INTO read_leases VALUES (?,?,?,?,?,NULL)',
                (token, table_name, snapshot_id, now, self._expiry(now, ttl_seconds)),
            )
        return {'token': token, 'table': table_name, 'snapshot_id': snapshot_id,
                'created_at': now, 'expires_at': self._expiry(now, ttl_seconds)}

    def _expiry(self, now_text, ttl_seconds):
        now = datetime.fromisoformat(now_text)
        return (now + timedelta(seconds=ttl_seconds)).isoformat(timespec='microseconds')

    def unpin(self, token):
        with self.catalog.transaction(True) as connection:
            row = connection.execute('SELECT * FROM read_leases WHERE token=?', (token,)).fetchone()
            if row is None:
                raise Missing('read lease does not exist')
            if row['released_at'] is not None:
                return dict(row)
            connection.execute('UPDATE read_leases SET released_at=? WHERE token=?', (timestamp(), token))
            row = connection.execute('SELECT * FROM read_leases WHERE token=?', (token,)).fetchone()
            return dict(row)

    def renew(self, token, ttl_seconds=None):
        if ttl_seconds is None:
            ttl_seconds = self.lease_ttl_seconds
        if type(ttl_seconds) is not int or ttl_seconds < 1:
            raise Invalid('lease ttl must be a positive integer of seconds')
        with self.catalog.transaction(True) as connection:
            row = connection.execute('SELECT * FROM read_leases WHERE token=?', (token,)).fetchone()
            if row is None:
                raise Missing('read lease does not exist')
            if row['released_at'] is not None:
                raise Conflict('read lease has already been released')
            now = timestamp()
            connection.execute('UPDATE read_leases SET expires_at=? WHERE token=?',
                               (self._expiry(now, ttl_seconds), token))
            row = connection.execute('SELECT * FROM read_leases WHERE token=?', (token,)).fetchone()
            return dict(row)

    def _purge_stale_leases(self, connection, now):
        connection.execute(
            'UPDATE read_leases SET released_at=? WHERE released_at IS NULL AND expires_at<=?',
            (now, now),
        )

    def _active_leases(self, connection, now):
        return [dict(row) for row in connection.execute(
            'SELECT table_name,snapshot_id FROM read_leases '
            'WHERE released_at IS NULL AND expires_at>?', (now,)
        )]

    def _has_preparing_batches(self, connection, table_name=None):
        sql = 'SELECT 1 FROM prepared_batches WHERE status=? LIMIT 1'
        params = ('preparing',)
        if table_name is not None:
            sql = 'SELECT 1 FROM prepared_batches WHERE table_name=? AND status=? LIMIT 1'
            params = (table_name, 'preparing')
        try:
            return connection.execute(sql, params).fetchone() is not None
        except sqlite3.OperationalError:
            # The staging extension table is created lazily; no staging means no preparation.
            return False

    def reclaim(self, table_name=None):
        """Reclaim objects that nobody can still read.

        Protected objects are: every manifest/data object referenced by a snapshot that
        is either retained (not expired) or covered by an active read lease, plus the
        data objects of staged batches that are still preparing or prepared. Objects of
        explicitly aborted batches and objects left behind by failed commits become
        reclaimable. The mark and the sweep run inside one write transaction so a
        concurrent writer cannot make an object needed between the two steps; an
        interrupted run rolls back fully and the retry simply recomputes everything.
        """
        run_id = uuid.uuid4().hex
        started = timestamp()
        with self.catalog.transaction(True) as connection:
            connection.execute(
                'INSERT INTO gc_runs VALUES (?,?,?,?,?,?,?,?,NULL)',
                (run_id, 'running', table_name, 0, 0, 0, 0, started),
            )
            now = timestamp()
            self._purge_stale_leases(connection, now)
            tables = [table_name] if table_name is not None else [
                row['name'] for row in connection.execute('SELECT name FROM lake_tables ORDER BY name')
            ]
            if table_name is not None:
                self.catalog.table(table_name, connection)

            active_leases = self._active_leases(connection, now)
            leased = {(row['table_name'], row['snapshot_id']) for row in active_leases}

            protected = set()
            for current in tables:
                snapshots = connection.execute(
                    'SELECT * FROM snapshots WHERE table_name=? ORDER BY id', (current,)
                ).fetchall()
                for record in snapshots:
                    if (not self.is_expired(current, record['id'], connection)
                            or (current, record['id']) in leased):
                        protected.add(record['manifest_path'])
                        document = self.objects.manifest(dict(record))
                        protected.update(file['path'] for file in document['files'])
                # Staged batches: prepared/preparing objects are not yet part of any
                # snapshot, so protect them explicitly; aborted batches stay reclaimable.
                try:
                    staged_rows = connection.execute(
                        "SELECT files FROM prepared_batches WHERE table_name=? "
                        "AND status IN ('preparing','prepared')",
                        (current,),
                    ).fetchall()
                except sqlite3.OperationalError:
                    staged_rows = []
                for row in staged_rows:
                    protected.update(file['path'] for file in json.loads(row['files']))

            preparation_active = self._has_preparing_batches(connection, table_name)
            inventory = self.objects.inventory()
            if table_name is not None:
                prefix = f'tables/{table_name}/'
                inventory = [item for item in inventory if item['path'].startswith(prefix)]
            candidates = sorted(item['path'] for item in inventory if item['path'] not in protected)

            deleted, deferred = [], []
            for relative in candidates:
                if preparation_active:
                    # A preparing writer may be creating objects outside any catalog
                    # transaction; never delete unreferenced data while one is live.
                    deferred.append(relative)
                    continue
                self.objects.delete(relative)
                deleted.append(relative)

            finished = timestamp()
            connection.execute(
                'UPDATE gc_runs SET status=?,protected_objects=?,deleted_objects=?,'
                'deferred_objects=?,active_preparation=?,finished_at=? WHERE id=?',
                ('completed', len(protected), len(deleted), len(deferred),
                 1 if preparation_active else 0, finished, run_id),
            )
            if table_name is None:
                targets = [row['name'] for row in connection.execute('SELECT name FROM lake_tables')]
            else:
                targets = [table_name]
            for current in targets:
                self.catalog.event(connection, current, 'objects_reclaimed',
                                   {'deleted': len(deleted), 'deferred': len(deferred),
                                    'active_preparation': preparation_active})
        return {'run_id': run_id, 'table': table_name, 'protected_objects': len(protected),
                'deleted_objects': deleted, 'deferred_objects': deferred,
                'active_preparation': preparation_active,
                'active_read_leases': len(active_leases), 'started_at': started, 'finished_at': finished}

    def status(self, table_name):
        self.catalog.table(table_name)
        with self.catalog.transaction() as connection:
            snapshots = [dict(row) for row in connection.execute(
                'SELECT id,operation,created_at FROM snapshots WHERE table_name=? ORDER BY id DESC',
                (table_name,),
            )]
            expired = {row[0]: row[1] for row in connection.execute(
                'SELECT snapshot_id,expired_at FROM snapshot_expirations WHERE table_name=?', (table_name,)
            )}
            leases = [dict(row) for row in connection.execute(
                'SELECT token,snapshot_id,created_at,expires_at FROM read_leases '
                'WHERE table_name=? AND released_at IS NULL ORDER BY created_at', (table_name,)
            )]
            try:
                staged = [dict(row) for row in connection.execute(
                    'SELECT batch_id,status FROM prepared_batches WHERE table_name=? ORDER BY batch_id',
                    (table_name,),
                )]
            except sqlite3.OperationalError:
                staged = []
        now = timestamp()
        for snapshot in snapshots:
            snapshot['expired'] = snapshot['id'] in expired
            if snapshot['expired']:
                snapshot['expired_at'] = expired[snapshot['id']]
            snapshot['pinned'] = any(lease['snapshot_id'] == snapshot['id']
                                     and lease['expires_at'] > now for lease in leases)
        return {'table': table_name, 'snapshots': snapshots, 'active_leases': leases,
                'staged_batches': staged}

    def runs(self, limit=50):
        if type(limit) is not int or not 1 <= limit <= 1000:
            raise Invalid('limit must be between 1 and 1000')
        with self.catalog.transaction() as connection:
            return [dict(row) for row in connection.execute(
                'SELECT * FROM gc_runs ORDER BY started_at DESC LIMIT ?', (limit,)
            )]
