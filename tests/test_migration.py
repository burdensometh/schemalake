from collections import Counter
from pathlib import Path
from shutil import copy2, copytree
from tempfile import TemporaryDirectory
from unittest.mock import patch
import json
import sqlite3
import unittest

from schemalake.app import Lake
from schemalake.manifests import FORMAT_V1, FORMAT_V2, derive_delta
from schemalake.mirror import Mirror
from test_core import SCHEMA, row


FIXTURE = Path(__file__).resolve().parent.parent / 'fixtures' / 'legacy-v1'
EXPORT_ID = '3e03f06678c14213acc7b97aef7a21c3'


class FixtureMigrationTest(unittest.TestCase):
    def setUp(self):
        self.temporary = TemporaryDirectory()
        self.root = Path(self.temporary.name)
        copytree(FIXTURE / 'state', self.root / 'state')
        copy2(FIXTURE / 'consumer.sqlite', self.root / 'consumer.sqlite')
        self.lake = Lake(self.root / 'state')

    def tearDown(self):
        self.temporary.cleanup()

    def observations(self):
        return {
            snapshot['id']: {
                'files': self.lake.objects.manifest(snapshot)['files'],
                'rows': list(self.lake.scan.rows('readings', snapshot['id'])),
                'rows_reported': snapshot['row_count'],
                'path': snapshot['manifest_path'],
            }
            for snapshot in self.lake.catalog.snapshots('readings')
        }

    def test_migration_preserves_identity_rows_and_export(self):
        before = self.observations()
        export = self.lake.exports.artifact(EXPORT_ID).read_bytes()
        mirror = Mirror(self.root / 'consumer.sqlite')
        self.assertTrue(self.lake.reconcile.against_mirror(mirror, 'readings')['ok'])
        result = self.lake.migration.migrate()
        self.assertEqual(result['migrated'], 3)
        after = self.observations()
        self.assertEqual({key: value['files'] for key, value in before.items()},
                         {key: value['files'] for key, value in after.items()})
        self.assertEqual({key: value['rows'] for key, value in before.items()},
                         {key: value['rows'] for key, value in after.items()})
        self.assertEqual({key: value['rows_reported'] for key, value in before.items()},
                         {key: value['rows_reported'] for key, value in after.items()})
        self.assertEqual(export, self.lake.exports.artifact(EXPORT_ID).read_bytes())
        self.assertTrue(self.lake.reconcile.against_mirror(mirror, 'readings')['ok'])
        self.assertTrue(self.lake.diagnostics.check('readings')['ok'])
        self.assertEqual(self.lake.diagnostics.inventory()['missing_paths'], [])
        for snapshot in self.lake.catalog.snapshots('readings'):
            self.assertEqual(self.lake.objects.manifest(snapshot)['format_version'], FORMAT_V2)

    def test_manifest_rewrites_do_not_touch_data_objects(self):
        data = sorted(path.stat().st_size for path in (self.root / 'state/objects/tables/readings/data').rglob('*.jsonl'))
        self.lake.migration.migrate()
        after = sorted(path.stat().st_size for path in (self.root / 'state/objects/tables/readings/data').rglob('*.jsonl'))
        self.assertEqual(data, after)

    def test_duplicate_object_references_survive(self):
        self.lake.migration.migrate()
        appearances = Counter()
        for snapshot in self.lake.catalog.snapshots('readings'):
            appearances.update(file['id'] for file in self.lake.objects.manifest(snapshot)['files'])
        self.assertEqual(appearances, Counter({
            'c1bb9be8d83cae8ab3018d342c0d73d3b0e76a822c18d2d04f1520c214e4fdc3': 2,
            'f59759037dac13145d8aef43e835c6163eed3519a11c588877bbd7fbdcf9043b': 3,
            'fda0a27012eb6b54e54b2f96cd2eea851d60533eaa693059b8cdcd1403dc3147': 1,
        }))

    def test_new_reader_reads_v1_before_migration(self):
        status = self.lake.migration.status()
        self.assertFalse(status['complete'])
        self.assertEqual(status['tables']['readings'], {'snapshots': 3, 'v1': 3, 'v2': 0})
        for snapshot in self.lake.catalog.snapshots('readings'):
            self.assertEqual(self.lake.objects.manifest(snapshot)['format_version'], FORMAT_V1)
        self.assertEqual([r['id'] for r in self.lake.scan.rows('readings')], [2, 3])

    def test_interrupted_migration_is_resumable(self):
        original = self.lake.objects.write_manifest
        calls = {'n': 0}

        def interrupted(table_name, document):
            calls['n'] += 1
            if calls['n'] == 2:
                raise OSError('author interruption')
            return original(table_name, document)

        with patch.object(self.lake.objects, 'write_manifest', interrupted):
            with self.assertRaises(OSError):
                self.lake.migration.migrate()
        partial = self.lake.migration.status()
        self.assertEqual(partial['tables']['readings']['v1'], 2)
        self.assertEqual(partial['tables']['readings']['v2'], 1)
        self.assertEqual(self.lake.catalog.table('readings')['head'], 3)
        self.assertEqual([r['id'] for r in self.lake.scan.rows('readings')], [2, 3])
        resumed = self.lake.migration.migrate()
        self.assertEqual(resumed['status']['tables']['readings']['v1'], 0)
        self.assertEqual(resumed['status']['tables']['readings']['v2'], 3)
        self.assertTrue(self.lake.diagnostics.check('readings')['ok'])

    def test_migration_failure_never_publishes_a_partial_snapshot(self):
        with patch.object(self.lake.migration, '_migrate_one', side_effect=OSError('author fault')):
            with self.assertRaises(OSError):
                self.lake.migration.migrate()
        self.assertEqual(self.lake.catalog.table('readings')['head'], 3)
        self.assertEqual([r['id'] for r in self.lake.scan.rows('readings')], [2, 3])
        self.assertEqual(self.lake.migration.status()['tables']['readings']['v1'], 3)
        self.lake.migration.migrate()
        self.assertTrue(self.lake.diagnostics.check('readings')['ok'])

    def test_migration_is_idempotent(self):
        first = self.lake.migration.migrate()
        self.assertEqual(first['migrated'], 3)
        second = self.lake.migration.migrate()
        self.assertEqual(second['migrated'], 0)
        self.assertTrue(second['status']['complete'])
        self.assertEqual(second['status']['format_version'], FORMAT_V2)

    def test_migration_can_run_in_steps(self):
        self.assertEqual(self.lake.migration.migrate(limit=1)['status']['tables']['readings']['v1'], 2)
        self.assertEqual(self.lake.migration.migrate(limit=1)['status']['tables']['readings']['v1'], 1)
        final = self.lake.migration.migrate()
        self.assertTrue(final['status']['complete'])

    def test_v1_children_read_while_parent_is_migrated(self):
        self.lake.migration.migrate(limit=1)
        self.assertEqual([r['id'] for r in self.lake.scan.rows('readings', 1)], [1, 2])
        self.assertEqual([r['id'] for r in self.lake.scan.rows('readings', 2)], [1, 2])
        self.assertEqual([r['id'] for r in self.lake.scan.rows('readings', 3)], [2, 3])

    def test_write_between_migration_steps_keeps_references_intact(self):
        # Migrate only the oldest snapshot, then append while the head's own
        # manifest is still v1, then finish migration. The append's base points
        # at the old v1 document, which must remain resolvable after the head
        # snapshot is rewritten.
        self.lake.migration.migrate(limit=1)
        self.lake.ingest.ingest(
            'readings', 'gap', [{'id': 8, 'day': '2026-11-05', 'site': 'c', 'reading': '2'}])
        self.assertEqual(self.lake.catalog.table('readings')['head'], 4)
        result = self.lake.migration.migrate()
        self.assertTrue(result['status']['complete'])
        self.assertEqual([r['id'] for r in self.lake.scan.rows('readings')], [2, 3, 8])
        for snapshot in self.lake.catalog.snapshots('readings'):
            document = self.lake.objects.manifest(snapshot)
            self.assertEqual(document['format_version'], FORMAT_V2)
            self.assertEqual(sum(f['row_count'] for f in document['files']), snapshot['row_count'])
        self.assertTrue(self.lake.diagnostics.check('readings')['ok'])
        self.assertEqual(self.lake.diagnostics.inventory()['missing_paths'], [])

    def test_migration_then_append_reuses_resolved_metadata(self):
        self.lake.migration.migrate()
        self.lake.ingest.ingest(
            'readings', 'after', [{'id': 9, 'day': '2026-11-04', 'site': 'b', 'reading': '1'}])
        record = self.lake.catalog.snapshots('readings')[0]
        document = json.loads(self.lake.objects.read(
            record['manifest_path'], record['manifest_sha256']))
        self.assertEqual(document['format_version'], FORMAT_V2)
        self.assertEqual(document['base']['snapshot_id'], 3)
        self.assertEqual(len(document['files']), 1)
        self.assertEqual(document['removed'], {})
        self.assertEqual([r['id'] for r in self.lake.scan.rows('readings')], [2, 3, 9])

    def test_prepared_batch_still_commits_after_migration(self):
        self.lake.migration.migrate()
        self.lake.staged.commit('readings', 'prepared')
        self.assertEqual([r['id'] for r in self.lake.scan.rows('readings')], [2, 3, 4])
        self.assertTrue(self.lake.diagnostics.check('readings')['ok'])


class V2WriteTest(unittest.TestCase):
    def setUp(self):
        self.temporary = TemporaryDirectory()
        self.root = Path(self.temporary.name)
        self.lake = Lake(self.root / 'lake', file_rows=1)
        self.lake.catalog.create_table('readings', SCHEMA)

    def tearDown(self):
        self.temporary.cleanup()

    def append(self, batch, rows):
        return self.lake.ingest.ingest('readings', batch, rows)

    def newest_document(self):
        snapshot = self.lake.catalog.snapshots('readings')[0]
        return json.loads(self.lake.objects.read(
            snapshot['manifest_path'], snapshot['manifest_sha256']))

    def test_appends_only_describe_changed_files(self):
        sizes = []
        for index in range(20):
            self.append('b%d' % index, [row(index)])
            document = self.newest_document()
            self.assertEqual(document['format_version'], FORMAT_V2)
            self.assertEqual(len(document['files']), 1)
            self.assertEqual(document['removed'], {})
            sizes.append(len(json.dumps(document)))
        self.assertLess(sizes[-1] - sizes[0], 400)
        self.assertEqual(len(list(self.lake.scan.rows('readings'))), 20)
        self.assertTrue(self.lake.diagnostics.check('readings')['ok'])

    def test_total_manifest_descriptors_stay_linear(self):
        for index in range(10):
            self.append('b%d' % index, [row(index)])
        records = self.lake.catalog.snapshots('readings')
        described = sum(len(json.loads(self.lake.objects.read(
            record['manifest_path'], record['manifest_sha256']))['files']) for record in records)
        self.assertEqual(described, 10)

    def test_compaction_and_replacement_keep_row_multisets(self):
        self.append('b0', [row(1), row(2, '2026-11-02')])
        self.append('b1', [row(3, '2026-11-03')])
        before = list(self.lake.scan.rows('readings'))
        self.lake.maintenance.compact('readings', target_rows=10)
        self.assertEqual(list(self.lake.scan.rows('readings')), before)
        self.lake.maintenance.replace('readings', [row(4)], {'day': '2026-11-01'})
        schema = self.lake.catalog.schema('readings')
        expected = sorted([r for r in before if r['day'] != '2026-11-01']
                          + [schema.normalize(row(4))], key=lambda r: (r['day'], r['id']))
        actual = sorted(self.lake.scan.rows('readings'), key=lambda r: (r['day'], r['id']))
        self.assertEqual(actual, expected)
        for snapshot in self.lake.catalog.snapshots('readings'):
            document = self.lake.objects.manifest(snapshot)
            self.assertEqual(document['format_version'], FORMAT_V2)
            self.assertEqual(sum(f['row_count'] for f in document['files']), snapshot['row_count'])
        self.assertTrue(self.lake.diagnostics.check('readings')['ok'])

    def test_staged_commit_into_delta(self):
        self.append('b0', [row(1)])
        self.lake.staged.prepare('readings', 'staged', [row(2)])
        self.lake.staged.commit('readings', 'staged')
        self.assertEqual([r['id'] for r in self.lake.scan.rows('readings')], [1, 2])
        document = self.newest_document()
        self.assertEqual(len(document['files']), 1)
        self.assertEqual(document['base']['snapshot_id'], 1)
        self.assertTrue(self.lake.diagnostics.check('readings')['ok'])

    def test_concurrent_reader_sees_one_complete_snapshot(self):
        self.append('b0', [row(1)])
        seen = {}
        original = self.lake.objects.write_manifest

        def observe(table_name, document):
            # Read the persisted catalog outside the uncommitted writer
            # transaction. WAL permits readers while one writer is active.
            connection = sqlite3.connect(self.lake.catalog.path, timeout=10)
            try:
                seen['head'] = connection.execute(
                    "SELECT head FROM lake_tables WHERE name='readings'").fetchone()[0]
            finally:
                connection.close()
            seen['document'] = document
            return original(table_name, document)

        with patch.object(self.lake.objects, 'write_manifest', observe):
            self.append('b1', [row(2)])
        # The manifest object exists before the catalog row does, so an
        # interrupted commit is never visible as a half-published snapshot.
        self.assertEqual(seen['head'], 1)
        self.assertEqual(seen['document']['base']['snapshot_id'], 1)
        self.assertEqual(self.lake.catalog.table('readings')['head'], 2)
        self.assertEqual([r['id'] for r in self.lake.scan.rows('readings')], [1, 2])
        self.assertTrue(self.lake.diagnostics.check('readings')['ok'])
        self.assertEqual(self.lake.diagnostics.inventory()['missing_paths'], [])

    def test_failed_commit_publishes_no_mixed_snapshot(self):
        self.append('b0', [row(1)])
        with patch.object(self.lake.objects, 'write_manifest', side_effect=OSError('author fault')):
            with self.assertRaises(OSError):
                self.append('b1', [row(2)])
        self.assertEqual(self.lake.catalog.table('readings')['head'], 1)
        self.assertIsNone(self.lake.catalog.batch('readings', 'b1'))
        self.assertEqual([r['id'] for r in self.lake.scan.rows('readings')], [1])
        self.assertTrue(self.lake.diagnostics.check('readings')['ok'])
        self.append('b2', [row(3)])
        self.assertEqual([r['id'] for r in self.lake.scan.rows('readings')], [1, 3])
        self.assertTrue(self.lake.diagnostics.check('readings')['ok'])

    def test_derive_delta_handles_repeated_references(self):
        descriptor = {'id': 'a' * 64, 'sha256': 'a' * 64, 'path': 'p', 'partition': {},
                      'row_count': 1, 'byte_count': 1, 'schema_version': 1,
                      'statistics': {'x': {'null_count': 0, 'min': 1, 'max': 1}}}
        second = dict(descriptor, id='b' * 64, sha256='b' * 64)
        removed, added = derive_delta([descriptor, second], [descriptor])
        self.assertEqual(removed, {'b' * 64: 1})
        self.assertEqual(added, [])

    def test_gzip_snapshot_reads_after_delta(self):
        self.append('b0', [row(1)])
        compressed = Lake(self.lake.home, file_rows=1, encoding='gzip-jsonl')
        compressed.ingest.ingest('readings', 'b1', [row(2)])
        self.assertEqual(len(list(compressed.scan.rows('readings'))), 2)
        self.assertTrue(compressed.diagnostics.check('readings')['ok'])
        record = compressed.catalog.snapshots('readings')[0]
        document = json.loads(compressed.objects.read(
            record['manifest_path'], record['manifest_sha256']))
        self.assertEqual(len(document['files']), 1)


if __name__ == '__main__':
    unittest.main()
