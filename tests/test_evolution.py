from pathlib import Path
from tempfile import TemporaryDirectory
import unittest

from schemalake.app import Lake
from schemalake.types import Conflict, Invalid


V1 = {'fields': [
    {'id': 1, 'name': 'id', 'type': 'int64'},
    {'id': 2, 'name': 'day', 'type': 'date'},
    {'id': 3, 'name': 'site', 'type': 'string'},
    {'id': 4, 'name': 'reading', 'type': 'int64'},
], 'primary_key': ['id'], 'partition_by': ['day']}

V2 = {'fields': [
    {'id': 1, 'name': 'id', 'type': 'int64'},
    {'id': 2, 'name': 'day', 'type': 'date'},
    {'id': 3, 'name': 'station', 'type': 'string'},
    {'id': 4, 'name': 'reading', 'type': 'decimal'},
    {'id': 5, 'name': 'quality', 'type': 'string', 'default': 'ok'},
], 'primary_key': ['id'], 'partition_by': ['day']}


def v1_row(i, day='2026-11-01', site='a', reading=12):
    return {'id': i, 'day': day, 'site': site, 'reading': reading}


def v2_row(i, day, station='b', reading='7.5', quality='ok'):
    return {'id': i, 'day': day, 'station': station, 'reading': reading, 'quality': quality}


class EvolutionTest(unittest.TestCase):
    def setUp(self):
        self.temporary = TemporaryDirectory()
        self.root = Path(self.temporary.name)
        self.lake = Lake(self.root / 'lake', file_rows=1)
        self.lake.catalog.create_table('readings', V1)
        self.lake.ingest.ingest('readings', 'b1', [v1_row(1), v1_row(2, reading=10)])

    def tearDown(self):
        self.temporary.cleanup()

    def evolve(self):
        return self.lake.evolution.evolve('readings', V2)

    def test_compatible_evolution_adds_version_and_describes_changes(self):
        result = self.evolve()
        self.assertEqual(result['schema_version'], 2)
        actions = {(c['action'], c['id']) for c in result['changes']}
        self.assertEqual(actions, {('rename', 3), ('widen', 4), ('add', 5)})
        self.assertEqual(self.lake.catalog.table('readings')['schema_version'], 2)
        versions = [item['version'] for item in self.lake.evolution.versions('readings')]
        self.assertEqual(versions, [1, 2])

    def test_identical_schema_is_idempotent(self):
        self.evolve()
        again = self.lake.evolution.evolve('readings', V2)
        self.assertTrue(again['replayed'])
        self.assertEqual(again['schema_version'], 2)
        self.assertEqual(len(self.lake.evolution.versions('readings')), 2)

    def test_old_files_project_to_unified_current_structure(self):
        self.evolve()
        current = list(self.lake.scan.rows('readings'))
        self.assertEqual(current, [
            {'id': 1, 'day': '2026-11-01', 'station': 'a', 'reading': '12', 'quality': 'ok'},
            {'id': 2, 'day': '2026-11-01', 'station': 'a', 'reading': '10', 'quality': 'ok'},
        ])
        # 整数测量值拓宽为精确小数字符串。
        self.assertTrue(all(isinstance(r['reading'], str) for r in current))
        plan = self.lake.scan.plan('readings')
        self.assertEqual(plan['schema_version'], 2)
        self.assertEqual(plan['manifest_schema_version'], 1)

    def test_historical_snapshot_keeps_original_meaning(self):
        before = list(self.lake.scan.rows('readings', 1))
        self.evolve()
        after = list(self.lake.scan.rows('readings', 1))
        self.assertEqual(before, after)
        self.assertTrue(all('site' in r and isinstance(r['reading'], int) for r in after))

    def test_new_batch_coexists_with_old_files(self):
        self.evolve()
        self.lake.ingest.ingest('readings', 'b2', [v2_row(3, '2026-11-02')])
        rows = list(self.lake.scan.rows('readings'))
        self.assertEqual(sorted(r['id'] for r in rows), [1, 2, 3])
        self.assertEqual(self.lake.scan.plan('readings')['file_schema_versions'], [1, 2])
        new_row = next(r for r in rows if r['id'] == 3)
        self.assertEqual((new_row['station'], new_row['reading']), ('b', '7.5'))

    def test_predicate_uses_new_names_across_old_files(self):
        self.evolve()
        self.lake.ingest.ingest('readings', 'b2', [v2_row(3, '2026-11-02', reading='7.25')])
        hits = [r['id'] for r in self.lake.scan.where(
            'readings', {'op': 'gt', 'field': 'reading', 'value': '11'})]
        self.assertEqual(hits, [1])
        stations = [r['id'] for r in self.lake.scan.where(
            'readings', {'op': 'eq', 'field': 'station', 'value': 'a'})]
        self.assertEqual(sorted(stations), [1, 2])

    def test_statistics_merge_across_versions(self):
        self.evolve()
        self.lake.ingest.ingest('readings', 'b2', [v2_row(3, '2026-11-02', reading='7.25')])
        stats = self.lake.scan.statistics('readings')['fields']
        self.assertEqual(str(stats['reading']['min']), '7.25')
        self.assertEqual(str(stats['reading']['max']), '12')
        self.assertEqual(stats['quality']['null_count'], 0)

    def test_nullable_new_column_projects_to_null(self):
        schema = {'fields': V2['fields'][:4] + [
            {'id': 5, 'name': 'note', 'type': 'string', 'nullable': True}],
            'primary_key': ['id'], 'partition_by': ['day']}
        self.lake.evolution.evolve('readings', schema)
        self.assertTrue(all(r['note'] is None for r in self.lake.scan.rows('readings')))

    def test_prepared_uncommitted_batch_cannot_mix_into_new_schema(self):
        prepared = self.lake.staged.prepare('readings', 'stale', [v1_row(9, '2026-11-03')])
        self.assertEqual(prepared['schema_version'], 1)
        self.evolve()
        with self.assertRaises(Conflict):
            self.lake.staged.commit('readings', 'stale')
        # 中止后表数据不变。
        self.assertEqual(sorted(r['id'] for r in self.lake.scan.rows('readings')), [1, 2])

    def test_incompatible_changes_are_rejected(self):
        self.evolve()

        def reject(schema):
            with self.assertRaises(Invalid):
                self.lake.evolution.evolve('readings', schema)

        # 主键列改名 / 主键集合变化
        reject({'fields': [dict(f, name='ident') if f['id'] == 1 else f for f in V2['fields']],
                'primary_key': ['ident'], 'partition_by': ['day']})
        # 分区列调整
        reject({'fields': V2['fields'], 'primary_key': ['id'], 'partition_by': ['station']})
        # 不支持的类型变更
        reject({'fields': [dict(f, type='int64') if f['id'] == 3 else f for f in V2['fields']],
                'primary_key': ['id'], 'partition_by': ['day']})
        # 删除字段
        reject({'fields': [f for f in V2['fields'] if f['id'] != 5],
                'primary_key': ['id'], 'partition_by': ['day']})
        # 新增非空且无默认值字段
        reject({'fields': V2['fields'] + [{'id': 6, 'name': 'mandatory', 'type': 'string'}],
                'primary_key': ['id'], 'partition_by': ['day']})
        # 既有字段可空性收紧：先演进一个可空新列
        nullable_v3 = {'fields': V2['fields'] + [
            {'id': 6, 'name': 'note', 'type': 'string', 'nullable': True}],
            'primary_key': ['id'], 'partition_by': ['day']}
        self.lake.evolution.evolve('readings', nullable_v3)
        reject({'fields': [dict(f, nullable=False) if f['id'] == 6 else f for f in nullable_v3['fields']],
                'primary_key': ['id'], 'partition_by': ['day']})
        # 所有拒绝都不推进版本号
        self.assertEqual(self.lake.catalog.table('readings')['schema_version'], 3)

    def test_expected_version_conflict(self):
        with self.assertRaises(Conflict):
            self.lake.evolution.evolve('readings', V2, expected_version=99)

    def test_compact_rewrites_mixed_files_while_history_stays(self):
        self.evolve()
        self.lake.ingest.ingest('readings', 'b2', [v2_row(3, '2026-11-02')])
        historical = list(self.lake.scan.rows('readings', 1))
        self.lake.maintenance.compact('readings', target_rows=100000)
        self.assertEqual(self.lake.scan.plan('readings')['file_schema_versions'], [2])
        self.assertEqual(list(self.lake.scan.rows('readings', 1)), historical)
        self.assertTrue(self.lake.diagnostics.check('readings')['ok'])

    def test_index_follows_evolution_and_historical_lookup_falls_back(self):
        self.evolve()
        self.lake.indexes.build('readings')
        current = self.lake.indexes.lookup('readings', {'id': 1})
        self.assertTrue(current['used_index'])
        self.assertEqual(current['rows'][0]['station'], 'a')
        historical = self.lake.indexes.lookup('readings', {'id': 1}, 1)
        self.assertFalse(historical['used_index'])
        self.assertEqual(historical['rows'][0]['site'], 'a')

    def test_derived_aggregate_reads_correct_values_per_snapshot(self):
        totals_v1 = {'fields': [
            {'id': 1, 'name': 'site', 'type': 'string'},
            {'id': 2, 'name': 'total', 'type': 'int64'},
            {'id': 3, 'name': 'n', 'type': 'int64'}], 'primary_key': ['site']}
        self.lake.catalog.create_table('totals_old', totals_v1)
        self.lake.aggregates.register('by_site', 'readings', 'totals_old', {
            'group_by': ['site'],
            'aggregates': {'total': {'op': 'sum', 'field': 'reading'},
                           'n': {'op': 'count', 'field': 'reading'}}})
        self.lake.aggregates.refresh('by_site', 1)
        self.assertEqual(sorted(self.lake.scan.rows('totals_old'), key=lambda r: r['site']),
                         [{'site': 'a', 'total': 22, 'n': 2}])
        self.assertEqual(self.lake.aggregates.preview('by_site', 1)['input_rows'], 2)
        self.evolve()
        # 引用旧列名的作业在新结构上被明确拒绝。
        with self.assertRaises(Invalid):
            self.lake.aggregates.preview('by_site')

    def test_rowdiff_across_schema_versions(self):
        self.evolve()
        self.lake.ingest.ingest('readings', 'b2', [v2_row(3, '2026-11-02', reading='5')])
        summary = self.lake.rowdiff.summary('readings', 1, 2)
        # 旧行因改名/加列/类型拓宽表现为 changed，新批次为 added，无 removed。
        self.assertEqual((summary['added'], summary['removed'], summary['changed']), (1, 0, 2))

    def test_predicate_is_scoped_to_snapshot_schema(self):
        self.evolve()
        # 历史快照只认旧列名和整数类型。
        hits = [r['id'] for r in self.lake.scan.where(
            'readings', {'op': 'gt', 'field': 'reading', 'value': 11}, snapshot_id=1)]
        self.assertEqual(hits, [1])
        with self.assertRaises(Invalid):
            list(self.lake.scan.where(
                'readings', {'op': 'eq', 'field': 'station', 'value': 'a'}, snapshot_id=1))
        # 当前快照用新列名、decimal 比较，跨旧文件生效。
        current = [r['id'] for r in self.lake.scan.where(
            'readings', {'op': 'ge', 'field': 'reading', 'value': '10'})]
        self.assertEqual(sorted(current), [1, 2])

    def test_head_services_use_new_structure_historical_keeps_old(self):
        self.evolve()
        self.lake.ingest.ingest('readings', 'b2', [v2_row(3, '2026-11-02', reading='7.25')])
        # 质量检查在 head 上按新列名运行。
        head_quality = self.lake.quality.evaluate(
            'readings', [{'kind': 'check', 'where': {'op': 'not_null', 'field': 'station'}}])
        self.assertTrue(head_quality['ok'])
        # 历史快照上的质量检查仍使用旧列名，新列名被拒绝。
        with self.assertRaises(Invalid):
            self.lake.quality.evaluate(
                'readings', [{'kind': 'check', 'where': {'op': 'not_null', 'field': 'station'}}],
                snapshot_id=1)
        old_quality = self.lake.quality.evaluate(
            'readings', [{'kind': 'check', 'where': {'op': 'not_null', 'field': 'site'}}],
            snapshot_id=1)
        self.assertTrue(old_quality['ok'])
        # 排序在 head 上按 decimal 语义跨旧文件。
        ordered = [r['id'] for r in self.lake.sort.rows('readings', [{'field': 'reading'}])]
        self.assertEqual(ordered, [3, 2, 1])
        # 导出 head 为统一新结构，导出历史快照保留旧结构。
        import json
        head_export = self.lake.exports.create('readings', format='jsonl')
        head_lines = [json.loads(l) for l in
                      self.lake.exports.artifact(head_export['id']).read_text().splitlines()]
        self.assertTrue(all('station' in r and isinstance(r['reading'], str) for r in head_lines))
        old_export = self.lake.exports.create('readings', snapshot_id=1, format='jsonl')
        old_lines = [json.loads(l) for l in
                     self.lake.exports.artifact(old_export['id']).read_text().splitlines()]
        self.assertTrue(all('site' in r and isinstance(r['reading'], int) for r in old_lines))

    def test_exchange_roundtrip_preserves_unified_rows(self):
        self.evolve()
        self.lake.ingest.ingest('readings', 'b2', [v2_row(3, '2026-11-02', reading='7.25')])
        package = self.root / 'snapshot.zip'
        self.lake.exchange.export('readings', package)
        inspected = self.lake.exchange.inspect(package)
        self.assertEqual(inspected['rows'], 3)
        self.lake.exchange.import_snapshot('imported', 'exchange1', package)
        self.assertEqual(list(self.lake.scan.rows('imported')),
                         list(self.lake.scan.rows('readings')))


if __name__ == '__main__':
    unittest.main()
