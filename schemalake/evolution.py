from .projection import WIDENINGS
from .types import Conflict, Invalid, Schema, canonical, timestamp
import json


class EvolutionPlanner:
    """按 field id 计算两个 schema 之间的兼容演进。

    只接受三类变更：
    1. 新增字段，且新字段可空或带默认值（旧文件没有它的值）；
    2. 普通字段改名，按相同 field id 识别，主键/分区列禁止改名；
    3. int64 -> decimal 的安全拓宽，其余类型变更一律拒绝。
    主键列、分区列及其顺序不允许任何调整。
    """

    def __init__(self, current, candidate):
        self.current = current
        self.candidate = candidate
        old_by_id = {field.id: field for field in current.fields}
        new_by_id = {field.id: field for field in candidate.fields}
        self.changes = []
        for field_id in sorted(set(old_by_id) | set(new_by_id)):
            old = old_by_id.get(field_id)
            new = new_by_id.get(field_id)
            action = 'add' if old is None else ('drop' if new is None else 'change')
            self.changes.append((action, old, new))

    def validate(self):
        if self.candidate.partition_by != self.current.partition_by:
            raise Invalid('partition columns cannot evolve')
        if self.candidate.primary_key != self.current.primary_key:
            raise Invalid('primary key columns cannot evolve')
        protected = set(self.current.primary_key) | set(self.current.partition_by)
        protected_ids = {field.id for field in self.current.fields if field.name in protected}
        max_old_id = max(field.id for field in self.current.fields)
        for action, old, new in self.changes:
            if action == 'drop':
                raise Invalid(f'dropping field {old.name} is not supported')
            if action == 'add':
                if new.id <= max_old_id:
                    raise Invalid('new fields must use an id larger than every existing id')
                if not new.nullable and not new.has_default:
                    raise Invalid(f'new field {new.name} must be nullable or carry a default')
                continue
            if new.id in protected_ids:
                if new.name != old.name:
                    raise Invalid(f'key or partition column {old.name} cannot be renamed')
                if new.type != old.type:
                    raise Invalid(f'key or partition column {old.name} cannot change type')
                if (new.nullable != old.nullable or new.has_default != old.has_default
                        or new.default != old.default):
                    raise Invalid(
                        f'key or partition column {old.name} cannot change nullability or default')
                continue
            if new.type != old.type and (old.type, new.type) not in WIDENINGS:
                raise Invalid(f'incompatible type change for {old.name}: {old.type} -> {new.type}')
            if new.nullable != old.nullable:
                raise Invalid(f'nullability of existing field {old.name} cannot change')
            if new.has_default != old.has_default or new.default != old.default:
                raise Invalid(f'default of existing field {old.name} cannot change')
        return self.candidate

    def describe(self):
        summary = []
        for action, old, new in self.changes:
            if action == 'add':
                summary.append({'action': 'add', 'id': new.id, 'name': new.name, 'type': new.type})
            elif action == 'change':
                if old.name != new.name:
                    summary.append({'action': 'rename', 'id': new.id, 'from': old.name, 'to': new.name})
                if old.type != new.type:
                    summary.append({'action': 'widen', 'id': new.id, 'name': new.name,
                                    'from': old.type, 'to': new.type})
        return summary


class SchemaEvolution:
    def __init__(self, catalog):
        self.catalog = catalog

    def evolve(self, table_name, new_schema, expected_version=None):
        candidate = Schema.parse(new_schema)
        with self.catalog.transaction(True) as connection:
            table = self.catalog.table(table_name, connection)
            current_version = table['schema_version']
            if expected_version is not None and expected_version != current_version:
                raise Conflict('schema version changed before evolution')
            current = self.catalog.schema(table_name, current_version, connection)
            if candidate.fingerprint() == current.fingerprint():
                return {'table': table_name, 'schema_version': current_version,
                        'changes': [], 'replayed': True}
            planner = EvolutionPlanner(current, candidate)
            planner.validate()
            next_version = current_version + 1
            connection.execute(
                'INSERT INTO schemas VALUES (?,?,?,?,?)',
                (table_name, next_version, canonical(candidate.encode()),
                 candidate.fingerprint(), timestamp()),
            )
            connection.execute(
                'UPDATE lake_tables SET schema_version=? WHERE name=?',
                (next_version, table_name),
            )
            summary = planner.describe()
            self.catalog.event(connection, table_name, 'schema_evolved',
                               {'from_version': current_version, 'to_version': next_version,
                                'changes': summary})
        return {'table': table_name, 'schema_version': next_version,
                'changes': summary, 'replayed': False}

    def versions(self, table_name):
        with self.catalog.transaction() as connection:
            self.catalog.table(table_name, connection)
            rows = connection.execute(
                'SELECT version, document, fingerprint, created_at FROM schemas '
                'WHERE table_name=? ORDER BY version', (table_name,),
            ).fetchall()
            return [{'version': row['version'], 'schema': json.loads(row['document']),
                     'fingerprint': row['fingerprint'], 'created_at': row['created_at']}
                    for row in rows]
