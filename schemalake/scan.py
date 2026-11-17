from decimal import Decimal

from .manifests import ManifestValidator
from .projection import project_row
from .types import Conflict, Invalid, Missing, convert


class Scanner:
    def __init__(self, catalog, objects):
        self.catalog = catalog
        self.objects = objects

    def where(self, table_name, expression, snapshot_id=None, partition=None, columns=None):
        from .predicates import Predicate
        plan = self.plan(table_name, snapshot_id, partition)
        schema = self.catalog.schema(table_name, plan['schema_version'])
        predicate = Predicate(schema, expression)
        known = [field.name for field in schema.fields]
        columns = known if columns is None else columns
        if not isinstance(columns, list) or set(columns) - set(known):
            raise Invalid('projection includes unknown columns')
        # 透传用户的快照参数：None 表示 head 并投影到当前 schema。
        for row in self.rows(table_name, snapshot_id, partition):
            if predicate.matches(row):
                yield {key: row[key] for key in columns}

    def resolve(self, table_name, snapshot_id=None, connection=None):
        """返回 (快照记录清单, 读取时使用的 schema 版本)。

        固定历史快照锁定清单自身的 schema 版本，旧文件按当时结构解释，
        历史含义不变；读取 head（当前快照）时使用表的当前 schema 版本，
        旧文件按写入版本解析后投影到新结构，与新批次统一呈现。
        """
        table = self.catalog.table(table_name, connection)
        requested_head = snapshot_id is None
        snapshot_id = table['head'] if requested_head else snapshot_id
        if type(snapshot_id) is not int or snapshot_id < 0:
            raise Invalid('snapshot id must be a nonnegative integer')
        if snapshot_id == 0:
            empty = {"table": table_name, "snapshot_id": 0,
                     "schema_version": table["schema_version"], "files": []}
            return empty, table["schema_version"]
        record = connection.execute(
            "SELECT * FROM snapshots WHERE table_name=? AND id=?", (table_name, snapshot_id)
        ).fetchone()
        if record is None:
            raise Missing("snapshot does not exist")
        manifest = self.objects.manifest(dict(record))
        read_version = table["schema_version"] if requested_head else manifest["schema_version"]
        return manifest, read_version

    def snapshot(self, table_name, snapshot_id=None):
        with self.catalog.transaction() as connection:
            manifest, _ = self.resolve(table_name, snapshot_id, connection)
            return manifest

    def file_schemas(self, table_name, manifest, connection=None):
        """加载快照内每个文件写入版本对应的 schema，按版本缓存。"""
        versions = {file['schema_version'] for file in manifest['files']}
        if connection is None:
            return {version: self.catalog.schema(table_name, version) for version in versions}
        return {version: self.catalog.schema(table_name, version, connection) for version in versions}

    def plan(self, table_name, snapshot_id=None, partition=None):
        with self.catalog.transaction() as connection:
            manifest, read_version = self.resolve(table_name, snapshot_id, connection)
            schema = self.catalog.schema(table_name, read_version, connection)
            partition = partition or {}
            if set(partition) - set(schema.partition_by):
                raise Invalid("partition filter references a nonpartition column")
            field_types = {field.name: field.type for field in schema.fields}
            normalized = {key: convert(value, field_types[key]) for key, value in partition.items()}
            selected = [file for file in manifest["files"] if all(file["partition"].get(k) == v for k, v in normalized.items())]
            validator = ManifestValidator()
            file_schemas = self.file_schemas(table_name, manifest, connection)
            for file in manifest['files']:
                # 每个文件按它自己写入时的 schema 版本绑定；演进后同一快照可混合多版本。
                validator.schema_binding(file_schemas[file['schema_version']], file, file['schema_version'])
        return {
            "table": table_name,
            "snapshot_id": manifest["snapshot_id"],
            "schema_version": read_version,
            "manifest_schema_version": manifest["schema_version"],
            "file_schema_versions": sorted(file_schemas),
            "files": selected,
            "files_skipped": len(manifest["files"]) - len(selected),
            "rows": sum(file["row_count"] for file in selected),
            "bytes": sum(file["byte_count"] for file in selected),
        }

    def rows(self, table_name, snapshot_id=None, partition=None, columns=None):
        with self.catalog.transaction() as connection:
            manifest, read_version = self.resolve(table_name, snapshot_id, connection)
            schema = self.catalog.schema(table_name, read_version, connection)
            known = [field.name for field in schema.fields]
            columns = known if columns is None else columns
            if not isinstance(columns, list) or set(columns) - set(known):
                raise Invalid("projection includes unknown columns")
            partition = partition or {}
            if set(partition) - set(schema.partition_by):
                raise Invalid("partition filter references a nonpartition column")
            field_types = {field.name: field.type for field in schema.fields}
            normalized_partition = {key: convert(value, field_types[key])
                                    for key, value in partition.items()}
            versions = {file['schema_version'] for file in manifest['files']}
            file_schemas = {version: self.catalog.schema(table_name, version, connection) for version in versions}
            files = [file for file in manifest['files']
                     if all(file["partition"].get(k) == v for k, v in normalized_partition.items())]
        for file in files:
            source_schema = file_schemas[file['schema_version']]
            for raw in self.objects.read_rows(file):
                if file['schema_version'] == read_version:
                    row = schema.normalize(raw)
                else:
                    row = project_row(raw, source_schema, schema)
                # 分区列不允许演进，名称与取值在各版本间稳定。
                if schema.partition(row) != file["partition"]:
                    raise Conflict("row does not belong to its declared partition")
                yield {column: row[column] for column in columns}

    def statistics(self, table_name, snapshot_id=None, partition=None):
        with self.catalog.transaction() as connection:
            manifest, read_version = self.resolve(table_name, snapshot_id, connection)
            schema = self.catalog.schema(table_name, read_version, connection)
            versions = {file['schema_version'] for file in manifest['files']}
            file_schemas = {version: self.catalog.schema(table_name, version, connection) for version in versions}
            partition = partition or {}
            if set(partition) - set(schema.partition_by):
                raise Invalid("partition filter references a nonpartition column")
            field_types = {field.name: field.type for field in schema.fields}
            normalized_partition = {key: convert(value, field_types[key])
                                    for key, value in (partition or {}).items()}
            files = [file for file in manifest['files']
                     if all(file["partition"].get(k) == v for k, v in normalized_partition.items())]
        output = {}
        for field in schema.fields:
            null_count = 0
            minima = []
            maxima = []
            for file in files:
                source = file_schemas[file['schema_version']]
                source_field = next((item for item in source.fields if item.id == field.id), None)
                if source_field is None:
                    # 新增字段：旧文件按默认值/空值投影，统计上全部视为该默认取值。
                    if field.default is not None:
                        bound = convert(field.default, field.type)
                        minima.append(bound)
                        maxima.append(bound)
                    else:
                        null_count += file['row_count']
                    continue
                stats = file["statistics"][source_field.name]
                null_count += stats["null_count"]
                if stats["min"] is not None:
                    low = convert(stats["min"], field.type)
                    high = convert(stats["max"], field.type)
                    if field.type == "decimal":
                        low, high = Decimal(low), Decimal(high)
                    minima.append(low)
                    maxima.append(high)
            key = Decimal if field.type == "decimal" else lambda value: value
            output[field.name] = {
                "null_count": null_count,
                "min": min(minima, key=key) if minima else None,
                "max": max(maxima, key=key) if maxima else None,
            }
        return {"rows": sum(file["row_count"] for file in files), "files": len(files), "fields": output}

    def verify(self, table_name):
        failures = []
        snapshots = self.catalog.snapshots(table_name)
        checked = set()
        for snapshot in snapshots:
            try:
                manifest = self.objects.manifest(snapshot)
                for file in manifest["files"]:
                    if file["path"] not in checked:
                        list(self.objects.read_rows(file))
                        checked.add(file["path"])
            except (Conflict, Invalid, Missing, ValueError) as error:
                failures.append({"snapshot": snapshot["id"], "error": str(error)})
        return {"ok": not failures, "snapshots": len(snapshots), "files": len(checked), "failures": failures}
