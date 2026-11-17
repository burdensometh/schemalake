from decimal import Decimal
import json

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
        selected = [file for file in plan['files'] if predicate.may_match_file(file)]
        for row in self.scan_files(plan, schema, selected):
            if predicate.matches(row):
                yield {key: row[key] for key in columns}

    def snapshot(self, table_name, snapshot_id=None):
        with self.catalog.transaction() as connection:
            table = self.catalog.table(table_name, connection)
            snapshot_id = table["head"] if snapshot_id is None else snapshot_id
            if type(snapshot_id) is not int or snapshot_id < 0:
                raise Invalid('snapshot id must be a nonnegative integer')
            if snapshot_id == 0:
                return {"table": table_name, "snapshot_id": 0, "schema_version": table["schema_version"], "files": []}
            record = connection.execute(
                "SELECT * FROM snapshots WHERE table_name=? AND id=?", (table_name, snapshot_id)
            ).fetchone()
            if record is None:
                raise Missing("snapshot does not exist")
            return self.objects.manifest(dict(record))

    def plan(self, table_name, snapshot_id=None, partition=None):
        manifest = self.snapshot(table_name, snapshot_id)
        schema = self.catalog.schema(table_name, manifest["schema_version"])
        partition = partition or {}
        if set(partition) - set(schema.partition_by):
            raise Invalid("partition filter references a nonpartition column")
        field_types = {field.name: field.type for field in schema.fields}
        normalized = {key: convert(value, field_types[key]) for key, value in partition.items()}
        selected = [file for file in manifest["files"] if all(file["partition"].get(k) == v for k, v in normalized.items())]
        from .manifests import ManifestValidator
        validator = ManifestValidator()
        for file in manifest['files']:
            validator.schema_binding(schema, file, manifest['schema_version'])
        return {
            "table": table_name,
            "snapshot_id": manifest["snapshot_id"],
            "schema_version": manifest["schema_version"],
            "files": selected,
            "files_skipped": len(manifest["files"]) - len(selected),
            "rows": sum(file["row_count"] for file in selected),
            "bytes": sum(file["byte_count"] for file in selected),
        }

    def rows(self, table_name, snapshot_id=None, partition=None, columns=None):
        plan = self.plan(table_name, snapshot_id, partition)
        schema = self.catalog.schema(table_name, plan["schema_version"])
        known = [field.name for field in schema.fields]
        columns = known if columns is None else columns
        if not isinstance(columns, list) or set(columns) - set(known):
            raise Invalid("projection includes unknown columns")
        for row in self.scan_files(plan, schema, plan["files"]):
            yield {column: row[column] for column in columns}

    def scan_files(self, plan, schema, files):
        for file in files:
            if file["schema_version"] != plan["schema_version"]:
                raise Conflict("baseline snapshots require one schema version")
            for raw in self.objects.read_rows(file):
                row = schema.normalize(raw)
                if schema.partition(row) != file["partition"]:
                    raise Conflict("row does not belong to its declared partition")
                yield row

    def statistics(self, table_name, snapshot_id=None, partition=None):
        plan = self.plan(table_name, snapshot_id, partition)
        schema = self.catalog.schema(table_name, plan["schema_version"])
        output = {}
        for field in schema.fields:
            stats = [file["statistics"][field.name] for file in plan["files"]]
            minima = [item["min"] for item in stats if item["min"] is not None]
            maxima = [item["max"] for item in stats if item["max"] is not None]
            key = Decimal if field.type == "decimal" else lambda value: value
            output[field.name] = {
                "null_count": sum(item["null_count"] for item in stats),
                "min": min(minima, key=key) if minima else None,
                "max": max(maxima, key=key) if maxima else None,
            }
        return {"rows": plan["rows"], "files": len(plan["files"]), "fields": output}

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
