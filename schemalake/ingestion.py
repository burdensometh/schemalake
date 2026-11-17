from collections import defaultdict
import re

from .types import Conflict, Invalid, canonical, sha, timestamp


class BatchIngestor:
    def __init__(self, catalog, objects, file_rows=1000):
        if type(file_rows) is not int or file_rows < 1:
            raise Invalid("file_rows must be a positive integer")
        self.catalog = catalog
        self.objects = objects
        self.file_rows = file_rows

    def validate_batch_id(self, batch_id):
        if not isinstance(batch_id, str) or not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_.-]{0,99}", batch_id):
            raise Invalid("invalid batch id")

    def ingest(self, table_name, batch_id, rows, invalid="reject", expected_head=None):
        self.validate_batch_id(batch_id)
        if invalid not in {"reject", "quarantine"}:
            raise Invalid("invalid row policy must be reject or quarantine")
        if not isinstance(rows, list) or len(rows) > 100000:
            raise Invalid("batch must be an array of at most 100000 rows")
        input_sha = sha({"rows": rows, "invalid": invalid})
        with self.catalog.transaction(True) as connection:
            previous = self.catalog.batch(table_name, batch_id, connection)
            if previous:
                if previous["input_sha256"] != input_sha:
                    raise Conflict("batch id reused with different input")
                return {**previous, "replayed": True}
            table = self.catalog.table(table_name, connection)
            if expected_head is not None and expected_head != table["head"]:
                raise Conflict("table head changed before ingestion")
            schema = self.catalog.schema(table_name, connection=connection)
            partitions = defaultdict(list)
            keys = set()
            rejects = []
            accepted = 0
            for ordinal, raw in enumerate(rows):
                try:
                    row = schema.normalize(raw)
                    key = schema.key(row)
                    if key is not None:
                        if key in keys or connection.execute(
                            "SELECT 1 FROM row_keys WHERE table_name=? AND row_key=?", (table_name, key)
                        ).fetchone():
                            raise Invalid("duplicate primary key")
                        keys.add(key)
                    partitions[canonical(schema.partition(row))].append(row)
                    accepted += 1
                except Invalid as error:
                    if invalid == "reject":
                        raise Invalid(f"row {ordinal}: {error}") from error
                    rejects.append((ordinal, str(error), canonical(raw)))
            old_files = []
            if table["head"]:
                head = dict(connection.execute(
                    "SELECT * FROM snapshots WHERE table_name=? AND id=?", (table_name, table["head"])
                ).fetchone())
                old_files = self.objects.manifest(head)["files"]
            snapshot_id = table["head"] + 1
            added = []
            for partition_json, partition_rows in sorted(partitions.items()):
                import json
                partition = json.loads(partition_json)
                for offset in range(0, len(partition_rows), self.file_rows):
                    file = self.objects.write_rows(
                        table_name,
                        table["schema_version"],
                        schema,
                        partition,
                        partition_rows[offset:offset + self.file_rows],
                    )
                    added.append(file)
            document = {
                "format_version": 1,
                "table": table_name,
                "snapshot_id": snapshot_id,
                "schema_version": table["schema_version"],
                "parent_id": table["head"] or None,
                "operation": "append",
                "batch_id": batch_id,
                "files": old_files + added,
            }
            manifest_path, manifest_sha = self.objects.write_manifest(table_name, document)
            total_rows = sum(file["row_count"] for file in document["files"])
            connection.execute(
                "INSERT INTO snapshots VALUES (?,?,?,?,?,?,?,?)",
                (table_name, snapshot_id, table["head"] or None, "append", manifest_path, manifest_sha, total_rows, timestamp()),
            )
            for file in added:
                connection.execute(
                    "INSERT OR IGNORE INTO data_files VALUES (?,?,?,?,?,?,?,?,?,?)",
                    (
                        table_name, file["id"], file["schema_version"], canonical(file["partition"]),
                        file["path"], file["sha256"], file["row_count"], file["byte_count"],
                        canonical(file["statistics"]), snapshot_id,
                    ),
                )
            for key in keys:
                connection.execute("INSERT INTO row_keys VALUES (?,?,?)", (table_name, key, batch_id))
            for ordinal, reason, raw_json in rejects:
                connection.execute(
                    "INSERT INTO rejected_rows VALUES (?,?,?,?,?)", (table_name, batch_id, ordinal, reason, raw_json)
                )
            connection.execute(
                "INSERT INTO batches VALUES (?,?,?,?,?,?,?,?)",
                (table_name, batch_id, input_sha, table["schema_version"], snapshot_id, accepted, len(rejects), timestamp()),
            )
            connection.execute("UPDATE lake_tables SET head=? WHERE name=?", (snapshot_id, table_name))
            self.catalog.event(connection, table_name, "batch_committed", {
                "batch_id": batch_id, "rows": accepted, "rejected": len(rejects), "snapshot_id": snapshot_id,
            })
        return {**self.catalog.batch(table_name, batch_id), "replayed": False}
