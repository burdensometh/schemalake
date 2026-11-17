from collections import defaultdict
import json

from .commit import SnapshotCommitter
from .ingestion import BatchIngestor
from .types import Conflict, Invalid, Missing, canonical, convert, sha, timestamp
from .writer import DataWriter


DDL = """
CREATE TABLE IF NOT EXISTS change_keys (
    table_name TEXT NOT NULL REFERENCES lake_tables(name),
    row_key TEXT NOT NULL,
    partition INTEGER NOT NULL,
    version INTEGER NOT NULL,
    op TEXT NOT NULL CHECK(op IN ('upsert','delete')),
    event_sha256 TEXT NOT NULL,
    batch_id TEXT NOT NULL,
    PRIMARY KEY(table_name, row_key)
);
CREATE TABLE IF NOT EXISTS change_positions (
    table_name TEXT NOT NULL REFERENCES lake_tables(name),
    partition INTEGER NOT NULL,
    position INTEGER NOT NULL,
    event_sha256 TEXT NOT NULL,
    batch_id TEXT NOT NULL,
    PRIMARY KEY(table_name, partition, position)
);
CREATE TABLE IF NOT EXISTS change_progress (
    table_name TEXT NOT NULL REFERENCES lake_tables(name),
    partition INTEGER NOT NULL,
    next_position INTEGER NOT NULL,
    updated_at TEXT NOT NULL,
    PRIMARY KEY(table_name, partition)
);
CREATE TABLE IF NOT EXISTS change_batches (
    table_name TEXT NOT NULL REFERENCES lake_tables(name),
    batch_id TEXT NOT NULL,
    input_sha256 TEXT NOT NULL,
    snapshot_id INTEGER NOT NULL,
    receipt TEXT NOT NULL,
    committed_at TEXT NOT NULL,
    PRIMARY KEY(table_name, batch_id)
);
"""

OPS = {"upsert": "upsert", "insert": "upsert", "update": "upsert", "delete": "delete"}


class ChangeIngestor:
    """Resumable upsert/delete ingestion over versioned change events.

    Every event names its source partition and a position that is continuous
    inside that partition. A business key is bound to the first source
    partition it arrives on; newer key versions win over older ones; data,
    snapshot, key state and source progress commit in one transaction.
    """

    def __init__(self, catalog, objects, file_rows=1000):
        self.catalog = catalog
        self.objects = objects
        self.committer = SnapshotCommitter(catalog, objects)
        self.writer = DataWriter(objects, file_rows)
        with catalog.transaction(True) as connection:
            connection.executescript(DDL)

    def integer(self, value, label):
        if type(value) is not int or not 0 <= value < 2**63:
            raise Invalid(f"{label} must be a nonnegative int64")
        return value

    def key_of(self, schema, value):
        fields = {field.name: field for field in schema.fields}
        if not isinstance(value, dict) or set(value) != set(schema.primary_key):
            raise Invalid("key must contain exactly the primary key columns")
        try:
            return canonical([convert(value[column], fields[column].type) for column in schema.primary_key])
        except Invalid as error:
            raise Invalid(f"key: {error}") from error

    def parse_event(self, schema, event, ordinal):
        try:
            if not isinstance(event, dict):
                raise Invalid("change event must be an object")
            unknown = set(event) - {"op", "key", "row", "version", "position", "partition"}
            if unknown:
                raise Invalid("unknown event fields: " + ", ".join(sorted(unknown)))
            op = OPS.get(event.get("op"))
            if op is None:
                raise Invalid("op must be one of upsert, insert, update, delete")
            for label in ("version", "position", "partition"):
                if label not in event:
                    raise Invalid(f"event requires a {label}")
            version = self.integer(event["version"], "version")
            position = self.integer(event["position"], "position")
            partition = self.integer(event["partition"], "partition")
            row = None
            if op == "upsert":
                if "row" not in event:
                    raise Invalid("upsert event requires a row")
                row = schema.normalize(event["row"])
                key = schema.key(row)
                if "key" in event and self.key_of(schema, event["key"]) != key:
                    raise Invalid("event key does not match its row")
            else:
                if "row" in event:
                    raise Invalid("delete event must not carry a row")
                if "key" not in event:
                    raise Invalid("delete event requires a key")
                key = self.key_of(schema, event["key"])
            fingerprint = sha({"op": op, "key": key, "version": version, "row": row})
            return {"op": op, "key": key, "row": row, "version": version,
                    "position": position, "partition": partition, "fingerprint": fingerprint}
        except Invalid as error:
            raise Invalid(f"event {ordinal}: {error}") from error

    def key_states(self, connection, table_name, keys):
        states = {}
        keys = sorted(keys)
        for offset in range(0, len(keys), 500):
            chunk = keys[offset:offset + 500]
            marks = ",".join("?" for _ in chunk)
            rows = connection.execute(
                "SELECT row_key, partition, version, op, event_sha256 FROM change_keys "
                f"WHERE table_name=? AND row_key IN ({marks})",
                (table_name, *chunk),
            )
            for row in rows:
                states[row[0]] = {"partition": row[1], "version": row[2], "op": row[3], "fingerprint": row[4]}
        return states

    def existing_keys(self, connection, table_name, keys):
        found = set()
        keys = sorted(keys)
        for offset in range(0, len(keys), 500):
            chunk = keys[offset:offset + 500]
            marks = ",".join("?" for _ in chunk)
            rows = connection.execute(
                f"SELECT row_key FROM row_keys WHERE table_name=? AND row_key IN ({marks})",
                (table_name, *chunk),
            )
            found.update(row[0] for row in rows)
        return found

    def ingest(self, table_name, batch_id, events, expected_head=None):
        BatchIngestor.validate_batch_id(batch_id)
        if not isinstance(events, list) or len(events) > 100000:
            raise Invalid("change batch must be an array of at most 100000 events")
        input_sha = sha({"events": events})
        with self.catalog.transaction(True) as connection:
            previous = connection.execute(
                "SELECT input_sha256, receipt FROM change_batches WHERE table_name=? AND batch_id=?",
                (table_name, batch_id),
            ).fetchone()
            if previous:
                if previous["input_sha256"] != input_sha:
                    raise Conflict("change batch id reused with different input")
                return {**json.loads(previous["receipt"]), "replayed": True}
            table = self.catalog.table(table_name, connection)
            if expected_head is not None and expected_head != table["head"]:
                raise Conflict("table head changed before change ingestion")
            schema = self.catalog.schema(table_name, connection=connection)
            if not schema.primary_key:
                raise Invalid("change ingestion requires a primary key")
            parsed = [self.parse_event(schema, event, ordinal) for ordinal, event in enumerate(events)]

            # Identical deliveries of one stream slot collapse; divergent ones cannot pass.
            slots = {}
            unique = []
            skipped = 0
            for event in parsed:
                slot = (event["partition"], event["position"])
                if slot in slots:
                    if slots[slot]["fingerprint"] != event["fingerprint"]:
                        raise Conflict("one stream position carries two different events")
                    skipped += 1
                else:
                    slots[slot] = event
                    unique.append(event)

            # Positions advance contiguously per source partition; redelivered
            # positions must carry the committed event content.
            by_partition = defaultdict(list)
            for event in unique:
                by_partition[event["partition"]].append(event)
            progress = {}
            for partition in by_partition:
                row = connection.execute(
                    "SELECT next_position FROM change_progress WHERE table_name=? AND partition=?",
                    (table_name, partition),
                ).fetchone()
                progress[partition] = row[0] if row else 0
            pending = []
            position_rows = []
            for partition in sorted(by_partition):
                next_position = progress[partition]
                for event in sorted(by_partition[partition], key=lambda item: item["position"]):
                    if event["position"] < next_position:
                        stored = connection.execute(
                            "SELECT event_sha256 FROM change_positions "
                            "WHERE table_name=? AND partition=? AND position=?",
                            (table_name, partition, event["position"]),
                        ).fetchone()
                        if stored is None or stored[0] != event["fingerprint"]:
                            raise Conflict("redelivered position carries different content")
                        skipped += 1
                    elif event["position"] == next_position:
                        pending.append(event)
                        position_rows.append((partition, event["position"], event["fingerprint"]))
                        next_position += 1
                    else:
                        raise Conflict(
                            f"partition {partition} expects position {next_position}, got {event['position']}"
                        )
                progress[partition] = next_position
            pending.sort(key=lambda item: (item["partition"], item["position"]))

            # Key version arbitrates new and old events; equal versions must
            # carry identical content; deletes leave a tombstone version.
            states = self.key_states(connection, table_name, {event["key"] for event in pending})
            applied = []
            dirty = {}
            for event in pending:
                state = states.get(event["key"])
                if state is not None and state["partition"] != event["partition"]:
                    raise Conflict("business key belongs to a different source partition")
                if state is None or event["version"] > state["version"]:
                    applied.append(event)
                    state = {"partition": event["partition"], "version": event["version"],
                             "op": event["op"], "fingerprint": event["fingerprint"]}
                    states[event["key"]] = state
                    dirty[event["key"]] = state
                elif event["version"] == state["version"]:
                    if event["fingerprint"] != state["fingerprint"]:
                        raise Conflict("same key version carries different content")
                    skipped += 1
                else:
                    skipped += 1

            # Copy-on-write merge of the lake partitions the winning events touch.
            current = self.committer.current(connection, table_name)
            locations = {}
            existing = self.existing_keys(connection, table_name, {event["key"] for event in applied})
            if existing:
                remaining = set(existing)
                for file in current:
                    if not remaining:
                        break
                    partition_json = canonical(file["partition"])
                    for raw in self.objects.read_rows(file):
                        key = schema.key(schema.normalize(raw))
                        if key in remaining:
                            locations[key] = partition_json
                            remaining.discard(key)
                if remaining:
                    raise Conflict("catalog key state diverges from snapshot content")
            affected = {canonical(schema.partition(event["row"])) for event in applied if event["op"] == "upsert"}
            affected.update(locations.values())
            retained = [file for file in current if canonical(file["partition"]) not in affected]
            merged = {}
            for file in current:
                if canonical(file["partition"]) in affected:
                    for raw in self.objects.read_rows(file):
                        row = schema.normalize(raw)
                        merged[schema.key(row)] = row
            old_keys = set(merged)
            for event in applied:
                if event["op"] == "upsert":
                    merged[event["key"]] = event["row"]
                else:
                    merged.pop(event["key"], None)
            added = self.writer.write(table_name, table["schema_version"], schema, list(merged.values()))
            files = retained + added

            snapshot_id = table["head"]
            snapshot_created = False
            if sorted(file["id"] for file in files) != sorted(file["id"] for file in current):
                snapshot_id = table["head"] + 1
                snapshot_created = True
                document = {
                    "format_version": 1, "table": table_name, "snapshot_id": snapshot_id,
                    "schema_version": table["schema_version"], "parent_id": table["head"] or None,
                    "operation": "change", "batch_id": batch_id,
                    "progress": {str(partition): progress[partition] for partition in sorted(progress)},
                    "files": files,
                }
                manifest_path, manifest_sha = self.objects.write_manifest(table_name, document)
                connection.execute(
                    "INSERT INTO snapshots VALUES (?,?,?,?,?,?,?,?)",
                    (table_name, snapshot_id, table["head"] or None, "change", manifest_path, manifest_sha,
                     sum(file["row_count"] for file in files), timestamp()),
                )
                for file in added:
                    connection.execute(
                        "INSERT OR IGNORE INTO data_files VALUES (?,?,?,?,?,?,?,?,?,?)",
                        (table_name, file["id"], file["schema_version"], canonical(file["partition"]),
                         file["path"], file["sha256"], file["row_count"], file["byte_count"],
                         canonical(file["statistics"]), snapshot_id),
                    )
                connection.execute("UPDATE lake_tables SET head=? WHERE name=?", (snapshot_id, table_name))
                for key in old_keys - set(merged):
                    connection.execute("DELETE FROM row_keys WHERE table_name=? AND row_key=?", (table_name, key))
                for key in set(merged) - old_keys:
                    connection.execute("INSERT INTO row_keys VALUES (?,?,?)", (table_name, key, batch_id))

            for key, state in dirty.items():
                connection.execute(
                    "INSERT INTO change_keys VALUES (?,?,?,?,?,?,?) "
                    "ON CONFLICT(table_name, row_key) DO UPDATE SET partition=excluded.partition, "
                    "version=excluded.version, op=excluded.op, event_sha256=excluded.event_sha256, "
                    "batch_id=excluded.batch_id",
                    (table_name, key, state["partition"], state["version"], state["op"],
                     state["fingerprint"], batch_id),
                )
            for partition, position, fingerprint in position_rows:
                connection.execute(
                    "INSERT INTO change_positions VALUES (?,?,?,?,?)",
                    (table_name, partition, position, fingerprint, batch_id),
                )
            for partition, next_position in progress.items():
                connection.execute(
                    "INSERT INTO change_progress VALUES (?,?,?,?) "
                    "ON CONFLICT(table_name, partition) DO UPDATE SET "
                    "next_position=excluded.next_position, updated_at=excluded.updated_at",
                    (table_name, partition, next_position, timestamp()),
                )
            receipt = {
                "table": table_name, "batch_id": batch_id, "input_sha256": input_sha,
                "snapshot_id": snapshot_id, "snapshot_created": snapshot_created,
                "events": len(events), "applied": len(applied), "skipped": skipped,
                "progress": {str(partition): progress[partition] for partition in sorted(progress)},
                "committed_at": timestamp(),
            }
            connection.execute(
                "INSERT INTO change_batches VALUES (?,?,?,?,?,?)",
                (table_name, batch_id, input_sha, snapshot_id, canonical(receipt), receipt["committed_at"]),
            )
            self.catalog.event(connection, table_name, "changes_committed", {
                "batch_id": batch_id, "applied": len(applied), "skipped": skipped,
                "snapshot_id": snapshot_id, "snapshot_created": snapshot_created,
            })
        return {**receipt, "replayed": False}

    def receipt(self, table_name, batch_id):
        with self.catalog.transaction() as connection:
            row = connection.execute(
                "SELECT receipt FROM change_batches WHERE table_name=? AND batch_id=?",
                (table_name, batch_id),
            ).fetchone()
            if row is None:
                raise Missing("change batch receipt does not exist")
            return json.loads(row[0])

    def progress(self, table_name):
        with self.catalog.transaction() as connection:
            self.catalog.table(table_name, connection)
            partitions = [dict(row) for row in connection.execute(
                "SELECT partition, next_position, updated_at FROM change_progress "
                "WHERE table_name=? ORDER BY partition",
                (table_name,),
            )]
            batches = connection.execute(
                "SELECT COUNT(*) FROM change_batches WHERE table_name=?", (table_name,),
            ).fetchone()[0]
        return {"table": table_name, "partitions": partitions,
                "committed_positions": sum(item["next_position"] for item in partitions),
                "batches": batches}
