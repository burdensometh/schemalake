from pathlib import Path
from decimal import Decimal
import hashlib
import json
import os
import tempfile

from .types import Conflict, Invalid, Missing, canonical


class Objects:
    def __init__(self, root):
        self.root = Path(root).resolve()
        self.root.mkdir(parents=True, exist_ok=True)

    def path(self, relative):
        path = (self.root / relative).resolve()
        if not path.is_relative_to(self.root):
            raise Invalid("object path escapes lake root")
        return path

    def put(self, prefix, raw, extension):
        checksum = hashlib.sha256(raw).hexdigest()
        relative = f"{prefix}/{checksum}.{extension}"
        path = self.path(relative)
        path.parent.mkdir(parents=True, exist_ok=True)
        if path.exists():
            if self.read(relative, checksum) != raw:
                raise Conflict("content-addressed object collision")
            return relative, checksum
        descriptor, temporary = tempfile.mkstemp(prefix=".object-", dir=path.parent)
        try:
            with os.fdopen(descriptor, "wb") as stream:
                stream.write(raw)
                stream.flush()
                os.fsync(stream.fileno())
            os.replace(temporary, path)
        finally:
            if os.path.exists(temporary):
                os.unlink(temporary)
        return relative, checksum

    def read(self, relative, checksum):
        path = self.path(relative)
        if not path.is_file():
            raise Missing(f"missing object: {relative}")
        raw = path.read_bytes()
        if hashlib.sha256(raw).hexdigest() != checksum:
            raise Conflict(f"checksum mismatch: {relative}")
        return raw

    def delete(self, relative):
        """Remove one content-addressed object. Idempotent: a missing file is already gone."""
        path = self.path(relative)
        try:
            os.unlink(path)
            return True
        except FileNotFoundError:
            return False

    def statistics(self, schema, rows):
        result = {}
        for field in schema.fields:
            values = [row[field.name] for row in rows if row[field.name] is not None]
            compare = Decimal if field.type == "decimal" else lambda value: value
            result[field.name] = {
                "null_count": len(rows) - len(values),
                "min": min(values, key=compare) if values else None,
                "max": max(values, key=compare) if values else None,
            }
        return result

    def write_rows(self, table_name, schema_version, schema, partition, rows):
        lines = [canonical(row) for row in rows]
        raw = ("\n".join(lines) + "\n").encode("utf-8")
        partition_id = hashlib.sha256(canonical(partition).encode()).hexdigest()[:24]
        path, checksum = self.put(f"tables/{table_name}/data/{partition_id}", raw, "jsonl")
        return {
            "id": checksum,
            "schema_version": schema_version,
            "partition": partition,
            "path": path,
            "sha256": checksum,
            "row_count": len(rows),
            "byte_count": len(raw),
            "statistics": self.statistics(schema, rows),
        }

    def read_rows(self, file):
        raw = self.read(file["path"], file["sha256"])
        lines = raw.decode("utf-8").splitlines()
        if len(lines) != file["row_count"] or len(raw) != file["byte_count"]:
            raise Conflict("data file counts do not match manifest")
        for line in lines:
            row = json.loads(line)
            if not isinstance(row, dict):
                raise Invalid("data object contains a non-object row")
            yield row

    def write_manifest(self, table_name, document):
        return self.put(f"tables/{table_name}/manifests", canonical(document).encode(), "json")

    def manifest(self, record):
        document = json.loads(self.read(record["manifest_path"], record["manifest_sha256"]))
        from .manifests import ManifestValidator
        ManifestValidator().document(document, record)
        if document["snapshot_id"] != record["id"]:
            raise Conflict("manifest snapshot identity mismatch")
        if sum(file["row_count"] for file in document["files"]) != record["row_count"]:
            raise Conflict("manifest row total mismatch")
        return document

    def inventory(self):
        return [
            {"path": str(path.relative_to(self.root)), "bytes": path.stat().st_size}
            for path in sorted(self.root.rglob("*"))
            if path.is_file() and path.suffix in {".jsonl", ".json"}
        ]
