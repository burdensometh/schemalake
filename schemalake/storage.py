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
        self.catalog = None

    def bind(self, catalog):
        self.catalog = catalog
        return self

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

    def chain(self, record):
        """Return [(reference, document, format_version)] newest first.

        Base references name immutable manifest documents.  Migration may
        supersede the document a parent catalog row points at while a child
        still records the older identity, so a base link is verified against
        its own embedded snapshot identity rather than the catalog row.
        """
        from .manifests import FORMAT_V1, ManifestValidator

        validator = ManifestValidator()
        table_name = record["table_name"]
        current = {
            "catalog": record,
            "snapshot_id": record["id"],
            "manifest_path": record["manifest_path"],
            "manifest_sha256": record["manifest_sha256"],
        }
        seen = set()
        result = []
        while True:
            identity = (current["manifest_path"], current["manifest_sha256"])
            if identity in seen:
                raise Conflict("manifest base chain contains a cycle")
            seen.add(identity)
            document = json.loads(self.read(*identity))
            catalog = current.get("catalog")
            if catalog is not None:
                version = validator.document(document, catalog)
            else:
                version = validator.fragment(document, table_name, current["snapshot_id"])
            result.append((current, document, version))
            if version == FORMAT_V1:
                return result
            base = document.get("base")
            if base is None:
                return result
            if base["snapshot_id"] != document["parent_id"]:
                raise Conflict("manifest base must be its parent snapshot")
            current = {
                "snapshot_id": base["snapshot_id"],
                "manifest_path": base["manifest_path"],
                "manifest_sha256": base["manifest_sha256"],
            }

    def manifest(self, record):
        from .manifests import FORMAT_V1, ManifestValidator, apply_delta

        chain = self.chain(record)
        files = None
        for _, document, version in reversed(chain):
            if version == FORMAT_V1 or document.get("base") is None:
                files = list(document["files"])
            else:
                files = apply_delta(files, document.get("removed"), document["files"])
        if files is None:
            raise Conflict("manifest base chain has no root file list")
        _, document, version = chain[0]
        resolved = {
            "format_version": version,
            "table": document["table"],
            "snapshot_id": document["snapshot_id"],
            "schema_version": document["schema_version"],
            "parent_id": document["parent_id"],
            "operation": document["operation"],
            "files": files,
        }
        if version != FORMAT_V1:
            resolved["base"] = document.get("base")
            resolved["removed"] = dict(document.get("removed") or {})
            resolved["added"] = list(document.get("files") or [])
        ManifestValidator().resolved(resolved, record)
        return resolved

    def manifest_paths(self, record):
        return {item["manifest_path"] for item, _, _ in self.chain(record)}

    def inventory(self):
        return [
            {"path": str(path.relative_to(self.root)), "bytes": path.stat().st_size}
            for path in sorted(self.root.rglob("*"))
            if path.is_file() and path.suffix in {".jsonl", ".json"}
        ]
