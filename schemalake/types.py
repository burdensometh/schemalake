from dataclasses import dataclass
from datetime import date, datetime, timezone
from decimal import Decimal, InvalidOperation
import hashlib
import json
import re


class LakeError(Exception):
    pass


class Invalid(LakeError):
    pass


class Conflict(LakeError):
    pass


class Missing(LakeError):
    pass


def canonical(value):
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False)


def sha(value):
    return hashlib.sha256(canonical(value).encode()).hexdigest()


def timestamp():
    return datetime.now(timezone.utc).isoformat(timespec="microseconds")


def name(value):
    if not isinstance(value, str) or not re.fullmatch(r"[a-z][a-z0-9_]{0,62}", value):
        raise Invalid("names must match [a-z][a-z0-9_]{0,62}")
    return value


def convert(value, type_name):
    if type_name == "string":
        if not isinstance(value, str):
            raise Invalid("string field requires a string")
        return value
    if type_name == "int64":
        if type(value) is not int or not -(2**63) <= value < 2**63:
            raise Invalid("int64 requires a signed 64-bit integer")
        return value
    if type_name == "boolean":
        if type(value) is not bool:
            raise Invalid("boolean field requires true or false")
        return value
    if type_name == "decimal":
        if type(value) not in {str, int}:
            raise Invalid("decimal requires an integer or decimal string")
        try:
            decimal = Decimal(value)
        except InvalidOperation:
            raise Invalid("invalid decimal") from None
        if not decimal.is_finite() or len(decimal.as_tuple().digits) > 38:
            raise Invalid("decimal must be finite with <=38 digits")
        result = format(decimal, "f")
        if "." in result:
            result = result.rstrip("0").rstrip(".")
        return "0" if decimal == 0 else result
    if type_name == "date":
        if not isinstance(value, str):
            raise Invalid("date requires ISO date text")
        try:
            result = date.fromisoformat(value)
        except ValueError:
            raise Invalid("invalid ISO date") from None
        if result.isoformat() != value:
            raise Invalid("date must use YYYY-MM-DD")
        return value
    if type_name == "timestamp":
        if not isinstance(value, str):
            raise Invalid("timestamp requires ISO text with a timezone")
        try:
            result = datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError:
            raise Invalid("invalid timestamp") from None
        if result.tzinfo is None:
            raise Invalid("timestamp must have a timezone")
        return result.astimezone(timezone.utc).isoformat(timespec="microseconds")
    raise Invalid(f"unsupported field type: {type_name}")


@dataclass(frozen=True)
class Field:
    id: int
    name: str
    type: str
    nullable: bool = False
    has_default: bool = False
    default: object = None

    @classmethod
    def parse(cls, value):
        if not isinstance(value, dict):
            raise Invalid("field must be an object")
        field_id = value.get("id")
        if type(field_id) is not int or field_id <= 0:
            raise Invalid("field id must be a positive integer")
        field_name = name(value.get("name"))
        type_name = value.get("type")
        if type_name not in {"string", "int64", "decimal", "boolean", "date", "timestamp"}:
            raise Invalid("unsupported field type")
        nullable = value.get("nullable", False)
        if type(nullable) is not bool:
            raise Invalid("nullable must be boolean")
        default = value.get("default")
        if "default" in value:
            if default is None and not nullable:
                raise Invalid("nonnullable field cannot default to null")
            if default is not None:
                default = convert(default, type_name)
        return cls(field_id, field_name, type_name, nullable, "default" in value, default)

    def encode(self):
        result = {"id": self.id, "name": self.name, "type": self.type, "nullable": self.nullable}
        if self.has_default:
            result["default"] = self.default
        return result


class Schema:
    def __init__(self, fields, partition_by=None, primary_key=None):
        if not isinstance(fields, list) or not fields:
            raise Invalid("schema requires a nonempty field array")
        self.fields = tuple(Field.parse(field) for field in fields)
        if len({field.id for field in self.fields}) != len(self.fields):
            raise Invalid("field ids must be unique")
        if len({field.name for field in self.fields}) != len(self.fields):
            raise Invalid("field names must be unique")
        self.partition_by = tuple(partition_by or [])
        self.primary_key = tuple(primary_key or [])
        names = {field.name: field for field in self.fields}
        for column in (*self.partition_by, *self.primary_key):
            if column not in names:
                raise Invalid(f"unknown key or partition column: {column}")
            if names[column].nullable:
                raise Invalid("key and partition columns cannot be nullable")
        if len(set(self.partition_by)) != len(self.partition_by):
            raise Invalid("duplicate partition column")
        if len(set(self.primary_key)) != len(self.primary_key):
            raise Invalid("duplicate primary key column")

    @classmethod
    def parse(cls, value):
        if not isinstance(value, dict):
            raise Invalid("schema must be an object")
        return cls(value.get("fields"), value.get("partition_by"), value.get("primary_key"))

    def encode(self):
        return {
            "fields": [field.encode() for field in self.fields],
            "partition_by": list(self.partition_by),
            "primary_key": list(self.primary_key),
        }

    def fingerprint(self):
        return sha(self.encode())

    def normalize(self, row):
        if not isinstance(row, dict):
            raise Invalid("row must be an object")
        names = {field.name for field in self.fields}
        extra = set(row) - names
        if extra:
            raise Invalid("unknown columns: " + ", ".join(sorted(extra)))
        result = {}
        for field in self.fields:
            if field.name in row:
                value = row[field.name]
            elif field.has_default:
                value = field.default
            elif field.nullable:
                value = None
            else:
                raise Invalid(f"missing required column: {field.name}")
            if value is None:
                if not field.nullable:
                    raise Invalid(f"null in nonnullable column: {field.name}")
                result[field.name] = None
            else:
                try:
                    result[field.name] = convert(value, field.type)
                except Invalid as error:
                    raise Invalid(f"{field.name}: {error}") from error
        return result

    def partition(self, row):
        return {column: row[column] for column in self.partition_by}

    def key(self, row):
        return canonical([row[column] for column in self.primary_key]) if self.primary_key else None
