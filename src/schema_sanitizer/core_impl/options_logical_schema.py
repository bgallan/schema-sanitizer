"""Logical schema SZOPT payload codec."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from ..adapters.pyarrow_common import ensure_pyarrow
from .options_bytes_codec import (
    _append_u8,
    _append_u32,
    _read_string,
    _read_u8,
    _read_u32,
)

_LOGICAL_SCHEMA_MAX_DEPTH = 512
_LOGICAL_SCHEMA_MAX_FIELDS = 1 << 20

_LOGICAL_KIND_NULL = 0
_LOGICAL_KIND_BOOL = 1
_LOGICAL_KIND_INT64 = 2
_LOGICAL_KIND_FLOAT64 = 3
_LOGICAL_KIND_UTF8 = 4
_LOGICAL_KIND_TIMESTAMP_NS = 5
_LOGICAL_KIND_DATE32 = 6
_LOGICAL_KIND_TIME32S = 7
_LOGICAL_KIND_STRUCT = 8
_LOGICAL_KIND_LIST = 9

_LOGICAL_KIND_VALID = frozenset(
    {
        _LOGICAL_KIND_NULL,
        _LOGICAL_KIND_BOOL,
        _LOGICAL_KIND_INT64,
        _LOGICAL_KIND_FLOAT64,
        _LOGICAL_KIND_UTF8,
        _LOGICAL_KIND_TIMESTAMP_NS,
        _LOGICAL_KIND_DATE32,
        _LOGICAL_KIND_TIME32S,
        _LOGICAL_KIND_STRUCT,
        _LOGICAL_KIND_LIST,
    }
)


@dataclass(frozen=True, slots=True)
class LogicalSchemaPayload:
    """Internal native logical-schema payload for registry-derived contracts."""

    payload: bytes

    def __post_init__(self) -> None:
        """Validate the native payload shape at construction time."""
        if not isinstance(self.payload, bytes):
            raise TypeError("LogicalSchemaPayload.payload must be bytes")
        _decode_logical_schema_payload(self.payload)


def _encode_logical_schema_payload_from_schema(schema: Any) -> bytes:
    """Encode a PyArrow schema as a logical schema payload."""
    pa = ensure_pyarrow(feature="options arrow_schema_contract")
    if not isinstance(schema, pa.Schema):
        raise TypeError("arrow_schema_contract must be a pyarrow.Schema")

    encode = _native_arrow_schema_contract_payload()
    if encode is None:
        raise RuntimeError(
            "arrow_schema_contract requires the native arrow_schema_contract_payload encoder"
        )
    try:
        payload = encode(schema)
    except (TypeError, RuntimeError) as exc:
        raise RuntimeError("native arrow_schema_contract payload encoding failed") from exc
    if isinstance(payload, bytes):
        return payload
    raise RuntimeError("native arrow_schema_contract payload encoder returned a non-bytes value")


def _native_arrow_schema_contract_payload() -> Any | None:
    """Return the native Arrow schema contract payload encoder when available."""
    try:
        from .native import _native
    except ImportError:
        return None
    encode = getattr(_native, "arrow_schema_contract_payload", None)
    return encode if callable(encode) else None


def _read_logical_field(data: memoryview, pos: int, depth: int) -> tuple[dict[str, Any], int]:
    """Read one logical field and return the next position."""
    name, pos = _read_string(data, pos)
    nullable_u8, pos = _read_u8(data, pos)
    if nullable_u8 > 1:
        raise ValueError("options deserialization: invalid logical field nullable")
    node, pos = _read_logical_type(data, pos, depth)
    return {"name": name, "nullable": bool(nullable_u8), "type": node}, pos


def _read_logical_fields(
    data: memoryview, pos: int, count: int, depth: int
) -> tuple[list[dict[str, Any]], int]:
    """Read a fixed number of logical fields."""
    fields: list[dict[str, Any]] = []
    for _ in range(count):
        field, pos = _read_logical_field(data, pos, depth)
        fields.append(field)
    return fields, pos


def _read_logical_type(data: memoryview, pos: int, depth: int) -> tuple[dict[str, Any], int]:
    """Read one logical type node and return the next position."""
    if depth > _LOGICAL_SCHEMA_MAX_DEPTH:
        raise ValueError("options deserialization: logical schema nesting too deep")

    kind, pos = _read_u8(data, pos)
    if kind not in _LOGICAL_KIND_VALID:
        raise ValueError("options deserialization: unknown logical kind")

    node: dict[str, Any] = {"kind": kind}
    if kind == _LOGICAL_KIND_STRUCT:
        n, pos = _read_u32(data, pos)
        if n > _LOGICAL_SCHEMA_MAX_FIELDS:
            raise ValueError("options deserialization: logical struct too large")
        node["fields"], pos = _read_logical_fields(data, pos, n, depth + 1)
        return node, pos

    if kind == _LOGICAL_KIND_LIST:
        node["value"], pos = _read_logical_type(data, pos, depth + 1)
        return node, pos

    return node, pos


def _decode_logical_schema_payload(payload: bytes) -> list[dict[str, Any]]:
    """Decode and validate a logical schema payload."""
    mv = memoryview(payload)
    pos = 0
    n, pos = _read_u32(mv, pos)
    if n > _LOGICAL_SCHEMA_MAX_FIELDS:
        raise ValueError("options deserialization: logical schema too large")

    fields, pos = _read_logical_fields(mv, pos, n, 1)
    if pos != len(mv):
        raise ValueError("options deserialization: trailing bytes in logical schema")
    return fields


def _pyarrow_type_from_logical_node(pa: Any, node: dict[str, Any]) -> Any:
    """Convert a decoded logical type node to a PyArrow type."""
    kind = node["kind"]
    if kind == _LOGICAL_KIND_NULL:
        return pa.null()
    if kind == _LOGICAL_KIND_BOOL:
        return pa.bool_()
    if kind == _LOGICAL_KIND_INT64:
        return pa.int64()
    if kind == _LOGICAL_KIND_FLOAT64:
        return pa.float64()
    if kind == _LOGICAL_KIND_UTF8:
        return pa.string()
    if kind == _LOGICAL_KIND_TIMESTAMP_NS:
        return pa.timestamp("ns")
    if kind == _LOGICAL_KIND_DATE32:
        return pa.date32()
    if kind == _LOGICAL_KIND_TIME32S:
        return pa.time32("s")
    if kind == _LOGICAL_KIND_LIST:
        return pa.list_(_pyarrow_type_from_logical_node(pa, node["value"]))
    if kind == _LOGICAL_KIND_STRUCT:
        return pa.struct([_pyarrow_field_from_logical_field(pa, field) for field in node["fields"]])
    raise ValueError(f"Unsupported logical kind: {kind!r}")


def _pyarrow_field_from_logical_field(pa: Any, field: dict[str, Any]) -> Any:
    """Convert a decoded logical field to a PyArrow field."""
    return pa.field(
        field["name"],
        _pyarrow_type_from_logical_node(pa, field["type"]),
        nullable=field["nullable"],
    )


def _pyarrow_schema_from_logical_schema_payload(payload: bytes) -> Any:
    """Decode a logical schema payload as a PyArrow schema."""
    pa = ensure_pyarrow(feature="schema registry logical schema")
    fields = _decode_logical_schema_payload(payload)
    return pa.schema([_pyarrow_field_from_logical_field(pa, field) for field in fields])


def _append_schema(out: bytearray, schema: Any) -> None:
    """Append an optional logical schema payload to options bytes."""
    if schema is None:
        _append_u8(out, 0)
        return

    if isinstance(schema, LogicalSchemaPayload):
        payload = schema.payload
    else:
        try:
            payload = _encode_logical_schema_payload_from_schema(schema)
        except Exception as e:  # pragma: no cover
            raise RuntimeError(
                "schema_contract must be a pyarrow.Schema or native logical schema payload"
            ) from e
    _append_u8(out, 1)
    _append_u32(out, len(payload))
    out.extend(payload)
