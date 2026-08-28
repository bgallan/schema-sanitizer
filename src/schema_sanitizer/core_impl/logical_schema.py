"""Validate native logical-schema payloads and convert them to and from Arrow.

The module checks the JSON wire shape and bridges PyArrow schemas through Arrow C schema
providers without exposing native capsule details to callers.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from .dependencies import ensure_pyarrow
from .native_runtime import native_core as _native


@dataclass(frozen=True, slots=True)
class LogicalSchemaPayload:
    """Validated native logical-schema payload shared across runtime domains."""

    payload: bytes

    def __post_init__(self) -> None:
        """Validate payload bytes through the canonical native codec."""
        if not isinstance(self.payload, bytes):
            raise TypeError("LogicalSchemaPayload.payload must be bytes")
        _native.logical_schema_payload_validate(self.payload)


class _ArrowSchemaProvider:
    """Expose one native logical schema through the Arrow PyCapsule protocol."""

    __slots__ = ("_payload",)

    def __init__(self, payload: bytes):
        """Store payload bytes for one Arrow C schema export."""
        self._payload = payload

    def __arrow_c_schema__(self) -> Any:
        """Return a fresh Arrow C schema capsule owned by the consumer."""
        return _native.logical_schema_payload_arrow_c_schema(self._payload)


def encode_arrow_schema_payload(schema: Any) -> bytes:
    """Encode a PyArrow schema using the canonical native logical-schema codec."""
    pa = ensure_pyarrow(feature="options arrow_schema_contract")
    if not isinstance(schema, pa.Schema):
        raise TypeError("arrow_schema_contract must be a pyarrow.Schema")
    payload = _native.arrow_schema_contract_payload(schema)
    if not isinstance(payload, bytes):
        raise RuntimeError("native arrow schema encoder returned a non-bytes value")
    return payload


def pyarrow_schema_from_payload(payload: bytes) -> Any:
    """Import a native logical-schema payload through Arrow C Data."""
    if not isinstance(payload, bytes):
        raise TypeError("logical schema payload must be bytes")
    pa = ensure_pyarrow(feature="schema registry logical schema")
    return pa.schema(_ArrowSchemaProvider(payload))
