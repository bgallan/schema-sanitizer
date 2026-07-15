"""Shared schema decision cache helpers for PyArrow adapter fast paths."""

from __future__ import annotations

from typing import Any


class SchemaDecisionCache:
    """Small bounded cache for adapter schema compatibility decisions."""

    def __init__(self, *, max_size: int = 128, max_key_bytes: int = 1 << 20) -> None:
        """Create an empty bounded schema decision cache."""
        self._max_size = max(0, int(max_size))
        self._max_key_bytes = max(0, int(max_key_bytes))
        self._by_object_id: dict[int, tuple[Any, bool]] = {}
        self._by_fingerprint: dict[bytes, bool] = {}
        self._by_schema_text: dict[str, bool] = {}
        self._fingerprint_bytes = 0
        self._schema_text_bytes = 0

    def get_by_object(self, schema: Any) -> bool | None:
        """Return a cached decision for the exact schema object, if present."""
        cached = self._by_object_id.get(id(schema))
        if cached is None:
            return None
        cached_schema, supported = cached
        if cached_schema is not schema:
            self._by_object_id.pop(id(schema), None)
            return None
        return supported

    def get_by_text(self, schema: Any) -> bool | None:
        """Return a cached decision for an equivalent schema string, if present."""
        key = schema_cache_key(schema)
        if len(key) > self._max_key_bytes:
            return None
        return self._by_schema_text.get(key)

    def get_by_fingerprint(self, fingerprint: bytes) -> bool | None:
        """Return a cached decision for a stable schema fingerprint, if present."""
        return self._by_fingerprint.get(fingerprint)

    def set_fingerprint(self, schema: Any, fingerprint: bytes, supported: bool) -> bool:
        """Store one fingerprint compatibility decision and return it."""
        if len(fingerprint) > self._max_key_bytes:
            return supported
        self._record_object(schema, supported)
        self._record_fingerprint(fingerprint, supported)
        return supported

    def set(self, schema: Any, supported: bool, *, include_text: bool) -> bool:
        """Store one compatibility decision and return it."""
        key = schema_cache_key(schema) if include_text else None
        if key is not None and len(key) > self._max_key_bytes:
            return supported
        self._record_object(schema, supported)
        if key is not None:
            self._record_text(key, supported)
        return supported

    def _record_object(self, schema: Any, supported: bool) -> None:
        """Store one object-identity compatibility decision."""
        if self._max_size == 0:
            return
        if len(self._by_object_id) >= self._max_size:
            self._by_object_id.clear()
        self._by_object_id[id(schema)] = (schema, supported)

    def _record_fingerprint(self, fingerprint: bytes, supported: bool) -> None:
        """Store one stable fingerprint compatibility decision."""
        key_bytes = len(fingerprint)
        if self._max_size == 0 or key_bytes > self._max_key_bytes or self._max_key_bytes == 0:
            return
        existing = fingerprint in self._by_fingerprint
        if len(self._by_fingerprint) >= self._max_size or (
            not existing and self._fingerprint_bytes > self._max_key_bytes - key_bytes
        ):
            self._by_fingerprint.clear()
            self._fingerprint_bytes = 0
            existing = False
        self._by_fingerprint[fingerprint] = supported
        if not existing:
            self._fingerprint_bytes += key_bytes

    def _record_text(self, key: str, supported: bool) -> None:
        """Store one schema-text compatibility decision."""
        key_bytes = len(key)
        if self._max_size == 0 or key_bytes > self._max_key_bytes or self._max_key_bytes == 0:
            return
        existing = key in self._by_schema_text
        if len(self._by_schema_text) >= self._max_size or (
            not existing and self._schema_text_bytes > self._max_key_bytes - key_bytes
        ):
            self._by_schema_text.clear()
            self._schema_text_bytes = 0
            existing = False
        self._by_schema_text[key] = supported
        if not existing:
            self._schema_text_bytes += key_bytes


def schema_cache_key(schema: Any) -> str:
    """Return a stable cache key for a PyArrow schema."""
    try:
        return schema.to_string(show_field_metadata=True, show_schema_metadata=True)
    except TypeError:
        return str(schema)
