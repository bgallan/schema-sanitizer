"""Shared schema decision cache helpers for PyArrow adapter fast paths."""

from __future__ import annotations

from typing import Any


class SchemaDecisionCache:
    """Small bounded cache for adapter schema compatibility decisions."""

    def __init__(self, *, max_size: int = 128) -> None:
        """Create an empty bounded schema decision cache."""
        self._max_size = max_size
        self._by_object_id: dict[int, tuple[Any, bool]] = {}
        self._by_fingerprint: dict[bytes, bool] = {}
        self._by_schema_text: dict[str, bool] = {}

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
        return self._by_schema_text.get(schema_cache_key(schema))

    def get_by_fingerprint(self, fingerprint: bytes) -> bool | None:
        """Return a cached decision for a stable schema fingerprint, if present."""
        return self._by_fingerprint.get(fingerprint)

    def set_fingerprint(self, schema: Any, fingerprint: bytes, supported: bool) -> bool:
        """Store one fingerprint compatibility decision and return it."""
        self._record_object(schema, supported)
        self._record_fingerprint(fingerprint, supported)
        return supported

    def set(self, schema: Any, supported: bool, *, include_text: bool) -> bool:
        """Store one compatibility decision and return it."""
        self._record_object(schema, supported)
        if include_text:
            self._record_text(schema_cache_key(schema), supported)
        return supported

    def _record_object(self, schema: Any, supported: bool) -> None:
        """Store one object-identity compatibility decision."""
        if len(self._by_object_id) >= self._max_size:
            self._by_object_id.clear()
        self._by_object_id[id(schema)] = (schema, supported)

    def _record_fingerprint(self, fingerprint: bytes, supported: bool) -> None:
        """Store one stable fingerprint compatibility decision."""
        if len(self._by_fingerprint) >= self._max_size:
            self._by_fingerprint.clear()
        self._by_fingerprint[fingerprint] = supported

    def _record_text(self, key: str, supported: bool) -> None:
        """Store one schema-text compatibility decision."""
        if len(self._by_schema_text) >= self._max_size:
            self._by_schema_text.clear()
        self._by_schema_text[key] = supported


def schema_cache_key(schema: Any) -> str:
    """Return a stable cache key for a PyArrow schema."""
    try:
        return schema.to_string(show_field_metadata=True, show_schema_metadata=True)
    except TypeError:
        return str(schema)
