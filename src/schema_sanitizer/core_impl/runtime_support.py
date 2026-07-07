"""Implements `schema_sanitizer.core_impl.runtime_support`."""

from __future__ import annotations

from contextlib import suppress
from typing import Any

from .json_payloads import json_object_loads
from .options_logical_schema import (
    _decode_logical_schema_payload,
    _pyarrow_schema_from_logical_schema_payload,
)


def _logical_schema_payload_field_names(payload: bytes) -> tuple[str, ...]:
    """Return top-level field names from a native logical schema payload."""
    try:
        from .native import _native

        native_field_names = getattr(_native, "logical_schema_payload_field_names", None)
        if callable(native_field_names):
            return tuple(str(name) for name in native_field_names(payload))
    except Exception:
        pass
    return tuple(str(field["name"]) for field in _decode_logical_schema_payload(payload))


class IngestDiagnostics:
    """Lightweight ABI3 diagnostics wrapper parsed from JSON."""

    def __init__(self, diag_json: str = "{}", diagnostics_capsule: Any = None):
        """Create diagnostics from JSON."""
        self._diagnostics_capsule = diagnostics_capsule
        self._diag_json = diag_json or "{}"
        self._obj: dict[str, Any] | None = None

    def _ensure_obj(self) -> dict[str, Any]:
        """Parse and return the cached diagnostics payload."""
        if self._obj is None:
            self._obj = json_object_loads(self._diag_json)
        return self._obj

    def __getattr__(self, name: str) -> Any:
        """Return a diagnostic value or zero when it is absent."""
        self.to_json()
        return self._ensure_obj().get(name, 0)

    def to_json(self) -> str:
        """Return the current diagnostics JSON."""
        if self._diagnostics_capsule is not None:
            try:
                from .native import _native

                self._diag_json = str(_native.diagnostics_json(self._diagnostics_capsule))
                self._obj = None
            except Exception:
                pass
        return self._diag_json


class _ArrowStream:
    """A tiny Arrow C Stream capsule wrapper."""

    def __init__(self, capsule: Any):
        """Wrap an Arrow C Stream capsule."""
        self._capsule = capsule

    def __arrow_c_stream__(self, requested_schema: Any = None):
        """Export the wrapped Arrow C Stream capsule."""
        if self._capsule is None:
            raise AttributeError("__arrow_c_stream__")
        return self._capsule

    def close(self) -> None:
        """Release the wrapped capsule reference."""
        self._capsule = None

    def __del__(self) -> None:
        """Best-effort release the wrapped capsule."""
        with suppress(Exception):
            self.close()


class SchemaProbeResult:
    """Schema-only native probe result with lazy PyArrow schema decoding."""

    __slots__ = ("_field_names", "_schema", "_schema_payload", "diagnostics")

    def __init__(self, *, schema_payload: bytes, diagnostics: IngestDiagnostics):
        """Store the native schema payload until the PyArrow schema is needed."""
        self._schema_payload = bytes(schema_payload)
        self._schema: Any | None = None
        self._field_names: tuple[str, ...] | None = None
        self.diagnostics = diagnostics

    @property
    def schema_payload(self) -> bytes:
        """Return the native logical schema payload."""
        return self._schema_payload

    @property
    def field_names(self) -> tuple[str, ...]:
        """Return top-level field names without constructing a PyArrow schema."""
        if self._field_names is None:
            self._field_names = _logical_schema_payload_field_names(self._schema_payload)
        return self._field_names

    @property
    def schema(self) -> Any:
        """Return the decoded PyArrow schema, decoding it on first access."""
        if self._schema is None:
            self._schema = _pyarrow_schema_from_logical_schema_payload(self._schema_payload)
        return self._schema

    @classmethod
    def from_native(cls, native_result: dict[str, Any]) -> SchemaProbeResult:
        """Build a typed result from a native probe dictionary."""
        return cls(
            schema_payload=native_result["schema"],
            diagnostics=IngestDiagnostics(str(native_result.get("diagnostics_json", "{}"))),
        )


class RegistryProbeResult:
    """Registry-only native probe result with lazy PyArrow schema decoding."""

    __slots__ = (
        "_field_names",
        "_schema",
        "_schema_payload",
        "conversion_timestamp",
        "diagnostics",
        "schema_drifts_json",
        "schema_registry_json",
        "native_registry_state",
    )

    def __init__(
        self,
        *,
        schema_payload: bytes,
        diagnostics: IngestDiagnostics,
        schema_registry_json: str,
        schema_drifts_json: str,
        conversion_timestamp: str,
        native_registry_state: Any = None,
    ):
        """Store registry metadata eagerly and defer PyArrow schema construction."""
        self._schema_payload = bytes(schema_payload)
        self._schema: Any | None = None
        self._field_names: tuple[str, ...] | None = None
        self.diagnostics = diagnostics
        self.schema_registry_json = schema_registry_json
        self.schema_drifts_json = schema_drifts_json
        self.conversion_timestamp = conversion_timestamp
        self.native_registry_state = native_registry_state

    @property
    def schema_payload(self) -> bytes:
        """Return the native logical schema payload."""
        return self._schema_payload

    @property
    def field_names(self) -> tuple[str, ...]:
        """Return top-level field names without constructing a PyArrow schema."""
        if self._field_names is None:
            self._field_names = _logical_schema_payload_field_names(self._schema_payload)
        return self._field_names

    def has_any_field_name(self, names: set[str] | frozenset[str]) -> bool:
        """Return whether this probe schema contains any of the supplied field names."""
        return bool(set(self.field_names) & names)

    @property
    def schema(self) -> Any:
        """Return the decoded PyArrow schema, decoding it on first access."""
        if self._schema is None:
            self._schema = _pyarrow_schema_from_logical_schema_payload(self._schema_payload)
        return self._schema

    @classmethod
    def from_native(cls, native_result: dict[str, Any]) -> RegistryProbeResult:
        """Build a typed result from a native registry probe dictionary."""
        return cls(
            schema_payload=native_result["schema"],
            diagnostics=IngestDiagnostics(str(native_result.get("diagnostics_json", "{}"))),
            schema_registry_json=str(native_result.get("schema_registry_json", "{}")),
            schema_drifts_json=str(native_result.get("schema_drifts_json", "[]")),
            conversion_timestamp=str(native_result.get("conversion_timestamp", "")),
            native_registry_state=native_result.get("native_registry_state"),
        )


class SinkOutput:
    """Output object returned by ABI3 sink helpers."""

    def __init__(
        self,
        *,
        sink: str,
        main_stream_capsule: Any = None,
        diagnostics_capsule: Any = None,
        diagnostics_json: str = "{}",
        schema_registry_json: str | None = None,
        schema_drifts_json: str | None = None,
        conversion_timestamp: str | None = None,
        native_registry_state: Any = None,
    ):
        """Create a sink output from native stream capsules and diagnostics."""
        self._sink = sink
        self._main = main_stream_capsule
        self.schema_registry_json = schema_registry_json
        self.schema_drifts_json = schema_drifts_json
        self.conversion_timestamp = conversion_timestamp
        self.native_registry_state = native_registry_state
        self._diagnostics = IngestDiagnostics(
            diagnostics_json,
            diagnostics_capsule=diagnostics_capsule,
        )

        # For materializing-table sinks, expose the main stream as table data.
        self._table_backend = (
            _ArrowStream(self._main) if (self._sink != "stream" and self._main) else None
        )

    @property
    def table(self) -> Any:
        """Return the materializable table backend when available."""
        return self._table_backend

    @property
    def sink(self) -> str:
        """Return the sink name."""
        return self._sink

    @property
    def diagnostics(self) -> IngestDiagnostics:
        """Return sink diagnostics."""
        return self._diagnostics

    def __arrow_c_stream__(self, requested_schema: Any = None):
        """Export the primary output stream capsule."""
        if self._main is None:
            raise AttributeError("__arrow_c_stream__")
        return self._main

    def close_main_stream(self) -> None:
        """Release only the primary output stream, leaving diagnostics readable."""

        if self._table_backend is not None:
            self._table_backend.close()
            self._table_backend = None
        self._main = None

    def close(self) -> None:
        """Close primary stream and diagnostics resources."""
        self.close_main_stream()
        keepalive = getattr(self, "_keepalive", None)
        if keepalive is not None:
            with suppress(Exception):
                while keepalive:
                    item = keepalive.pop()
                    close = getattr(item, "close", None)
                    if callable(close):
                        close()

    def __del__(self) -> None:
        """Best-effort close sink resources."""
        with suppress(Exception):
            self.close()
