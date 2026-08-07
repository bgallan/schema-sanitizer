"""Typed objects returned by the native ABI."""

from __future__ import annotations

import os
from contextlib import suppress
from threading import Lock
from typing import Any

from .finalization import runtime_is_finalizing
from .json_payloads import json_object_loads
from .logical_schema import pyarrow_schema_from_payload
from .native_runtime import native_core as _native
from .resource_lifecycle import _close_sequence_retryably, _close_suppressing_errors


class IngestDiagnostics:
    """Lightweight ABI3 diagnostics wrapper parsed lazily from JSON."""

    def __init__(self, diag_json: str = "{}", diagnostics_capsule: Any = None):
        """Create diagnostics from JSON and an optional live native capsule."""
        self._diagnostics_capsule = diagnostics_capsule
        self._diag_json = diag_json or "{}"
        self._obj: dict[str, Any] | None = None
        self._pid = os.getpid()
        self._lock = Lock()

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
        if os.getpid() != self._pid:
            return self._diag_json
        with self._lock:
            if self._diagnostics_capsule is not None:
                try:
                    self._diag_json = str(_native.diagnostics_json(self._diagnostics_capsule))
                    self._obj = None
                except Exception:
                    pass
            return self._diag_json

    def close(self) -> None:
        """Freeze current JSON and release the live native diagnostics capsule."""
        if os.getpid() != self._pid:
            return
        with self._lock:
            capsule = self._diagnostics_capsule
            if capsule is None:
                return
            try:
                self._diag_json = str(_native.diagnostics_json(capsule))
                self._obj = None
            except Exception:
                pass
            self._diagnostics_capsule = None

    def __del__(self) -> None:
        """Release native diagnostics outside interpreter teardown only."""
        try:
            if runtime_is_finalizing() or os.getpid() != getattr(self, "_pid", os.getpid()):
                return
            self.close()
        except BaseException:
            pass


def _logical_schema_payload_field_names(payload: bytes) -> tuple[str, ...]:
    """Return top-level field names through the native payload codec."""
    return tuple(str(name) for name in _native.logical_schema_payload_field_names(payload))


class _LazySchemaPayloadResult:
    """Shared lazy schema payload decoding for native probe results."""

    __slots__ = ("_field_names", "_schema", "_schema_payload")

    def __init__(self, schema_payload: bytes):
        """Store one native payload without constructing PyArrow objects."""
        self._schema_payload = bytes(schema_payload)
        self._schema: Any | None = None
        self._field_names: tuple[str, ...] | None = None

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
            self._schema = pyarrow_schema_from_payload(self._schema_payload)
        return self._schema


class SchemaProbeResult(_LazySchemaPayloadResult):
    """Schema-only native probe result with lazy PyArrow schema decoding."""

    __slots__ = ("diagnostics",)

    def __init__(self, *, schema_payload: bytes, diagnostics: IngestDiagnostics):
        """Store the native schema payload until the PyArrow schema is needed."""
        super().__init__(schema_payload)
        self.diagnostics = diagnostics

    @classmethod
    def from_native(cls, native_result: dict[str, Any]) -> SchemaProbeResult:
        """Build a typed result from a native probe dictionary."""
        return cls(
            schema_payload=native_result["schema"],
            diagnostics=IngestDiagnostics(str(native_result.get("diagnostics_json", "{}"))),
        )


class RegistryProbeResult(_LazySchemaPayloadResult):
    """Registry-only native probe result with lazy PyArrow schema decoding."""

    __slots__ = (
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
        super().__init__(schema_payload)
        self.diagnostics = diagnostics
        self.schema_registry_json = schema_registry_json
        self.schema_drifts_json = schema_drifts_json
        self.conversion_timestamp = conversion_timestamp
        self.native_registry_state = native_registry_state

    def has_any_field_name(self, names: set[str] | frozenset[str]) -> bool:
        """Return whether this probe schema contains any supplied field name."""
        return any(name in names for name in self.field_names)

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


class _ArrowStream:
    """A tiny Arrow C Stream capsule wrapper."""

    def __init__(self, capsule: Any):
        """Wrap an Arrow C Stream capsule."""
        self._capsule = capsule
        self._pid = os.getpid()
        self._lock = Lock()

    def __arrow_c_stream__(self, requested_schema: Any = None):
        """Export the wrapped Arrow C Stream capsule."""
        if self._capsule is None:
            raise AttributeError("__arrow_c_stream__")
        return self._capsule

    def close(self) -> None:
        """Release the wrapped capsule reference."""
        if os.getpid() != self._pid:
            return
        with self._lock:
            self._capsule = None

    def __del__(self) -> None:
        """Best-effort release the wrapped capsule."""
        if runtime_is_finalizing() or os.getpid() != getattr(self, "_pid", os.getpid()):
            return
        with suppress(Exception):
            self.close()


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
        self._pid = os.getpid()
        self._main = main_stream_capsule
        self.schema_registry_json = schema_registry_json
        self.schema_drifts_json = schema_drifts_json
        self.conversion_timestamp = conversion_timestamp
        self.native_registry_state = native_registry_state
        self._diagnostics = IngestDiagnostics(
            diagnostics_json,
            diagnostics_capsule=diagnostics_capsule,
        )
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
        """Release the primary output without losing failed backend ownership."""
        backend = self._table_backend
        if backend is not None and not _close_suppressing_errors(backend):
            return
        if self._table_backend is backend:
            self._table_backend = None
        self._main = None

    def close(self) -> None:
        """Close primary stream, keepalives, and diagnostics transactionally."""
        if os.getpid() != self._pid:
            return
        self.close_main_stream()
        if self._table_backend is None and self._main is None:
            keepalive = list(getattr(self, "_keepalive", ()) or ())
            _close_sequence_retryably(keepalive)
            self._keepalive = keepalive
        self._diagnostics.close()

    def __del__(self) -> None:
        """Best-effort close sink resources."""
        if runtime_is_finalizing() or os.getpid() != getattr(self, "_pid", os.getpid()):
            return
        with suppress(Exception):
            self.close()


def _registry_sink_output(sink: str, native_result: tuple[Any, ...]) -> SinkOutput:
    """Wrap a registry-backed native sink result."""
    (
        main,
        diagnostics,
        registry_json,
        drifts_json,
        conversion_timestamp,
        native_state,
    ) = native_result
    return SinkOutput(
        sink=sink,
        main_stream_capsule=main,
        diagnostics_capsule=diagnostics,
        schema_registry_json=str(registry_json),
        schema_drifts_json=str(drifts_json),
        conversion_timestamp=str(conversion_timestamp),
        native_registry_state=native_state,
    )
