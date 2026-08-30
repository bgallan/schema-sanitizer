"""Normalize typed results returned by the native ABI.

Wrappers decode ingestion, probe, and sink results together with diagnostics JSON, logical schema
payloads, and capsule ownership into stable Python values.
"""

from __future__ import annotations

import os
from collections.abc import Iterable
from threading import Lock
from typing import Any, cast

from .finalization import runtime_is_finalizing
from .finalizer_cleanup import (
    PreparedFinalizerCleanup,
    cancel_prepared_finalizer_cleanup,
    defer_prepared_finalizer_cleanup,
    reserve_detached_resources_finalizer_cleanup,
    reserve_reference_finalizer_cleanup,
)
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
        self._finalizer_capsule: PreparedFinalizerCleanup | None = (
            reserve_reference_finalizer_cleanup()
        )
        self._finalizer_ticket: int | None = self._finalizer_capsule.ticket

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
                # Retain the last valid diagnostic snapshot.
                except Exception as ignored_error:
                    del ignored_error
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
            # Retain the last valid diagnostic snapshot.
            except Exception as ignored_error:
                del ignored_error
            self._diagnostics_capsule = None
            ticket = getattr(self, "_finalizer_ticket", None)
            capsule_owner = getattr(self, "_finalizer_capsule", None)
            if ticket is not None and capsule_owner is not None:
                cancel_prepared_finalizer_cleanup(capsule_owner)
                self._finalizer_ticket = None
                self._finalizer_capsule = None

    def __del__(self) -> None:
        """Transfer only the native capsule to a preallocated safe-point slot."""
        try:
            if runtime_is_finalizing() or os.getpid() != getattr(self, "_pid", os.getpid()):
                return
            capsule = getattr(self, "_diagnostics_capsule", None)
            ticket = getattr(self, "_finalizer_ticket", None)
            cleanup = getattr(self, "_finalizer_capsule", None)
            if capsule is None or ticket is None or cleanup is None:
                return
            cleanup.arg0 = capsule
            if defer_prepared_finalizer_cleanup(cleanup):
                self._diagnostics_capsule = None
                self._finalizer_ticket = None
                self._finalizer_capsule = None
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
        self._finalizer_capsule: PreparedFinalizerCleanup | None = (
            reserve_reference_finalizer_cleanup()
        )
        self._finalizer_ticket: int | None = self._finalizer_capsule.ticket

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
            ticket = getattr(self, "_finalizer_ticket", None)
            cleanup = getattr(self, "_finalizer_capsule", None)
            if ticket is not None and cleanup is not None:
                cancel_prepared_finalizer_cleanup(cleanup)
                self._finalizer_ticket = None
                self._finalizer_capsule = None

    def __del__(self) -> None:
        """Transfer only the wrapped native capsule to a preallocated safe point."""
        try:
            if runtime_is_finalizing() or os.getpid() != getattr(self, "_pid", os.getpid()):
                return
            capsule = getattr(self, "_capsule", None)
            ticket = getattr(self, "_finalizer_ticket", None)
            cleanup = getattr(self, "_finalizer_capsule", None)
            if capsule is None or ticket is None or cleanup is None:
                return
            cleanup.arg0 = capsule
            if defer_prepared_finalizer_cleanup(cleanup):
                self._capsule = None
                self._finalizer_ticket = None
                self._finalizer_capsule = None
        except BaseException:
            pass


def _cleanup_sink_output_capsule(capsule: PreparedFinalizerCleanup) -> None:
    """Close only detached sink resources while retaining failures for retry."""
    backend = capsule.arg0
    if backend is not None:
        if not _close_suppressing_errors(backend):
            raise RuntimeError("sink table backend cleanup remains retryable")
        capsule.arg0 = None
        # The primary native capsule aliases the backend stream when present.
        capsule.arg3 = None
    keepalive = capsule.arg1
    if keepalive is not None:
        retained = (
            list(cast(Iterable[Any], keepalive)) if not isinstance(keepalive, list) else keepalive
        )
        _close_sequence_retryably(retained)
        if retained:
            capsule.arg1 = retained
            raise RuntimeError("sink keepalive cleanup remains retryable")
        capsule.arg1 = None
    diagnostics = capsule.arg2
    if diagnostics is not None:
        cast(IngestDiagnostics, diagnostics).close()
        capsule.arg2 = None
    # arg3 (raw main capsule) and arg4 (native registry state) only need their
    # references dropped at this governed safe point.
    capsule.arg3 = None
    capsule.arg4 = None


class SinkOutput:
    """Output object returned by ABI3 sink helpers."""

    _keepalive: list[Any] | tuple[Any, ...]

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
        self._diagnostics: IngestDiagnostics | None = IngestDiagnostics(
            diagnostics_json,
            diagnostics_capsule=diagnostics_capsule,
        )
        self._table_backend = (
            _ArrowStream(self._main) if (self._sink != "stream" and self._main) else None
        )
        self._finalizer_capsule: PreparedFinalizerCleanup | None = (
            reserve_detached_resources_finalizer_cleanup()
        )
        self._finalizer_ticket: int | None = self._finalizer_capsule.ticket
        self._finalizer_capsule.callback = _cleanup_sink_output_capsule

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
        """Return diagnostics reported by the native sink result."""
        diagnostics = self._diagnostics
        if diagnostics is None:
            raise RuntimeError("sink diagnostics have already been detached")
        return diagnostics

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
        diagnostics = self._diagnostics
        if diagnostics is not None:
            diagnostics.close()
        diagnostics_capsule = getattr(diagnostics, "_diagnostics_capsule", None)
        if (
            self._table_backend is None
            and self._main is None
            and not (getattr(self, "_keepalive", ()) or ())
            and diagnostics_capsule is None
        ):
            ticket = getattr(self, "_finalizer_ticket", None)
            cleanup = getattr(self, "_finalizer_capsule", None)
            if ticket is not None and cleanup is not None:
                cancel_prepared_finalizer_cleanup(cleanup)
                self._finalizer_ticket = None
                self._finalizer_capsule = None

    def __del__(self) -> None:
        """Detach only sink cleanup resources into a preallocated safe-point capsule."""
        try:
            if runtime_is_finalizing() or os.getpid() != getattr(self, "_pid", os.getpid()):
                return
            ticket = getattr(self, "_finalizer_ticket", None)
            cleanup = getattr(self, "_finalizer_capsule", None)
            if ticket is None or cleanup is None:
                return
            cleanup.arg0 = getattr(self, "_table_backend", None)
            cleanup.arg1 = getattr(self, "_keepalive", ()) or None
            cleanup.arg2 = getattr(self, "_diagnostics", None)
            cleanup.arg3 = getattr(self, "_main", None)
            cleanup.arg4 = getattr(self, "native_registry_state", None)
            if defer_prepared_finalizer_cleanup(cleanup):
                self._table_backend = None
                self._main = None
                self._keepalive = ()
                self._diagnostics = None
                self.native_registry_state = None
                self._finalizer_ticket = None
                self._finalizer_capsule = None
        except BaseException:
            pass


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
