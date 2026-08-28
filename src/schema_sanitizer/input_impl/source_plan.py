"""Build canonical source plans, native path capsules, and registry sinks.

Descriptors and batches flatten path sources into native inputs, open automatic registry streams
and sinks, and retain every capsule until acquisition-safe cleanup.
"""

from __future__ import annotations

import os
from collections.abc import Iterable
from dataclasses import dataclass, field
from typing import Any, cast

from ..core_impl.error_translation import call_core
from ..core_impl.finalization import runtime_is_finalizing
from ..core_impl.finalizer_cleanup import (
    PreparedFinalizerCleanup,
    cancel_prepared_finalizer_cleanup,
    defer_prepared_finalizer_cleanup,
    reserve_finalizer_cleanup,
)
from ..core_impl.generated_metadata import (
    SCHEMA_DRIFTS_COLUMN,
    SCHEMA_REGISTRY_COLUMN,
    TimestampColumns,
)
from ..core_impl.native_symbols import PATH_SOURCE_PLAN_CREATE
from ..core_impl.resource_lifecycle import (
    _close_sequence_retryably,
    _close_suppressing_errors,
)

PATH_SOURCES = "path_sources"
REMOTE_CHUNKS = "remote_chunks"
PARQUET_ARROW_SOURCES = "parquet_arrow_sources"
SEQUENCE = "sequence"


@dataclass(frozen=True, slots=True)
class SourceDescriptor:
    """One local source file and the native reader kind that should consume it."""

    kind: str
    path: str
    source_file: str


@dataclass(frozen=True, slots=True)
class PreparedSourceBatch:
    """A native-readable group of local source files plus shared input options."""

    sources: tuple[SourceDescriptor, ...]
    input_format: str
    input_mode: str | None = None
    csv_delimiter: str = ","
    csv_has_header: bool = True
    xml_row_tag: str | None = None
    memory_limit_bytes: int | None = None

    def path_source_tuples(self) -> list[tuple[str, str, str]]:
        """Return the ABI representation expected by native path-source calls."""
        return path_source_tuples(self)


def _cleanup_native_source_plan_capsule(capsule: PreparedFinalizerCleanup) -> None:
    """Retry only the detached source-plan owners, never the rich wrapper."""
    kind = capsule.arg0
    payload = capsule.arg1
    if payload is not None:
        if kind == SEQUENCE:
            owned = list(cast(Iterable[Any], payload))
            _close_sequence_retryably(owned)
            if owned:
                capsule.arg1 = owned
                raise RuntimeError("deferred source-plan sequence cleanup remains retryable")
        elif not _close_suppressing_errors(payload):
            raise RuntimeError("deferred source-plan payload cleanup remains retryable")
        capsule.arg1 = None

    close_items = capsule.arg2
    if close_items is not None:
        owned_close_items = cast(list[Any], close_items)
        _close_sequence_retryably(owned_close_items)
        if owned_close_items:
            raise RuntimeError("deferred source-plan close items remain retryable")
        capsule.arg2 = None
    # native_payload is deliberately only rooted until all closeable owners are
    # gone; clearing the capsule after success drops it at this governed point.


@dataclass(slots=True)
class NativeSourcePlan:
    """Canonical native source execution plan."""

    kind: str
    payload: Any
    input_format: str
    route_name: str
    xml_row_tag: str | None = None
    source_batch: PreparedSourceBatch | None = None
    native_payload: Any | None = None
    close_items: list[Any] = field(default_factory=list)
    _pid: int = field(default_factory=os.getpid, init=False, repr=False)
    _finalizer_ticket: int = field(default=0, init=False, repr=False)
    _finalizer_capsule: PreparedFinalizerCleanup | None = field(
        default=None, init=False, repr=False
    )

    def __post_init__(self) -> None:
        """Validate and normalize the initialized instance state."""
        capsule = reserve_finalizer_cleanup(_cleanup_native_source_plan_capsule)
        ticket = capsule.ticket
        self._finalizer_ticket = ticket
        self._finalizer_capsule = capsule

    def _retire_finalizer_if_clean(self) -> None:
        """Retire finalizer if clean."""
        if self.payload is not None or self.close_items or self.native_payload is not None:
            return
        ticket = self._finalizer_ticket
        capsule = self._finalizer_capsule
        if ticket and capsule is not None:
            cancel_prepared_finalizer_cleanup(capsule)
            self._finalizer_ticket = 0
            self._finalizer_capsule = None

    def close(self) -> None:
        """Close plan resources while retaining failures for a later retry."""
        if os.getpid() != self._pid:
            return
        if self.kind == SEQUENCE:
            sequence = list(self.payload)
            _close_sequence_retryably(sequence)
            self.payload = tuple(sequence) if isinstance(self.payload, tuple) else sequence
            payload_closed = not sequence
        else:
            payload = self.payload
            payload_closed = _close_suppressing_errors(payload)
            if payload_closed and self.payload is payload:
                self.payload = None
        _close_sequence_retryably(self.close_items)
        if payload_closed and not self.close_items:
            self.native_payload = None
            self.source_batch = None
        self._retire_finalizer_if_clean()

    def __del__(self) -> None:
        """Retry abandoned cleanup outside shutdown and post-fork children."""
        try:
            if runtime_is_finalizing():
                return
            ticket = getattr(self, "_finalizer_ticket", 0)
            capsule = getattr(self, "_finalizer_capsule", None)
            if ticket and capsule is not None:
                capsule.arg0 = getattr(self, "kind", None)
                capsule.arg1 = getattr(self, "payload", None)
                capsule.arg2 = getattr(self, "close_items", None)
                capsule.arg3 = getattr(self, "native_payload", None)
                if defer_prepared_finalizer_cleanup(capsule):
                    # Detach the rich wrapper immediately.  Only the resources
                    # captured above remain rooted by the prepared capsule.
                    self.payload = None
                    self.close_items = None  # type: ignore[assignment]
                    self.native_payload = None
                    self.source_batch = None
                    self._finalizer_ticket = 0
                    self._finalizer_capsule = None
                return
        except BaseException:
            pass


@dataclass(frozen=True, slots=True)
class SourcePlanRegistryProbeResult:
    """Registry probe result plus the source-plan route used to produce it."""

    raw: Any
    route_name: str


def source_kind_for_format(input_format: str) -> str | None:
    """Return the native path-source kind for one public input format."""
    if input_format in {"json", "jsonl", "csv", "json_array", "xml"}:
        return input_format
    return None


def path_source_tuples(
    batch_or_sources: PreparedSourceBatch | Iterable[SourceDescriptor],
) -> list[tuple[str, str, str]]:
    """Return native ABI path-source tuples for a batch or source iterable."""
    sources = (
        batch_or_sources.sources
        if isinstance(batch_or_sources, PreparedSourceBatch)
        else batch_or_sources
    )
    return [(source.kind, source.path, source.source_file) for source in sources]


def _memory_limit_stage(input_format: str) -> str:
    """Return the document-size stage label for native path-source files."""
    if input_format in {"json", "json_array", "jsonl"}:
        return "json_parse"
    if input_format == "xml":
        return "xml_parse"
    if input_format == "csv":
        return "csv_parse"
    return f"{input_format}_parse"


def _path_source_native_payload(source_batch: PreparedSourceBatch) -> Any:
    """Return a reusable native plan with file limits validated in C++."""
    memory_limit_bytes = source_batch.memory_limit_bytes
    return call_core(
        PATH_SOURCE_PLAN_CREATE,
        path_source_tuples(source_batch),
        -1 if memory_limit_bytes is None else memory_limit_bytes,
        _memory_limit_stage(source_batch.input_format),
    )


def _native_path_source_plan(
    *,
    source_batch: PreparedSourceBatch,
    input_format: str,
    route_name: str,
    xml_row_tag: str | None = None,
) -> NativeSourcePlan:
    """Build a path-source plan around one canonical native capsule."""
    native_payload = _path_source_native_payload(source_batch)
    return NativeSourcePlan(
        kind=PATH_SOURCES,
        payload=None,
        input_format=input_format,
        route_name=route_name,
        xml_row_tag=xml_row_tag,
        source_batch=source_batch,
        native_payload=native_payload,
    )


def _flatten_path_source_sequence_or_none(
    plan: NativeSourcePlan,
) -> NativeSourcePlan | None:
    """Return one native path-source plan for a sequence of path-source children."""
    if plan.kind != SEQUENCE:
        return None
    descriptors: list[SourceDescriptor] = []
    for child in plan.payload:
        if (
            not isinstance(child, NativeSourcePlan)
            or child.kind != PATH_SOURCES
            or child.source_batch is None
        ):
            return None
        descriptors.extend(child.source_batch.sources)
    if not descriptors:
        return None
    source_batch = PreparedSourceBatch(
        sources=tuple(descriptors),
        input_format=plan.input_format,
        input_mode=None,
        xml_row_tag=plan.xml_row_tag,
    )
    return _native_path_source_plan(
        source_batch=source_batch,
        input_format=plan.input_format,
        route_name="native_sequence_path_sources",
        xml_row_tag=plan.xml_row_tag,
    )


def _path_sources_for_native(plan: NativeSourcePlan) -> Any:
    """Return the canonical native path-source plan capsule."""
    if plan.native_payload is None:
        raise RuntimeError("path-source plan is missing its native payload")
    return plan.native_payload


def _open_path_sources_auto_registry_stream(
    raw_context: Any,
    plan: NativeSourcePlan,
    call_options: Any,
    *,
    registry_json: str,
    field_name_policy: str,
    schema_mode: str,
    first_row_columns: dict[str, Any],
    timestamp_columns: TimestampColumns,
    skip_invalid_json_sources: bool = False,
    native_registry_state: Any = None,
) -> Any:
    """Open the current auto-registry stream for a path-source plan."""
    metadata_first_row_columns = dict(first_row_columns or {})
    metadata_first_row_columns.pop(SCHEMA_REGISTRY_COLUMN, None)
    metadata_first_row_columns.pop(SCHEMA_DRIFTS_COLUMN, None)
    common: dict[str, Any] = {
        "field_name_policy": field_name_policy,
        "schema_mode": schema_mode,
        "first_row_columns": metadata_first_row_columns,
        "timestamp_columns": timestamp_columns,
        "skip_invalid_json_sources": skip_invalid_json_sources,
    }
    if native_registry_state is not None:
        raw = call_core(
            raw_context.to_registry_sink_path_sources_auto_registry_state,
            "stream",
            _path_sources_for_native(plan),
            call_options,
            native_registry_state=native_registry_state,
            **common,
        )
    else:
        raw = call_core(
            raw_context.to_registry_sink_path_sources_auto_registry,
            "stream",
            _path_sources_for_native(plan),
            call_options,
            registry_json=registry_json,
            **common,
        )
    return raw
