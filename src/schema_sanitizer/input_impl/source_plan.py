"""Canonical source plans, native path-source capsules, and sink helpers."""

from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass, field
from typing import Any

from ..core_impl.error_translation import call_core
from ..core_impl.generated_metadata import SCHEMA_DRIFTS_COLUMN, SCHEMA_REGISTRY_COLUMN
from ..core_impl.native_symbols import PATH_SOURCE_PLAN_CREATE
from ..core_impl.resource_lifecycle import _close_suppressing_errors
from .selection import native_input_format

PATH_SOURCES = "path_sources"
REMOTE_CHUNKS = "remote_chunks"
PARQUET_ARROW_SOURCES = "parquet_arrow_sources"
SEQUENCE = "sequence"

_LAST_NATIVE_MULTISOURCE_ROUTE = "none"


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

    def close(self) -> None:
        """Close resources owned by this plan."""
        if self.kind == SEQUENCE:
            for item in self.payload:
                _close_suppressing_errors(item)
        while self.close_items:
            _close_suppressing_errors(self.close_items.pop())


@dataclass(frozen=True, slots=True)
class SourcePlanRegistryProbeResult:
    """Registry probe result plus the source-plan route used to produce it."""

    raw: Any
    route_name: str


def source_kind_for_format(input_format: str) -> str | None:
    """Return the native path-source kind for one public input format."""
    native_format = native_input_format(input_format)
    if native_format in {"json", "csv", "json_array", "xml"}:
        return native_format
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


def last_native_multisource_route() -> str:
    """Return the route used by the most recent native multi-source conversion."""
    return _LAST_NATIVE_MULTISOURCE_ROUTE


def _mark_native_path_sources_route() -> None:
    """Preserve the native multi-source diagnostic route marker."""
    global _LAST_NATIVE_MULTISOURCE_ROUTE
    _LAST_NATIVE_MULTISOURCE_ROUTE = "cxx_path_sources"


def _memory_limit_stage(input_format: str) -> str:
    """Return the document-size stage label for native path-source files."""
    if input_format in {"json", "json_array", "jsonl", "ndjson"}:
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
    timestamp_columns: tuple[str, ...],
    skip_invalid_json_sources: bool = False,
    native_registry_state: Any = None,
) -> Any:
    """Open the current auto-registry stream for a path-source plan."""
    metadata_first_row_columns = dict(first_row_columns or {})
    metadata_first_row_columns.pop(SCHEMA_REGISTRY_COLUMN, None)
    metadata_first_row_columns.pop(SCHEMA_DRIFTS_COLUMN, None)
    common = {
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
    _mark_native_path_sources_route()
    return raw
