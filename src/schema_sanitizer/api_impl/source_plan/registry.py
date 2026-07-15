"""Registry-backed source-plan streams, materialization, and file output."""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from types import SimpleNamespace
from typing import Any

from schema_sanitizer.input_impl.source_plan import (
    PARQUET_ARROW_SOURCES,
    PATH_SOURCES,
    REMOTE_CHUNKS,
    SEQUENCE,
    NativeSourcePlan,
    _flatten_path_source_sequence_or_none,
    _mark_native_path_sources_route,
    _open_path_sources_auto_registry_stream,
)

from ...adapters.pyarrow import streams as _pyarrow_streams
from ...core_impl.resource_lifecycle import _close_suppressing_errors
from ...options_impl.call_options import unwrap_options
from ...options_impl.options import Options
from ..output_diagnostics import patch_file_output_diagnostics, patch_table_diagnostics
from ..parquet.multisource import parquet_multisource_registry_sink_raw_or_none
from ..results import Result, convert_arrow_table_output
from ..stream_output import write_raw_stream_to_file
from ..streams import Stream
from .remote import RemotePathSourceChunkProvider


@dataclass(slots=True)
class OpenedSourcePlanRegistryStream:
    """Opened registry-backed stream plus registry metadata and ownership."""

    stream: Any | None
    schema_registry_json: str
    schema_drifts_json: str
    native_registry_state: Any = None
    diagnostics: Any = None
    raw_stream: Any | None = None
    close_items: list[Any] = field(default_factory=list)

    def output_stream(self) -> Any:
        """Return a Python stream wrapper for file writers."""
        if self.stream is None:
            if self.raw_stream is None:
                raise RuntimeError("opened registry stream has no stream backend")
            self.stream = Stream(self.raw_stream)
        return self.stream

    def materialization_stream(self) -> Any:
        """Return the most direct Arrow C Stream object for table materialization."""
        return self.raw_stream if self.raw_stream is not None else self.output_stream()

    def take_raw_output_stream(self) -> Any | None:
        """Transfer the raw stream to a direct file writer when no wrapper exists."""
        if self.raw_stream is None or self.stream is not None:
            return None
        raw = self.raw_stream
        self.raw_stream = None
        self.close_items = [item for item in self.close_items if item is not raw]
        return raw

    def close(self) -> None:
        """Close opened stream resources exactly once."""
        closed_raw: Any | None = None
        if self.stream is not None:
            _close_suppressing_errors(self.stream)
            self.stream = None
        elif self.raw_stream is not None:
            closed_raw = self.raw_stream
            _close_suppressing_errors(closed_raw)
            self.raw_stream = None
        while self.close_items:
            item = self.close_items.pop()
            if item is not closed_raw:
                _close_suppressing_errors(item)


def append_schema_drifts(target: list[Any], raw_json: str | None) -> None:
    """Append JSON array items from a native result JSON string."""
    if not raw_json or raw_json == "[]":
        return
    value = json.loads(raw_json)
    if isinstance(value, list):
        target.extend(value)


def _opened_raw_registry_stream(raw: Any) -> OpenedSourcePlanRegistryStream:
    """Wrap a native raw registry stream for source-plan callers."""
    return OpenedSourcePlanRegistryStream(
        stream=None,
        schema_registry_json=raw.schema_registry_json,
        schema_drifts_json=raw.schema_drifts_json,
        native_registry_state=raw.native_registry_state,
        diagnostics=raw.diagnostics,
        raw_stream=raw,
        close_items=[raw],
    )


def _open_remote_registry_stream(
    raw_context: Any,
    plan: NativeSourcePlan,
    call_options: Any,
    *,
    registry_json: str,
    field_name_policy: str,
    schema_mode: str,
    first_row_columns: dict[str, Any],
    timestamp_columns: tuple[str, ...],
    native_registry_state: Any,
) -> OpenedSourcePlanRegistryStream:
    """Open a remote stream through the native paired-provider registry route."""
    probe_provider = RemotePathSourceChunkProvider(
        retained_chunks=[], remaining_manifest=plan.payload
    )
    stream_provider = RemotePathSourceChunkProvider(
        retained_chunks=[], remaining_manifest=plan.payload
    )
    try:
        raw = raw_context.to_registry_sink_path_source_chunk_provider_auto_registry(
            "stream",
            probe_provider,
            stream_provider,
            call_options,
            registry_json=registry_json,
            field_name_policy=field_name_policy,
            schema_mode=schema_mode,
            first_row_columns=first_row_columns,
            timestamp_columns=timestamp_columns,
            native_registry_state=native_registry_state,
            skip_invalid_json_sources=True,
        )
        _mark_native_path_sources_route()
    except Exception:
        probe_provider.close_all()
        stream_provider.close_all()
        raise
    return _opened_raw_registry_stream(raw)


def open_source_plan_registry_stream(
    raw_context: Any,
    plan: NativeSourcePlan,
    call_options: Any,
    *,
    registry_json: str,
    field_name_policy: str,
    schema_mode: str,
    first_row_columns: dict[str, Any],
    timestamp_columns: tuple[str, ...],
    native_registry_state: Any = None,
) -> OpenedSourcePlanRegistryStream | None:
    """Open a registry-backed stream from the canonical native source plan."""
    if plan.kind == SEQUENCE:
        flattened = _flatten_path_source_sequence_or_none(plan)
        if flattened is None:
            return None
        plan = flattened

    common = {
        "registry_json": registry_json,
        "field_name_policy": field_name_policy,
        "schema_mode": schema_mode,
        "first_row_columns": first_row_columns,
        "timestamp_columns": timestamp_columns,
        "native_registry_state": native_registry_state,
    }
    if plan.kind == PATH_SOURCES:
        raw = _open_path_sources_auto_registry_stream(
            raw_context,
            plan,
            call_options,
            **common,
        )
        return _opened_raw_registry_stream(raw)
    if plan.kind == REMOTE_CHUNKS:
        return _open_remote_registry_stream(raw_context, plan, call_options, **common)
    if plan.kind == PARQUET_ARROW_SOURCES:
        raw = parquet_multisource_registry_sink_raw_or_none(
            raw_context,
            plan.payload,
            call_options,
            **common,
        )
        return None if raw is None else _opened_raw_registry_stream(raw)
    return None


def _result_from_opened(opened: OpenedSourcePlanRegistryStream, owner: Any) -> Result:
    """Return a Result carrying registry metadata from an opened stream."""
    return Result(
        owner,
        schema_registry_json=opened.schema_registry_json,
        schema_drifts_json=opened.schema_drifts_json,
        native_registry_state=opened.native_registry_state,
    )


def materialize_opened_registry_stream(
    opened: OpenedSourcePlanRegistryStream,
    *,
    target: str,
) -> Result:
    """Materialize an opened registry stream into an analytical target."""
    try:
        table = _pyarrow_streams.table_from_stream_like(
            opened.materialization_stream(),
            feature=f"to_{target}",
        )
        owner = SimpleNamespace(diagnostics=opened.diagnostics)
        result = Result(
            owner,
            clean_data=convert_arrow_table_output(table, target, feature=f"to_{target}"),
            schema_registry_json=opened.schema_registry_json,
            schema_drifts_json=opened.schema_drifts_json,
            native_registry_state=opened.native_registry_state,
        )
        patch_table_diagnostics(owner, result, table, fill_inferred_rows_when_missing=True)
        return result
    finally:
        opened.close()


def write_opened_registry_stream_to_file(
    opened: OpenedSourcePlanRegistryStream,
    out_path: Any,
    *,
    writer: Any,
    feature: str,
    parquet_compression: str | None = None,
    parquet_gzip_level: int | None = None,
    memory_limit_bytes: int | None = None,
) -> Result:
    """Write an opened registry stream whose generated metadata is already present."""
    try:
        raw_stream = opened.take_raw_output_stream()
        if raw_stream is not None:
            result = write_raw_stream_to_file(
                raw_stream,
                out_path,
                writer=writer,
                feature=feature,
                first_row_columns=None,
                all_row_columns=None,
                row_span_columns=None,
                timestamp_columns=(),
                parquet_compression=parquet_compression,
                parquet_gzip_level=parquet_gzip_level,
                memory_limit_bytes=memory_limit_bytes,
            )
            result.schema_registry_json = opened.schema_registry_json
            result.schema_drifts_json = opened.schema_drifts_json
            result.native_registry_state = opened.native_registry_state
            return result

        parquet_kwargs = {}
        if parquet_compression is not None or parquet_gzip_level is not None:
            parquet_kwargs = {
                "parquet_compression": parquet_compression,
                "parquet_gzip_level": parquet_gzip_level,
            }
        native_stats = writer(
            opened.output_stream(),
            out_path,
            feature=feature,
            first_row_columns=None,
            all_row_columns=None,
            row_span_columns=None,
            timestamp_columns=(),
            memory_limit_bytes=memory_limit_bytes,
            **parquet_kwargs,
        )
        owner = SimpleNamespace(diagnostics=opened.diagnostics)
        result = _result_from_opened(opened, owner)
        patch_file_output_diagnostics(result, out_path, feature, native_stats=native_stats)
        return result
    finally:
        opened.close()


def write_source_plan_registry_to_file(
    raw_context: Any,
    plan: NativeSourcePlan,
    out_path: Any,
    *,
    writer: Any,
    feature: str,
    call_options: Options | None,
    first_row_columns: dict[str, Any],
    timestamp_columns: tuple[str, ...],
    schema_registry_json: str,
    schema_mode: str,
    field_name_policy: str,
    native_registry_state: Any = None,
    parquet_compression: str | None = None,
    parquet_gzip_level: int | None = None,
) -> Result | None:
    """Write a registry-backed output from the canonical native source plan."""
    opened = open_source_plan_registry_stream(
        raw_context,
        plan,
        unwrap_options(call_options),
        registry_json=schema_registry_json,
        field_name_policy=field_name_policy,
        schema_mode=schema_mode,
        first_row_columns=dict(first_row_columns),
        timestamp_columns=timestamp_columns,
        native_registry_state=native_registry_state,
    )
    if opened is None:
        return None
    return write_opened_registry_stream_to_file(
        opened,
        out_path,
        writer=writer,
        feature=feature,
        parquet_compression=parquet_compression,
        parquet_gzip_level=parquet_gzip_level,
        memory_limit_bytes=(
            call_options.performance.memory_limit_bytes if call_options is not None else None
        ),
    )
