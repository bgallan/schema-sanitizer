"""Registry-backed source-plan streams, materialization, and file output.

It opens owned registry streams, appends drift metadata, and drives either analytical
materialization or file output with authoritative cleanup.
"""

from __future__ import annotations

import json
import os
from dataclasses import dataclass, field
from types import SimpleNamespace
from typing import Any, cast

from schema_sanitizer.input_impl.source_plan import (
    PARQUET_ARROW_SOURCES,
    PATH_SOURCES,
    REMOTE_CHUNKS,
    SEQUENCE,
    NativeSourcePlan,
    _flatten_path_source_sequence_or_none,
    _open_path_sources_auto_registry_stream,
)

from ...adapters.pyarrow import streams as _pyarrow_streams
from ...core_impl.concurrency_stage_evidence import observe_successful_output_runtime_stage
from ...core_impl.execution_policy import execution_policy, normalize_threading_mode
from ...core_impl.finalization import runtime_is_finalizing
from ...core_impl.finalizer_cleanup import (
    PreparedFinalizerCleanup,
    cancel_prepared_finalizer_cleanup,
    defer_prepared_finalizer_cleanup,
    reserve_finalizer_cleanup,
)
from ...core_impl.generated_metadata import TimestampColumns
from ...core_impl.resource_lifecycle import (
    _cleanup_with_note,
    _close_suppressing_errors,
)
from ...options_impl.call_options import unwrap_options
from ...options_impl.options import Options, memory_limit_bytes_or_none
from ..output_diagnostics import patch_file_output_diagnostics, patch_table_diagnostics
from ..parquet.multisource import parquet_multisource_registry_sink_raw_or_none
from ..results import (
    AnalyticalOutputConversion,
    Result,
    convert_arrow_stream_output,
)
from ..stream_output import write_raw_stream_to_file
from ..streams import Stream, patch_input_route_diagnostics
from .remote import RemotePathSourceChunkProvider
from .remote_cleanup import take_prefetched_chunks


def _cleanup_opened_registry_stream_capsule(capsule: PreparedFinalizerCleanup) -> None:
    """Close detached registry stream owners without retaining metadata caches."""
    stream = capsule.arg0
    raw = capsule.arg1
    close_items = cast(list[Any] | None, capsule.arg2)
    if stream is not None:
        wrapped_raw = stream._raw if hasattr(stream, "_raw") else None
        if not _close_suppressing_errors(stream):
            raise RuntimeError("registry stream cleanup remains retryable")
        capsule.arg0 = None
        if raw is not None and wrapped_raw is raw:
            capsule.arg1 = None
            raw = None
    if raw is not None:
        if not _close_suppressing_errors(raw):
            raise RuntimeError("raw registry stream cleanup remains retryable")
        capsule.arg1 = None
    if close_items is not None:
        while close_items:
            item = close_items[-1]
            if item is not stream and item is not raw and not _close_suppressing_errors(item):
                raise RuntimeError("registry close-item cleanup remains retryable")
            close_items.pop()
        capsule.arg2 = None


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
    _pid: int = field(default_factory=os.getpid, init=False, repr=False)
    _finalizer_ticket: int | None = field(default=None, init=False, repr=False)
    _finalizer_capsule: PreparedFinalizerCleanup | None = field(
        default=None, init=False, repr=False
    )
    # Preallocate the terminal public list so __del__ can detach ownership
    # without allocating while preserving the stable post-cleanup object shape.
    _terminal_close_items: list[Any] = field(default_factory=list, init=False, repr=False)

    def __post_init__(self) -> None:
        """Validate and normalize the initialized instance state."""
        self._finalizer_capsule = reserve_finalizer_cleanup(_cleanup_opened_registry_stream_capsule)
        self._finalizer_ticket = self._finalizer_capsule.ticket

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
        """Close owned stream resources and retain failures for a later retry."""
        if os.getpid() != self._pid:
            return
        closed_ids: set[int] = set()
        blocked_ids: set[int] = set()
        stream = self.stream
        raw = self.raw_stream
        if stream is not None:
            wrapped_raw = stream._raw if hasattr(stream, "_raw") else None
            succeeded = _close_suppressing_errors(stream)
            if succeeded:
                if self.stream is stream:
                    self.stream = None
                if raw is not None and wrapped_raw is raw:
                    if self.raw_stream is raw:
                        self.raw_stream = None
                    closed_ids.add(id(raw))
            elif raw is not None and wrapped_raw is raw:
                blocked_ids.add(id(raw))
        elif raw is not None:
            if _close_suppressing_errors(raw):
                if self.raw_stream is raw:
                    self.raw_stream = None
                closed_ids.add(id(raw))
            else:
                blocked_ids.add(id(raw))

        failed: list[Any] = []
        failed_ids: set[int] = set()
        outcomes: dict[int, bool] = {}
        while self.close_items:
            item = self.close_items.pop()
            ident = id(item)
            if ident in closed_ids:
                continue
            if ident in blocked_ids:
                item_succeeded = False
            else:
                cached_succeeded = outcomes.get(ident)
                if cached_succeeded is None:
                    cached_succeeded = _close_suppressing_errors(item)
                    outcomes[ident] = cached_succeeded
                item_succeeded = cached_succeeded
            if not item_succeeded and ident not in failed_ids:
                failed.append(item)
                failed_ids.add(ident)
        self.close_items.extend(reversed(failed))
        if self.stream is None and self.raw_stream is None and not self.close_items:
            ticket = self._finalizer_ticket
            cleanup = self._finalizer_capsule
            if ticket is not None and cleanup is not None:
                cancel_prepared_finalizer_cleanup(cleanup)
                self._finalizer_ticket = None
                self._finalizer_capsule = None

    def __del__(self) -> None:
        """Detach only stream owners into a preallocated cleanup capsule."""
        try:
            if runtime_is_finalizing() or os.getpid() != getattr(self, "_pid", os.getpid()):
                return
            ticket = getattr(self, "_finalizer_ticket", None)
            cleanup = getattr(self, "_finalizer_capsule", None)
            if ticket is None or cleanup is None:
                return
            cleanup.arg0 = getattr(self, "stream", None)
            cleanup.arg1 = getattr(self, "raw_stream", None)
            cleanup.arg2 = getattr(self, "close_items", None)
            if defer_prepared_finalizer_cleanup(cleanup):
                self.stream = None
                self.raw_stream = None
                self.close_items = self._terminal_close_items
                self.diagnostics = None
                self.native_registry_state = None
                self._finalizer_ticket = None
                self._finalizer_capsule = None
        except BaseException:
            pass


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
    timestamp_columns: TimestampColumns,
    native_registry_state: Any,
) -> OpenedSourcePlanRegistryStream:
    """Open a remote stream through the native paired-provider registry route."""
    policy = execution_policy(
        plan.payload.threading_mode,
        plan.payload.memory_limit_bytes,
    )
    retained_chunks, remaining_start = take_prefetched_chunks(plan.payload)
    probe_provider = RemotePathSourceChunkProvider(
        retained_chunks=retained_chunks,
        remaining_manifest=plan.payload,
        retain_consumed_chunks=max(1, policy.remote_chunk_prefetch),
        remaining_start=remaining_start,
    )
    stream_provider = RemotePathSourceChunkProvider(
        retained_chunks=[],
        remaining_manifest=plan.payload,
        retained_chunk_donor=probe_provider,
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
    except BaseException as exc:
        for label, provider in (("probe", probe_provider), ("stream", stream_provider)):
            _cleanup_with_note(
                exc,
                provider,
                label=f"remote registry {label} provider cleanup also failed",
                method="close_all",
            )
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
    timestamp_columns: TimestampColumns,
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
    opened: OpenedSourcePlanRegistryStream | None
    if plan.kind == PATH_SOURCES:
        raw = _open_path_sources_auto_registry_stream(
            raw_context,
            plan,
            call_options,
            **common,
        )
        opened = _opened_raw_registry_stream(raw)
    elif plan.kind == REMOTE_CHUNKS:
        opened = _open_remote_registry_stream(raw_context, plan, call_options, **common)
    elif plan.kind == PARQUET_ARROW_SOURCES:
        raw = parquet_multisource_registry_sink_raw_or_none(
            raw_context,
            plan.payload,
            call_options,
            **common,
        )
        opened = None if raw is None else _opened_raw_registry_stream(raw)
    else:
        opened = None
    if opened is not None:
        patch_input_route_diagnostics(
            opened.diagnostics,
            source_route=plan.kind,
            plan_route=plan.route_name,
            parquet_route=(
                "native_registry_source_plan" if plan.kind == PARQUET_ARROW_SOURCES else None
            ),
        )
    return opened


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
    threading_mode: str = "single",
) -> Result:
    """Materialize an opened registry stream into an analytical target."""
    conversion: AnalyticalOutputConversion | None = None
    resource_owner_transferred = False
    try:
        if target == "pyarrow":
            table = _pyarrow_streams.table_from_stream_like(
                opened.materialization_stream(),
                feature="to_pyarrow",
            )
            observe_successful_output_runtime_stage("pyarrow")
            conversion = AnalyticalOutputConversion(
                clean_data=table,
                diagnostics_shape=table,
                route="arrow_c_stream_to_pyarrow_table",
            )
        else:
            conversion = convert_arrow_stream_output(
                opened.materialization_stream(),
                target,
                feature=f"to_{target}",
                threading_mode=threading_mode,
            )
        owner = SimpleNamespace(diagnostics=opened.diagnostics)
        result = Result(
            owner,
            clean_data=conversion.clean_data,
            schema_registry_json=opened.schema_registry_json,
            schema_drifts_json=opened.schema_drifts_json,
            native_registry_state=opened.native_registry_state,
            conversion_route=conversion.route,
        )
        resource_owner_transferred = conversion.transfer_resource_owner_to(result)
        patch_table_diagnostics(
            owner,
            result,
            conversion.diagnostics_shape,
            fill_inferred_rows_when_missing=True,
        )
        return result
    except BaseException as primary:
        if conversion is not None and not resource_owner_transferred:
            conversion.rollback_resource_owner(primary)
        raise
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
    threading_mode: str = "single",
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
                threading_mode=threading_mode,
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
            threading_mode=threading_mode,
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
    timestamp_columns: TimestampColumns,
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
        memory_limit_bytes=(memory_limit_bytes_or_none(call_options)),
        threading_mode=(
            normalize_threading_mode(call_options.performance.threading_mode)
            if call_options is not None
            else "single"
        ),
    )
