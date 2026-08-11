"""Lazy analytical record-batch API and resource ownership."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import TYPE_CHECKING, Any

from ..core_impl.resource_lifecycle import _close_and_clear_attrs
from ..options_impl.options import CsvHeaderMode
from ..sources.models import PublicInput
from .streams import Stream

if TYPE_CHECKING:
    from schema_sanitizer.input_impl.prepared import PreparedPublicInput

    from .operation_context import OperationExecutionContext
    from .source_plan.registry import OpenedSourcePlanRegistryStream


class _AnalyticalStreamResources:
    """Keep native input and operation state alive until a lazy stream closes."""

    def __init__(
        self,
        opened: OpenedSourcePlanRegistryStream,
        prepared_input: PreparedPublicInput,
        operation_context: OperationExecutionContext,
        payload_owner: Any = None,
    ) -> None:
        """Own all resources transferred by one lazy analytical call."""
        self._opened: OpenedSourcePlanRegistryStream | None = opened
        self._payload_owner: Any = payload_owner
        self._prepared_input: PreparedPublicInput | None = prepared_input
        self._operation_context: OperationExecutionContext | None = operation_context

    def retains(
        self,
        prepared_input: PreparedPublicInput,
        operation_context: OperationExecutionContext,
        payload_owner: Any = None,
    ) -> bool:
        """Confirm an exact lazy-stream handoff after an asynchronous unwind."""
        return (
            self._prepared_input is prepared_input
            and self._operation_context is operation_context
            and (payload_owner is None or self._payload_owner is payload_owner)
        )

    def close(self) -> None:
        """Release resources while retaining any cleanup failures for retry."""
        _close_and_clear_attrs(
            self,
            "_opened",
            "_payload_owner",
            "_prepared_input",
            "_operation_context",
        )


def lazy_stream_from_opened(
    opened: OpenedSourcePlanRegistryStream,
    prepared_input: PreparedPublicInput,
    operation_context: OperationExecutionContext,
    *,
    payload_owner: Any = None,
    handoff: list[Stream] | None = None,
) -> Stream:
    """Transfer an opened analytical operation to a lazy batch iterator."""
    stream = Stream(opened.materialization_stream())
    # Publish the wrapper in caller-owned plain storage before attaching rich
    # owners. If an async exception lands during the handoff, the outer cleanup
    # can inspect this exact Stream instead of double-closing its authorities.
    if handoff is not None:
        handoff.append(stream)
    stream._keepalive = _AnalyticalStreamResources(
        opened,
        prepared_input,
        operation_context,
        payload_owner,
    )
    stream._close_on_exhaustion = True
    stream.schema_registry_json = opened.schema_registry_json
    stream.schema_drifts_json = opened.schema_drifts_json
    stream.native_registry_state = opened.native_registry_state
    stream.execution_policy = operation_context.policy.to_dict()
    return stream


def iter_batches(
    input_path: PublicInput,
    *,
    input_format: str | None = None,
    input_mode: str = "single_file",
    schema_mode: str = "additive",
    column_order: str = "alphabetically",
    field_name_policy: str = "lower_alpha",
    timestamp_precision: str = "TIMESTAMP_MICROS",
    parse_integers: bool = False,
    parse_floats: bool = False,
    parse_float_decimal_separator: str = ".",
    parse_float_thousands_separator: str = ",",
    parse_iso_timestamps: bool = False,
    parse_iso_dates: bool = False,
    parse_iso_times: bool = False,
    true_tokens: Sequence[str] = (),
    false_tokens: Sequence[str] = (),
    custom_timestamp_patterns: Sequence[str] = (),
    custom_date_patterns: Sequence[str] = (),
    custom_time_patterns: Sequence[str] = (),
    arrow_max_depth: int = 32,
    parquet_max_depth: int = 15,
    scalar_object_key: str = "default_key",
    csv_has_header: bool = True,
    csv_delimiter: str = ",",
    csv_escape_char: str | None = None,
    csv_header_mode: CsvHeaderMode = "exact",
    input_text_encoding: str = "utf-8",
    xml_row_tag: str | None = None,
    on_error: str = "emit_null_row",
    multi_threading: bool = False,
    memory_limit_bytes: int | None = None,
    schema_registry: Mapping[str, Any] | str | None = None,
) -> Stream:
    """Sanitize lazily and yield bounded PyArrow record batches."""
    options = locals()
    from .analytical import convert_analytical_with_options

    result = convert_analytical_with_options(
        input_path,
        target="pyarrow_reader",
        input_format=input_format,
        input_mode=input_mode,
        options=options,
        schema_registry=schema_registry,
    )
    if not isinstance(result, Stream):  # pragma: no cover - internal invariant
        raise RuntimeError("iter_batches did not receive a streaming result")
    return result
