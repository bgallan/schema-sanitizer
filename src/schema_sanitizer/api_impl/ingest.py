"""Native ingestion planning and binary input routing."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Literal, TypeAlias

from ..core_impl.resource_lifecycle import _close_suppressing_errors
from ..input_impl.selection import (
    _Format,
    _Source,
    prepare_native_text_data,
    resolve_source_and_format,
)
from ..options_impl.options import Options
from .input.memory_limits import enforce_materialized_input_limit
from .parquet.errors import unsupported_direct_parquet_ingestion

_BinarySource: TypeAlias = Literal["auto", "text", "path", "python", "uri", "stream"]


def reject_unsupported_binary_direct_input(
    data: Any,
    *,
    source: _BinarySource,
    format: str,
    memory_limit_bytes: int | None = None,
) -> tuple[Any, _BinarySource, str]:
    """Reject binary formats that no longer have direct native routes."""
    if format != "parquet":
        return data, source, format

    del data, source, memory_limit_bytes
    raise unsupported_direct_parquet_ingestion()


@dataclass(slots=True)
class NativeIngestPlan:
    """Resolved input state for one native ingest call."""

    data: Any
    source: _Source
    format: _Format
    call_options: Options | None
    memory_limit_bytes: int | None
    input_text_encoding: str
    keepalive: Any = None

    def close_keepalive(self) -> None:
        """Close any input stream opened while preparing the plan."""
        if self.keepalive is not None:
            _close_suppressing_errors(self.keepalive)
            self.keepalive = None


def normalize_options(options: Options | dict[str, Any] | None) -> Options | None:
    """Normalize accepted high-level option inputs."""
    if options is None:
        return None
    if isinstance(options, dict):
        return Options.from_dict(options)
    if not isinstance(options, Options):
        raise TypeError("options must be None, dict, or internal call options")
    return options


def _option_io_settings(call_options: Options | None) -> tuple[int | None, str]:
    """Return memory and text-encoding settings from normalized options."""
    if call_options is None:
        return None, "utf-8"
    return call_options.performance.memory_limit_bytes, call_options.io.input_text_encoding


def native_ingest_plan(
    data: Any,
    *,
    options: Options | dict[str, Any] | None,
    format: _Format,
    source: _Source,
) -> NativeIngestPlan:
    """Resolve input selectors and prepare a native-ingest source."""
    call_options = normalize_options(options)
    data, source, format = resolve_source_and_format(data, format=format, source=source)
    memory_limit_bytes, input_text_encoding = _option_io_settings(call_options)
    enforce_materialized_input_limit(
        data, format, memory_limit_bytes=memory_limit_bytes, source=source
    )
    data, source, format = reject_unsupported_binary_direct_input(
        data,
        source=source,
        format=format,
        memory_limit_bytes=memory_limit_bytes,
    )

    if source == "uri":
        raise ValueError(
            "Remote URI inputs must be staged by the public file APIs before native ingestion"
        )

    native_data, source = prepare_native_text_data(
        data,
        source=source,
        format_name=format,
        input_text_encoding=input_text_encoding,
        memory_limit_bytes=memory_limit_bytes,
    )
    return NativeIngestPlan(
        data=native_data,
        source=source,
        format=format,
        call_options=call_options,
        memory_limit_bytes=memory_limit_bytes,
        input_text_encoding=input_text_encoding,
    )
