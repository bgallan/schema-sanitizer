"""Shared native-ingest planning helpers for stream writer paths."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from .async_remote_io import looks_like_remote_uri
from .ingest_input_prepare import prepare_native_text_data
from .ingest_lifecycle import _close_suppressing_errors
from .ingest_runtime_binary import reject_unsupported_binary_direct_input
from .ingest_runtime_selectors import _Format, _resolve_source_and_format, _Source
from .shared import Options, _maybe_enforce_memory_limit


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


def source_for_file_input(input_path: Any) -> _Source:
    """Return the already-known source selector for a converter input path."""
    return "uri" if looks_like_remote_uri(input_path) else "path"


def _option_io_settings(call_options: Options | None) -> tuple[int | None, str]:
    """Return memory and text-encoding settings from normalized options."""
    if not isinstance(call_options, Options):
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
    data, source, format = _resolve_source_and_format(
        data,
        format=format,
        source=source,
    )
    memory_limit_bytes, input_text_encoding = _option_io_settings(call_options)
    _maybe_enforce_memory_limit(data, format, memory_limit_bytes=memory_limit_bytes, source=source)
    data, source, format = reject_unsupported_binary_direct_input(
        data,
        source=source,
        format=format,
        memory_limit_bytes=memory_limit_bytes,
    )

    keepalive = None
    if source == "uri":
        raise ValueError(
            "Remote URI inputs must be staged by the public file APIs before native ingestion"
        )

    native_data, source = prepare_native_text_data(
        data,
        src=source,
        fmt=format,
        input_text_encoding=input_text_encoding,
    )
    return NativeIngestPlan(
        data=native_data,
        source=source,
        format=format,
        call_options=call_options,
        memory_limit_bytes=memory_limit_bytes,
        input_text_encoding=input_text_encoding,
        keepalive=keepalive,
    )
