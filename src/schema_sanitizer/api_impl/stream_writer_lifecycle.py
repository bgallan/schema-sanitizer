"""Shared lifecycle helpers for streaming file writers."""

from __future__ import annotations

from types import SimpleNamespace
from typing import Any

from .ingest_lifecycle import _close_suppressing_errors
from .ingest_runtime_types import Result


def close_sink_output_or_stream(sink_out: Any, stream: Any = None) -> None:
    """Close a sink output or its fallback stream without surfacing cleanup errors."""
    target = sink_out if callable(getattr(sink_out, "close", None)) else stream
    _close_suppressing_errors(target)


def close_consumed_stream(stream: Any) -> None:
    """Close a stream after a writer has consumed its main Arrow stream."""
    close_main = getattr(stream, "close_main_stream", None)
    if callable(close_main):
        close_main()
    else:
        stream.close()


def diagnostics_only_result(raw: Any) -> Result:
    """Return a file-writer Result that carries diagnostics but no materialized table."""
    return Result(
        SimpleNamespace(diagnostics=getattr(raw, "diagnostics", None)),
        clean_data=None,
    )
