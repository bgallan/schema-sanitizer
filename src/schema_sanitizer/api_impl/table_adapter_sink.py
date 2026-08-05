"""Materialization helpers for analytical table-adapter sinks."""

from __future__ import annotations

from typing import Any

from ..options_impl.options import Options
from .results import convert_arrow_stream_output


def materialize_table_adapter_sink(
    context: Any,
    data: Any,
    *,
    sink: str,
    options: Any,
    format: Any,
    source: Any,
) -> Any:
    """Consume the native Arrow stream directly in an analytical adapter."""
    threading_mode = (
        options.performance.threading_mode if isinstance(options, Options) else "single"
    )
    output = context.to_sink(
        data,
        sink="stream",
        options=options,
        format=format,
        source=source,
    )
    try:
        conversion = convert_arrow_stream_output(
            output.raw,
            sink,
            feature=f"sink={sink!r}",
            threading_mode=threading_mode,
        )
        return conversion.clean_data
    finally:
        output.close()
