"""Implements `schema_sanitizer.api_impl.context`."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from ..core_impl.runtime import ExecutionContext as _CoreExecutionContext
from .ingest_runtime_selectors import _Format, _Source
from .shared import Options, _call_core

if TYPE_CHECKING:  # pragma: no cover
    from .ingest_runtime_types import Result


class ExecutionContext:
    """Ingestion execution context."""

    def __init__(self):
        """Create a high-level execution context."""
        self._raw = _call_core(_CoreExecutionContext)

    def memory_stats(self) -> dict[str, Any]:
        """Return memory statistics from the native context."""
        return _call_core(self._raw.memory_stats)

    def to_sink(
        self,
        data: Any,
        *,
        sink: str = "table",
        options: Options | None = None,
        format: _Format = "auto",
        source: _Source = "auto",
    ) -> Any:
        """Route input data to a named sink."""
        from .context_ops import execution_context_to_sink

        return execution_context_to_sink(
            self,
            data,
            sink=sink,
            options=options,
            format=format,
            source=source,
        )

    def to_table(
        self,
        data: Any,
        options: Options | None = None,
        *,
        format: _Format = "auto",
        source: _Source = "auto",
    ) -> Result:
        """Materialize input data as a table result."""
        from .context_ops import execution_context_to_table

        return execution_context_to_table(
            self,
            data,
            options=options,
            format=format,
            source=source,
        )
