"""Own the package ABI3 execution context and its process-local lifecycle.

The context wraps native execution methods, while the module creates, reuses, closes, and resets
the default process context across shutdown and fork boundaries.
"""

from __future__ import annotations

from typing import Any

from .generated_metadata import TimestampColumns
from .json_payloads import json_object_loads
from .native_options import _options_capsule, memory_limit_from_options
from .native_results import SinkOutput, _registry_sink_output
from .native_runtime import native_core as _native
from .probes import (
    _ExecutionRegistryInputProbeMethods,
    _ExecutionRegistryPathSourceProbeMethods,
    _ExecutionSchemaProbeMethods,
)
from .python_rows import PythonRowsJsonlByteReader
from .registry_sinks import (
    _RegistryArrowSinkMethods,
    _RegistryPathProviderSinkMethods,
    _RegistryPathSourceSinkMethods,
)


class ExecutionContext(
    _ExecutionSchemaProbeMethods,
    _ExecutionRegistryInputProbeMethods,
    _ExecutionRegistryPathSourceProbeMethods,
    _RegistryPathSourceSinkMethods,
    _RegistryPathProviderSinkMethods,
    _RegistryArrowSinkMethods,
):
    """Low-level ABI3 ingestion execution context."""

    def __init__(self) -> None:
        """Create a native execution context."""
        self._capsule = _native.context_new()

    def memory_stats(self) -> dict[str, Any]:
        """Return native context memory statistics."""
        return json_object_loads(_native.context_memory_stats_json(self._capsule))

    def performance_stats(self) -> dict[str, Any]:
        """Return telemetry for the latest operation run by this context."""
        return json_object_loads(_native.context_performance_stats_json(self._capsule))

    @staticmethod
    def _sink_output(sink: str, native_result: tuple[Any, Any]) -> SinkOutput:
        """Wrap a native sink result."""
        main, diagnostics = native_result
        return SinkOutput(sink=sink, main_stream_capsule=main, diagnostics_capsule=diagnostics)

    def _call_native_sink_from_source(
        self, sink: str, frontend: str, source: str, payload: Any, options: Any
    ) -> SinkOutput:
        """Prepare options and invoke the source-selected native sink."""
        return self._sink_output(
            sink,
            _native.context_to_sink_from_source(
                self._capsule,
                sink,
                frontend,
                source,
                payload,
                _options_capsule(options),
            ),
        )

    def _call_native_registry_sink_from_source(
        self,
        sink: str,
        frontend: str,
        source: str,
        payload: Any,
        options: Any,
        *,
        registry_json: str,
        field_name_policy: str,
        schema_mode: str,
        first_row_columns: dict[str, Any] | None = None,
        all_row_columns: dict[str, Any] | None = None,
        row_span_columns: dict[str, list[tuple[int, str | None]]] | None = None,
        timestamp_columns: TimestampColumns = (),
    ) -> SinkOutput:
        """Prepare options and invoke a source-selected registry sink."""
        args = [
            self._capsule,
            sink,
            frontend,
            source,
            payload,
            _options_capsule(options),
            registry_json,
            field_name_policy,
            schema_mode,
        ]
        if (
            first_row_columns is not None
            or all_row_columns is not None
            or row_span_columns is not None
            or timestamp_columns
        ):
            args.extend(
                [
                    first_row_columns or {},
                    all_row_columns or {},
                    row_span_columns or {},
                    timestamp_columns,
                ]
            )
        native_result = _native.context_to_registry_sink_from_source(*args)
        return _registry_sink_output(sink, native_result)

    def to_registry_sink_text(
        self,
        sink: str,
        frontend: str,
        text: Any,
        options: Any = None,
        *,
        registry_json: str,
        field_name_policy: str,
        schema_mode: str,
    ) -> SinkOutput:
        """Send text input to a native registry-backed sink."""
        return self.to_registry_sink_from_source(
            sink,
            frontend,
            "text",
            text,
            options,
            registry_json=registry_json,
            field_name_policy=field_name_policy,
            schema_mode=schema_mode,
        )

    def to_registry_sink_path(
        self,
        sink: str,
        frontend: str,
        path: Any,
        options: Any = None,
        *,
        registry_json: str,
        field_name_policy: str,
        schema_mode: str,
    ) -> SinkOutput:
        """Send path input to a native registry-backed sink."""
        return self.to_registry_sink_from_source(
            sink,
            frontend,
            "path",
            path,
            options,
            registry_json=registry_json,
            field_name_policy=field_name_policy,
            schema_mode=schema_mode,
        )

    def to_registry_sink_reader(
        self,
        sink: str,
        frontend: str,
        reader: Any,
        options: Any = None,
        *,
        registry_json: str,
        field_name_policy: str,
        schema_mode: str,
    ) -> SinkOutput:
        """Send a seekable reader to a native registry-backed sink."""
        return self.to_registry_sink_from_source(
            sink,
            frontend,
            "stream",
            reader,
            options,
            registry_json=registry_json,
            field_name_policy=field_name_policy,
            schema_mode=schema_mode,
        )

    def to_registry_sink_python(
        self,
        sink: str,
        data: Any,
        options: Any = None,
        *,
        registry_json: str,
        field_name_policy: str,
        schema_mode: str,
        first_row_columns: dict[str, Any] | None = None,
        all_row_columns: dict[str, Any] | None = None,
        row_span_columns: dict[str, list[tuple[int, str | None]]] | None = None,
        timestamp_columns: TimestampColumns = (),
    ) -> SinkOutput:
        """Serialize Python row iterables into the registry-backed native pipeline."""
        memory_limit = getattr(options, "memory_limit_bytes", None) if options is not None else None
        reader = PythonRowsJsonlByteReader(data, memory_limit_bytes=memory_limit)
        return self.to_registry_sink_from_source(
            sink,
            "jsonl",
            "stream",
            reader,
            options,
            registry_json=registry_json,
            field_name_policy=field_name_policy,
            schema_mode=schema_mode,
            first_row_columns=first_row_columns,
            all_row_columns=all_row_columns,
            row_span_columns=row_span_columns,
            timestamp_columns=timestamp_columns,
        )

    def to_registry_sink_from_source(
        self,
        sink: str,
        frontend: str,
        source: str,
        payload: Any,
        options: Any = None,
        *,
        registry_json: str,
        field_name_policy: str,
        schema_mode: str,
        first_row_columns: dict[str, Any] | None = None,
        all_row_columns: dict[str, Any] | None = None,
        row_span_columns: dict[str, list[tuple[int, str | None]]] | None = None,
        timestamp_columns: TimestampColumns = (),
    ) -> SinkOutput:
        """Send source-selected input to a native registry-backed sink."""
        return self._call_native_registry_sink_from_source(
            sink,
            frontend,
            source,
            payload,
            options,
            registry_json=registry_json,
            field_name_policy=field_name_policy,
            schema_mode=schema_mode,
            first_row_columns=first_row_columns,
            all_row_columns=all_row_columns,
            row_span_columns=row_span_columns,
            timestamp_columns=timestamp_columns,
        )

    def to_sink_text(self, sink: str, frontend: str, text: Any, options: Any = None) -> SinkOutput:
        """Send text input to a native sink."""
        return self.to_sink_from_source(sink, frontend, "text", text, options)

    def to_sink_path(self, sink: str, frontend: str, path: Any, options: Any = None) -> SinkOutput:
        """Send path input to a native sink."""
        return self.to_sink_from_source(sink, frontend, "path", path, options)

    def to_sink_reader(
        self, sink: str, frontend: str, reader: Any, options: Any = None
    ) -> SinkOutput:
        """Send a seekable byte reader to a native sink."""
        return self.to_sink_from_source(sink, frontend, "stream", reader, options)

    def to_sink_from_source(
        self, sink: str, frontend: str, source: str, payload: Any, options: Any = None
    ) -> SinkOutput:
        """Send source-selected input to a native sink."""
        return self._call_native_sink_from_source(sink, frontend, source, payload, options)

    def to_sink_python(self, sink: str, data: Any, options: Any = None) -> SinkOutput:
        """Serialize Python rows and send them to a native JSON sink."""
        memory_limit = memory_limit_from_options(options)
        reader = PythonRowsJsonlByteReader(data, memory_limit_bytes=memory_limit)
        return self.to_sink_reader(sink, "jsonl", reader, options)

    def to_sink_path_sources(
        self,
        sink: str,
        sources: Any,
        options: Any = None,
        *,
        include_source_file: bool,
        first_row_columns: dict[str, Any],
        timestamp_columns: TimestampColumns,
    ) -> SinkOutput:
        """Send multiple local path sources to a native sink."""
        return self._sink_output(
            sink,
            _native.context_to_sink_from_path_sources(
                self._capsule,
                sink,
                sources,
                _options_capsule(options),
                include_source_file,
                first_row_columns,
                timestamp_columns,
            ),
        )

    def to_sink_path_source_chunk_provider(
        self,
        sink: str,
        provider: Any,
        options: Any = None,
        *,
        include_source_file: bool,
        first_row_columns: dict[str, Any],
        timestamp_columns: TimestampColumns,
    ) -> SinkOutput:
        """Send lazily provided path-source chunks to a native sink."""
        return self._sink_output(
            sink,
            _native.context_to_sink_from_path_source_chunk_provider(
                self._capsule,
                sink,
                provider,
                _options_capsule(options),
                include_source_file,
                first_row_columns,
                timestamp_columns,
            ),
        )

    def to_sink_arrow_stream(
        self, sink: str, frontend: str, stream: Any, options: Any = None
    ) -> SinkOutput:
        """Send an Arrow C stream to a native sink."""
        return self._sink_output(
            sink,
            _native.context_to_sink_arrow_stream(
                self._capsule,
                sink,
                frontend,
                stream,
                _options_capsule(options),
            ),
        )


_DEFAULT_CONTEXT: ExecutionContext | None = None


def default_execution_context() -> ExecutionContext:
    """Return the shared low-level execution context, creating it when needed."""
    global _DEFAULT_CONTEXT
    if _DEFAULT_CONTEXT is None:
        _DEFAULT_CONTEXT = ExecutionContext()
    return _DEFAULT_CONTEXT


def reset_default_execution_context() -> None:
    """Discard the shared low-level execution context."""
    global _DEFAULT_CONTEXT
    _DEFAULT_CONTEXT = None
