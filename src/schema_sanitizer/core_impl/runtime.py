"""Implements `schema_sanitizer.core_impl.runtime`."""

from __future__ import annotations

from typing import Any

from .json_payloads import json_object_loads as _json_loads
from .native import _native
from .options_bytes import _options_capsule
from .python_rows import PythonRowsJsonlByteReader
from .runtime_registry import (
    call_native_registry_sink_arrow_source_chunk_provider,
    call_native_registry_sink_arrow_source_chunk_provider_auto_registry,
    call_native_registry_sink_arrow_sources,
    call_native_registry_sink_arrow_sources_auto_registry,
    call_native_registry_sink_arrow_stream,
    call_native_registry_sink_from_path_source_chunk_provider,
    call_native_registry_sink_from_path_source_chunk_provider_auto_registry,
    call_native_registry_sink_from_path_sources,
    call_native_registry_sink_from_path_sources_auto_registry,
    call_native_registry_sink_from_path_sources_auto_registry_state,
    call_native_registry_sink_from_source,
)
from .runtime_support import RegistryProbeResult, SchemaProbeResult, SinkOutput

_LAST_SINK_SOURCE_ROUTE = "none"


def last_sink_source_route() -> str:
    """Return the route used by the most recent normal native sink call."""
    return _LAST_SINK_SOURCE_ROUTE


def _registry_probe_path_source_error_is_skippable(exc: Exception) -> bool:
    """Return whether a best-effort path-source probe may skip this failure."""
    message = str(exc)
    return "JSON parse error" in message or "Invalid JSON file" in message


class ExecutionContext:
    """ABI3 ingestion execution context."""

    _accepts_native_path_source_plan = True

    def __init__(self) -> None:
        """Create a native execution context."""
        self._capsule = _native.context_new()

    @staticmethod
    def supports_path_source_chunk_provider() -> bool:
        """Return whether the loaded native module supports chunk providers."""
        return (
            getattr(
                _native,
                "context_to_registry_sink_from_path_source_chunk_provider_registry_state",
                None,
            )
            is not None
        )

    @staticmethod
    def supports_path_source_chunk_provider_auto_registry() -> bool:
        """Return whether native chunk providers support auto-registry streams."""
        return (
            getattr(
                _native,
                "context_to_registry_sink_from_path_source_chunk_provider_auto_registry",
                None,
            )
            is not None
        )

    @staticmethod
    def supports_sink_path_source_chunk_provider() -> bool:
        """Return whether the loaded native module supports plain chunk providers."""
        return getattr(_native, "context_to_sink_from_path_source_chunk_provider", None) is not None

    @staticmethod
    def supports_registry_probe_path_source_chunk_provider() -> bool:
        """Return whether native registry probes support chunk providers."""
        return (
            getattr(_native, "context_registry_probe_from_path_source_chunk_provider", None)
            is not None
        )

    @staticmethod
    def supports_arrow_source_chunk_provider() -> bool:
        """Return whether native registry sinks support Arrow-source chunk providers."""
        return (
            getattr(
                _native,
                "context_to_registry_sink_arrow_source_chunk_provider_registry_state",
                None,
            )
            is not None
        )

    @staticmethod
    def supports_arrow_source_chunk_provider_auto_registry() -> bool:
        """Return whether Arrow-source chunk providers support native auto-registry."""
        return (
            getattr(
                _native,
                "context_to_registry_sink_arrow_source_chunk_provider_auto_registry",
                None,
            )
            is not None
        )

    def memory_stats(self) -> dict[str, Any]:
        """Return native context memory statistics."""
        return _json_loads(_native.context_memory_stats_json(self._capsule))

    @staticmethod
    def _sink_output(sink: str, native_result: tuple[Any, Any]) -> SinkOutput:
        """Wrap a native sink result."""
        main, diagnostics = native_result
        return SinkOutput(
            sink=sink,
            main_stream_capsule=main,
            diagnostics_capsule=diagnostics,
        )

    def _call_native_sink_from_source(
        self, sink: str, frontend: str, source: str, payload: Any, options: Any
    ) -> SinkOutput:
        """Prepare options and invoke the source-selected native sink."""
        global _LAST_SINK_SOURCE_ROUTE
        _LAST_SINK_SOURCE_ROUTE = source
        prepared = _options_capsule(options)
        return self._sink_output(
            sink,
            _native.context_to_sink_from_source(
                self._capsule,
                sink,
                frontend,
                source,
                payload,
                prepared,
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
        timestamp_columns: tuple[str, ...] = (),
    ) -> SinkOutput:
        """Prepare options and invoke the source-selected native registry sink."""
        return call_native_registry_sink_from_source(
            self._capsule,
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

    @staticmethod
    def supports_sink_path_sources() -> bool:
        """Return whether the loaded native module exposes plain path sources."""
        return hasattr(_native, "context_to_sink_from_path_sources")

    def to_sink_path_sources(
        self,
        sink: str,
        sources: Any,
        options: Any = None,
        *,
        include_source_file: bool,
        first_row_columns: dict[str, Any],
        timestamp_columns: tuple[str, ...],
    ) -> SinkOutput:
        """Send multiple local path sources to a native sink."""
        global _LAST_SINK_SOURCE_ROUTE
        _LAST_SINK_SOURCE_ROUTE = "path_sources"
        prepared = _options_capsule(options)
        return self._sink_output(
            sink,
            _native.context_to_sink_from_path_sources(
                self._capsule,
                sink,
                sources,
                prepared,
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
        timestamp_columns: tuple[str, ...],
    ) -> SinkOutput:
        """Send lazily provided path-source chunks to a native sink."""
        global _LAST_SINK_SOURCE_ROUTE
        _LAST_SINK_SOURCE_ROUTE = "path_source_chunk_provider"
        prepared = _options_capsule(options)
        native_call = getattr(
            _native,
            "context_to_sink_from_path_source_chunk_provider",
            None,
        )
        if native_call is None:
            raise AttributeError("native runtime does not support path-source chunk providers")
        return self._sink_output(
            sink,
            native_call(
                self._capsule,
                sink,
                provider,
                prepared,
                include_source_file,
                first_row_columns,
                timestamp_columns,
            ),
        )

    def to_sink_arrow_stream(
        self, sink: str, frontend: str, stream: Any, options: Any = None
    ) -> SinkOutput:
        """Send an Arrow C stream to a native sink."""
        global _LAST_SINK_SOURCE_ROUTE
        _LAST_SINK_SOURCE_ROUTE = "arrow"
        prepared = _options_capsule(options)
        return self._sink_output(
            sink,
            _native.context_to_sink_arrow_stream(
                self._capsule,
                sink,
                frontend,
                stream,
                prepared,
            ),
        )

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
        """Send a seekable byte reader to a native registry-backed sink."""
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
        timestamp_columns: tuple[str, ...] = (),
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

    def to_registry_sink_arrow_stream(
        self,
        sink: str,
        frontend: str,
        stream: Any,
        options: Any = None,
        *,
        registry_json: str,
        field_name_policy: str,
        schema_mode: str,
    ) -> SinkOutput:
        """Send an Arrow C stream to a native registry-backed sink."""
        return call_native_registry_sink_arrow_stream(
            self._capsule,
            sink,
            frontend,
            stream,
            options,
            registry_json=registry_json,
            field_name_policy=field_name_policy,
            schema_mode=schema_mode,
        )

    def to_registry_sink_path_sources(
        self,
        sink: str,
        sources: Any,
        options: Any = None,
        *,
        registry_json: str,
        drifts_json: str,
        conversion_timestamp: str,
        field_name_policy: str,
        schema_mode: str,
        first_row_columns: dict[str, Any],
        timestamp_columns: tuple[str, ...],
        native_registry_state: Any = None,
    ) -> SinkOutput:
        """Send multiple local path sources to a native registry-backed sink."""
        return call_native_registry_sink_from_path_sources(
            self._capsule,
            sink,
            sources,
            options,
            registry_json=registry_json,
            drifts_json=drifts_json,
            conversion_timestamp=conversion_timestamp,
            field_name_policy=field_name_policy,
            schema_mode=schema_mode,
            first_row_columns=first_row_columns,
            timestamp_columns=timestamp_columns,
            native_registry_state=native_registry_state,
        )

    def to_registry_sink_path_source_chunk_provider(
        self,
        sink: str,
        provider: Any,
        options: Any = None,
        *,
        native_registry_state: Any,
        schema_mode: str,
        first_row_columns: dict[str, Any],
        timestamp_columns: tuple[str, ...],
    ) -> SinkOutput:
        """Send lazily provided path-source chunks to a native registry sink."""
        return call_native_registry_sink_from_path_source_chunk_provider(
            self._capsule,
            sink,
            provider,
            options,
            native_registry_state=native_registry_state,
            schema_mode=schema_mode,
            first_row_columns=first_row_columns,
            timestamp_columns=timestamp_columns,
        )

    def to_registry_sink_path_source_chunk_provider_auto_registry(
        self,
        sink: str,
        probe_provider: Any,
        stream_provider: Any,
        options: Any = None,
        *,
        registry_json: str,
        field_name_policy: str,
        schema_mode: str,
        first_row_columns: dict[str, Any],
        timestamp_columns: tuple[str, ...],
        native_registry_state: Any = None,
        skip_invalid_json_sources: bool = False,
    ) -> SinkOutput:
        """Infer and stream path-source chunks using paired lazy providers."""
        return call_native_registry_sink_from_path_source_chunk_provider_auto_registry(
            self._capsule,
            sink,
            probe_provider,
            stream_provider,
            options,
            registry_json=registry_json,
            field_name_policy=field_name_policy,
            schema_mode=schema_mode,
            first_row_columns=first_row_columns,
            timestamp_columns=timestamp_columns,
            native_registry_state=native_registry_state,
            skip_invalid_json_sources=skip_invalid_json_sources,
        )

    def to_registry_sink_path_sources_auto_registry(
        self,
        sink: str,
        sources: Any,
        options: Any = None,
        *,
        registry_json: str,
        field_name_policy: str,
        schema_mode: str,
        first_row_columns: dict[str, Any],
        timestamp_columns: tuple[str, ...],
        skip_invalid_json_sources: bool = False,
    ) -> SinkOutput:
        """Infer and stream multiple local path sources in one native call."""
        return call_native_registry_sink_from_path_sources_auto_registry(
            self._capsule,
            sink,
            sources,
            options,
            registry_json=registry_json,
            field_name_policy=field_name_policy,
            schema_mode=schema_mode,
            first_row_columns=first_row_columns,
            timestamp_columns=timestamp_columns,
            skip_invalid_json_sources=skip_invalid_json_sources,
        )

    def to_registry_sink_path_sources_auto_registry_state(
        self,
        sink: str,
        sources: Any,
        options: Any = None,
        *,
        native_registry_state: Any,
        field_name_policy: str,
        schema_mode: str,
        first_row_columns: dict[str, Any],
        timestamp_columns: tuple[str, ...],
        skip_invalid_json_sources: bool = False,
    ) -> SinkOutput:
        """Infer and stream path sources using an existing registry-state capsule."""
        return call_native_registry_sink_from_path_sources_auto_registry_state(
            self._capsule,
            sink,
            sources,
            options,
            native_registry_state=native_registry_state,
            field_name_policy=field_name_policy,
            schema_mode=schema_mode,
            first_row_columns=first_row_columns,
            timestamp_columns=timestamp_columns,
            skip_invalid_json_sources=skip_invalid_json_sources,
        )

    def to_registry_sink_arrow_sources(
        self,
        sink: str,
        sources: list[tuple[Any, str]],
        options: Any = None,
        *,
        registry_json: str,
        field_name_policy: str,
        schema_mode: str,
        first_row_columns: dict[str, Any],
        timestamp_columns: tuple[str, ...],
        native_registry_state: Any = None,
    ) -> SinkOutput:
        """Send multiple Arrow stream sources to a native registry-backed sink."""
        return call_native_registry_sink_arrow_sources(
            self._capsule,
            sink,
            sources,
            options,
            registry_json=registry_json,
            field_name_policy=field_name_policy,
            schema_mode=schema_mode,
            first_row_columns=first_row_columns,
            timestamp_columns=timestamp_columns,
            native_registry_state=native_registry_state,
        )

    def to_registry_sink_arrow_sources_auto_registry(
        self,
        sink: str,
        sources: list[tuple[Any, str]],
        options: Any = None,
        *,
        registry_json: str,
        field_name_policy: str,
        schema_mode: str,
        first_row_columns: dict[str, Any],
        timestamp_columns: tuple[str, ...],
        native_registry_state: Any = None,
    ) -> SinkOutput:
        """Infer and stream multiple Arrow sources in one native call."""
        return call_native_registry_sink_arrow_sources_auto_registry(
            self._capsule,
            sink,
            sources,
            options,
            registry_json=registry_json,
            field_name_policy=field_name_policy,
            schema_mode=schema_mode,
            first_row_columns=first_row_columns,
            timestamp_columns=timestamp_columns,
            native_registry_state=native_registry_state,
        )

    def to_registry_sink_arrow_source_chunk_provider(
        self,
        sink: str,
        provider: Any,
        options: Any = None,
        *,
        native_registry_state: Any,
        schema_mode: str,
        first_row_columns: dict[str, Any],
        timestamp_columns: tuple[str, ...],
    ) -> SinkOutput:
        """Send lazily provided Arrow-source chunks to a native registry sink."""
        return call_native_registry_sink_arrow_source_chunk_provider(
            self._capsule,
            sink,
            provider,
            options,
            native_registry_state=native_registry_state,
            schema_mode=schema_mode,
            first_row_columns=first_row_columns,
            timestamp_columns=timestamp_columns,
        )

    def to_registry_sink_arrow_source_chunk_provider_auto_registry(
        self,
        sink: str,
        probe_provider: Any,
        stream_provider: Any,
        options: Any = None,
        *,
        registry_json: str,
        field_name_policy: str,
        schema_mode: str,
        first_row_columns: dict[str, Any],
        timestamp_columns: tuple[str, ...],
        native_registry_state: Any = None,
    ) -> SinkOutput:
        """Infer and stream lazily provided Arrow-source chunks in one native call."""
        return call_native_registry_sink_arrow_source_chunk_provider_auto_registry(
            self._capsule,
            sink,
            probe_provider,
            stream_provider,
            options,
            registry_json=registry_json,
            field_name_policy=field_name_policy,
            schema_mode=schema_mode,
            first_row_columns=first_row_columns,
            timestamp_columns=timestamp_columns,
            native_registry_state=native_registry_state,
        )

    def registry_probe_arrow_sources(
        self,
        sources: list[tuple[Any, str]],
        options: Any = None,
        *,
        registry_json: str,
        field_name_policy: str,
        schema_mode: str,
        native_registry_state: Any = None,
    ) -> RegistryProbeResult:
        """Infer and merge registry state from multiple Arrow stream sources."""
        prepared = _options_capsule(options)
        if native_registry_state is not None:
            native_probe_state = getattr(
                _native,
                "context_registry_probe_from_arrow_sources_registry_state",
                None,
            )
            if native_probe_state is not None:
                return RegistryProbeResult.from_native(
                    native_probe_state(
                        self._capsule,
                        sources,
                        prepared,
                        native_registry_state,
                        field_name_policy,
                        schema_mode,
                    )
                )
        return RegistryProbeResult.from_native(
            _native.context_registry_probe_from_arrow_sources(
                self._capsule,
                sources,
                prepared,
                registry_json,
                field_name_policy,
                schema_mode,
            )
        )

    def schema_probe_from_source(
        self, frontend: str, source: str, payload: Any, options: Any = None
    ) -> SchemaProbeResult:
        """Infer schema from a source-selected input without materializing a sink."""
        prepared = _options_capsule(options)
        return SchemaProbeResult.from_native(
            _native.context_schema_probe_from_source(
                self._capsule,
                frontend,
                source,
                payload,
                prepared,
            )
        )

    def schema_probe_paths(
        self,
        frontend: str,
        paths: list[str],
        options: Any = None,
        *,
        separator: str = "\n",
    ) -> SchemaProbeResult:
        """Infer schema from multiple local files as one logical input."""
        prepared = _options_capsule(options)
        return SchemaProbeResult.from_native(
            _native.context_schema_probe_from_paths(
                self._capsule,
                frontend,
                paths,
                prepared,
                separator,
            )
        )

    def registry_probe_from_source(
        self,
        frontend: str,
        source: str,
        payload: Any,
        options: Any = None,
        *,
        registry_json: str,
        field_name_policy: str,
        schema_mode: str,
    ) -> RegistryProbeResult:
        """Infer and merge registry state without materializing a sink."""
        prepared = _options_capsule(options)
        return RegistryProbeResult.from_native(
            _native.context_registry_probe_from_source(
                self._capsule,
                frontend,
                source,
                payload,
                prepared,
                registry_json,
                field_name_policy,
                schema_mode,
            )
        )

    def registry_probe_paths(
        self,
        frontend: str,
        paths: list[str],
        options: Any = None,
        *,
        separator: str = "\n",
        registry_json: str,
        field_name_policy: str,
        schema_mode: str,
    ) -> RegistryProbeResult:
        """Infer and merge registry state from multiple local files."""
        prepared = _options_capsule(options)
        return RegistryProbeResult.from_native(
            _native.context_registry_probe_from_paths(
                self._capsule,
                frontend,
                paths,
                prepared,
                separator,
                registry_json,
                field_name_policy,
                schema_mode,
            )
        )

    def registry_probe_path_sources(
        self,
        sources: Any,
        options: Any = None,
        *,
        registry_json: str,
        field_name_policy: str,
        schema_mode: str,
        native_registry_state: Any = None,
    ) -> RegistryProbeResult:
        """Infer and merge registry state from native path-source inputs."""
        prepared = _options_capsule(options)
        if native_registry_state is not None:
            native_probe_state = getattr(
                _native,
                "context_registry_probe_from_path_sources_registry_state",
                None,
            )
            if native_probe_state is not None:
                return RegistryProbeResult.from_native(
                    native_probe_state(
                        self._capsule,
                        sources,
                        prepared,
                        native_registry_state,
                        field_name_policy,
                        schema_mode,
                    )
                )
        return RegistryProbeResult.from_native(
            _native.context_registry_probe_from_path_sources(
                self._capsule,
                sources,
                prepared,
                registry_json,
                field_name_policy,
                schema_mode,
            )
        )

    def registry_probe_path_sources_best_effort(
        self,
        sources: Any,
        options: Any = None,
        *,
        registry_json: str,
        field_name_policy: str,
        schema_mode: str,
        native_registry_state: Any = None,
    ) -> RegistryProbeResult:
        """Infer registry state from path sources, skipping JSON parse failures."""
        prepared = _options_capsule(options)
        using_native_state_probe = False
        native_probe = None
        if native_registry_state is not None:
            native_probe = getattr(
                _native,
                "context_registry_probe_from_path_sources_best_effort_registry_state",
                None,
            )
            using_native_state_probe = native_probe is not None
        if native_probe is None:
            native_probe = getattr(
                _native,
                "context_registry_probe_from_path_sources_best_effort",
                None,
            )
        if native_probe is None:
            current_registry = registry_json
            current_native_registry_state = native_registry_state
            last_raw: RegistryProbeResult | None = None
            skipped_errors: list[str] = []
            for source in sources:
                try:
                    last_raw = self.registry_probe_path_sources(
                        [source],
                        options,
                        registry_json=current_registry,
                        field_name_policy=field_name_policy,
                        schema_mode=schema_mode,
                        native_registry_state=current_native_registry_state,
                    )
                except Exception as exc:
                    if source[0] not in {"json", "json_array"} or (
                        not _registry_probe_path_source_error_is_skippable(exc)
                    ):
                        raise
                    skipped_errors.append(str(exc))
                    continue
                current_registry = last_raw.schema_registry_json
                current_native_registry_state = getattr(last_raw, "native_registry_state", None)
            if last_raw is None:
                detail = "; ".join(skipped_errors[-3:]) if skipped_errors else "no readable sources"
                raise ValueError(f"Schema warm-up found no valid JSON sources: {detail}")
            return last_raw
        if using_native_state_probe:
            return RegistryProbeResult.from_native(
                native_probe(
                    self._capsule,
                    sources,
                    prepared,
                    native_registry_state,
                    field_name_policy,
                    schema_mode,
                )
            )
        return RegistryProbeResult.from_native(
            native_probe(
                self._capsule,
                sources,
                prepared,
                registry_json,
                field_name_policy,
                schema_mode,
            )
        )

    def registry_probe_path_source_chunk_provider(
        self,
        provider: Any,
        options: Any = None,
        *,
        registry_json: str,
        field_name_policy: str,
        schema_mode: str,
        native_registry_state: Any = None,
        skip_invalid_json_sources: bool = True,
    ) -> RegistryProbeResult:
        """Infer registry state from lazily provided path-source chunks."""
        prepared = _options_capsule(options)
        if native_registry_state is not None:
            native_probe_state = getattr(
                _native,
                "context_registry_probe_from_path_source_chunk_provider_registry_state",
                None,
            )
            if native_probe_state is not None:
                return RegistryProbeResult.from_native(
                    native_probe_state(
                        self._capsule,
                        provider,
                        prepared,
                        native_registry_state,
                        field_name_policy,
                        schema_mode,
                        bool(skip_invalid_json_sources),
                    )
                )
        native_probe = getattr(
            _native,
            "context_registry_probe_from_path_source_chunk_provider",
            None,
        )
        if native_probe is None:
            raise AttributeError(
                "native runtime does not support registry path-source chunk providers"
            )
        return RegistryProbeResult.from_native(
            native_probe(
                self._capsule,
                provider,
                prepared,
                registry_json,
                field_name_policy,
                schema_mode,
                bool(skip_invalid_json_sources),
            )
        )

    def to_sink_python(self, sink: str, data: Any, options: Any = None) -> SinkOutput:
        """Serialize Python rows and send them to a native JSON sink."""
        # ABI3 cannot pass Python objects directly into the native layer. A
        # seekable JSONL reader keeps replay semantics without building one
        # giant text payload.
        return self.to_sink_reader(
            sink,
            "json",
            PythonRowsJsonlByteReader(data),
            options=options,
        )
