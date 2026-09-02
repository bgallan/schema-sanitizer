"""Registry-backed sink methods for Arrow and path sources.

The three private mixins attach native Arrow-stream, path-provider, and path-source routes to the
execution context without widening its public surface.
"""

from __future__ import annotations

from typing import Any

from .generated_metadata import TimestampColumns
from .native_options import _options_capsule
from .native_results import SinkOutput, _registry_sink_output
from .native_runtime import native_core as _native


class _RegistryArrowSinkMethods:
    """Mixin containing direct Arrow-source registry sink routes."""

    _capsule: Any

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
        return _registry_sink_output(
            sink,
            _native.context_to_registry_sink_arrow_stream(
                self._capsule,
                sink,
                frontend,
                stream,
                _options_capsule(options),
                registry_json,
                field_name_policy,
                schema_mode,
            ),
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
        timestamp_columns: TimestampColumns,
        native_registry_state: Any = None,
    ) -> SinkOutput:
        """Send multiple Arrow stream sources to a native registry-backed sink."""
        prepared = _options_capsule(options)
        if native_registry_state is not None:
            raw = _native.context_to_registry_sink_arrow_sources_registry_state(
                self._capsule,
                sink,
                sources,
                prepared,
                native_registry_state,
                schema_mode,
                first_row_columns,
                timestamp_columns,
            )
        else:
            raw = _native.context_to_registry_sink_arrow_sources(
                self._capsule,
                sink,
                sources,
                prepared,
                registry_json,
                field_name_policy,
                schema_mode,
                first_row_columns,
                timestamp_columns,
            )
        return _registry_sink_output(sink, raw)

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
        timestamp_columns: TimestampColumns,
        native_registry_state: Any = None,
    ) -> SinkOutput:
        """Infer and stream multiple Arrow sources in one native call."""
        prepared = _options_capsule(options)
        if native_registry_state is not None:
            raw = _native.context_to_registry_sink_arrow_sources_auto_registry_state(
                self._capsule,
                sink,
                sources,
                prepared,
                native_registry_state,
                field_name_policy,
                schema_mode,
                first_row_columns,
                timestamp_columns,
            )
        else:
            raw = _native.context_to_registry_sink_arrow_sources_auto_registry(
                self._capsule,
                sink,
                sources,
                prepared,
                registry_json,
                field_name_policy,
                schema_mode,
                first_row_columns,
                timestamp_columns,
            )
        return _registry_sink_output(sink, raw)

    def to_registry_sink_arrow_source_chunk_provider(
        self,
        sink: str,
        provider: Any,
        options: Any = None,
        *,
        native_registry_state: Any,
        schema_mode: str,
        first_row_columns: dict[str, Any],
        timestamp_columns: TimestampColumns,
    ) -> SinkOutput:
        """Send lazily provided Arrow-source chunks to a native registry sink."""
        return _registry_sink_output(
            sink,
            _native.context_to_registry_sink_arrow_source_chunk_provider_registry_state(
                self._capsule,
                sink,
                provider,
                _options_capsule(options),
                native_registry_state,
                schema_mode,
                first_row_columns,
                timestamp_columns,
            ),
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
        timestamp_columns: TimestampColumns,
        native_registry_state: Any = None,
    ) -> SinkOutput:
        """Infer and stream lazily provided Arrow-source chunks in one native call."""
        common = (
            self._capsule,
            sink,
            probe_provider,
            stream_provider,
            _options_capsule(options),
        )
        if native_registry_state is not None:
            raw = _native.context_to_registry_sink_arrow_source_chunk_provider_auto_registry_state(
                *common,
                native_registry_state,
                field_name_policy,
                schema_mode,
                first_row_columns,
                timestamp_columns,
            )
        else:
            raw = _native.context_to_registry_sink_arrow_source_chunk_provider_auto_registry(
                *common,
                registry_json,
                field_name_policy,
                schema_mode,
                first_row_columns,
                timestamp_columns,
            )
        return _registry_sink_output(sink, raw)


class _RegistryPathProviderSinkMethods:
    """Mixin containing direct registry sink routes for lazy path providers."""

    _capsule: Any

    def to_registry_sink_path_source_chunk_provider(
        self,
        sink: str,
        provider: Any,
        options: Any = None,
        *,
        native_registry_state: Any,
        schema_mode: str,
        first_row_columns: dict[str, Any],
        timestamp_columns: TimestampColumns,
    ) -> SinkOutput:
        """Send lazily provided path-source chunks to a native registry sink."""
        return _registry_sink_output(
            sink,
            _native.context_to_registry_sink_from_path_source_chunk_provider_registry_state(
                self._capsule,
                sink,
                provider,
                _options_capsule(options),
                native_registry_state,
                schema_mode,
                first_row_columns,
                timestamp_columns,
            ),
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
        timestamp_columns: TimestampColumns,
        native_registry_state: Any = None,
        skip_invalid_json_sources: bool = False,
    ) -> SinkOutput:
        """Infer and stream path-source chunks using paired lazy providers."""
        common = (
            self._capsule,
            sink,
            probe_provider,
            stream_provider,
            _options_capsule(options),
        )
        if native_registry_state is not None:
            native_call = (
                _native.context_to_registry_sink_from_path_source_chunk_provider_auto_registry_state
            )
            args: tuple[Any, ...] = (
                *common,
                native_registry_state,
                field_name_policy,
                schema_mode,
                first_row_columns,
                timestamp_columns,
            )
        else:
            native_call = (
                _native.context_to_registry_sink_from_path_source_chunk_provider_auto_registry
            )
            args = (
                *common,
                registry_json,
                field_name_policy,
                schema_mode,
                first_row_columns,
                timestamp_columns,
            )
        if skip_invalid_json_sources:
            args = (*args, True)
        return _registry_sink_output(sink, native_call(*args))


class _RegistryPathSourceSinkMethods:
    """Mixin containing direct registry sink routes for path collections."""

    _capsule: Any

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
        timestamp_columns: TimestampColumns,
        native_registry_state: Any = None,
    ) -> SinkOutput:
        """Send multiple local path sources to a native registry-backed sink."""
        prepared = _options_capsule(options)
        if native_registry_state is not None:
            raw = _native.context_to_registry_sink_from_path_sources_registry_state(
                self._capsule,
                sink,
                sources,
                prepared,
                native_registry_state,
                schema_mode,
                first_row_columns,
                timestamp_columns,
            )
        else:
            raw = _native.context_to_registry_sink_from_path_sources(
                self._capsule,
                sink,
                sources,
                prepared,
                registry_json,
                drifts_json,
                conversion_timestamp,
                field_name_policy,
                schema_mode,
                first_row_columns,
                timestamp_columns,
            )
        return _registry_sink_output(sink, raw)

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
        timestamp_columns: TimestampColumns,
        skip_invalid_json_sources: bool = False,
    ) -> SinkOutput:
        """Infer and stream multiple local path sources in one native call."""
        args: tuple[Any, ...] = (
            self._capsule,
            sink,
            sources,
            _options_capsule(options),
            registry_json,
            field_name_policy,
            schema_mode,
            first_row_columns,
            timestamp_columns,
        )
        if skip_invalid_json_sources:
            args = (*args, True)
        return _registry_sink_output(
            sink,
            _native.context_to_registry_sink_from_path_sources_auto_registry(*args),
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
        timestamp_columns: TimestampColumns,
        skip_invalid_json_sources: bool = False,
    ) -> SinkOutput:
        """Infer and stream path sources using an existing registry-state capsule."""
        args: tuple[Any, ...] = (
            self._capsule,
            sink,
            sources,
            _options_capsule(options),
            native_registry_state,
            field_name_policy,
            schema_mode,
            first_row_columns,
            timestamp_columns,
        )
        if skip_invalid_json_sources:
            args = (*args, True)
        return _registry_sink_output(
            sink,
            _native.context_to_registry_sink_from_path_sources_auto_registry_state(*args),
        )
