"""Registry-backed native runtime helpers."""

from __future__ import annotations

from typing import Any

from .native import _native
from .options_bytes import _options_capsule
from .runtime_support import SinkOutput


def registry_sink_output(
    sink: str,
    native_result: tuple[Any, ...],
) -> SinkOutput:
    """Wrap a registry-backed native sink result."""
    main, diagnostics, registry_json, drifts_json, conversion_timestamp, *extra = native_result
    return SinkOutput(
        sink=sink,
        main_stream_capsule=main,
        diagnostics_capsule=diagnostics,
        schema_registry_json=str(registry_json),
        schema_drifts_json=str(drifts_json),
        conversion_timestamp=str(conversion_timestamp),
        native_registry_state=extra[0] if extra else None,
    )


def call_native_registry_sink_from_source(
    capsule: Any,
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
    prepared = _options_capsule(options)
    args = [
        capsule,
        sink,
        frontend,
        source,
        payload,
        prepared,
        registry_json,
        field_name_policy,
        schema_mode,
    ]
    if (
        first_row_columns is not None
        or all_row_columns is not None
        or row_span_columns is not None
        or bool(timestamp_columns)
    ):
        args.extend(
            [
                first_row_columns or {},
                all_row_columns or {},
                row_span_columns or {},
                timestamp_columns,
            ]
        )
    return registry_sink_output(
        sink,
        _native.context_to_registry_sink_from_source(*args),
    )


def call_native_registry_sink_arrow_stream(
    capsule: Any,
    sink: str,
    frontend: str,
    stream: Any,
    options: Any,
    *,
    registry_json: str,
    field_name_policy: str,
    schema_mode: str,
) -> SinkOutput:
    """Prepare options and invoke the Arrow-stream native registry sink."""
    prepared = _options_capsule(options)
    return registry_sink_output(
        sink,
        _native.context_to_registry_sink_arrow_stream(
            capsule,
            sink,
            frontend,
            stream,
            prepared,
            registry_json,
            field_name_policy,
            schema_mode,
        ),
    )


def call_native_registry_sink_from_path_sources(
    capsule: Any,
    sink: str,
    sources: Any,
    options: Any,
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
    """Prepare options and invoke the native multi-path registry sink."""
    prepared = _options_capsule(options)
    if native_registry_state is not None:
        native_call = getattr(
            _native,
            "context_to_registry_sink_from_path_sources_registry_state",
            None,
        )
        if native_call is not None:
            return registry_sink_output(
                sink,
                native_call(
                    capsule,
                    sink,
                    sources,
                    prepared,
                    native_registry_state,
                    schema_mode,
                    first_row_columns,
                    timestamp_columns,
                ),
            )
    return registry_sink_output(
        sink,
        _native.context_to_registry_sink_from_path_sources(
            capsule,
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
        ),
    )


def call_native_registry_sink_from_path_source_chunk_provider(
    capsule: Any,
    sink: str,
    provider: Any,
    options: Any,
    *,
    native_registry_state: Any,
    schema_mode: str,
    first_row_columns: dict[str, Any],
    timestamp_columns: tuple[str, ...],
) -> SinkOutput:
    """Invoke the native registry sink that pulls path-source chunks lazily."""
    native_call = getattr(
        _native,
        "context_to_registry_sink_from_path_source_chunk_provider_registry_state",
        None,
    )
    if native_call is None:
        raise AttributeError("native runtime does not support path-source chunk providers")
    prepared = _options_capsule(options)
    return registry_sink_output(
        sink,
        native_call(
            capsule,
            sink,
            provider,
            prepared,
            native_registry_state,
            schema_mode,
            first_row_columns,
            timestamp_columns,
        ),
    )


def call_native_registry_sink_from_path_source_chunk_provider_auto_registry(
    capsule: Any,
    sink: str,
    probe_provider: Any,
    stream_provider: Any,
    options: Any,
    *,
    registry_json: str,
    field_name_policy: str,
    schema_mode: str,
    first_row_columns: dict[str, Any],
    timestamp_columns: tuple[str, ...],
    native_registry_state: Any = None,
    skip_invalid_json_sources: bool = False,
) -> SinkOutput:
    """Invoke native provider auto-registry with separate probe and stream providers."""
    prepared = _options_capsule(options)
    if native_registry_state is not None:
        native_call = getattr(
            _native,
            "context_to_registry_sink_from_path_source_chunk_provider_auto_registry_state",
            None,
        )
        if native_call is not None:
            args = (
                capsule,
                sink,
                probe_provider,
                stream_provider,
                prepared,
                native_registry_state,
                field_name_policy,
                schema_mode,
                first_row_columns,
                timestamp_columns,
            )
            if skip_invalid_json_sources:
                args = (*args, True)
            return registry_sink_output(sink, native_call(*args))

    native_call = getattr(
        _native,
        "context_to_registry_sink_from_path_source_chunk_provider_auto_registry",
        None,
    )
    if native_call is None:
        raise AttributeError("native runtime does not support provider auto registry")
    args = (
        capsule,
        sink,
        probe_provider,
        stream_provider,
        prepared,
        registry_json,
        field_name_policy,
        schema_mode,
        first_row_columns,
        timestamp_columns,
    )
    if skip_invalid_json_sources:
        args = (*args, True)
    return registry_sink_output(sink, native_call(*args))


def call_native_registry_sink_from_path_sources_auto_registry(
    capsule: Any,
    sink: str,
    sources: Any,
    options: Any,
    *,
    registry_json: str,
    field_name_policy: str,
    schema_mode: str,
    first_row_columns: dict[str, Any],
    timestamp_columns: tuple[str, ...],
    skip_invalid_json_sources: bool = False,
) -> SinkOutput:
    """Prepare options and invoke the native self-bootstrapping multi-path sink."""
    prepared = _options_capsule(options)
    args = (
        capsule,
        sink,
        sources,
        prepared,
        registry_json,
        field_name_policy,
        schema_mode,
        first_row_columns,
        timestamp_columns,
    )
    if skip_invalid_json_sources:
        args = (*args, True)
    return registry_sink_output(
        sink,
        _native.context_to_registry_sink_from_path_sources_auto_registry(*args),
    )


def call_native_registry_sink_from_path_sources_auto_registry_state(
    capsule: Any,
    sink: str,
    sources: Any,
    options: Any,
    *,
    native_registry_state: Any,
    field_name_policy: str,
    schema_mode: str,
    first_row_columns: dict[str, Any],
    timestamp_columns: tuple[str, ...],
    skip_invalid_json_sources: bool = False,
) -> SinkOutput:
    """Prepare options and invoke native path-source auto-registry from state."""
    native_call = getattr(
        _native,
        "context_to_registry_sink_from_path_sources_auto_registry_state",
        None,
    )
    if native_call is None:
        raise AttributeError("native runtime does not support path-source auto registry state")
    prepared = _options_capsule(options)
    args = (
        capsule,
        sink,
        sources,
        prepared,
        native_registry_state,
        field_name_policy,
        schema_mode,
        first_row_columns,
        timestamp_columns,
    )
    if skip_invalid_json_sources:
        args = (*args, True)
    return registry_sink_output(sink, native_call(*args))


def call_native_registry_sink_arrow_sources(
    capsule: Any,
    sink: str,
    sources: list[tuple[Any, str]],
    options: Any,
    *,
    registry_json: str,
    field_name_policy: str,
    schema_mode: str,
    first_row_columns: dict[str, Any],
    timestamp_columns: tuple[str, ...],
    native_registry_state: Any = None,
) -> SinkOutput:
    """Prepare options and invoke the native multi-Arrow-source registry sink."""
    prepared = _options_capsule(options)
    if native_registry_state is not None:
        native_call = getattr(
            _native,
            "context_to_registry_sink_arrow_sources_registry_state",
            None,
        )
        if native_call is not None:
            return registry_sink_output(
                sink,
                native_call(
                    capsule,
                    sink,
                    sources,
                    prepared,
                    native_registry_state,
                    schema_mode,
                    first_row_columns,
                    timestamp_columns,
                ),
            )
    return registry_sink_output(
        sink,
        _native.context_to_registry_sink_arrow_sources(
            capsule,
            sink,
            sources,
            prepared,
            registry_json,
            field_name_policy,
            schema_mode,
            first_row_columns,
            timestamp_columns,
        ),
    )


def call_native_registry_sink_arrow_sources_auto_registry(
    capsule: Any,
    sink: str,
    sources: list[tuple[Any, str]],
    options: Any,
    *,
    registry_json: str,
    field_name_policy: str,
    schema_mode: str,
    first_row_columns: dict[str, Any],
    timestamp_columns: tuple[str, ...],
    native_registry_state: Any = None,
) -> SinkOutput:
    """Prepare options and invoke the native self-bootstrapping multi-Arrow sink."""
    prepared = _options_capsule(options)
    if native_registry_state is not None:
        native_call = getattr(
            _native,
            "context_to_registry_sink_arrow_sources_auto_registry_state",
            None,
        )
        if native_call is not None:
            return registry_sink_output(
                sink,
                native_call(
                    capsule,
                    sink,
                    sources,
                    prepared,
                    native_registry_state,
                    field_name_policy,
                    schema_mode,
                    first_row_columns,
                    timestamp_columns,
                ),
            )
    native_call = getattr(_native, "context_to_registry_sink_arrow_sources_auto_registry", None)
    if native_call is None:
        return call_native_registry_sink_arrow_sources(
            capsule,
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
    return registry_sink_output(
        sink,
        native_call(
            capsule,
            sink,
            sources,
            prepared,
            registry_json,
            field_name_policy,
            schema_mode,
            first_row_columns,
            timestamp_columns,
        ),
    )


def call_native_registry_sink_arrow_source_chunk_provider(
    capsule: Any,
    sink: str,
    provider: Any,
    options: Any,
    *,
    native_registry_state: Any,
    schema_mode: str,
    first_row_columns: dict[str, Any],
    timestamp_columns: tuple[str, ...],
) -> SinkOutput:
    """Prepare options and invoke the native Arrow-source chunk-provider sink."""
    native_call = getattr(
        _native,
        "context_to_registry_sink_arrow_source_chunk_provider_registry_state",
        None,
    )
    if native_call is None:
        raise AttributeError("native runtime does not support Arrow-source chunk providers")
    prepared = _options_capsule(options)
    return registry_sink_output(
        sink,
        native_call(
            capsule,
            sink,
            provider,
            prepared,
            native_registry_state,
            schema_mode,
            first_row_columns,
            timestamp_columns,
        ),
    )


def call_native_registry_sink_arrow_source_chunk_provider_auto_registry(
    capsule: Any,
    sink: str,
    probe_provider: Any,
    stream_provider: Any,
    options: Any,
    *,
    registry_json: str,
    field_name_policy: str,
    schema_mode: str,
    first_row_columns: dict[str, Any],
    timestamp_columns: tuple[str, ...],
    native_registry_state: Any = None,
) -> SinkOutput:
    """Infer one registry from Arrow-source chunks, then stream from another provider."""
    prepared = _options_capsule(options)
    if native_registry_state is not None:
        native_call = getattr(
            _native,
            "context_to_registry_sink_arrow_source_chunk_provider_auto_registry_state",
            None,
        )
        if native_call is not None:
            return registry_sink_output(
                sink,
                native_call(
                    capsule,
                    sink,
                    probe_provider,
                    stream_provider,
                    prepared,
                    native_registry_state,
                    field_name_policy,
                    schema_mode,
                    first_row_columns,
                    timestamp_columns,
                ),
            )
    native_call = getattr(
        _native,
        "context_to_registry_sink_arrow_source_chunk_provider_auto_registry",
        None,
    )
    if native_call is None:
        raise AttributeError("native runtime does not support Arrow-source auto providers")
    return registry_sink_output(
        sink,
        native_call(
            capsule,
            sink,
            probe_provider,
            stream_provider,
            prepared,
            registry_json,
            field_name_policy,
            schema_mode,
            first_row_columns,
            timestamp_columns,
        ),
    )
