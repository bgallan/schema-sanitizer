"""Native lazy multi-source execution for Parquet directory inputs.

It turns directory manifests into lazily opened per-file Arrow sources while preserving
registry state, source spans, and close ownership.
"""

from __future__ import annotations

from dataclasses import dataclass
from types import SimpleNamespace
from typing import Any

from ...core_impl.generated_metadata import TimestampColumns
from ...core_impl.resource_lifecycle import _close_suppressing_errors
from .arrow_sources import (
    ParquetArrowSource,
    ParquetArrowSourceChunkProvider,
    parquet_arrow_sources_or_none,
)


@dataclass(frozen=True, slots=True)
class ParquetDirectorySourceFile:
    """One local Parquet child and its displayed source-file metadata value."""

    path: str
    source_file: str


@dataclass(frozen=True, slots=True)
class ParquetDirectorySourceManifest:
    """A Parquet directory whose children can be processed as Arrow streams."""

    files: list[ParquetDirectorySourceFile]
    memory_limit_bytes: int | None = None


def parquet_multisource_manifest_from_data(data: Any) -> ParquetDirectorySourceManifest | None:
    """Return an attached native Parquet multi-source manifest, if present."""
    manifest = getattr(data, "native_parquet_multisource_manifest", None)
    return manifest if isinstance(manifest, ParquetDirectorySourceManifest) else None


def parquet_arrow_source_inputs(
    manifest: ParquetDirectorySourceManifest,
) -> list[ParquetArrowSource]:
    """Return normalized Arrow inputs for a Parquet directory manifest."""
    return [
        ParquetArrowSource(source.path, "path", source.source_file) for source in manifest.files
    ]


def parquet_arrow_source_provider(
    manifest: ParquetDirectorySourceManifest,
    *,
    call_options: Any,
    feature: str,
) -> ParquetArrowSourceChunkProvider:
    """Return a lazy Arrow-source provider for a Parquet manifest."""
    return ParquetArrowSourceChunkProvider(
        parquet_arrow_source_inputs(manifest),
        call_options=call_options,
        feature=feature,
    )


def parquet_arrow_source_factories_or_none(
    manifest: ParquetDirectorySourceManifest,
    *,
    call_options: Any,
    feature: str,
) -> list[tuple[Any, str]] | None:
    """Return reusable Arrow stream factories for every child source."""
    return parquet_arrow_sources_or_none(
        parquet_arrow_source_inputs(manifest),
        call_options=call_options,
        feature=feature,
    )


def _auto_registry_provider_sink(
    raw_context: Any,
    manifest: ParquetDirectorySourceManifest,
    call_options: Any,
    **sink_options: Any,
) -> Any:
    """Open the native auto-registry chunk-provider route."""
    probe_provider = parquet_arrow_source_provider(
        manifest,
        call_options=call_options,
        feature="parquet directory native Arrow source auto-registry probe provider",
    )
    stream_provider = parquet_arrow_source_provider(
        manifest,
        call_options=call_options,
        feature="parquet directory native Arrow source auto-registry stream provider",
    )
    try:
        raw = raw_context.to_registry_sink_arrow_source_chunk_provider_auto_registry(
            "stream",
            probe_provider,
            stream_provider,
            call_options,
            **sink_options,
        )
        return raw
    except Exception:
        probe_provider.close()
        stream_provider.close()
        raise


def parquet_multisource_registry_sink_raw_or_none(
    raw_context: Any,
    manifest: ParquetDirectorySourceManifest,
    call_options: Any,
    *,
    registry_json: str,
    field_name_policy: str,
    schema_mode: str,
    first_row_columns: dict[str, Any],
    timestamp_columns: TimestampColumns,
    native_registry_state: Any = None,
) -> Any:
    """Return the lazy native multi-Arrow-source registry stream."""
    return _auto_registry_provider_sink(
        raw_context,
        manifest,
        call_options,
        registry_json=registry_json,
        field_name_policy=field_name_policy,
        schema_mode=schema_mode,
        first_row_columns=first_row_columns,
        timestamp_columns=timestamp_columns,
        native_registry_state=native_registry_state,
    )


def infer_parquet_multisource_registry(
    raw_context: Any,
    manifest: ParquetDirectorySourceManifest,
    call_options: Any,
    *,
    registry_json: str,
    field_name_policy: str,
    schema_mode: str,
    native_registry_state: Any = None,
) -> Any:
    """Infer one registry in C++ through a lazy Arrow-source provider."""
    probe_provider = parquet_arrow_source_provider(
        manifest,
        call_options=call_options,
        feature="parquet directory registry probe provider",
    )
    stream_provider = parquet_arrow_source_provider(
        manifest,
        call_options=call_options,
        feature="parquet directory registry probe stream placeholder",
    )
    try:
        raw = raw_context.to_registry_sink_arrow_source_chunk_provider_auto_registry(
            "stream",
            probe_provider,
            stream_provider,
            call_options,
            registry_json=registry_json,
            field_name_policy=field_name_policy,
            schema_mode=schema_mode,
            first_row_columns={},
            timestamp_columns=(),
            native_registry_state=native_registry_state,
        )
    except Exception:
        probe_provider.close()
        stream_provider.close()
        raise
    try:
        return SimpleNamespace(
            schema_registry_json=raw.schema_registry_json,
            schema_drifts_json=raw.schema_drifts_json,
            conversion_timestamp=raw.conversion_timestamp,
            field_names=(),
            native_registry_state=raw.native_registry_state,
            diagnostics=raw.diagnostics,
        )
    finally:
        _close_suppressing_errors(raw)
