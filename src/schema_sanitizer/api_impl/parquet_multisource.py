"""Native Arrow multi-source handling for Parquet directory inputs."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from .file_conversion_metadata import SCHEMA_DRIFTS_COLUMN, SCHEMA_REGISTRY_COLUMN
from .parquet_arrow_sources import (
    ParquetArrowSource,
    ParquetArrowSourceChunkProvider,
    close_parquet_arrow_sources,
    parquet_arrow_sources_or_none,
)


@dataclass(frozen=True, slots=True)
class ParquetDirectorySourceFile:
    """One local Parquet child and the displayed source_file metadata value."""

    path: str
    source_file: str


@dataclass(frozen=True, slots=True)
class ParquetDirectorySourceManifest:
    """A Parquet directory whose children can be processed as native Arrow streams."""

    files: list[ParquetDirectorySourceFile]
    memory_limit_bytes: int | None = None


_LAST_PARQUET_MULTISOURCE_ROUTE = "none"


def last_parquet_multisource_route() -> str:
    """Return the route used by the most recent Parquet multi-source conversion."""
    return _LAST_PARQUET_MULTISOURCE_ROUTE


def parquet_multisource_manifest_from_data(data: Any) -> ParquetDirectorySourceManifest | None:
    """Return an attached native Parquet multi-source manifest, if present."""
    manifest = getattr(data, "native_parquet_multisource_manifest", None)
    return manifest if isinstance(manifest, ParquetDirectorySourceManifest) else None


def _arrow_source_inputs(manifest: ParquetDirectorySourceManifest) -> list[ParquetArrowSource]:
    """Return normalized Arrow source inputs for a Parquet directory manifest."""
    return [
        ParquetArrowSource(
            data=source.path,
            source="path",
            source_file=source.source_file,
        )
        for source in manifest.files
    ]


def _arrow_source_provider(
    manifest: ParquetDirectorySourceManifest,
    *,
    call_options: Any,
    feature: str,
) -> ParquetArrowSourceChunkProvider:
    """Return a lazy Arrow-source provider for a Parquet manifest."""
    return ParquetArrowSourceChunkProvider(
        _arrow_source_inputs(manifest),
        call_options=call_options,
        feature=feature,
    )


def _arrow_source_factories_or_none(
    manifest: ParquetDirectorySourceManifest,
    *,
    call_options: Any,
    feature: str,
) -> list[tuple[Any, str]] | None:
    """Return reusable Arrow stream factories for every child source."""
    return parquet_arrow_sources_or_none(
        _arrow_source_inputs(manifest),
        call_options=call_options,
        feature=feature,
    )


def parquet_multisource_registry_sink_raw_or_none(
    raw_context: Any,
    manifest: ParquetDirectorySourceManifest,
    call_options: Any,
    *,
    registry_json: str,
    field_name_policy: str,
    schema_mode: str,
    first_row_columns: dict[str, Any],
    timestamp_columns: tuple[str, ...],
    native_registry_state: Any = None,
) -> Any | None:
    """Return a lazy native multi-Arrow-source registry stream when supported."""
    global _LAST_PARQUET_MULTISOURCE_ROUTE
    auto_provider_call = getattr(
        raw_context,
        "to_registry_sink_arrow_source_chunk_provider_auto_registry",
        None,
    )
    supports_auto_provider = getattr(
        raw_context,
        "supports_arrow_source_chunk_provider_auto_registry",
        lambda: auto_provider_call is not None,
    )
    if auto_provider_call is not None and supports_auto_provider():
        probe_provider = _arrow_source_provider(
            manifest,
            call_options=call_options,
            feature="parquet directory native Arrow source auto-registry probe provider",
        )
        stream_provider = _arrow_source_provider(
            manifest,
            call_options=call_options,
            feature="parquet directory native Arrow source auto-registry stream provider",
        )
        try:
            raw = auto_provider_call(
                "stream",
                probe_provider,
                stream_provider,
                call_options,
                registry_json=registry_json,
                field_name_policy=field_name_policy,
                schema_mode=schema_mode,
                first_row_columns=first_row_columns,
                timestamp_columns=timestamp_columns,
                native_registry_state=native_registry_state,
            )
            _LAST_PARQUET_MULTISOURCE_ROUTE = "native_arrow_source_chunk_provider_auto_registry"
            return raw
        except AttributeError:
            _LAST_PARQUET_MULTISOURCE_ROUTE = "unsupported"
            probe_provider.close()
            stream_provider.close()
            return None
        except Exception:
            probe_provider.close()
            stream_provider.close()
            raise

    provider_call = getattr(
        raw_context,
        "to_registry_sink_arrow_source_chunk_provider",
        None,
    )
    supports_provider = getattr(
        raw_context,
        "supports_arrow_source_chunk_provider",
        lambda: provider_call is not None,
    )
    if provider_call is None or not supports_provider():
        native_call = getattr(raw_context, "to_registry_sink_arrow_sources_auto_registry", None)
        if native_call is None:
            native_call = getattr(raw_context, "to_registry_sink_arrow_sources", None)
            if native_call is None:
                _LAST_PARQUET_MULTISOURCE_ROUTE = "unsupported"
                return None
            _LAST_PARQUET_MULTISOURCE_ROUTE = "native_arrow_sources"
        else:
            _LAST_PARQUET_MULTISOURCE_ROUTE = "native_arrow_sources_auto_registry"
        sources = _arrow_source_factories_or_none(
            manifest,
            call_options=call_options,
            feature="parquet directory native Arrow sources",
        )
        if sources is None:
            _LAST_PARQUET_MULTISOURCE_ROUTE = "unsupported_schema"
            return None
        try:
            return native_call(
                "stream",
                sources,
                call_options,
                registry_json=registry_json,
                field_name_policy=field_name_policy,
                schema_mode=schema_mode,
                first_row_columns=first_row_columns,
                timestamp_columns=timestamp_columns,
                native_registry_state=native_registry_state,
            )
        except AttributeError:
            _LAST_PARQUET_MULTISOURCE_ROUTE = "unsupported"
            close_parquet_arrow_sources(sources)
            return None
        except Exception:
            close_parquet_arrow_sources(sources)
            raise

    probe = infer_parquet_multisource_registry(
        raw_context,
        manifest,
        call_options=call_options,
        registry_json=registry_json,
        field_name_policy=field_name_policy,
        schema_mode=schema_mode,
        native_registry_state=native_registry_state,
    )
    provider_state = getattr(probe, "native_registry_state", None)
    if provider_state is None:
        _LAST_PARQUET_MULTISOURCE_ROUTE = "unsupported_provider_state"
        return None
    merged_first_row_columns = dict(first_row_columns or {})
    merged_first_row_columns.update(
        {
            SCHEMA_REGISTRY_COLUMN: probe.schema_registry_json,
            SCHEMA_DRIFTS_COLUMN: probe.schema_drifts_json,
        }
    )
    provider = _arrow_source_provider(
        manifest,
        call_options=call_options,
        feature="parquet directory native Arrow source provider",
    )
    try:
        raw = provider_call(
            "stream",
            provider,
            call_options,
            native_registry_state=provider_state,
            schema_mode=schema_mode,
            first_row_columns=merged_first_row_columns,
            timestamp_columns=timestamp_columns,
        )
        _LAST_PARQUET_MULTISOURCE_ROUTE = "native_arrow_source_chunk_provider"
        return raw
    except Exception:
        provider.close()
        raise


def infer_parquet_multisource_registry(
    raw_context: Any,
    manifest: ParquetDirectorySourceManifest,
    call_options: Any,
    *,
    registry_json: str,
    field_name_policy: str,
    schema_mode: str,
    native_registry_state: Any = None,
) -> Any | None:
    """Infer and merge one registry across Parquet children through lazy Arrow chunks."""
    if getattr(raw_context, "registry_probe_arrow_sources", None) is None:
        return None
    provider = _arrow_source_provider(
        manifest,
        call_options=call_options,
        feature="parquet directory registry probe",
    )
    current_registry = registry_json
    current_native_registry_state = native_registry_state
    last_raw: Any | None = None
    try:
        while True:
            sources = provider.next_sources()
            if sources is None:
                break
            last_raw = raw_context.registry_probe_arrow_sources(
                sources,
                call_options,
                registry_json=current_registry,
                field_name_policy=field_name_policy,
                schema_mode=schema_mode,
                native_registry_state=current_native_registry_state,
            )
            current_registry = last_raw.schema_registry_json
            current_native_registry_state = getattr(last_raw, "native_registry_state", None)
        return last_raw
    finally:
        provider.close()
