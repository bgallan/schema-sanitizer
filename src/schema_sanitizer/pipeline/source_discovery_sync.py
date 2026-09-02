"""Strict blocking source discovery used by threading_mode='single'.

It performs provider listing and existence checks inline on the caller thread while
preserving the same ordering and diagnostic contract as multi mode.
"""

from __future__ import annotations

from collections import defaultdict
from time import perf_counter

from ..core_impl.memory_budget import normalize_memory_limit
from ..core_impl.uris import LocationKind, RemoteProvider, normalize_extensions
from ..input_impl.directory_inputs import (
    DiscoveredDirectoryInput,
    directory_metadata_budget_scope,
)
from ..remote_impl import sync_backend
from .source_discovery import (
    _LOCAL_LOCATION_KINDS,
    _local_directories_containing_files,
    _local_directory_matching_files,
    _local_path,
    _partition_plans,
    _record_discovered_inputs,
    _source_extensions,
)
from .source_discovery_memory import precharge_source_locations, source_summary
from .types import PartitionRunPlan, SourcePlanDiscovery


def _discover_directories_sync(
    source_locations: dict[str, LocationKind],
    *,
    extensions: tuple[str, ...],
    input_format: str,
    exists_by_uri: dict[str, bool],
    discovered_by_uri: dict[str, DiscoveredDirectoryInput],
    discovery_seconds_by_uri: dict[str, float],
    memory_limit_bytes: int | None,
) -> set[str]:
    """Discover grouped remote and local directories serially."""
    grouped: dict[LocationKind, list[str]] = defaultdict(list)
    for uri, kind in source_locations.items():
        grouped[kind].append(uri)

    checked: set[str] = set()
    for provider in ("gcs", "s3", "azure"):
        typed_provider: RemoteProvider = provider
        uris = grouped.get(typed_provider)
        if not uris:
            continue
        started_at = perf_counter()
        result = sync_backend.directories_containing_files(
            typed_provider,
            uris,
            extensions,
            memory_limit_bytes=memory_limit_bytes,
        )
        elapsed = max(perf_counter() - started_at, 0.0)
        discovery_seconds_by_uri.update(dict.fromkeys(uris, elapsed))
        exists_by_uri.update(result.exists_by_uri)
        _record_discovered_inputs(
            discovered_by_uri,
            input_format=input_format,
            remote_files_by_uri=result.files_by_uri,
        )
        checked.update(uris)

    local_locations: dict[str, LocationKind] = {
        uri: kind for kind in _LOCAL_LOCATION_KINDS for uri in grouped.get(kind, ())
    }
    if local_locations:
        started_at = perf_counter()
        local_result = _local_directories_containing_files(local_locations, extensions)
        elapsed = max(perf_counter() - started_at, 0.0)
        discovery_seconds_by_uri.update(dict.fromkeys(local_locations, elapsed))
        exists_by_uri.update(local_result.exists_by_uri)
        _record_discovered_inputs(
            discovered_by_uri,
            input_format=input_format,
            local_files_by_uri=local_result.files_by_uri,
        )
        checked.update(local_locations)
    return checked


def _discover_source_sync(
    uri: str,
    *,
    kind: LocationKind,
    input_mode: str,
    input_format: str,
    extensions: tuple[str, ...],
    memory_limit_bytes: int | None,
) -> tuple[bool, DiscoveredDirectoryInput | None, int | None, int | None]:
    """Discover one source with no event loop or async provider SDK."""
    if kind not in {"path", "file"}:
        if input_mode == "directory":
            remote_files = sync_backend.list_remote_directory(
                uri,
                extensions,
                memory_limit_bytes=memory_limit_bytes,
            )
            discovered = (
                DiscoveredDirectoryInput(
                    input_format=input_format,
                    remote_files=tuple(remote_files),
                )
                if remote_files
                else None
            )
            return bool(remote_files), discovered, *source_summary(discovered)
        remote_file = sync_backend.remote_file_metadata(
            uri,
            memory_limit_bytes=memory_limit_bytes,
        )
        if remote_file is None:
            return False, None, None, None
        return True, None, 1, remote_file.size

    if input_mode == "directory":
        local_files = _local_directory_matching_files(
            uri,
            normalize_extensions(extensions),
            kind=kind,
            memory_limit_bytes=memory_limit_bytes,
        )
        discovered = (
            DiscoveredDirectoryInput(
                input_format=input_format,
                local_files=tuple(local_files),
            )
            if local_files
            else None
        )
        return bool(local_files), discovered, *source_summary(discovered)
    path = _local_path(uri, kind)
    if not path.is_file():
        return False, None, None, None
    try:
        size = path.stat().st_size
    except OSError:
        size = None
    return True, None, 1, size


def _discover_existing_source_plans_sync_impl(
    plans: list[PartitionRunPlan],
    *,
    input_mode: str = "single_file",
    input_format: str = "json_array",
    source_file_extension: str | None = None,
    memory_limit_bytes: int | None = None,
) -> SourcePlanDiscovery:
    """Return existing plans using only serial blocking discovery."""
    if not plans:
        return SourcePlanDiscovery(existing_plans=[], skipped_plans=[])
    if input_mode not in {"single_file", "directory"}:
        raise ValueError("input_mode must be 'single_file' or 'directory'")

    extensions = _source_extensions(input_format, source_file_extension)
    metadata_budget, source_locations, metadata_owner = precharge_source_locations(
        plans, memory_limit_bytes=memory_limit_bytes
    )
    exists_by_uri: dict[str, bool] = {}
    discovered_by_uri: dict[str, DiscoveredDirectoryInput] = {}
    discovery_seconds_by_uri: dict[str, float] = {}
    source_file_count_by_uri: dict[str, int | None] = {}
    source_bytes_by_uri: dict[str, int | None] = {}
    bulk_checked: set[str] = set()
    if input_mode == "directory":
        bulk_checked = _discover_directories_sync(
            source_locations,
            extensions=extensions,
            input_format=input_format,
            exists_by_uri=exists_by_uri,
            discovered_by_uri=discovered_by_uri,
            discovery_seconds_by_uri=discovery_seconds_by_uri,
            memory_limit_bytes=memory_limit_bytes,
        )

    for uri, kind in source_locations.items():
        if uri in bulk_checked:
            continue
        started_at = perf_counter()
        try:
            exists, discovered, source_file_count, source_bytes = _discover_source_sync(
                uri,
                kind=kind,
                input_mode=input_mode,
                input_format=input_format,
                extensions=extensions,
                memory_limit_bytes=memory_limit_bytes,
            )
        finally:
            discovery_seconds_by_uri[uri] = max(perf_counter() - started_at, 0.0)
        exists_by_uri[uri] = exists
        source_file_count_by_uri[uri] = source_file_count
        source_bytes_by_uri[uri] = source_bytes
        if discovered is not None:
            discovered_by_uri[uri] = discovered

    return _partition_plans(
        plans,
        metadata_owner=metadata_owner,
        exists_by_uri=exists_by_uri,
        discovered_by_uri=discovered_by_uri,
        discovery_seconds_by_uri=discovery_seconds_by_uri,
        source_file_count_by_uri=source_file_count_by_uri,
        source_bytes_by_uri=source_bytes_by_uri,
    )


def discover_existing_source_plans_sync(
    plans: list[PartitionRunPlan],
    *,
    input_mode: str = "single_file",
    input_format: str = "json_array",
    source_file_extension: str | None = None,
    memory_limit_bytes: int | None = None,
) -> SourcePlanDiscovery:
    """Discover sources under one shared directory-metadata budget."""
    normalized_limit = normalize_memory_limit(memory_limit_bytes)
    with directory_metadata_budget_scope(normalized_limit):
        return _discover_existing_source_plans_sync_impl(
            plans,
            input_mode=input_mode,
            input_format=input_format,
            source_file_extension=source_file_extension,
            memory_limit_bytes=normalized_limit,
        )


__all__ = ["discover_existing_source_plans_sync"]
