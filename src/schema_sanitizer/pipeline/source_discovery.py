"""Discover local and remote inputs for partitioned pipeline plans."""

from __future__ import annotations

from collections import defaultdict
from pathlib import Path
from typing import Protocol
from urllib.parse import urlparse

from ..core_impl.async_scheduler import read_int_env, unordered_indexed_results
from ..core_impl.uris import (
    LocationKind,
    RemoteProvider,
    local_path_from_file_uri,
    location_kind,
    looks_like_file_uri,
    normalize_extensions,
)
from ..input_impl.directory_inputs import (
    DirectoryDiscovery,
    DirectoryDiscoveryBuilder,
    DiscoveredDirectoryInput,
    FolderFile,
    RemoteFile,
    folder_files,
)
from ..input_impl.selection import input_format_extensions
from ..remote_impl import routing
from ..remote_impl.providers import azure, gcs, s3
from ..remote_impl.transport import run_sync
from .types import PartitionRunPlan, SourcePlanDiscovery


class _DirectoryDiscoveryModule(Protocol):
    """Provider module exposing grouped directory discovery."""

    async def directories_containing_files(
        self,
        uris: list[str],
        suffixes: tuple[str, ...],
    ) -> DirectoryDiscovery[RemoteFile]:
        """Discover matching files for a group of provider URIs."""


_REMOTE_DISCOVERY: dict[RemoteProvider, _DirectoryDiscoveryModule] = {
    "gcs": gcs,
    "s3": s3,
    "azure": azure,
}


def _source_extensions(
    input_format: str,
    source_file_extension: str | None,
) -> tuple[str, ...]:
    """Return accepted source extensions without leading dots."""
    if source_file_extension:
        return (source_file_extension.strip().lstrip(".").lower(),)
    return input_format_extensions(input_format)


def _local_path(value: str, kind: LocationKind | None = None) -> Path:
    """Return a local path, reusing a caller-provided URI classification."""
    is_file_uri = kind == "file" if kind is not None else looks_like_file_uri(value)
    return Path(local_path_from_file_uri(value) if is_file_uri else value)


def _local_directory_matching_files(
    uri: str,
    suffixes: tuple[str, ...],
    *,
    kind: LocationKind | None = None,
) -> list[FolderFile]:
    """Return direct local directory children matching accepted suffixes."""
    path = _local_path(uri, kind)
    if not path.is_dir():
        return []
    try:
        return folder_files(path, suffix=suffixes, reader_name="source discovery")
    except (OSError, ValueError):
        return []


def _local_directories_containing_files(
    locations: dict[str, LocationKind],
    extensions: tuple[str, ...],
) -> DirectoryDiscovery[FolderFile]:
    """List matching children while scanning each parent directory only once."""
    suffixes = normalize_extensions(extensions)
    discovery = DirectoryDiscoveryBuilder[FolderFile].from_uris(locations)
    groups: dict[Path, dict[str, list[str]]] = defaultdict(lambda: defaultdict(list))
    for uri, kind in locations.items():
        path = _local_path(uri, kind)
        groups[path.parent][path.name].append(uri)

    for parent, children in groups.items():
        try:
            entries = {entry.name: entry for entry in parent.iterdir()}
        except OSError:
            continue
        for child_name, child_uris in children.items():
            child_path = entries.get(child_name)
            if child_path is None or not child_path.is_dir():
                continue
            files = _local_directory_matching_files(str(child_path), suffixes)
            if not files:
                continue
            discovery.extend(child_uris, files)
    return discovery.finish(sort_files=False)


def _record_discovered_inputs(
    discovered_by_uri: dict[str, DiscoveredDirectoryInput],
    *,
    input_format: str,
    local_files_by_uri: dict[str, list[FolderFile]] | None = None,
    remote_files_by_uri: dict[str, list[RemoteFile]] | None = None,
) -> None:
    """Store reusable local or remote directory listings."""
    for uri, local_files in (local_files_by_uri or {}).items():
        if local_files:
            discovered_by_uri[uri] = DiscoveredDirectoryInput(
                input_format=input_format,
                local_files=tuple(local_files),
            )
    for uri, remote_files in (remote_files_by_uri or {}).items():
        if remote_files:
            discovered_by_uri[uri] = DiscoveredDirectoryInput(
                input_format=input_format,
                remote_files=tuple(remote_files),
            )


async def _discover_directories(
    source_locations: dict[str, LocationKind],
    *,
    extensions: tuple[str, ...],
    input_format: str,
    exists_by_uri: dict[str, bool],
    discovered_by_uri: dict[str, DiscoveredDirectoryInput],
) -> set[str]:
    """Discover provider-grouped directories and preserve their listings."""
    grouped: dict[LocationKind, list[str]] = defaultdict(list)
    for uri, kind in source_locations.items():
        grouped[kind].append(uri)

    checked: set[str] = set()
    for provider, discovery in _REMOTE_DISCOVERY.items():
        uris = grouped.get(provider)
        if not uris:
            continue
        remote_result = await discovery.directories_containing_files(uris, extensions)
        exists_by_uri.update(remote_result.exists_by_uri)
        _record_discovered_inputs(
            discovered_by_uri,
            input_format=input_format,
            remote_files_by_uri=remote_result.files_by_uri,
        )
        checked.update(uris)

    local_locations: dict[str, LocationKind] = {
        uri: kind for kind in ("path", "file") for uri in grouped.get(kind, ())
    }
    if local_locations:
        local_result = _local_directories_containing_files(local_locations, extensions)
        exists_by_uri.update(local_result.exists_by_uri)
        _record_discovered_inputs(
            discovered_by_uri,
            input_format=input_format,
            local_files_by_uri=local_result.files_by_uri,
        )
        checked.update(local_locations)
    return checked


async def _discover_source(
    uri: str,
    *,
    kind: LocationKind,
    input_mode: str,
    input_format: str,
    extensions: tuple[str, ...],
) -> tuple[bool, DiscoveredDirectoryInput | None]:
    """Return whether one source exists plus reusable discovered metadata."""
    if kind not in {"path", "file"}:
        if input_mode == "directory":
            remote_files = await routing.list_remote_directory(uri, extensions)
            discovered = (
                DiscoveredDirectoryInput(
                    input_format=input_format, remote_files=tuple(remote_files)
                )
                if remote_files
                else None
            )
            return bool(remote_files), discovered
        return await routing.remote_file_exists(uri), None

    if input_mode == "directory":
        local_files = _local_directory_matching_files(
            uri, normalize_extensions(extensions), kind=kind
        )
        discovered = (
            DiscoveredDirectoryInput(input_format=input_format, local_files=tuple(local_files))
            if local_files
            else None
        )
        return bool(local_files), discovered
    return _local_path(uri, kind).is_file(), None


def _unique_source_locations(plans: list[PartitionRunPlan]) -> dict[str, LocationKind]:
    """Classify each unique source once while preserving first-seen order."""
    locations: dict[str, LocationKind] = {}
    for plan in plans:
        source_uri = plan.source_uri
        if source_uri in locations:
            continue
        kind = location_kind(source_uri)
        if kind is None:
            scheme = urlparse(source_uri).scheme
            if scheme:
                raise ValueError(f"Unsupported source URI scheme: {scheme!r}")
            kind = "path"
        locations[source_uri] = kind
    return locations


def _partition_plans(
    plans: list[PartitionRunPlan],
    *,
    exists_by_uri: dict[str, bool],
    discovered_by_uri: dict[str, DiscoveredDirectoryInput],
) -> SourcePlanDiscovery:
    """Split plans into existing and skipped groups with reusable metadata."""
    existing: list[PartitionRunPlan] = []
    skipped: list[PartitionRunPlan] = []
    for plan in plans:
        if not exists_by_uri.get(plan.source_uri, False):
            skipped.append(plan)
            continue
        discovered = discovered_by_uri.get(plan.source_uri)
        existing.append(plan.with_discovered_input(discovered) if discovered is not None else plan)
    return SourcePlanDiscovery(existing_plans=existing, skipped_plans=skipped)


async def discover_existing_source_plans_async(
    plans: list[PartitionRunPlan],
    *,
    input_mode: str = "single_file",
    input_format: str = "json_array",
    source_file_extension: str | None = None,
    concurrency: int | None = None,
) -> SourcePlanDiscovery:
    """Return plans whose source object or non-recursive directory has input."""
    if not plans:
        return SourcePlanDiscovery(existing_plans=[], skipped_plans=[])
    if input_mode not in {"single_file", "directory"}:
        raise ValueError("input_mode must be 'single_file' or 'directory'")

    extensions = _source_extensions(input_format, source_file_extension)
    source_locations = _unique_source_locations(plans)
    window = concurrency or read_int_env("SCHEMA_SANITIZER_SOURCE_DISCOVERY_CONCURRENCY", 128)
    exists_by_uri: dict[str, bool] = {}
    discovered_by_uri: dict[str, DiscoveredDirectoryInput] = {}
    bulk_checked_uris: set[str] = set()
    if input_mode == "directory":
        bulk_checked_uris = await _discover_directories(
            source_locations,
            extensions=extensions,
            input_format=input_format,
            exists_by_uri=exists_by_uri,
            discovered_by_uri=discovered_by_uri,
        )

    remaining = [
        (uri, kind) for uri, kind in source_locations.items() if uri not in bulk_checked_uris
    ]

    async def check_index(index: int) -> tuple[bool, DiscoveredDirectoryInput | None]:
        """Discover one source URI selected by the scheduler."""
        uri, kind = remaining[index]
        return await _discover_source(
            uri,
            kind=kind,
            input_mode=input_mode,
            input_format=input_format,
            extensions=extensions,
        )

    async for index, discovered in unordered_indexed_results(
        len(remaining),
        check_index,
        window=window,
    ):
        exists, discovered_input = discovered
        source_uri, _kind = remaining[index]
        exists_by_uri[source_uri] = exists
        if discovered_input is not None:
            discovered_by_uri[source_uri] = discovered_input

    return _partition_plans(
        plans,
        exists_by_uri=exists_by_uri,
        discovered_by_uri=discovered_by_uri,
    )


def discover_existing_source_plans(
    plans: list[PartitionRunPlan],
    *,
    input_mode: str = "single_file",
    input_format: str = "json_array",
    source_file_extension: str | None = None,
    concurrency: int | None = None,
) -> SourcePlanDiscovery:
    """Synchronously discover existing source plans."""
    return run_sync(
        discover_existing_source_plans_async(
            plans,
            input_mode=input_mode,
            input_format=input_format,
            source_file_extension=source_file_extension,
            concurrency=concurrency,
        )
    )
