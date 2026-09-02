"""Discover local and remote inputs for partitioned pipeline plans.

It lists or probes local and remote sources, filters missing inputs, and builds
deterministic manifests with discovery timing and transfer evidence.
"""

from __future__ import annotations

from collections import defaultdict
from pathlib import Path
from time import perf_counter
from typing import Protocol
from urllib.parse import urlparse

from ..core_impl.async_scheduler import (
    AsyncResultMemoryContract,
    AsyncResultOwnershipMode,
    ordered_indexed_results,
)
from ..core_impl.execution_policy import execution_policy, normalize_threading_mode
from ..core_impl.memory_budget import (
    GovernedResultOwnership,
    no_retained_result_ownership_capability,
    normalize_memory_limit,
    operation_memory_ownership_capability,
)
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
    folder_files,
)
from ..input_impl.selection import input_format_extensions
from ..remote_impl import routing
from ..remote_impl.providers import azure, gcs, s3
from ..sources.models import RemoteFile
from .source_discovery_budget import (
    run_async_discovery_with_budget,
    run_public_source_discovery,
)
from .source_discovery_memory import (
    bounded_remaining_sources,
    cached_source_summary,
    precharge_source_locations,
    source_summary,
)
from .types import PartitionRunPlan, SourcePlanDiscovery


def _discovery_result_external_ownership_capability(
    value: object,
) -> GovernedResultOwnership | None:
    """Return a runtime-issued lease capability for escaped discovery metadata."""
    if not isinstance(value, tuple) or len(value) < 2:
        return None
    discovered = value[1]
    if discovered is None:
        # The runtime, not the caller, certifies that there is no external
        # payload requiring a memory lease for this result generation.
        return no_retained_result_ownership_capability()
    if not isinstance(discovered, DiscoveredDirectoryInput):
        return None
    owner = getattr(discovered, "_metadata_owner", None)
    live_lease = getattr(owner, "live_lease", None)
    if not callable(live_lease):
        return None
    # Every independently escapable file record carries the same stable owner.
    # The scheduler therefore does not accept a container-level proof
    # if any payload element could outlive that ownership graph on its own.
    for local_file in discovered.local_files:
        if getattr(local_file, "_metadata_owner", None) is not owner:
            return None
    for remote_file in discovered.remote_files:
        if getattr(remote_file, "_metadata_owner", None) is not owner:
            return None
    return operation_memory_ownership_capability(live_lease())


_DISCOVERY_RESULT_MEMORY_CONTRACT = AsyncResultMemoryContract(
    preflight_bytes=512,
    ownership_mode=AsyncResultOwnershipMode.EXTERNALLY_GOVERNED,
    external_ownership_capability=_discovery_result_external_ownership_capability,
)


class _DirectoryDiscoveryModule(Protocol):
    """Provider module exposing grouped directory discovery."""

    async def directories_containing_files(
        self,
        uris: list[str],
        suffixes: tuple[str, ...],
        *,
        memory_limit_bytes: int | None = None,
        threading_mode: str = "single",
    ) -> DirectoryDiscovery[RemoteFile]:
        """Discover matching files for a group of provider URIs."""


_REMOTE_DISCOVERY: dict[RemoteProvider, _DirectoryDiscoveryModule] = {
    "gcs": gcs,
    "s3": s3,
    "azure": azure,
}
_LOCAL_LOCATION_KINDS: tuple[LocationKind, ...] = ("path", "file")


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
    memory_limit_bytes: int | None = None,
) -> list[FolderFile]:
    """Return direct local directory children matching accepted suffixes."""
    path = _local_path(uri, kind)
    if not path.is_dir():
        return []
    try:
        return folder_files(
            path,
            suffix=suffixes,
            reader_name="source discovery",
            memory_limit_bytes=memory_limit_bytes,
        )
    except (OSError, ValueError):
        return []


def _local_directories_containing_files(
    locations: dict[str, LocationKind],
    extensions: tuple[str, ...],
    *,
    memory_limit_bytes: int | None = None,
) -> DirectoryDiscovery[FolderFile]:
    """List matching children while scanning each parent directory only once."""
    suffixes = normalize_extensions(extensions)
    from ..input_impl.directory_inputs import current_directory_metadata_budget

    metadata_budget = current_directory_metadata_budget(memory_limit_bytes)
    discovery = DirectoryDiscoveryBuilder[FolderFile].from_uris(
        locations, metadata_budget=metadata_budget
    )
    groups: dict[Path, dict[str, list[str]]] = defaultdict(lambda: defaultdict(list))
    for uri, kind in locations.items():
        path = _local_path(uri, kind)
        discovery.publish_group_association(lambda: groups[path.parent][path.name].append(uri))

    for parent, children in groups.items():
        try:
            # Retain only requested child names rather than every entry in a
            # potentially hostile parent directory.
            entries = {entry.name: entry for entry in parent.iterdir() if entry.name in children}
        except OSError:
            continue
        for child_name, child_uris in children.items():
            child_path = entries.get(child_name)
            if child_path is None or not child_path.is_dir():
                continue
            files = _local_directory_matching_files(
                str(child_path),
                suffixes,
                memory_limit_bytes=memory_limit_bytes,
            )
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
    discovery_seconds_by_uri: dict[str, float] | None = None,
    memory_limit_bytes: int | None = None,
    threading_mode: str = "single",
) -> set[str]:
    """Discover provider-grouped directories and preserve their listings."""
    if discovery_seconds_by_uri is None:
        discovery_seconds_by_uri = {}
    grouped: dict[LocationKind, list[str]] = defaultdict(list)
    for uri, kind in source_locations.items():
        grouped[kind].append(uri)

    checked: set[str] = set()
    for provider, discovery in _REMOTE_DISCOVERY.items():
        uris = grouped.get(provider)
        if not uris:
            continue
        started_at = perf_counter()
        remote_result = await discovery.directories_containing_files(
            uris,
            extensions,
            memory_limit_bytes=memory_limit_bytes,
            threading_mode=threading_mode,
        )
        elapsed = max(perf_counter() - started_at, 0.0)
        discovery_seconds_by_uri.update(dict.fromkeys(uris, elapsed))
        exists_by_uri.update(remote_result.exists_by_uri)
        _record_discovered_inputs(
            discovered_by_uri,
            input_format=input_format,
            remote_files_by_uri=remote_result.files_by_uri,
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


async def _discover_source(
    uri: str,
    *,
    kind: LocationKind,
    input_mode: str,
    input_format: str,
    extensions: tuple[str, ...],
    memory_limit_bytes: int | None,
    threading_mode: str,
) -> tuple[bool, DiscoveredDirectoryInput | None, int | None, int | None]:
    """Return source existence, reusable input metadata, file count, and bytes."""
    if kind not in {"path", "file"}:
        if input_mode == "directory":
            remote_files = await routing.list_remote_directory(
                uri,
                extensions,
                memory_limit_bytes=memory_limit_bytes,
                threading_mode=threading_mode,
            )
            discovered = (
                DiscoveredDirectoryInput(
                    input_format=input_format, remote_files=tuple(remote_files)
                )
                if remote_files
                else None
            )
            return bool(remote_files), discovered, *source_summary(discovered)
        remote_file = await routing.remote_file_metadata(
            uri,
            memory_limit_bytes=memory_limit_bytes,
            threading_mode=threading_mode,
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
            DiscoveredDirectoryInput(input_format=input_format, local_files=tuple(local_files))
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
    metadata_owner: object | None = None,
    exists_by_uri: dict[str, bool],
    discovered_by_uri: dict[str, DiscoveredDirectoryInput],
    discovery_seconds_by_uri: dict[str, float],
    source_file_count_by_uri: dict[str, int | None],
    source_bytes_by_uri: dict[str, int | None],
) -> SourcePlanDiscovery:
    """Split plans into existing and skipped groups with reusable metadata."""
    existing: list[PartitionRunPlan] = []
    skipped: list[PartitionRunPlan] = []
    # Many partition plans can share one discovered directory object. Cache the
    # streaming count/byte summary by object identity so high-cardinality plans
    # do not rescan the same file tuple repeatedly.
    summary_by_identity: dict[int, tuple[int | None, int | None]] = {}
    for plan in plans:
        if not exists_by_uri.get(plan.source_uri, False):
            skipped.append(plan.with_metadata_owner(metadata_owner))
            continue
        discovered = discovered_by_uri.get(plan.source_uri)
        discovered_count, discovered_bytes = cached_source_summary(summary_by_identity, discovered)
        existing.append(
            plan.with_metadata_owner(metadata_owner).with_discovery_timing(
                discovered,
                discovery_seconds_by_uri.get(plan.source_uri, 0.0),
                source_file_count=source_file_count_by_uri.get(
                    plan.source_uri,
                    discovered_count,
                ),
                source_bytes=source_bytes_by_uri.get(
                    plan.source_uri,
                    discovered_bytes,
                ),
            )
        )
    return SourcePlanDiscovery(existing_plans=existing, skipped_plans=skipped)


async def _discover_existing_source_plans_async_impl(
    plans: list[PartitionRunPlan],
    *,
    input_mode: str = "single_file",
    input_format: str = "json_array",
    source_file_extension: str | None = None,
    memory_limit_bytes: int | None = None,
    threading_mode: str = "single",
) -> SourcePlanDiscovery:
    """Return plans whose source object or non-recursive directory has input."""
    memory_limit_bytes = normalize_memory_limit(memory_limit_bytes)
    if normalize_threading_mode(threading_mode) == "single":
        from .source_discovery_sync import discover_existing_source_plans_sync

        return discover_existing_source_plans_sync(
            plans,
            input_mode=input_mode,
            input_format=input_format,
            source_file_extension=source_file_extension,
            memory_limit_bytes=memory_limit_bytes,
        )
    if not plans:
        return SourcePlanDiscovery(existing_plans=[], skipped_plans=[])
    if input_mode not in {"single_file", "directory"}:
        raise ValueError("input_mode must be 'single_file' or 'directory'")

    extensions = _source_extensions(input_format, source_file_extension)
    metadata_budget, source_locations, metadata_owner = precharge_source_locations(
        plans, memory_limit_bytes=memory_limit_bytes
    )
    window = execution_policy(threading_mode, memory_limit_bytes).source_discovery_concurrency
    exists_by_uri: dict[str, bool] = {}
    discovered_by_uri: dict[str, DiscoveredDirectoryInput] = {}
    discovery_seconds_by_uri: dict[str, float] = {}
    source_file_count_by_uri: dict[str, int | None] = {}
    source_bytes_by_uri: dict[str, int | None] = {}
    bulk_checked_uris: set[str] = set()
    if input_mode == "directory":
        bulk_checked_uris = await _discover_directories(
            source_locations,
            extensions=extensions,
            input_format=input_format,
            exists_by_uri=exists_by_uri,
            discovered_by_uri=discovered_by_uri,
            discovery_seconds_by_uri=discovery_seconds_by_uri,
            memory_limit_bytes=memory_limit_bytes,
            threading_mode=threading_mode,
        )

    remaining = bounded_remaining_sources(source_locations, bulk_checked_uris, metadata_budget)

    async def check_index(
        index: int,
    ) -> tuple[bool, DiscoveredDirectoryInput | None, int | None, int | None, float]:
        """Discover one source URI selected by the scheduler."""
        uri, kind = remaining[index]
        started_at = perf_counter()
        try:
            exists, discovered_input, source_file_count, source_bytes = await _discover_source(
                uri,
                kind=kind,
                input_mode=input_mode,
                input_format=input_format,
                extensions=extensions,
                memory_limit_bytes=memory_limit_bytes,
                threading_mode=threading_mode,
            )
            return (
                exists,
                discovered_input,
                source_file_count,
                source_bytes,
                max(perf_counter() - started_at, 0.0),
            )
        except Exception:
            discovery_seconds_by_uri[uri] = max(perf_counter() - started_at, 0.0)
            raise

    async for index, discovered in ordered_indexed_results(
        len(remaining),
        check_index,
        window=window,
        memory_contract=_DISCOVERY_RESULT_MEMORY_CONTRACT,
    ):
        (
            exists,
            discovered_input,
            source_file_count,
            source_bytes,
            discovery_seconds,
        ) = discovered
        source_uri, _kind = remaining[index]
        exists_by_uri[source_uri] = exists
        discovery_seconds_by_uri[source_uri] = discovery_seconds
        source_file_count_by_uri[source_uri] = source_file_count
        source_bytes_by_uri[source_uri] = source_bytes
        if discovered_input is not None:
            discovered_by_uri[source_uri] = discovered_input

    return _partition_plans(
        plans,
        metadata_owner=metadata_owner,
        exists_by_uri=exists_by_uri,
        discovered_by_uri=discovered_by_uri,
        discovery_seconds_by_uri=discovery_seconds_by_uri,
        source_file_count_by_uri=source_file_count_by_uri,
        source_bytes_by_uri=source_bytes_by_uri,
    )


async def discover_existing_source_plans_async(
    plans: list[PartitionRunPlan],
    *,
    input_mode: str = "single_file",
    input_format: str = "json_array",
    source_file_extension: str | None = None,
    memory_limit_bytes: int | None = None,
    threading_mode: str = "single",
) -> SourcePlanDiscovery:
    """Discover sources under one shared directory-metadata budget."""
    return await run_async_discovery_with_budget(
        _discover_existing_source_plans_async_impl,
        plans,
        input_mode=input_mode,
        input_format=input_format,
        source_file_extension=source_file_extension,
        memory_limit_bytes=memory_limit_bytes,
        threading_mode=threading_mode,
    )


def discover_existing_source_plans(
    plans: list[PartitionRunPlan],
    *,
    input_mode: str = "single_file",
    input_format: str = "json_array",
    source_file_extension: str | None = None,
    memory_limit_bytes: int | None = None,
    threading_mode: str = "single",
) -> SourcePlanDiscovery:
    """Synchronously discover existing source plans."""
    return run_public_source_discovery(
        discover_existing_source_plans_async,
        plans,
        input_mode=input_mode,
        input_format=input_format,
        source_file_extension=source_file_extension,
        memory_limit_bytes=memory_limit_bytes,
        threading_mode=threading_mode,
    )
