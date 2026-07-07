"""Reusable source discovery for partitioned pipelines."""

from __future__ import annotations

import re
from collections import defaultdict
from pathlib import Path
from urllib.parse import urlparse

from ..api_impl.async_remote_io import (
    _azure_directories_containing_files,
    _gcs_directories_containing_files,
    _list_remote_directory,
    _remote_file_exists,
    _run_async,
    _s3_directories_containing_files,
    looks_like_file_uri,
    looks_like_remote_uri,
)
from ..api_impl.async_remote_scheduler import read_int_env, unordered_indexed_results
from ..api_impl.folder_listing import FolderFile, folder_files
from ..api_impl.public_input import DiscoveredDirectoryInput
from ..core_impl.path_uris import local_path_from_file_uri
from .hive import FORMAT_EXTENSIONS
from .types import PartitionRunPlan, SourcePlanDiscovery

_WINDOWS_DRIVE_PATH_RE = re.compile(r"^[A-Za-z]:[\\/]")
_GCS_SCHEMES = {"gs", "gcs"}
_S3_SCHEMES = {"s3"}
_AZURE_SCHEMES = {"abfs", "abfss", "adl", "az", "azure", "wasb", "wasbs"}


def _normalize_extension(value: str) -> str:
    """Return one extension without a leading dot."""
    return value.strip().lstrip(".").lower()


def source_extensions(
    input_format: str, source_file_extension: str | None = None
) -> tuple[str, ...]:
    """Return accepted source extensions for one input format."""
    if source_file_extension:
        return (_normalize_extension(source_file_extension),)
    return FORMAT_EXTENSIONS[input_format]


def _local_path(value: str) -> Path:
    """Return a local path for a filesystem path or file URI."""
    if looks_like_file_uri(value):
        return Path(local_path_from_file_uri(value))
    return Path(value)


def _looks_like_windows_drive_path(value: str) -> bool:
    """Return whether a string is a Windows drive-letter filesystem path."""
    return bool(_WINDOWS_DRIVE_PATH_RE.match(value))


def _is_gcs_uri(value: str) -> bool:
    """Return whether a string is a GCS URI."""
    parsed = urlparse(value)
    return bool(parsed.scheme.lower() in _GCS_SCHEMES and parsed.netloc)


def _is_s3_uri(value: str) -> bool:
    """Return whether a string is an S3 URI."""
    parsed = urlparse(value)
    return bool(parsed.scheme.lower() in _S3_SCHEMES and parsed.netloc)


def _is_azure_uri(value: str) -> bool:
    """Return whether a string is an Azure Blob/ADLS URI."""
    parsed = urlparse(value)
    scheme = parsed.scheme.lower()
    return bool(
        (scheme in _AZURE_SCHEMES and parsed.netloc)
        or (scheme in {"http", "https"} and ".blob.core.windows.net" in parsed.netloc)
    )


def _local_directory_contains_matching_file(uri: str, extensions: tuple[str, ...]) -> bool:
    """Return whether a local directory has a direct child with an accepted extension."""
    return bool(_local_directory_matching_files(uri, extensions))


def _local_directory_matching_files(uri: str, extensions: tuple[str, ...]) -> list[FolderFile]:
    """Return direct local directory children matching accepted extensions."""
    suffixes = tuple(f".{_normalize_extension(extension)}" for extension in extensions)
    path = _local_path(uri)
    if not path.is_dir():
        return []
    try:
        return folder_files(path, suffix=suffixes, reader_name="source discovery")
    except (OSError, ValueError):
        return []


class _LocalDirectoryDiscovery(dict[str, bool]):
    """Local directory existence map plus files found while checking it."""

    def __init__(
        self,
        values: dict[str, bool],
        *,
        files_by_uri: dict[str, list[FolderFile]],
    ):
        """Store bool compatibility values and optional child listings."""
        super().__init__(values)
        self.files_by_uri = files_by_uri


def _local_directories_containing_files(
    uris: list[str],
    extensions: tuple[str, ...],
) -> dict[str, bool]:
    """Return whether local directories contain a direct child matching extensions."""
    out = {uri: False for uri in uris}
    files_by_uri = {uri: [] for uri in uris}
    groups: dict[Path, dict[str, list[str]]] = defaultdict(lambda: defaultdict(list))
    for uri in uris:
        path = _local_path(uri)
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
            files = _local_directory_matching_files(str(child_path), extensions)
            if files:
                for uri in child_uris:
                    out[uri] = True
                    files_by_uri[uri] = files
    return _LocalDirectoryDiscovery(out, files_by_uri=files_by_uri)


async def _discover_source(
    uri: str,
    *,
    input_mode: str,
    input_format: str,
    extensions: tuple[str, ...],
) -> tuple[bool, DiscoveredDirectoryInput | None]:
    """Return whether one source exists plus reusable discovered source metadata."""
    if looks_like_remote_uri(uri):
        if input_mode == "directory":
            files = await _list_remote_directory(uri, extensions)
            return bool(files), (
                DiscoveredDirectoryInput(input_format=input_format, remote_files=tuple(files))
                if files
                else None
            )
        return await _remote_file_exists(uri), None

    if input_mode == "directory":
        files = _local_directory_matching_files(uri, extensions)
        return bool(files), (
            DiscoveredDirectoryInput(input_format=input_format, local_files=tuple(files))
            if files
            else None
        )
    return _local_path(uri).is_file(), None


def _record_discovered_inputs(
    discovered_by_uri: dict[str, DiscoveredDirectoryInput],
    *,
    input_format: str,
    files_by_uri: dict[str, list] | None,
    remote: bool,
) -> None:
    """Store reusable discovered directory input payloads from a bulk listing result."""
    if not files_by_uri:
        return
    for uri, files in files_by_uri.items():
        if not files:
            continue
        if remote:
            discovered_by_uri[uri] = DiscoveredDirectoryInput(
                input_format=input_format,
                remote_files=tuple(files),
            )
        else:
            discovered_by_uri[uri] = DiscoveredDirectoryInput(
                input_format=input_format,
                local_files=tuple(files),
            )


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

    extensions = source_extensions(input_format, source_file_extension)
    source_uri_to_indices: dict[str, list[int]] = defaultdict(list)
    for index, plan in enumerate(plans):
        parsed = urlparse(plan.source_uri)
        if (
            parsed.scheme
            and not _looks_like_windows_drive_path(plan.source_uri)
            and not (looks_like_remote_uri(plan.source_uri) or looks_like_file_uri(plan.source_uri))
        ):
            raise ValueError(f"Unsupported source URI scheme: {parsed.scheme!r}")
        source_uri_to_indices[plan.source_uri].append(index)

    unique_source_uris = list(source_uri_to_indices)
    window = concurrency or read_int_env(
        "SCHEMA_SANITIZER_SOURCE_DISCOVERY_CONCURRENCY",
        read_int_env("SOURCE_DISCOVERY_CONCURRENCY", 128),
    )
    exists_by_uri: dict[str, bool] = {}
    discovered_by_uri: dict[str, DiscoveredDirectoryInput] = {}
    bulk_checked_uris: set[str] = set()

    if input_mode == "directory":
        gcs_uris = [uri for uri in unique_source_uris if _is_gcs_uri(uri)]
        if gcs_uris:
            bulk_result = await _gcs_directories_containing_files(gcs_uris, extensions)
            exists_by_uri.update(bulk_result)
            _record_discovered_inputs(
                discovered_by_uri,
                input_format=input_format,
                files_by_uri=getattr(bulk_result, "files_by_uri", None),
                remote=True,
            )
            bulk_checked_uris.update(gcs_uris)
        s3_uris = [uri for uri in unique_source_uris if _is_s3_uri(uri)]
        if s3_uris:
            bulk_result = await _s3_directories_containing_files(s3_uris, extensions)
            exists_by_uri.update(bulk_result)
            _record_discovered_inputs(
                discovered_by_uri,
                input_format=input_format,
                files_by_uri=getattr(bulk_result, "files_by_uri", None),
                remote=True,
            )
            bulk_checked_uris.update(s3_uris)
        azure_uris = [uri for uri in unique_source_uris if _is_azure_uri(uri)]
        if azure_uris:
            bulk_result = await _azure_directories_containing_files(azure_uris, extensions)
            exists_by_uri.update(bulk_result)
            _record_discovered_inputs(
                discovered_by_uri,
                input_format=input_format,
                files_by_uri=getattr(bulk_result, "files_by_uri", None),
                remote=True,
            )
            bulk_checked_uris.update(azure_uris)
        local_uris = [
            uri
            for uri in unique_source_uris
            if uri not in bulk_checked_uris and not looks_like_remote_uri(uri)
        ]
        if local_uris:
            bulk_result = _local_directories_containing_files(local_uris, extensions)
            exists_by_uri.update(bulk_result)
            _record_discovered_inputs(
                discovered_by_uri,
                input_format=input_format,
                files_by_uri=getattr(bulk_result, "files_by_uri", None),
                remote=False,
            )
            bulk_checked_uris.update(local_uris)

    async def check_index(index: int) -> tuple[bool, DiscoveredDirectoryInput | None]:
        """Check one indexed unique source URI."""
        return await _discover_source(
            remaining_source_uris[index],
            input_mode=input_mode,
            input_format=input_format,
            extensions=extensions,
        )

    remaining_source_uris = [uri for uri in unique_source_uris if uri not in bulk_checked_uris]

    async for index, discovered in unordered_indexed_results(
        len(remaining_source_uris),
        check_index,
        window=window,
    ):
        exists, discovered_input = discovered
        source_uri = remaining_source_uris[index]
        exists_by_uri[source_uri] = exists
        if discovered_input is not None:
            discovered_by_uri[source_uri] = discovered_input

    existing_plans: list[PartitionRunPlan] = []
    skipped_plans: list[PartitionRunPlan] = []
    for plan in plans:
        if exists_by_uri.get(plan.source_uri, False):
            discovered_input = discovered_by_uri.get(plan.source_uri)
            existing_plans.append(
                plan.with_discovered_input(discovered_input)
                if discovered_input is not None
                else plan
            )
        else:
            skipped_plans.append(plan)

    return SourcePlanDiscovery(existing_plans=existing_plans, skipped_plans=skipped_plans)


def discover_existing_source_plans(
    plans: list[PartitionRunPlan],
    *,
    input_mode: str = "single_file",
    input_format: str = "json_array",
    source_file_extension: str | None = None,
    concurrency: int | None = None,
) -> SourcePlanDiscovery:
    """Sync wrapper around async source discovery."""
    return _run_async(
        discover_existing_source_plans_async(
            plans,
            input_mode=input_mode,
            input_format=input_format,
            source_file_extension=source_file_extension,
            concurrency=concurrency,
        )
    )


def discovery_environment_notes() -> dict[str, str]:
    """Return environment variables used by source discovery."""
    return {
        "SCHEMA_SANITIZER_SOURCE_DISCOVERY_CONCURRENCY": (
            "Maximum concurrent source discovery checks."
        ),
        "SOURCE_DISCOVERY_CONCURRENCY": (
            "Legacy alias for maximum concurrent source discovery checks."
        ),
        "SCHEMA_SANITIZER_ASYNC_TIMEOUT": "Total timeout per async HTTP request.",
        "SCHEMA_SANITIZER_ASYNC_RETRIES": "Retry count for async remote operations.",
        "SCHEMA_SANITIZER_SOURCE_DISCOVERY_GCS_BULK_CONCURRENCY": (
            "Maximum concurrent GCS parent-prefix discovery listings."
        ),
        "SCHEMA_SANITIZER_SOURCE_DISCOVERY_GCS_RETRIES": (
            "Retry count for GCS parent-prefix discovery listings."
        ),
        "SCHEMA_SANITIZER_SOURCE_DISCOVERY_S3_BULK_CONCURRENCY": (
            "Maximum concurrent S3 parent-prefix discovery listings."
        ),
        "SCHEMA_SANITIZER_SOURCE_DISCOVERY_S3_RETRIES": (
            "Retry count for S3 parent-prefix discovery listings."
        ),
        "SCHEMA_SANITIZER_SOURCE_DISCOVERY_AZURE_BULK_CONCURRENCY": (
            "Maximum concurrent Azure parent-prefix discovery listings."
        ),
    }
