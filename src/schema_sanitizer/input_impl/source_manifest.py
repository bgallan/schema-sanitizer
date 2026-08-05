"""Immutable public manifests for already discovered remote objects."""

from __future__ import annotations

import os
from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any, TypeAlias
from urllib.parse import urlparse

from ..core_impl.uris import RemoteProvider, remote_provider
from .remote_files import RemoteFile, remote_file_sort_key

_VERSIONED_MANIFEST_PROVIDERS = frozenset({"gcs"})


def _normalized_remote_path(uri: str) -> tuple[str, str]:
    """Return a canonical remote authority and path for prefix validation."""
    parsed = urlparse(uri)
    return parsed.netloc.lower(), parsed.path.lstrip("/").rstrip("/")


def _belongs_to_source(source_uri: str, object_uri: str) -> bool:
    """Return whether one object URI is inside the declared source prefix."""
    source_authority, source_path = _normalized_remote_path(source_uri)
    object_authority, object_path = _normalized_remote_path(object_uri)
    if source_authority != object_authority:
        return False
    if not source_path:
        return True
    return object_path == source_path or object_path.startswith(f"{source_path}/")


@dataclass(frozen=True, slots=True, init=False)
class SourceManifest:
    """A deterministic immutable collection of exact remote-object versions.

    Version-one public consumption is intentionally restricted to GCS because
    GCS discovery and staging carry an immutable ``generation`` and enforce it
    during download. ``source_uri`` identifies the prefix from which ``files``
    were selected; each entry must remain inside that prefix.
    """

    source_uri: str
    files: tuple[RemoteFile, ...]
    provider: RemoteProvider

    def __init__(self, source_uri: str, files: Iterable[RemoteFile]) -> None:
        """Validate, freeze, and canonically order one selected collection."""
        if not isinstance(source_uri, str) or not source_uri.strip():
            raise ValueError("source_uri must be a non-empty string")
        provider = remote_provider(source_uri)
        if provider is None:
            raise ValueError("SourceManifest source_uri must be a supported remote URI")
        if provider not in _VERSIONED_MANIFEST_PROVIDERS:
            raise ValueError(
                "SourceManifest currently supports only versioned GCS object identities"
            )

        ordered_files = list(files)
        ordered_files.sort(key=remote_file_sort_key)
        ordered = tuple(ordered_files)
        seen: set[tuple[str, str | None]] = set()
        for file in ordered:
            if not isinstance(file, RemoteFile):
                raise TypeError("SourceManifest entries must be RemoteFile values")
            if remote_provider(file.uri) != provider:
                raise ValueError(
                    "SourceManifest entries must use the same supported filesystem as source_uri: "
                    f"{file.uri!r}"
                )
            if not _belongs_to_source(source_uri, file.uri):
                raise ValueError(
                    f"SourceManifest entry is outside the declared source prefix: {file.uri!r}"
                )
            if not isinstance(file.generation, str) or not file.generation.strip():
                raise ValueError(
                    "SourceManifest GCS entries require an immutable object generation: "
                    f"{file.uri!r}"
                )
            if not isinstance(file.name, str) or not file.name.strip():
                raise ValueError(f"SourceManifest entry has no usable file name: {file.uri!r}")
            if file.updated is not None and (
                file.updated.tzinfo is None or file.updated.utcoffset() is None
            ):
                raise ValueError(
                    f"SourceManifest object modification times must be timezone-aware: {file.uri!r}"
                )
            identity = file.content_identity
            if identity in seen:
                raise ValueError(
                    f"SourceManifest contains a duplicate content identity: {identity!r}"
                )
            seen.add(identity)

        object.__setattr__(self, "source_uri", source_uri)
        object.__setattr__(self, "files", ordered)
        object.__setattr__(self, "provider", provider)

    @property
    def object_count(self) -> int:
        """Return the number of exact remote-object identities selected."""
        return len(self.files)

    @property
    def total_bytes(self) -> int | None:
        """Return the exact byte total, or ``None`` if any size is unavailable."""
        if any(file.size is None for file in self.files):
            return None
        return sum(file.size or 0 for file in self.files)

    @property
    def earliest_update(self) -> datetime | None:
        """Return the earliest update when every selected object has one."""
        earliest: datetime | None = None
        for file in self.files:
            value = file.updated
            if value is None:
                return None
            if earliest is None or value < earliest:
                earliest = value
        return earliest.astimezone(UTC) if earliest is not None else None

    @property
    def latest_update(self) -> datetime | None:
        """Return the latest update when every selected object has one."""
        latest: datetime | None = None
        for file in self.files:
            value = file.updated
            if value is None:
                return None
            if latest is None or value > latest:
                latest = value
        return latest.astimezone(UTC) if latest is not None else None

    @property
    def content_identities(self) -> tuple[tuple[str, str | None], ...]:
        """Return the deterministic object identities represented by the manifest."""
        return tuple(file.content_identity for file in self.files)

    @property
    def diagnostic_objects(self) -> tuple[Mapping[str, str], ...]:
        """Return immutable URI/generation pairs suitable for diagnostics."""
        return tuple({"uri": file.uri, "generation": file.generation or ""} for file in self.files)


PublicInput: TypeAlias = str | os.PathLike[str] | Iterable[Mapping[str, Any]] | SourceManifest

__all__ = ["PublicInput", "SourceManifest"]
