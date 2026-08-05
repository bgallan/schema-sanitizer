"""Immutable remote-object discovery values."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime


@dataclass(frozen=True, slots=True)
class RemoteFile:
    """One immutable remote object selected for staging.

    Provider-specific metadata is optional so existing S3, Azure, HTTP, and
    local discovery paths remain source-compatible. GCS discovery populates
    the version and integrity fields, making ``(uri, generation)`` the stable
    identity of the selected object contents.
    """

    uri: str
    name: str
    size: int | None = None
    updated: datetime | None = None
    time_created: datetime | None = None
    generation: str | None = None
    metageneration: str | None = None
    etag: str | None = None
    crc32c: str | None = None

    @property
    def content_identity(self) -> tuple[str, str | None]:
        """Return the provider-stable object identity available at discovery."""
        return (self.uri, self.generation)


def remote_file_sort_key(file: RemoteFile) -> tuple[str, str]:
    """Return the deterministic identity ordering used by discovery."""
    return (file.uri, file.generation or "")


__all__ = ["RemoteFile", "remote_file_sort_key"]
