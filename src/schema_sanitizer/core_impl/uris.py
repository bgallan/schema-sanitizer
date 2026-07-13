"""Canonical local-path and remote-URI classification helpers."""

from __future__ import annotations

import mimetypes
import os
from collections.abc import Sequence
from pathlib import Path
from typing import Any, Literal, TypeAlias
from urllib.parse import unquote, urlparse
from urllib.request import url2pathname

RemoteProvider: TypeAlias = Literal["gcs", "s3", "azure", "http"]
LocationKind: TypeAlias = Literal["path", "file", "gcs", "s3", "azure", "http"]

_GCS_SCHEMES = frozenset({"gcs", "gs"})
_S3_SCHEMES = frozenset({"s3"})
_AZURE_SCHEMES = frozenset({"abfs", "abfss", "adl", "az", "azure", "wasb", "wasbs"})
_HTTP_SCHEMES = frozenset({"http", "https"})


def looks_like_windows_drive_path(value: str) -> bool:
    """Return whether a string starts with a Windows drive prefix."""
    return len(value) >= 2 and value[1] == ":" and value[0].isalpha()


def location_kind(value: Any) -> LocationKind | None:
    """Classify one path-like string without parsing it more than once."""
    if not isinstance(value, str):
        return None
    if looks_like_windows_drive_path(value):
        return "path"
    parsed = urlparse(value)
    scheme = parsed.scheme.lower()
    if not scheme:
        return "path"
    if scheme == "file":
        return "file"
    if not parsed.netloc:
        return None
    if scheme in _GCS_SCHEMES:
        return "gcs"
    if scheme in _S3_SCHEMES:
        return "s3"
    if scheme in _AZURE_SCHEMES:
        return "azure"
    if scheme in _HTTP_SCHEMES:
        return "azure" if ".blob.core.windows.net" in parsed.netloc.lower() else "http"
    return None


def remote_provider(value: Any) -> RemoteProvider | None:
    """Return the canonical provider for a supported remote URI."""
    kind = location_kind(value)
    if kind is None or kind == "path" or kind == "file":
        return None
    return kind


def looks_like_remote_uri(value: Any) -> bool:
    """Return whether a value is a supported non-local remote URI."""
    return remote_provider(value) is not None


def looks_like_file_uri(value: object) -> bool:
    """Return whether a value is a local file URI."""
    return location_kind(value) == "file"


def looks_like_supported_uri(value: object) -> bool:
    """Return whether a value is a supported local or remote URI."""
    kind = location_kind(value)
    return kind is not None and kind != "path"


def local_path_from_file_uri(uri: str) -> str:
    """Convert a file URI into a platform-native local path."""
    parsed = urlparse(uri)
    if parsed.scheme.lower() != "file":
        raise ValueError(f"not a file URI: {uri!r}")
    if (
        os.name == "nt"
        and parsed.path.startswith("/")
        and looks_like_windows_drive_path(parsed.path[1:3])
    ):
        return unquote(parsed.path[1:]).replace("/", "\\")
    if parsed.netloc and parsed.netloc.lower() != "localhost":
        return url2pathname(f"//{parsed.netloc}{parsed.path}")
    return url2pathname(parsed.path)


def local_path_or_reject_remote(value: str | os.PathLike[str], *, remote_error: str) -> str:
    """Return a local path string or reject URI schemes that need staging."""
    raw = os.fspath(value)
    kind = location_kind(raw)
    if kind == "file":
        return local_path_from_file_uri(raw)
    if kind == "path":
        return raw
    if kind is not None or urlparse(raw).scheme:
        raise ValueError(remote_error)
    return raw


def local_output_path_or_reject_remote(
    value: str | os.PathLike[str],
    *,
    sink_name: str,
) -> str:
    """Return a local output path with one canonical staging error."""
    return local_path_or_reject_remote(
        value,
        remote_error=f"Remote outputs must be staged before {sink_name} sink writing",
    )


def suffix_from_uri(uri: str, *, default: str = "") -> str:
    """Return a suffix suitable for staging one URI."""
    return Path(urlparse(uri).path).suffix or default


def content_type_for_uri(uri: str) -> str:
    """Return a best-effort content type for one upload URI."""
    guessed, _ = mimetypes.guess_type(uri)
    return guessed or "application/octet-stream"


def normalize_extensions(suffixes: Sequence[str]) -> tuple[str, ...]:
    """Normalize suffixes to lowercase values with leading dots."""
    return tuple(
        value if value.startswith(".") else f".{value}"
        for suffix in suffixes
        if (value := suffix.lower())
    )


def name_matches(name: str, suffixes: tuple[str, ...]) -> bool:
    """Return whether a child object name matches accepted suffixes."""
    return name.lower().endswith(suffixes)
