"""Helpers for local path and file:// URI normalization."""

from __future__ import annotations

import os
from urllib.parse import unquote, urlparse
from urllib.request import url2pathname


def looks_like_file_uri(value: object) -> bool:
    """Return whether a value is a local file:// URI."""
    return isinstance(value, str) and urlparse(value).scheme.lower() == "file"


def looks_like_windows_drive_path(value: str) -> bool:
    """Return whether a string starts with a Windows drive prefix."""
    return len(value) >= 2 and value[1] == ":" and value[0].isalpha()


def local_path_from_file_uri(uri: str) -> str:
    """Convert a file:// URI into a platform-native local path."""
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


def local_path_or_reject_remote(value: object, *, remote_error: str) -> str:
    """Return a local path string or reject non-file URI schemes."""
    raw = os.fspath(value)
    parsed = urlparse(raw)
    scheme = parsed.scheme.lower()
    if scheme == "file":
        return local_path_from_file_uri(raw)
    if scheme and not looks_like_windows_drive_path(raw):
        raise ValueError(remote_error)
    return raw
