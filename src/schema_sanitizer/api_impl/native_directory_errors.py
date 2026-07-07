"""Shared native directory ingestion error helpers."""

from __future__ import annotations


def unsupported_native_directory_ingestion(reason: str | None = None) -> RuntimeError:
    """Return the public error for directory inputs that cannot use native path sources."""
    message = (
        "Directory input requires the native C++ path-source ingestion path; "
        "this directory source or option set is not supported by native directory ingestion."
    )
    if reason:
        message = f"{message} {reason}"
    return RuntimeError(message)
