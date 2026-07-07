"""Reusable Parquet helpers for pipeline outputs."""

from __future__ import annotations

from typing import Any

from ..api_impl.async_remote_io import (
    local_path_from_file_uri,
    looks_like_file_uri,
    looks_like_remote_uri,
    stage_remote_single_file,
)


def read_parquet_schema(uri: str) -> Any:
    """Read a Parquet schema after staging remote URIs locally."""
    import pyarrow.parquet as pq

    if looks_like_file_uri(uri):
        return pq.read_schema(local_path_from_file_uri(uri))
    if not looks_like_remote_uri(uri):
        return pq.read_schema(uri)
    staged = stage_remote_single_file(uri, memory_limit_bytes=None)
    try:
        return pq.read_schema(staged.path)
    finally:
        staged.close()
