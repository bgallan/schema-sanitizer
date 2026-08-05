"""Stable remote-source discovery and publication facade."""

from __future__ import annotations

from collections.abc import Sequence
from datetime import UTC, datetime
from pathlib import Path

from ..config import ResourceOptions
from .models import RemoteFile, SourceManifest


def _aware_utc(value: datetime, *, name: str) -> datetime:
    """Validate one timezone-aware timestamp and normalize it to UTC."""
    if not isinstance(value, datetime):
        raise TypeError(f"{name} must be a datetime")
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{name} must be timezone-aware")
    return value.astimezone(UTC)


def list_objects(
    source_uri: str,
    *,
    suffixes: Sequence[str] = (),
    resources: ResourceOptions | None = None,
    multi_threading: bool | None = None,
    memory_limit_bytes: int | None = None,
) -> tuple[RemoteFile, ...]:
    """List remote objects deterministically under one supported prefix."""
    from ..core_impl.execution_policy import threading_mode_from_multi_threading
    from ..core_impl.memory_budget import (
        OperationMemoryLedger,
        activate_operation_memory_ledger,
        normalize_memory_limit,
    )
    from ..input_impl.directory_inputs import (
        DirectoryMetadataBudget,
        directory_metadata_budget_scope,
    )
    from ..remote_impl import routing, sync_backend
    from ..remote_impl.transport import run_sync

    if resources is not None:
        if multi_threading is not None or memory_limit_bytes is not None:
            raise ValueError("pass resources or individual resource arguments, not both")
        multi_threading = resources.multi_threading
        memory_limit_bytes = resources.memory_limit_bytes
    enabled = False if multi_threading is None else multi_threading
    if not isinstance(enabled, bool):
        raise TypeError("multi_threading must be a bool")
    normalized_limit = normalize_memory_limit(memory_limit_bytes)
    mode = threading_mode_from_multi_threading(enabled)
    ledger = OperationMemoryLedger(normalized_limit)
    metadata_budget = DirectoryMetadataBudget(normalized_limit, operation_memory_ledger=ledger)
    try:
        with activate_operation_memory_ledger(ledger):
            with directory_metadata_budget_scope(normalized_limit, budget=metadata_budget):
                if enabled:
                    files = run_sync(
                        routing.list_remote_directory(
                            source_uri,
                            suffixes,
                            memory_limit_bytes=normalized_limit,
                            threading_mode=mode,
                        ),
                        threading_mode=mode,
                    )
                else:
                    files = sync_backend.list_remote_directory(
                        source_uri,
                        suffixes,
                        memory_limit_bytes=normalized_limit,
                    )
        return tuple(files)
    finally:
        metadata_budget.close()
        ledger.close()


def discover(
    source_uri: str,
    *,
    suffixes: Sequence[str] = (),
    modified_between: tuple[datetime, datetime] | None = None,
    resources: ResourceOptions | None = None,
    multi_threading: bool | None = None,
    memory_limit_bytes: int | None = None,
) -> SourceManifest:
    """Discover one immutable GCS manifest, optionally filtered by update time."""
    from ..core_impl.uris import remote_provider

    if remote_provider(source_uri) != "gcs":
        raise ValueError("immutable SourceManifest discovery currently supports only GCS")
    files = list_objects(
        source_uri,
        suffixes=suffixes,
        resources=resources,
        multi_threading=multi_threading,
        memory_limit_bytes=memory_limit_bytes,
    )
    if modified_between is not None:
        start = _aware_utc(modified_between[0], name="modified_between[0]")
        end = _aware_utc(modified_between[1], name="modified_between[1]")
        if start >= end:
            raise ValueError("modified_between start must be earlier than end")
        files = tuple(
            file for file in files if file.updated is not None and start <= file.updated < end
        )
    return SourceManifest(source_uri, files)


def publish_file_atomic(
    local_path: str | Path,
    destination_uri: str,
    *,
    memory_limit_bytes: int | None = None,
) -> int:
    """Publish one closed local file and return its verified local byte count."""
    from ..core_impl.memory_budget import normalize_memory_limit
    from ..remote_impl import sync_backend

    path = Path(local_path)
    if not path.is_file():
        raise FileNotFoundError(path)
    size = path.stat().st_size
    sync_backend.upload_file(
        str(path),
        destination_uri,
        memory_limit_bytes=normalize_memory_limit(memory_limit_bytes),
    )
    return size


__all__ = [
    "RemoteFile",
    "SourceManifest",
    "discover",
    "list_objects",
    "publish_file_atomic",
]
