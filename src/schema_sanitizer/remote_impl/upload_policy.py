"""Memory-bounded policy and helpers for remote multipart publication."""

from __future__ import annotations

import math
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from ..core_impl.execution_policy import execution_policy
from ..core_impl.finalization import runtime_is_finalizing
from ..core_impl.memory_budget import acquire_operation_memory, memory_budget
from ..core_impl.process_resources import reserve_file_descriptors

_MIB = 1024 * 1024
_S3_MIN_PART_BYTES = 5 * _MIB
_GCS_CHUNK_ALIGNMENT = 256 * 1024
_MAX_UPLOAD_PARTS = 10_000


class _BudgetedUploadBytes(bytes):
    """Multipart bytes retaining their operation-memory reservation."""

    _operation_memory_lease: Any | None

    def __new__(cls, value: bytes, lease: object):
        """Create immutable upload bytes with one attached lease."""
        obj = super().__new__(cls, value)
        obj._operation_memory_lease = lease
        return obj

    def close(self) -> None:
        """Release the retained upload charge before clearing ownership."""
        lease = getattr(self, "_operation_memory_lease", None)
        if lease is None:
            return
        lease.close()
        if getattr(self, "_operation_memory_lease", None) is lease:
            self._operation_memory_lease = None

    def __del__(self) -> None:
        """Return the lease unless interpreter teardown has begun."""
        try:
            if runtime_is_finalizing():
                return
            self.close()
        except BaseException:
            pass


def release_upload_payload(payload: bytes) -> None:
    """Release a multipart payload lease when one is attached."""
    close = getattr(payload, "close", None)
    if callable(close):
        close()


@dataclass(frozen=True, slots=True)
class RemoteUploadPolicy:
    """Derived controls for one local-spool remote publication."""

    provider: str
    file_size: int
    multipart: bool
    part_bytes: int
    concurrency: int
    part_count: int
    buffered_bytes: int


def _round_up(value: int, alignment: int) -> int:
    """Round one positive byte count up to the requested alignment."""
    return ((max(1, value) + alignment - 1) // alignment) * alignment


def _provider_min_part_bytes(provider: str) -> int:
    """Return the provider-specific minimum safe upload part size."""
    if provider == "s3":
        return _S3_MIN_PART_BYTES
    if provider == "gcs":
        return _GCS_CHUNK_ALIGNMENT
    return 1 * _MIB


def remote_upload_policy(
    provider: str,
    local_path: str,
    *,
    memory_limit_bytes: int | None,
    threading_mode: str,
) -> RemoteUploadPolicy:
    """Derive bounded multipart/resumable controls from the operation budget."""
    file_size = Path(local_path).stat().st_size
    policy = execution_policy(threading_mode, memory_limit_bytes)
    budget = memory_budget(memory_limit_bytes)
    minimum = _provider_min_part_bytes(provider)

    # Keep upload bodies to at most one eighth of operation memory. The complete
    # spool is disk-backed, but SDK/TLS buffers and provider metadata remain in
    # memory while publication runs.
    buffer_budget = max(minimum, budget.total_bytes // 8)
    desired_part = max(minimum, budget.io_chunk_bytes * 8)
    if provider == "gcs":
        desired_part = _round_up(desired_part, _GCS_CHUNK_ALIGNMENT)
    elif provider == "s3":
        desired_part = max(
            desired_part,
            _round_up(math.ceil(max(1, file_size) / _MAX_UPLOAD_PARTS), _MIB),
        )

    requested_workers = 1 if policy.is_single else max(1, policy.async_concurrency)
    max_workers_by_memory = max(1, buffer_budget // desired_part)
    concurrency = min(requested_workers, max_workers_by_memory)
    part_count = max(1, math.ceil(max(1, file_size) / desired_part))
    threshold = max(desired_part * 2, 16 * _MIB)

    multipart = provider in {"s3", "gcs"} and file_size >= threshold and part_count > 1
    if not multipart:
        part_count = 1
    if provider == "gcs":
        # Resumable GCS chunks are sequential because each response determines
        # the next committed offset. Multi still gains retry/resume semantics.
        concurrency = 1
    elif not multipart and provider != "azure":
        concurrency = 1

    return RemoteUploadPolicy(
        provider=provider,
        file_size=file_size,
        multipart=multipart,
        part_bytes=desired_part,
        concurrency=max(1, concurrency),
        part_count=part_count,
        buffered_bytes=desired_part * max(1, concurrency),
    )


def read_upload_range(local_path: str, offset: int, size: int, file_size: int) -> bytes:
    """Read one deterministic range while retaining its resident-memory lease."""
    if offset < 0 or size < 0 or offset + size > file_size:
        raise ValueError("remote upload range is outside the completed local spool")
    # Reading plus the retained bytes subclass can coexist briefly. Charge
    # both immutable buffers before materializing either one.
    lease = acquire_operation_memory(size * 2 + 256, stage="remote_upload_part")
    try:
        with reserve_file_descriptors(label="remote_upload_file"):
            with Path(local_path).open("rb") as handle:
                handle.seek(offset)
                payload = handle.read(size)
        if len(payload) != size:
            raise OSError(
                "remote upload source changed while reading: "
                f"expected {size} bytes at offset {offset}, got {len(payload)}"
            )
        if lease is None:
            return payload
        retained = _BudgetedUploadBytes(payload, lease)
        lease.resize(sys.getsizeof(retained))
        lease = None
        return retained
    finally:
        if lease is not None:
            lease.close()


def read_upload_part(local_path: str, index: int, part_bytes: int, file_size: int) -> bytes:
    """Read one indexed deterministic local-file part."""
    offset = index * part_bytes
    if offset >= file_size:
        return b""
    return read_upload_range(local_path, offset, min(part_bytes, file_size - offset), file_size)


__all__ = [
    "RemoteUploadPolicy",
    "read_upload_part",
    "release_upload_payload",
    "read_upload_range",
    "remote_upload_policy",
]
