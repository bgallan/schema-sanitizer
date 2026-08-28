"""Bounded local-file writers shared by synchronous and asynchronous providers.

It copies synchronous or asynchronous readers into governed local files, enforces
declared lengths, and removes partial output on failure.
"""

from __future__ import annotations

from collections.abc import Awaitable, Callable
from typing import Any

from ..core_impl.cancellation import check_operation_cancelled
from ..core_impl.memory_budget import acquire_operation_memory
from ..core_impl.temporary_storage import StreamingStorageReservation
from .io_footprint import open_remote_local_file


async def write_async_reader_to_file(
    reader: Callable[[int], Awaitable[bytes]],
    local_path: str,
    *,
    chunk_bytes: int,
    storage_reservation: StreamingStorageReservation | None = None,
    stage: str = "remote_transfer_chunk",
) -> int:
    """Read chunks asynchronously, reserving disk before every local write."""
    if storage_reservation is not None:
        storage_reservation.reset_after_truncate()
    written = 0
    with open_remote_local_file(local_path, "wb", label="remote_download_file") as file_handle:
        while True:
            check_operation_cancelled(stage=stage)
            lease = acquire_operation_memory(chunk_bytes, stage=stage)
            try:
                chunk = await reader(chunk_bytes)
                if not chunk:
                    break
                if lease is not None and len(chunk) > chunk_bytes:
                    lease.resize(len(chunk))
                if storage_reservation is not None:
                    storage_reservation.before_write(len(chunk))
                file_handle.write(chunk)
                written += len(chunk)
            finally:
                if lease is not None:
                    lease.release()
    if storage_reservation is not None:
        storage_reservation.finalize(written)
    return written


def write_sync_reader_to_file(
    reader: Callable[[int], bytes],
    local_path: str,
    *,
    chunk_bytes: int,
    storage_reservation: StreamingStorageReservation | None = None,
    stage: str = "remote_transfer_chunk",
) -> int:
    """Read chunks synchronously, reserving disk before every local write."""
    if storage_reservation is not None:
        storage_reservation.reset_after_truncate()
    written = 0
    with open_remote_local_file(local_path, "wb", label="remote_download_file") as file_handle:
        while True:
            check_operation_cancelled(stage=stage)
            lease = acquire_operation_memory(chunk_bytes, stage=stage)
            try:
                chunk = reader(chunk_bytes)
                if not chunk:
                    break
                if lease is not None and len(chunk) > chunk_bytes:
                    lease.resize(len(chunk))
                if storage_reservation is not None:
                    storage_reservation.before_write(len(chunk))
                file_handle.write(chunk)
                written += len(chunk)
            finally:
                if lease is not None:
                    lease.release()
    if storage_reservation is not None:
        storage_reservation.finalize(written)
    return written


async def write_async_iterator_to_file(
    iterator: Any,
    local_path: str,
    *,
    reservation_bytes: int,
    storage_reservation: StreamingStorageReservation | None = None,
) -> int:
    """Write an async chunk iterator with the same memory and disk admission."""

    async def read(_size: int) -> bytes:
        """Return the next asynchronous chunk, or empty bytes after iterator exhaustion."""
        try:
            return await anext(iterator)
        except StopAsyncIteration:
            return b""

    return await write_async_reader_to_file(
        read,
        local_path,
        chunk_bytes=max(1, reservation_bytes),
        storage_reservation=storage_reservation,
    )


__all__ = [
    "write_async_iterator_to_file",
    "write_async_reader_to_file",
    "write_sync_reader_to_file",
]
