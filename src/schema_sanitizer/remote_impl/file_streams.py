"""Bounded local-file writers shared by synchronous and asynchronous providers."""

from __future__ import annotations

from collections.abc import Awaitable, Callable
from pathlib import Path
from typing import Any

from ..core_impl.cancellation import check_operation_cancelled
from ..core_impl.memory_budget import acquire_operation_memory
from ..core_impl.process_resources import reserve_file_descriptors
from ..core_impl.temporary_storage import StreamingStorageReservation


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
    with reserve_file_descriptors(label="remote_download_file"):
        with Path(local_path).open("wb") as file_handle:
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
    with reserve_file_descriptors(label="remote_download_file"):
        with Path(local_path).open("wb") as file_handle:
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
        """Provide a deterministic test or worker helper."""
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
