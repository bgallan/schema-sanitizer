"""Operation-wide permits for bounded temporary filesystem usage."""

from __future__ import annotations

import shutil
import tempfile
from dataclasses import dataclass
from pathlib import Path
from threading import Lock

from ..errors import SchemaSanitizerResourceError
from .memory_budget import memory_budget

_MINIMUM_FREE_BYTES = 64 * 1024 * 1024


@dataclass(frozen=True, slots=True)
class TemporaryStorageSnapshot:
    """Immutable diagnostics for one temporary-storage permit pool."""

    limit_bytes: int
    reserved_bytes: int
    peak_reserved_bytes: int
    active_leases: int


class TemporaryStorageLease:
    """Own one byte reservation until its staged artifact is released."""

    def __init__(
        self,
        pool: TemporaryStoragePermitPool,
        reserved_bytes: int,
        *,
        label: str,
    ) -> None:
        """Store the pool, reservation size, and diagnostic label."""
        self._pool = pool
        self._reserved_bytes = reserved_bytes
        self.label = label
        self._released = False

    @property
    def reserved_bytes(self) -> int:
        """Return the currently reserved byte count."""
        return self._reserved_bytes

    def resize(self, size_bytes: int, *, path: str | Path | None = None) -> None:
        """Resize this reservation after the exact artifact size is known."""
        if self._released:
            raise RuntimeError("temporary-storage lease is already released")
        self._pool._resize(self, size_bytes, path=path)

    def release(self) -> None:
        """Return this reservation to the operation pool exactly once."""
        if self._released:
            return
        self._released = True
        self._pool._release(self._reserved_bytes)
        self._reserved_bytes = 0

    def __enter__(self) -> TemporaryStorageLease:
        """Return this active lease."""
        return self

    def __exit__(self, *_exc: object) -> None:
        """Release this lease."""
        self.release()


class TemporaryStoragePermitPool:
    """Bound operation-owned staging bytes without adding a public option."""

    def __init__(self, memory_limit_bytes: int | None) -> None:
        """Derive the spool ceiling from the canonical memory budget."""
        self.limit_bytes = memory_budget(memory_limit_bytes).replay_spool_bytes
        self._lock = Lock()
        self._reserved_bytes = 0
        self._peak_reserved_bytes = 0
        self._active_leases = 0
        self._closed = False

    def try_acquire(
        self,
        size_bytes: int,
        *,
        label: str,
        path: str | Path | None = None,
    ) -> TemporaryStorageLease | None:
        """Reserve bytes or return ``None`` while earlier artifacts hold space."""
        requested = self._normalize_size(size_bytes)
        self._validate_one_artifact(requested, label=label)
        self._ensure_filesystem_capacity(requested, path=path, label=label)
        with self._lock:
            if self._closed:
                raise RuntimeError("temporary-storage permit pool is closed")
            if self._reserved_bytes + requested > self.limit_bytes:
                return None
            self._reserved_bytes += requested
            self._peak_reserved_bytes = max(self._peak_reserved_bytes, self._reserved_bytes)
            self._active_leases += 1
        return TemporaryStorageLease(self, requested, label=label)

    def acquire(
        self,
        size_bytes: int,
        *,
        label: str,
        path: str | Path | None = None,
    ) -> TemporaryStorageLease:
        """Reserve bytes or raise when the operation window is exhausted."""
        lease = self.try_acquire(size_bytes, label=label, path=path)
        if lease is None:
            snapshot = self.snapshot()
            raise SchemaSanitizerResourceError(
                "temporary storage window exhausted: "
                f"{size_bytes} requested with {snapshot.reserved_bytes} bytes already "
                f"reserved and a {snapshot.limit_bytes}-byte operation limit",
                detail={
                    "stage": "temporary_storage",
                    "limit_name": "temporary_storage_bytes",
                    "limit_bytes": snapshot.limit_bytes,
                    "actual_bytes": snapshot.reserved_bytes + max(0, int(size_bytes)),
                    "artifact": label,
                },
            )
        return lease

    def snapshot(self) -> TemporaryStorageSnapshot:
        """Return current and peak reservations for diagnostics and tests."""
        with self._lock:
            return TemporaryStorageSnapshot(
                limit_bytes=self.limit_bytes,
                reserved_bytes=self._reserved_bytes,
                peak_reserved_bytes=self._peak_reserved_bytes,
                active_leases=self._active_leases,
            )

    def close(self) -> None:
        """Prevent new reservations while allowing outstanding leases to drain."""
        with self._lock:
            self._closed = True

    def _resize(
        self,
        lease: TemporaryStorageLease,
        size_bytes: int,
        *,
        path: str | Path | None,
    ) -> None:
        """Atomically resize one active lease under the operation ceiling."""
        requested = self._normalize_size(size_bytes)
        self._validate_one_artifact(requested, label=lease.label)
        growth = requested - lease._reserved_bytes
        if growth > 0:
            self._ensure_filesystem_capacity(growth, path=path, label=lease.label)
        with self._lock:
            if self._closed:
                raise RuntimeError("temporary-storage permit pool is closed")
            next_reserved = self._reserved_bytes + growth
            if next_reserved > self.limit_bytes:
                raise SchemaSanitizerResourceError(
                    "temporary storage limit exceeded after staging: "
                    f"{next_reserved} bytes > {self.limit_bytes} bytes",
                    detail={
                        "stage": "temporary_storage",
                        "limit_name": "temporary_storage_bytes",
                        "limit_bytes": self.limit_bytes,
                        "actual_bytes": next_reserved,
                        "artifact": lease.label,
                    },
                )
            self._reserved_bytes = next_reserved
            self._peak_reserved_bytes = max(self._peak_reserved_bytes, next_reserved)
            lease._reserved_bytes = requested

    def _release(self, size_bytes: int) -> None:
        """Release one active reservation without throwing from cleanup paths."""
        with self._lock:
            self._reserved_bytes = max(0, self._reserved_bytes - max(0, size_bytes))
            self._active_leases = max(0, self._active_leases - 1)

    def _validate_one_artifact(self, size_bytes: int, *, label: str) -> None:
        """Reject an artifact that cannot fit even in an otherwise empty pool."""
        if size_bytes <= self.limit_bytes:
            return
        raise SchemaSanitizerResourceError(
            "temporary storage limit exceeded: "
            f"{size_bytes} bytes > {self.limit_bytes} bytes; artifact: {label}",
            detail={
                "stage": "temporary_storage",
                "limit_name": "temporary_storage_bytes",
                "limit_bytes": self.limit_bytes,
                "actual_bytes": size_bytes,
                "artifact": label,
            },
        )

    @staticmethod
    def _normalize_size(size_bytes: int) -> int:
        """Return a non-negative integer reservation size."""
        if isinstance(size_bytes, bool) or not isinstance(size_bytes, int):
            raise TypeError("temporary-storage reservation must be an integer")
        if size_bytes < 0:
            raise ValueError("temporary-storage reservation must be >= 0")
        return size_bytes

    @staticmethod
    def _ensure_filesystem_capacity(
        growth_bytes: int,
        *,
        path: str | Path | None,
        label: str,
    ) -> None:
        """Keep a fixed free-space reserve before admitting staged growth."""
        if growth_bytes <= 0:
            return
        target = Path(path) if path is not None else Path(tempfile.gettempdir())
        if target.exists() and target.is_file():
            target = target.parent
        elif not target.exists():
            target = target.parent
        try:
            free_bytes = shutil.disk_usage(target).free
        except OSError as exc:
            raise OSError(f"unable to inspect temporary filesystem for {label!r}") from exc
        required = growth_bytes + _MINIMUM_FREE_BYTES
        if free_bytes < required:
            raise SchemaSanitizerResourceError(
                "temporary filesystem has insufficient free space: "
                f"{free_bytes} bytes available, {required} bytes required",
                detail={
                    "stage": "temporary_storage",
                    "limit_name": "filesystem_free_bytes",
                    "limit_bytes": free_bytes,
                    "actual_bytes": required,
                    "artifact": label,
                },
            )


__all__ = [
    "TemporaryStorageLease",
    "TemporaryStoragePermitPool",
    "TemporaryStorageSnapshot",
]
