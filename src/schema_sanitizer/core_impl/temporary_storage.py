"""Operation-wide permits for bounded temporary filesystem usage."""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path
from threading import Condition, Lock
from time import monotonic

from ..errors import SchemaSanitizerResourceError
from .cancellation import check_operation_cancelled
from .finalization import runtime_is_finalizing
from .memory_budget import memory_budget
from .safe_errors import add_bounded_note
from .temporary_storage_governor import (
    _MINIMUM_FREE_BYTES,
    _PROCESS_TEMPORARY_STORAGE,
    ProcessTemporaryStorageDiagnostics,
    ProcessTemporaryStorageSnapshot,
    process_temporary_storage_diagnostics,
    process_temporary_storage_snapshot,
)


@dataclass(frozen=True, slots=True)
class TemporaryStorageDiagnostics:
    """Cleanup anomalies and live bytes observed when an operation closes."""

    close_outstanding_bytes: int
    close_active_leases: int
    over_release_count: int
    over_release_bytes: int


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
        filesystem_key: int,
        filesystem_path: Path,
        inode_count: int,
        _active: bool = True,
    ) -> None:
        """Store the pool, reservation size, filesystem, and label."""
        self._pool = pool
        self._pid = os.getpid()
        self._reserved_bytes = reserved_bytes
        self._filesystem_key = filesystem_key
        self._filesystem_path = filesystem_path
        self._inode_count = max(0, int(inode_count))
        self.label = label
        self._lock = Lock()
        self._released = not _active

    def _activate(self, filesystem_key: int) -> None:
        """Publish an already-admitted lease without any rollback side effect."""
        self._filesystem_key = filesystem_key
        self._released = False

    @property
    def reserved_bytes(self) -> int:
        """Return the currently reserved byte count."""
        if os.getpid() != self._pid:
            return 0
        with self._lock:
            return self._reserved_bytes

    def resize(self, size_bytes: int, *, path: str | Path | None = None) -> None:
        """Resize this reservation atomically against concurrent release."""
        if os.getpid() != self._pid:
            raise RuntimeError("temporary-storage lease cannot be reused after fork")
        with self._lock:
            if self._released:
                raise RuntimeError("temporary-storage lease is already released")
            effective_path = self._filesystem_path if path is None else Path(path)
            requested, filesystem_key, filesystem_path = self._pool._resize(
                self._reserved_bytes,
                size_bytes,
                filesystem_key=self._filesystem_key,
                label=self.label,
                path=effective_path,
                inode_count=self._inode_count,
            )
            self._reserved_bytes = requested
            self._filesystem_key = filesystem_key
            self._filesystem_path = filesystem_path

    def adjust(self, delta_bytes: int, *, path: str | Path | None = None) -> int:
        """Atomically add or subtract bytes and return the new reservation."""
        if os.getpid() != self._pid:
            raise RuntimeError("temporary-storage lease cannot be reused after fork")
        if isinstance(delta_bytes, bool) or not isinstance(delta_bytes, int):
            raise TypeError("temporary-storage adjustment must be an integer")
        with self._lock:
            if self._released:
                raise RuntimeError("temporary-storage lease is already released")
            requested_size = self._reserved_bytes + delta_bytes
            if requested_size < 0:
                raise ValueError("temporary-storage adjustment exceeds the active lease")
            effective_path = self._filesystem_path if path is None else Path(path)
            requested, filesystem_key, filesystem_path = self._pool._resize(
                self._reserved_bytes,
                requested_size,
                filesystem_key=self._filesystem_key,
                label=self.label,
                path=effective_path,
                inode_count=self._inode_count,
            )
            self._reserved_bytes = requested
            self._filesystem_key = filesystem_key
            self._filesystem_path = filesystem_path
            return requested

    def release(self) -> None:
        """Return this reservation exactly once across competing threads."""
        if os.getpid() != self._pid:
            return
        with self._lock:
            if self._released:
                return
            reserved_bytes = self._reserved_bytes
            filesystem_key = self._filesystem_key
            inode_count = self._inode_count
            self._pool._release(
                reserved_bytes,
                filesystem_key=filesystem_key,
                inode_count=inode_count,
            )
            self._released = True
            self._reserved_bytes = 0
            self._inode_count = 0

    def __enter__(self) -> TemporaryStorageLease:
        """Return this active lease."""
        return self

    def __exit__(self, *_exc: object) -> None:
        """Release this lease."""
        self.release()

    def __del__(self) -> None:
        """Return abandoned space unless interpreter teardown has begun."""
        try:
            if runtime_is_finalizing():
                return
            self.release()
        except BaseException:
            pass


class StreamingStorageReservation:
    """Grow one shared storage lease before each streamed write.

    ``initial_credit_bytes`` is the caller's existing estimate for this file.
    Growth beyond that credit is reserved in amortized blocks before bytes reach
    disk. Finalization reconciles the shared lease to the exact file size.
    """

    def __init__(
        self,
        lease: TemporaryStorageLease | None,
        *,
        initial_credit_bytes: int,
        path: str | Path,
        quantum_bytes: int = 4 * 1024 * 1024,
    ) -> None:
        """Initialize this helper."""
        self._lease = lease
        self._credit = max(0, int(initial_credit_bytes))
        self._extra = 0
        self._written = 0
        self._path = Path(path)
        self._quantum = max(64 * 1024, int(quantum_bytes))
        self._lock = Lock()

    def before_write(self, chunk_bytes: int) -> None:
        """Reserve any required growth before writing one chunk."""
        amount = max(0, int(chunk_bytes))
        if amount == 0:
            return
        check_operation_cancelled(stage="temporary_stream_write")
        with self._lock:
            desired = self._written + amount
            covered = self._credit + self._extra
            if self._lease is not None and desired > covered:
                shortage = desired - covered
                growth = ((shortage + self._quantum - 1) // self._quantum) * self._quantum
                self._lease.adjust(growth, path=self._path)
                self._extra += growth
            self._written = desired

    def reset_after_truncate(self) -> None:
        """Return retry-only growth after a failed attempt truncates the file."""
        with self._lock:
            if self._lease is not None and self._extra:
                self._lease.adjust(-self._extra, path=self._path)
            self._extra = 0
            self._written = 0

    def finalize(self, actual_size_bytes: int | None = None) -> None:
        """Reconcile this file's credit to its exact retained size."""
        actual = (
            self._path.stat().st_size
            if actual_size_bytes is None
            else max(0, int(actual_size_bytes))
        )
        with self._lock:
            reserved = self._credit + self._extra
            if self._lease is not None and actual != reserved:
                self._lease.adjust(actual - reserved, path=self._path)
            self._credit = actual
            self._extra = 0
            self._written = actual


class TemporaryStoragePermitPool:
    """Bound operation-owned staging bytes without adding a public option."""

    def __init__(self, memory_limit_bytes: int | None) -> None:
        """Derive the spool ceiling from the canonical memory budget."""
        self.limit_bytes = memory_budget(memory_limit_bytes).replay_spool_bytes
        self._lock = Lock()
        self._condition = Condition(self._lock)
        self._reserved_bytes = 0
        self._pending_reserved_bytes = 0
        self._pending_active_leases = 0
        self._resize_inflight = 0
        self._pending_resize_growth = 0
        self._peak_reserved_bytes = 0
        self._active_leases = 0
        self._closed = False
        self._close_complete = False
        self._close_outstanding_bytes = 0
        self._close_active_leases = 0
        self._over_release_count = 0
        self._over_release_bytes = 0

    def try_acquire(
        self,
        size_bytes: int,
        *,
        label: str,
        path: str | Path | None = None,
        artifact_count: int = 1,
    ) -> TemporaryStorageLease | None:
        """Reserve bytes without holding the operation lock across filesystem I/O."""
        check_operation_cancelled(stage="temporary_storage_admission")
        requested = self._normalize_size(size_bytes)
        inode_count = self._normalize_artifact_count(artifact_count)
        self._validate_one_artifact(requested, label=label)
        filesystem_key, filesystem_path, _free_bytes = _PROCESS_TEMPORARY_STORAGE.filesystem(path)
        # Construct the only rollback owner before publishing either local or
        # process-wide accounting. It remains inert until both commits succeed.
        lease = TemporaryStorageLease(
            self,
            requested,
            label=label,
            filesystem_key=filesystem_key,
            filesystem_path=filesystem_path,
            inode_count=inode_count,
            _active=False,
        )
        with self._condition:
            if self._closed:
                raise RuntimeError("temporary-storage permit pool is closed")
            next_reserved = self._reserved_bytes + self._pending_reserved_bytes + requested
            if next_reserved > self.limit_bytes:
                return None
            self._pending_reserved_bytes += requested
            self._pending_active_leases += 1

        try:
            actual_filesystem_key = _PROCESS_TEMPORARY_STORAGE.reserve(
                requested,
                path=filesystem_path,
                label=label,
                inode_count=inode_count,
            )
        except BaseException:
            with self._condition:
                self._pending_reserved_bytes = max(0, self._pending_reserved_bytes - requested)
                self._pending_active_leases = max(0, self._pending_active_leases - 1)
                self._condition.notify_all()
            raise

        with self._condition:
            self._pending_reserved_bytes = max(0, self._pending_reserved_bytes - requested)
            self._pending_active_leases = max(0, self._pending_active_leases - 1)
            self._reserved_bytes += requested
            self._peak_reserved_bytes = max(self._peak_reserved_bytes, self._reserved_bytes)
            self._active_leases += 1
            lease._activate(actual_filesystem_key)
            self._condition.notify_all()
            return lease

    def acquire(
        self,
        size_bytes: int,
        *,
        label: str,
        path: str | Path | None = None,
        artifact_count: int = 1,
    ) -> TemporaryStorageLease:
        """Reserve bytes or raise when the operation window is exhausted."""
        lease = self.try_acquire(
            size_bytes,
            label=label,
            path=path,
            artifact_count=artifact_count,
        )
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

    def diagnostics(self) -> TemporaryStorageDiagnostics:
        """Return operation-local close and over-release anomalies."""
        with self._lock:
            return TemporaryStorageDiagnostics(
                self._close_outstanding_bytes,
                self._close_active_leases,
                self._over_release_count,
                self._over_release_bytes,
            )

    def close(self) -> None:
        """Stop admission and wait for already-started reservations to commit."""
        with self._condition:
            self._closed = True
            deadline = monotonic() + 30.0
            while self._pending_active_leases or getattr(self, "_resize_inflight", 0):
                remaining = deadline - monotonic()
                if remaining <= 0 or not self._condition.wait(timeout=remaining):
                    raise RuntimeError("temporary-storage admissions exceeded their close deadline")
            if self._close_complete:
                return
            self._close_outstanding_bytes = self._reserved_bytes
            self._close_active_leases = self._active_leases
            self._close_complete = True
            self._condition.notify_all()

    def _resize(
        self,
        current_bytes: int,
        size_bytes: int,
        *,
        filesystem_key: int,
        label: str,
        path: str | Path,
        inode_count: int,
    ) -> tuple[int, int, Path]:
        """Resize one lease without holding the pool lock across filesystem I/O."""
        check_operation_cancelled(stage="temporary_storage_resize")
        requested = self._normalize_size(size_bytes)
        self._validate_one_artifact(requested, label=label)
        target_key, target_path, _free = _PROCESS_TEMPORARY_STORAGE.filesystem(path)
        growth = requested - current_bytes
        moved = target_key != filesystem_key
        growth_charge = max(0, growth)

        with self._condition:
            if self._closed:
                raise RuntimeError("temporary-storage permit pool is closed")
            next_committed = self._reserved_bytes + growth
            admission_total = (
                self._reserved_bytes
                + self._pending_reserved_bytes
                + self._pending_resize_growth
                + growth_charge
            )
            if next_committed < 0 or admission_total > self.limit_bytes:
                raise SchemaSanitizerResourceError(
                    "temporary storage limit exceeded after staging: "
                    f"{max(next_committed, admission_total)} bytes > "
                    f"{self.limit_bytes} bytes",
                    detail={
                        "stage": "temporary_storage",
                        "limit_name": "temporary_storage_bytes",
                        "limit_bytes": self.limit_bytes,
                        "actual_bytes": max(next_committed, admission_total),
                        "artifact": label,
                    },
                )
            self._resize_inflight += 1
            self._pending_resize_growth += growth_charge

        new_reserved_bytes = 0
        new_reserved_inodes = 0
        try:
            if moved:
                target_key = _PROCESS_TEMPORARY_STORAGE.reserve(
                    requested,
                    path=path,
                    label=label,
                    inode_count=inode_count,
                )
                new_reserved_bytes = requested
                new_reserved_inodes = inode_count
            elif growth > 0:
                _PROCESS_TEMPORARY_STORAGE.reserve(growth, path=target_path, label=label)
                new_reserved_bytes = growth

            try:
                if moved:
                    _PROCESS_TEMPORARY_STORAGE.release(
                        filesystem_key,
                        current_bytes,
                        inode_count=inode_count,
                    )
                elif growth < 0:
                    _PROCESS_TEMPORARY_STORAGE.release(filesystem_key, -growth)
            except BaseException as primary:
                if new_reserved_bytes or new_reserved_inodes:
                    try:
                        _PROCESS_TEMPORARY_STORAGE.release(
                            target_key,
                            new_reserved_bytes,
                            inode_count=new_reserved_inodes,
                        )
                    except BaseException as cleanup_error:
                        add_bounded_note(
                            primary,
                            "temporary-storage speculative reservation rollback also failed",
                            cleanup_error,
                        )
                raise
        except BaseException:
            with self._condition:
                self._pending_resize_growth = max(0, self._pending_resize_growth - growth_charge)
                self._resize_inflight = max(0, self._resize_inflight - 1)
                self._condition.notify_all()
            raise

        with self._condition:
            self._pending_resize_growth = max(0, self._pending_resize_growth - growth_charge)
            self._resize_inflight = max(0, self._resize_inflight - 1)
            self._reserved_bytes += growth
            self._peak_reserved_bytes = max(self._peak_reserved_bytes, self._reserved_bytes)
            self._condition.notify_all()
        return requested, target_key, target_path

    def _release(
        self,
        size_bytes: int,
        *,
        filesystem_key: int,
        inode_count: int = 0,
    ) -> None:
        """Release one reservation while retaining cleanup anomaly diagnostics."""
        amount = max(0, int(size_bytes))
        # Keep local ownership visible while the device journal is pending, but
        # do not serialize unrelated filesystem releases behind this pool lock.
        _PROCESS_TEMPORARY_STORAGE.release(filesystem_key, amount, inode_count=inode_count)
        with self._condition:
            excess = max(0, amount - self._reserved_bytes)
            missing_lease = self._active_leases <= 0
            if excess or missing_lease:
                self._over_release_count += 1
                self._over_release_bytes += excess
            self._reserved_bytes = max(0, self._reserved_bytes - amount)
            self._active_leases = max(0, self._active_leases - 1)
            self._condition.notify_all()

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
    def _normalize_artifact_count(artifact_count: int) -> int:
        """Return a non-negative inode reservation count."""
        if isinstance(artifact_count, bool) or not isinstance(artifact_count, int):
            raise TypeError("temporary-storage artifact_count must be an integer")
        if artifact_count < 0:
            raise ValueError("temporary-storage artifact_count must be >= 0")
        return artifact_count

    @staticmethod
    def _ensure_filesystem_capacity(
        growth_bytes: int,
        *,
        path: str | Path | None,
        label: str,
    ) -> None:
        """Compatibility check retained for callers and test doubles."""
        if growth_bytes <= 0:
            return
        _device, _target, free_bytes = _PROCESS_TEMPORARY_STORAGE.filesystem(path)
        required = int(growth_bytes) + _MINIMUM_FREE_BYTES
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
    "ProcessTemporaryStorageDiagnostics",
    "ProcessTemporaryStorageSnapshot",
    "TemporaryStorageDiagnostics",
    "TemporaryStorageLease",
    "TemporaryStoragePermitPool",
    "TemporaryStorageSnapshot",
    "StreamingStorageReservation",
    "process_temporary_storage_diagnostics",
    "process_temporary_storage_snapshot",
]
