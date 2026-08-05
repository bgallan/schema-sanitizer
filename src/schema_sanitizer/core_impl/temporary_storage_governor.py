"""Process and optional host-wide temporary filesystem admission."""

from __future__ import annotations

import os
import shutil
import tempfile
from dataclasses import dataclass, field
from pathlib import Path
from threading import Lock

from ..errors import SchemaSanitizerResourceError
from .cross_process_storage import (
    cross_process_storage_directory,
    cross_process_storage_enabled,
    release_cross_process,
    reserve_cross_process,
)
from .fork_safety import quarantine_inherited_state
from .safety_margins import record_resource_telemetry, tuned_temporary_free_bytes

_MINIMUM_FREE_BYTES = 64 * 1024 * 1024


def _minimum_free_bytes() -> int:
    """Return the bounded telemetry-tuned emergency disk reserve."""
    return tuned_temporary_free_bytes(_MINIMUM_FREE_BYTES)


@dataclass(frozen=True, slots=True)
class ProcessTemporaryStorageSnapshot:
    """Aggregate temporary-storage reservations for one filesystem."""

    capacity_bytes: int
    reserved_bytes: int
    peak_reserved_bytes: int
    capacity_inodes: int
    reserved_inodes: int
    peak_reserved_inodes: int


@dataclass(frozen=True, slots=True)
class ProcessTemporaryStorageDiagnostics:
    """Cleanup anomalies observed by the process filesystem governor."""

    over_release_count: int
    over_release_bytes: int


@dataclass(slots=True)
class _FilesystemReservationState:
    """Mutable process-wide reservation state for one filesystem device."""

    capacity_bytes: int
    capacity_inodes: int
    reserved_bytes: int = 0
    peak_reserved_bytes: int = 0
    reserved_inodes: int = 0
    peak_reserved_inodes: int = 0
    cross_process_enabled: bool = False
    coordination_directory: Path | None = None
    users: int = 0
    lock: object = field(default_factory=Lock, repr=False, compare=False)


_FORKED_STORAGE_KEEPALIVE: list[object] = []


class _ProcessTemporaryStorageGovernor:
    """Serialize temporary-space admission across independent operations."""

    def __init__(self) -> None:
        """Create an empty device-indexed reservation registry."""
        self._lock = Lock()
        self._states: dict[int, _FilesystemReservationState] = {}
        self._over_release_count = 0
        self._over_release_bytes = 0

    def _borrow_state(
        self,
        device: int,
        *,
        create: _FilesystemReservationState | None = None,
    ) -> _FilesystemReservationState | None:
        """Retain one device state without holding the registry across I/O."""
        with self._lock:
            state = self._states.get(device)
            if state is None and create is not None:
                state = create
                self._states[device] = state
            if state is not None:
                state.users += 1
            return state

    def _return_state(self, device: int, state: _FilesystemReservationState) -> None:
        """Drop one borrowed reference and retire an idle device safely."""
        with self._lock:
            state.users = max(0, state.users - 1)
            if (
                state.users == 0
                and state.reserved_bytes == 0
                and state.reserved_inodes == 0
                and self._states.get(device) is state
            ):
                self._states.pop(device, None)

    @staticmethod
    def target(path: str | Path | None) -> Path:
        """Return the nearest existing directory used for capacity checks."""
        target = Path(path) if path is not None else Path(tempfile.gettempdir())
        if target.exists() and target.is_file():
            target = target.parent
        while not target.exists() and target != target.parent:
            target = target.parent
        if not target.exists():
            raise OSError(f"unable to locate temporary filesystem for {path!r}")
        return target

    @classmethod
    def filesystem(cls, path: str | Path | None) -> tuple[int, Path, int]:
        """Resolve a device key, existing target, and currently free bytes."""
        target = cls.target(path)
        try:
            device = int(os.stat(target).st_dev)
            free_bytes = int(shutil.disk_usage(target).free)
        except OSError as exc:
            raise OSError(f"unable to inspect temporary filesystem at {target}") from exc
        return device, target, free_bytes

    @staticmethod
    def free_inodes(path: Path) -> int:
        """Return available inodes, or a conservative large fallback."""
        try:
            stats = os.statvfs(path)
            available = int(stats.f_favail)
            return available if available > 0 else 1 << 30
        except (AttributeError, OSError):
            return 1 << 30

    def reserve(
        self,
        size_bytes: int,
        *,
        path: str | Path | None,
        label: str,
        inode_count: int = 0,
    ) -> int:
        """Reserve bytes on one device without stalling unrelated devices."""
        requested = max(0, int(size_bytes))
        requested_inodes = max(0, int(inode_count))
        device, target, free_bytes = self.filesystem(path)
        free_inodes = self.free_inodes(target)
        if requested == 0 and requested_inodes == 0:
            return device
        state = self._borrow_state(device)
        if state is None:
            cross_process_enabled = cross_process_storage_enabled()
            candidate = _FilesystemReservationState(
                capacity_bytes=max(0, free_bytes - _minimum_free_bytes()),
                capacity_inodes=max(0, free_inodes - min(1024, max(32, free_inodes // 100))),
                cross_process_enabled=cross_process_enabled,
                coordination_directory=(
                    cross_process_storage_directory() if cross_process_enabled else None
                ),
            )
            state = self._borrow_state(device, create=candidate)
        assert state is not None
        try:
            with state.lock:  # type: ignore[attr-defined]
                current_headroom = max(0, free_bytes - _minimum_free_bytes())
                effective_capacity = min(
                    state.capacity_bytes, state.reserved_bytes + current_headroom
                )
                next_reserved = state.reserved_bytes + requested
                current_inode_headroom = max(
                    0, free_inodes - min(1024, max(32, free_inodes // 100))
                )
                effective_inode_capacity = min(
                    state.capacity_inodes,
                    state.reserved_inodes + current_inode_headroom,
                )
                next_inodes = state.reserved_inodes + requested_inodes
                if next_reserved > effective_capacity:
                    self._raise_exhausted(
                        next_reserved,
                        effective_capacity,
                        label,
                        cross_process=False,
                    )
                if next_inodes > effective_inode_capacity:
                    raise SchemaSanitizerResourceError(
                        "temporary filesystem inode capacity exhausted: "
                        f"{next_inodes} inodes > {effective_inode_capacity} inodes; "
                        f"artifact: {label}",
                        detail={
                            "stage": "temporary_storage",
                            "limit_name": "filesystem_free_inodes",
                            "limit_bytes": effective_inode_capacity,
                            "actual_bytes": next_inodes,
                            "artifact": label,
                        },
                    )
                try:
                    reserve_cross_process(
                        device,
                        requested,
                        effective_capacity,
                        inode_count=requested_inodes,
                        inode_capacity=effective_inode_capacity,
                        enabled=state.cross_process_enabled,
                        coordination_directory=state.coordination_directory,
                    )
                except OSError as exc:
                    if "inode" in str(exc).lower():
                        raise SchemaSanitizerResourceError(
                            str(exc),
                            detail={
                                "stage": "temporary_storage",
                                "limit_name": ("cross_process_temporary_storage_inodes"),
                                "limit_bytes": effective_inode_capacity,
                                "actual_bytes": next_inodes,
                                "artifact": label,
                            },
                        ) from exc
                    self._raise_exhausted(
                        next_reserved,
                        effective_capacity,
                        label,
                        cross_process=True,
                        message=str(exc),
                    )
                state.reserved_bytes = next_reserved
                state.peak_reserved_bytes = max(state.peak_reserved_bytes, next_reserved)
                state.reserved_inodes = next_inodes
                state.peak_reserved_inodes = max(state.peak_reserved_inodes, next_inodes)
        finally:
            self._return_state(device, state)
        record_resource_telemetry(
            temporary_free_floor_bytes=max(_MINIMUM_FREE_BYTES, requested),
            source="temporary_reservation",
        )
        return device

    @staticmethod
    def _raise_exhausted(
        actual: int,
        capacity: int,
        label: str,
        *,
        cross_process: bool,
        message: str | None = None,
    ) -> None:
        """Raise one stable public capacity error."""
        limit_name = (
            "cross_process_temporary_storage_bytes"
            if cross_process
            else "process_temporary_storage_bytes"
        )
        raise SchemaSanitizerResourceError(
            message
            or f"process temporary-storage capacity exhausted: "
            f"{actual} bytes > {capacity} bytes; artifact: {label}",
            detail={
                "stage": "temporary_storage",
                "limit_name": limit_name,
                "limit_bytes": capacity,
                "actual_bytes": actual,
                "artifact": label,
            },
        )

    def release(self, device: int, size_bytes: int, *, inode_count: int = 0) -> None:
        """Release one device without serializing unrelated filesystems."""
        amount = max(0, int(size_bytes))
        amount_inodes = max(0, int(inode_count))
        if amount == 0 and amount_inodes == 0:
            return
        state = self._borrow_state(device)
        if state is None:
            with self._lock:
                self._over_release_count += 1
                self._over_release_bytes += amount
            return
        excess = 0
        excess_inodes = False
        try:
            with state.lock:  # type: ignore[attr-defined]
                released = min(amount, state.reserved_bytes)
                excess = max(0, amount - state.reserved_bytes)
                released_inodes = min(amount_inodes, state.reserved_inodes)
                excess_inodes = amount_inodes > state.reserved_inodes
                next_reserved = max(0, state.reserved_bytes - amount)
                next_inodes = max(0, state.reserved_inodes - amount_inodes)
                release_cross_process(
                    device,
                    released,
                    inode_count=released_inodes,
                    enabled=state.cross_process_enabled,
                    coordination_directory=state.coordination_directory,
                )
                state.reserved_bytes = next_reserved
                state.reserved_inodes = next_inodes
        finally:
            self._return_state(device, state)
        if excess or excess_inodes:
            with self._lock:
                if excess:
                    self._over_release_count += 1
                    self._over_release_bytes += excess
                if excess_inodes:
                    self._over_release_count += 1

    def diagnostics(self) -> ProcessTemporaryStorageDiagnostics:
        """Return aggregate process-governor cleanup anomalies."""
        with self._lock:
            return ProcessTemporaryStorageDiagnostics(
                self._over_release_count, self._over_release_bytes
            )

    def snapshot(self, path: str | Path | None) -> ProcessTemporaryStorageSnapshot:
        """Return one device snapshot without blocking other filesystems."""
        device, target, free_bytes = self.filesystem(path)
        state = self._borrow_state(device)
        if state is None:
            free_inodes = self.free_inodes(target)
            return ProcessTemporaryStorageSnapshot(
                capacity_bytes=max(0, free_bytes - _minimum_free_bytes()),
                reserved_bytes=0,
                peak_reserved_bytes=0,
                capacity_inodes=max(0, free_inodes - min(1024, max(32, free_inodes // 100))),
                reserved_inodes=0,
                peak_reserved_inodes=0,
            )
        try:
            with state.lock:  # type: ignore[attr-defined]
                return ProcessTemporaryStorageSnapshot(
                    capacity_bytes=state.capacity_bytes,
                    reserved_bytes=state.reserved_bytes,
                    peak_reserved_bytes=state.peak_reserved_bytes,
                    capacity_inodes=state.capacity_inodes,
                    reserved_inodes=state.reserved_inodes,
                    peak_reserved_inodes=state.peak_reserved_inodes,
                )
        finally:
            self._return_state(device, state)

    def reset_after_fork(self) -> None:
        """Quarantine inherited reservations without running child finalizers."""
        quarantine_inherited_state("temporary-storage", self._states)
        self._lock = Lock()
        self._states = {}
        self._over_release_count = 0
        self._over_release_bytes = 0


_PROCESS_TEMPORARY_STORAGE = _ProcessTemporaryStorageGovernor()

if hasattr(os, "register_at_fork"):
    os.register_at_fork(after_in_child=_PROCESS_TEMPORARY_STORAGE.reset_after_fork)


def process_temporary_storage_snapshot(
    path: str | Path | None = None,
) -> ProcessTemporaryStorageSnapshot:
    """Return process-wide temporary-space accounting for one filesystem."""
    return _PROCESS_TEMPORARY_STORAGE.snapshot(path)


def process_temporary_storage_diagnostics() -> ProcessTemporaryStorageDiagnostics:
    """Return process-wide temporary-storage cleanup anomalies."""
    return _PROCESS_TEMPORARY_STORAGE.diagnostics()


__all__ = [
    "ProcessTemporaryStorageDiagnostics",
    "ProcessTemporaryStorageSnapshot",
    "_PROCESS_TEMPORARY_STORAGE",
    "process_temporary_storage_diagnostics",
    "process_temporary_storage_snapshot",
]
