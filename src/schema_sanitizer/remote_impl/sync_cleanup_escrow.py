"""Bounded terminal ownership for synchronous provider resources.

Cleanup reserves an escrow slot and its network-FD capability *before* an SDK
resource is created.  A constructor, context-body, physical close, or logical
release failure therefore always leaves one authoritative retry owner.
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from threading import Lock
from time import monotonic_ns
from typing import Any

from ..core_impl.process_resources import acquire_file_descriptors
from ..core_impl.runtime_registry import (
    RuntimeCloseStatus,
    RuntimeServicePhase,
    register_runtime_service,
)
from ..core_impl.safe_errors import clear_exception_traceback
from ..core_impl.shutdown_observers import register_shutdown_observer

_CAPACITY = 256
_FREE = 0
_RESERVED = 1
_LIVE = 2


@dataclass(frozen=True, slots=True)
class SyncCleanupEscrowSnapshot:
    """Report reserved/live synchronous provider cleanup ownership."""

    capacity: int
    active: int
    reserved: int
    live: int
    rejected: int
    retries: int
    oldest_live_ns: int


class _Slot:
    __slots__ = (
        "state",
        "owner",
        "descriptor_lease",
        "label",
        "created_ns",
        "attempts",
    )

    def __init__(self) -> None:
        self.state = _FREE
        self.owner: Any | None = None
        self.descriptor_lease: Any | None = None
        self.label = ""
        self.created_ns = 0
        self.attempts = 0

    def reset(self) -> None:
        if self.owner is not None or self.descriptor_lease is not None:
            raise RuntimeError("cannot recycle live synchronous cleanup owner")
        self.state = _FREE
        self.label = ""
        self.created_ns = 0
        self.attempts = 0


class SyncCleanupReservation:
    """Exact pre-reserved slot for one synchronous SDK owner."""

    __slots__ = ("_escrow", "_index", "_pid", "_closed")

    def __init__(self, escrow: "_SyncCleanupEscrow", index: int) -> None:
        self._escrow = escrow
        self._index = index
        self._pid = os.getpid()
        self._closed = False

    def bind_owner(self, owner: object) -> None:
        if self._closed or os.getpid() != self._pid:
            raise RuntimeError("synchronous cleanup reservation is not bindable")
        self._escrow.bind_owner(self._index, owner)

    def close_and_commit(self) -> None:
        if self._closed or os.getpid() != self._pid:
            return
        self._escrow.close_slot(self._index)
        self._closed = True

    def abandon_to_escrow(self) -> None:
        """Leave the live slot published for runtime-shutdown retry."""
        self._closed = True

    @property
    def slot_index(self) -> int:
        return self._index


class _SyncCleanupEscrow:
    def __init__(self) -> None:
        self._pid = os.getpid()
        self._lock = Lock()
        self._slots = [_Slot() for _ in range(_CAPACITY)]
        self._cursor = 0
        self._rejected = 0
        self._retries = 0

    def _ensure_process(self) -> None:
        if self._pid != os.getpid():
            # Quarantine inherited resources: never close/release parent-owned
            # sockets or capabilities in the child.
            raise RuntimeError("synchronous provider cleanup escrow is quarantined after fork")

    def reserve(self, *, label: str, network_fds: int = 1) -> SyncCleanupReservation:
        self._ensure_process()
        if type(network_fds) is not int or network_fds <= 0:
            raise ValueError("synchronous cleanup network_fds must be a positive integer")
        index = -1
        with self._lock:
            start = self._cursor
            for offset in range(_CAPACITY):
                candidate = (start + offset) % _CAPACITY
                slot = self._slots[candidate]
                if slot.state == _FREE:
                    slot.state = _RESERVED
                    slot.label = str(label)[:128]
                    slot.created_ns = monotonic_ns()
                    self._cursor = (candidate + 1) % _CAPACITY
                    index = candidate
                    break
            if index < 0:
                self._rejected += 1
                raise RuntimeError("synchronous provider cleanup escrow exhausted")
        try:
            lease = acquire_file_descriptors(network_fds)
        except BaseException:
            with self._lock:
                slot = self._slots[index]
                slot.owner = None
                slot.descriptor_lease = None
                slot.reset()
            raise
        with self._lock:
            slot = self._slots[index]
            if slot.state != _RESERVED:
                try:
                    lease.release()
                finally:
                    raise RuntimeError("synchronous cleanup reservation state corrupted")
            slot.descriptor_lease = lease
        return SyncCleanupReservation(self, index)

    def bind_owner(self, index: int, owner: object) -> None:
        with self._lock:
            slot = self._slots[index]
            if slot.state != _RESERVED or slot.owner is not None:
                raise RuntimeError("synchronous cleanup slot is not reserved")
            slot.owner = owner
            slot.state = _LIVE

    def close_slot(self, index: int) -> None:
        self._ensure_process()
        with self._lock:
            slot = self._slots[index]
            if slot.state == _FREE:
                return
            owner = slot.owner
            lease = slot.descriptor_lease
            slot.attempts += 1
            if slot.attempts > 1:
                self._retries += 1
        first_error: BaseException | None = None
        if owner is not None:
            close = getattr(owner, "close", None)
            if not callable(close):
                first_error = TypeError("synchronous cleanup owner exposes no close()")
            else:
                try:
                    close()
                except BaseException as exc:
                    first_error = exc
        if first_error is None and lease is not None:
            try:
                lease.release()
            except BaseException as exc:
                first_error = exc
            else:
                lease = None
        if first_error is not None:
            # No authoritative state is retired on failure.
            raise first_error
        with self._lock:
            slot = self._slots[index]
            slot.owner = None
            slot.descriptor_lease = None
            slot.reset()

    def retry_noexcept(self) -> int:
        progressed = 0
        for index in range(_CAPACITY):
            with self._lock:
                state = self._slots[index].state
                owner = self._slots[index].owner
            if state == _RESERVED and owner is None:
                continue
            if state == _FREE:
                continue
            try:
                self.close_slot(index)
            except BaseException as exc:
                clear_exception_traceback(exc)
                continue
            progressed += 1
        return progressed

    def snapshot(self) -> SyncCleanupEscrowSnapshot:
        reserved = 0
        live = 0
        oldest = 0
        with self._lock:
            for slot in self._slots:
                if slot.state == _RESERVED:
                    reserved += 1
                elif slot.state == _LIVE:
                    live += 1
                if (
                    slot.state != _FREE
                    and slot.created_ns
                    and (oldest == 0 or slot.created_ns < oldest)
                ):
                    oldest = slot.created_ns
            return SyncCleanupEscrowSnapshot(
                _CAPACITY,
                reserved + live,
                reserved,
                live,
                self._rejected,
                self._retries,
                oldest,
            )

    def _runtime_shutdown(self, *, deadline_seconds: float) -> RuntimeCloseStatus:
        del deadline_seconds
        before = self.snapshot().active
        if before == 0:
            return RuntimeCloseStatus.QUIESCENT
        progressed = self.retry_noexcept()
        after = self.snapshot().active
        if after == 0:
            return RuntimeCloseStatus.QUIESCENT
        return RuntimeCloseStatus.PROGRESS if progressed else RuntimeCloseStatus.RETRY


_SYNC_CLEANUP_ESCROW = _SyncCleanupEscrow()
_RUNTIME_REGISTRATION = register_runtime_service(
    _SYNC_CLEANUP_ESCROW,
    kind="sync_provider_cleanup_escrow",
    close_name="_runtime_shutdown",
    phase=RuntimeServicePhase.CLEANUP_PRODUCER,
    priority=10,
)


def process_sync_cleanup_escrow_snapshot() -> SyncCleanupEscrowSnapshot:
    """Return the process escrow snapshot through a reload-stable callback."""
    return _SYNC_CLEANUP_ESCROW.snapshot()


register_shutdown_observer("sync_provider_cleanup_escrow", process_sync_cleanup_escrow_snapshot)


def reserve_sync_cleanup(*, label: str, network_fds: int = 1) -> SyncCleanupReservation:
    """Reserve terminal ownership and FD credit before constructing an SDK owner."""
    _SYNC_CLEANUP_ESCROW.retry_noexcept()
    return _SYNC_CLEANUP_ESCROW.reserve(label=label, network_fds=network_fds)


__all__ = [
    "SyncCleanupEscrowSnapshot",
    "SyncCleanupReservation",
    "process_sync_cleanup_escrow_snapshot",
    "reserve_sync_cleanup",
]
