"""Composite remote-I/O resource footprints shared by sync and async paths.

It acquires memory, external, request, and descriptor capabilities as one unit and
releases or rolls them back in authoritative order.
"""

from __future__ import annotations

import threading
from contextlib import contextmanager
from contextvars import ContextVar
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterator

from ..core_impl.process_resources import (
    acquire_file_descriptors,
    open_governed_file,
    record_physical_file_descriptors_closed,
    record_physical_file_descriptors_opened,
    retain_uncertain_fd_close,
)
from ..core_impl.safe_errors import add_bounded_note


@dataclass(frozen=True, slots=True)
class RemoteIoFootprint:
    """Resources that must be admitted atomically before one remote operation.

    ``remote_weight`` is logical transport pressure and is intentionally
    independent from descriptor counts. ``network_fds`` represents transient
    network descriptors that can be active while the operation runs;
    ``local_file_fds`` represents files opened by the transfer itself.
    Provider-pool descriptors account persistent/control-plane ownership and
    are therefore not encoded in this footprint.
    """

    remote_weight: int = 1
    network_fds: int = 1
    local_file_fds: int = 0

    def __post_init__(self) -> None:
        """Validate and normalize the initialized instance state."""
        for name, value, allow_zero in (
            ("remote_weight", self.remote_weight, False),
            ("network_fds", self.network_fds, True),
            ("local_file_fds", self.local_file_fds, True),
        ):
            if type(value) is not int:
                raise TypeError(f"{name} must be an exact integer")
            if value < (0 if allow_zero else 1):
                relation = ">= 0" if allow_zero else "> 0"
                raise ValueError(f"{name} must be {relation}")

    @property
    def total_file_descriptors(self) -> int:
        """Return the total file descriptors."""
        return self.network_fds + self.local_file_fds


class ActiveRemoteIoFootprint:
    """Track local-file subcredits already covered by a composite FD lease."""

    __slots__ = ("footprint", "_local_in_use", "_lock", "_descriptor_lease")

    def __init__(self, footprint: RemoteIoFootprint, descriptor_lease: Any | None = None) -> None:
        """Bind the composite descriptor lease and start with no local subcredits borrowed."""
        self.footprint = footprint
        self._local_in_use = 0
        self._lock = threading.Lock()
        self._descriptor_lease = descriptor_lease

    def retain_descriptor_lease_as_debt(self, *, label: str) -> bool:
        """Terminally retain the composite lease after an uncertain local close."""
        with self._lock:
            lease = self._descriptor_lease
        if lease is None:
            return True
        retained = retain_uncertain_fd_close(lease, label=label)
        if retained:
            with self._lock:
                if self._descriptor_lease is lease:
                    self._descriptor_lease = None
        return retained

    def release_descriptor_lease(self) -> None:
        """Release the composite FD lease only after all borrowed local FDs retire."""
        with self._lock:
            lease = self._descriptor_lease
            local_in_use = self._local_in_use
        if lease is None:
            return
        if local_in_use != 0:
            raise RuntimeError(
                "cannot release remote descriptor footprint while local files remain open"
            )
        lease.release()
        with self._lock:
            if self._descriptor_lease is lease:
                self._descriptor_lease = None

    @contextmanager
    def borrow_local_file_descriptor(self, *, label: str) -> Iterator[None]:
        """Borrow local file descriptor."""
        borrowed = False
        with self._lock:
            if self._local_in_use < self.footprint.local_file_fds:
                self._local_in_use += 1
                borrowed = True
        if borrowed:
            try:
                yield
            finally:
                with self._lock:
                    self._local_in_use = max(0, self._local_in_use - 1)
            return

        # Footprint under-declaration is a contract violation. Acquiring
        # another FD while this operation already owns part of the same composite
        # resource can create a circular wait under low capacity. Fail before the
        # physical open instead of recursively acquiring from the same governor.
        raise RuntimeError(f"remote I/O footprint under-declared local file descriptor for {label}")


_ACTIVE_REMOTE_IO_FOOTPRINT: ContextVar[ActiveRemoteIoFootprint | None] = ContextVar(
    "schema_sanitizer_active_remote_io_footprint", default=None
)


@contextmanager
def activate_remote_io_footprint(owner: ActiveRemoteIoFootprint) -> Iterator[None]:
    """Publish one already-admitted footprint to provider/local-file helpers."""
    token = _ACTIVE_REMOTE_IO_FOOTPRINT.set(owner)
    try:
        yield
    finally:
        _ACTIVE_REMOTE_IO_FOOTPRINT.reset(token)


@contextmanager
def open_remote_local_file(
    path: str | Path,
    mode: str = "rb",
    *args: Any,
    label: str = "remote_local_file",
    **kwargs: Any,
) -> Iterator[Any]:
    """Open a local transfer file from the already-admitted FD footprint.

    Standalone callers fall back to ``open_governed_file``.  Submitted remote
    operations borrow their pre-admitted local-file subcredit, record the
    physical descriptor only after ``open`` succeeds, and retain the exact
    composite lease if close cannot be proven.
    """
    owner = _ACTIVE_REMOTE_IO_FOOTPRINT.get()
    if owner is None:
        with open_governed_file(path, mode, *args, **kwargs) as governed_handle:
            yield governed_handle
        return

    with owner.borrow_local_file_descriptor(label=label):
        handle: Any | None = None
        opened = False
        try:
            handle = open(path, mode, *args, **kwargs)
            record_physical_file_descriptors_opened(1)
            opened = True
            yield handle
        except BaseException as primary:
            if handle is not None:
                try:
                    handle.close()
                except BaseException as cleanup_error:
                    if opened:
                        try:
                            owner.retain_descriptor_lease_as_debt(label=label)
                        except BaseException as debt_error:
                            add_bounded_note(
                                primary,
                                f"{label} descriptor debt retention also failed",
                                debt_error,
                            )
                    add_bounded_note(primary, f"{label} physical close also failed", cleanup_error)
                else:
                    if opened:
                        record_physical_file_descriptors_closed(1)
                        opened = False
            raise
        else:
            assert handle is not None
            try:
                handle.close()
            except BaseException:
                if opened:
                    owner.retain_descriptor_lease_as_debt(label=label)
                raise
            else:
                if opened:
                    record_physical_file_descriptors_closed(1)


@contextmanager
def reserve_remote_local_file_descriptor(*, label: str = "remote_local_file") -> Iterator[None]:
    """Consume a local-file subcredit or acquire a standalone FD lease."""
    owner = _ACTIVE_REMOTE_IO_FOOTPRINT.get()
    if owner is None:
        with acquire_file_descriptors(1):
            yield
        return
    with owner.borrow_local_file_descriptor(label=label):
        yield


__all__ = [
    "ActiveRemoteIoFootprint",
    "RemoteIoFootprint",
    "activate_remote_io_footprint",
    "open_remote_local_file",
    "reserve_remote_local_file_descriptor",
]
