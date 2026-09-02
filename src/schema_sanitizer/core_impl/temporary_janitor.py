"""Quarantine temporary artifacts that resist immediate deletion and retry cleanup.

Descriptor-pinned identities preserve exact storage and file-descriptor ownership while a
governed worker handles retries, diagnostics, shutdown, and fork recovery.
"""

from __future__ import annotations

import atexit
import errno
import os
import shutil
import stat
import sys
import tempfile
import threading
import time
import uuid
from collections import deque
from collections.abc import Iterator
from dataclasses import dataclass
from dataclasses import field as dataclass_field
from pathlib import Path
from typing import TYPE_CHECKING, Protocol

from .control_plane_budget import (
    ControlPlaneTicket,
    release_control_plane,
    reserve_control_plane,
)
from .durations import deadline_ns_from_timeout, remaining_seconds
from .fork_manager import register_fork_handler
from .fork_safety import ensure_runtime_fork_safe, quarantine_inherited_state
from .governed_thread import defer_governed_thread_retirement, start_governed_thread
from .path_identity import (
    PathIdentity,
    claim_path_identity,
    lstat_identity,
    release_path_identity,
    transfer_identity_matches,
)
from .process_resources import (
    AvailabilityEvent,
    acquire_file_descriptor_capability,
    acquire_project_threads,
    acquire_teardown_file_descriptors,
    acquire_teardown_project_threads,
    record_physical_file_descriptors_closed,
    record_physical_file_descriptors_opened,
    register_project_thread_availability,
    retain_uncertain_fd_close,
    unregister_project_thread_availability,
)
from .retry_scheduler import adopt_failed_release, cancel_retry, schedule_retry
from .safe_errors import add_bounded_note, clear_exception_traceback

if TYPE_CHECKING:
    from .temporary_storage import TemporaryStorageLease

_ENV_DIRECTORY = "SCHEMA_SANITIZER_COORDINATION_DIR"
_RETRY_SECONDS = 1.0
_SWEEP_BATCH_SIZE = 64
_MAX_FAILED_THREAD_LEASES = 1
_MAX_PENDING_ARTIFACTS = 4096
_MAX_PENDING_METADATA_BYTES = 8 * 1024 * 1024
_PENDING_METADATA_BASE = 512
_MAX_JANITOR_RETRY_OWNERS = 1024
_JANITOR_RETRY_OWNERS_LOCK = threading.Lock()
_JANITOR_RETRY_OWNERS: dict[int, object] = {}
_JANITOR_RETRY_FORK_BANKS: tuple[tuple[threading.Lock, dict[int, object]], ...] = (
    (threading.Lock(), {}),
    (threading.Lock(), {}),
)
_JANITOR_RETRY_FORK_BANK_INDEX = 0
_JANITOR_RETRY_FORK_PREPARED: tuple[threading.Lock, dict[int, object]] | None = None


def _retry_janitor_owner(token: int) -> None:
    """Retry cleanup for a retained temporary-artifact owner."""
    with _JANITOR_RETRY_OWNERS_LOCK:
        owner = _JANITOR_RETRY_OWNERS.pop(token, None)
    if owner is None:
        return
    owner._retry_worker_start()  # type: ignore[attr-defined]


@dataclass(frozen=True, slots=True)
class TemporaryJanitorSnapshot:
    """Current cleanup backlog and cumulative janitor outcomes."""

    pending_artifacts: int
    quarantined_artifacts: int
    deleted_artifacts: int
    failed_attempts: int
    identity_mismatches: int = 0
    worker_start_failures: int = 0
    pending_without_worker: int = 0
    oldest_pending_age_seconds: float = 0.0
    failed_thread_leases: int = 0
    stale_private_artifacts: int = 0
    pending_metadata_bytes: int = 0
    rejected_artifacts: int = 0
    scan_fd_active: bool = False
    failed_scan_fd_leases: int = 0
    root_identity_mismatches: int = 0
    worker_alive: bool = False
    worker_starting: bool = False
    root_fd_active: bool = False
    worker_retiring: bool = False
    protocol_violations: int = 0


def _replace_into_root(source: Path, target_name: str, handle: _QuarantineRootHandle) -> None:
    """Publish relative to the pinned quarantine root."""
    os.replace(source, target_name, dst_dir_fd=handle.descriptor)


def _replace_from_root(source_name: str, target: Path, handle: _QuarantineRootHandle) -> None:
    """Move a quarantined entry relative to the trusted root descriptor."""
    os.replace(source_name, target, src_dir_fd=handle.descriptor)


class _StorageLeaseOwner(Protocol):
    def release(self) -> None:
        """Release exact storage ownership retained by this owner."""


@dataclass(slots=True)
class _PendingArtifact:
    """Internal _PendingArtifact helper."""

    path: Path
    is_dir: bool
    lease: _StorageLeaseOwner
    identity: PathIdentity | None = None
    enqueued_at: float = 0.0
    metadata_bytes: int = 0
    control_ticket: ControlPlaneTicket | None = None


class _StaleArtifactLease:
    """No-op storage owner for crash leftovers with no live process lease."""

    __slots__ = ()

    def release(self) -> None:
        """Release resources owned by this stale artifact lease."""
        return None


_STALE_ARTIFACT_LEASE = _StaleArtifactLease()

_FORKED_JANITOR_KEEPALIVE: list[tuple[object, ...]] = []
_FAILED_SCAN_FD_LEASES: deque[object] = deque()
_FAILED_SCAN_FD_LEASES_LOCK = threading.Lock()
_MAX_FAILED_SCAN_FD_LEASES = 2
_ROOT_IDENTITY_MISMATCHES = 0


@dataclass(slots=True)
class _QuarantineRootHandle:
    path: Path
    descriptor: int
    lease: object
    device: int
    inode: int
    pid: int


_ROOT_HANDLE_LOCK = threading.Lock()
_ROOT_HANDLE: _QuarantineRootHandle | None = None
_FORKED_ROOT_HANDLES: list[_QuarantineRootHandle] = []
_RETIRED_ROOT_HANDLES: list[_QuarantineRootHandle] = []
_MAX_ROOT_GENERATIONS = 4


class _Releasable(Protocol):
    def release(self) -> None:
        """Release resources owned by this releasable."""
        ...


_CLOSING_ROOT_OWNERS: deque[_Releasable] = deque()
_MAX_CLOSING_ROOT_OWNERS = _MAX_ROOT_GENERATIONS


def _configured_root_location() -> tuple[Path, str, Path]:
    """Return the configured quarantine root and whether it was explicit."""
    configured_base = os.getenv(_ENV_DIRECTORY)
    base = Path(configured_base or tempfile.gettempdir())
    getuid = getattr(os, "geteuid", None)
    uid = getuid() if getuid is not None else None
    directory_name = "schema-sanitizer-quarantine"
    if configured_base is None and uid is not None:
        directory_name = f"{directory_name}-{uid}"
    return base, directory_name, base / directory_name


def _root_handle() -> _QuarantineRootHandle:
    """Return the process-generation root pinned by a live directory FD."""
    global _ROOT_HANDLE, _ROOT_IDENTITY_MISMATCHES
    with _ROOT_HANDLE_LOCK:
        base, directory_name, root = _configured_root_location()
        current = _ROOT_HANDLE
        if current is not None and current.pid == os.getpid() and current.path == root:
            metadata = os.fstat(current.descriptor)
            try:
                path_metadata = os.lstat(current.path)
            except FileNotFoundError:
                path_metadata = None
            if (
                (metadata.st_dev, metadata.st_ino) != (current.device, current.inode)
                or path_metadata is None
                or (path_metadata.st_dev, path_metadata.st_ino) != (current.device, current.inode)
            ):
                _ROOT_IDENTITY_MISMATCHES += 1
                raise OSError("temporary quarantine root path no longer names the pinned directory")
            return current
        if current is not None:
            if len(_RETIRED_ROOT_HANDLES) >= _MAX_ROOT_GENERATIONS - 1:
                raise OSError("temporary quarantine root generation capacity exhausted")
            # Configuration changes are rare but tests and embedded runtimes may
            # legitimately select a new coordination base.  Keep the old FD and
            # lease alive so in-flight operations retain their pinned authority.
            _RETIRED_ROOT_HANDLES.append(current)
            _ROOT_HANDLE = None

        base.mkdir(parents=True, exist_ok=True)
        getuid = getattr(os, "geteuid", None)
        uid = getuid() if getuid is not None else None
        flags = (
            os.O_RDONLY
            | getattr(os, "O_DIRECTORY", 0)
            | getattr(os, "O_CLOEXEC", 0)
            | getattr(os, "O_NOFOLLOW", 0)
        )
        # Reserve the parent/root pair atomically.  Acquiring the root lease only
        # after opening the parent creates a hold-and-wait cycle when teardown FD
        # capacity is one; one exact two-credit capability removes that cycle.
        descriptor_lease = acquire_teardown_file_descriptors(2, timeout_seconds=0)
        base_fd = -1
        root_fd = -1
        published = False
        uncertain_close = False
        try:
            base_fd = os.open(base, flags)
            record_physical_file_descriptors_opened(1)
            try:
                os.mkdir(directory_name, stat.S_IRWXU, dir_fd=base_fd)
            except FileExistsError:
                pass
            try:
                root_fd = os.open(directory_name, flags, dir_fd=base_fd)
                record_physical_file_descriptors_opened(1)
            except OSError as exc:
                if exc.errno in {errno.ELOOP, errno.ENOTDIR}:
                    raise OSError("temporary quarantine must be a real directory") from exc
                raise
            metadata = os.fstat(root_fd)
            path_metadata = os.stat(directory_name, dir_fd=base_fd, follow_symlinks=False)
            if (metadata.st_dev, metadata.st_ino) != (path_metadata.st_dev, path_metadata.st_ino):
                _ROOT_IDENTITY_MISMATCHES += 1
                raise OSError("temporary quarantine identity changed during open")
            if not stat.S_ISDIR(metadata.st_mode):
                raise OSError("temporary quarantine must be a real directory")
            if uid is not None and metadata.st_uid != uid:
                raise OSError("temporary quarantine must be owned by the current user")
            os.fchmod(root_fd, stat.S_IRWXU)

            # The parent authority is no longer needed.  Prove its physical close
            # before shrinking the exact two-credit lease to the one persistent
            # root descriptor transferred into the process root handle.
            try:
                os.close(base_fd)
            except BaseException:
                # A failed close leaves the numeric descriptor authority
                # ambiguous.  Never retry that number: it may already have been
                # closed/reused.  Keep its physical count and the exact bundle
                # as terminal debt instead.
                uncertain_close = True
                base_fd = -1
                raise
            else:
                record_physical_file_descriptors_closed(1)
                base_fd = -1
            descriptor_lease.shrink(1)

            handle = _QuarantineRootHandle(
                root, root_fd, descriptor_lease, metadata.st_dev, metadata.st_ino, os.getpid()
            )
            _ROOT_HANDLE = handle
            root_fd = -1
            published = True
            return handle
        finally:
            if not published:
                _close_descriptor_bundle_or_retain(
                    (root_fd, base_fd), descriptor_lease, force_debt=uncertain_close
                )


def _close_root_handle() -> bool:
    """Transactionally close pinned roots without losing a failed owner."""
    global _ROOT_HANDLE
    with _ROOT_HANDLE_LOCK:
        handles = list(_RETIRED_ROOT_HANDLES)
        _RETIRED_ROOT_HANDLES.clear()
        if _ROOT_HANDLE is not None:
            handles.append(_ROOT_HANDLE)
        if len(_CLOSING_ROOT_OWNERS) + len(handles) > _MAX_CLOSING_ROOT_OWNERS:
            # Keep still-pinned handles authoritative rather than dropping a
            # descriptor/lease pair that cannot enter the closing registry.
            if _ROOT_HANDLE is None and handles:
                _ROOT_HANDLE = handles.pop()
            _RETIRED_ROOT_HANDLES.extend(handles)
            return False
        _ROOT_HANDLE = None
        for handle in handles:
            _CLOSING_ROOT_OWNERS.append(_JanitorDescriptorOwner(handle.descriptor, handle.lease))
        owners = tuple(_CLOSING_ROOT_OWNERS)

    for owner in owners:
        transferred = False
        try:
            owner.release()
            transferred = True
        except BaseException as exc:
            clear_exception_traceback(exc)
            try:
                transferred = bool(adopt_failed_release(owner, retained_bytes=256))
            except BaseException as adopt_error:
                clear_exception_traceback(adopt_error)
                transferred = False
        if transferred:
            with _ROOT_HANDLE_LOCK:
                try:
                    _CLOSING_ROOT_OWNERS.remove(owner)
                except ValueError:
                    pass
    with _ROOT_HANDLE_LOCK:
        return not _CLOSING_ROOT_OWNERS


def _release_scan_fd_lease(lease: object) -> None:
    """Release or retain one scandir permit without masking iteration errors."""
    try:
        lease.release()  # type: ignore[attr-defined]
    except BaseException:
        try:
            adopted = adopt_failed_release(lease, retained_bytes=256)
        except BaseException:
            adopted = False
        if adopted:
            return
        with _FAILED_SCAN_FD_LEASES_LOCK:
            if any(existing is lease for existing in _FAILED_SCAN_FD_LEASES):
                return
            if len(_FAILED_SCAN_FD_LEASES) < _MAX_FAILED_SCAN_FD_LEASES:
                _FAILED_SCAN_FD_LEASES.append(lease)
                return
        # The janitor serializes scans, so this indicates a violated ownership
        # invariant rather than ordinary pressure. Keep the current exception
        # visible instead of silently forgetting the only permit owner.
        raise RuntimeError("janitor scandir lease retention invariant exceeded")


def _drain_failed_scan_fd_leases() -> None:
    """Retry every locally retained scan lease through the central guardian."""
    with _FAILED_SCAN_FD_LEASES_LOCK:
        failed = deque(_FAILED_SCAN_FD_LEASES)
        _FAILED_SCAN_FD_LEASES.clear()
    retry: deque[object] = deque()
    while failed:
        lease = failed.popleft()
        try:
            lease.release()  # type: ignore[attr-defined]
        except BaseException:
            try:
                adopted = adopt_failed_release(lease, retained_bytes=256)
            except BaseException:
                adopted = False
            if not adopted:
                retry.append(lease)
    if retry:
        with _FAILED_SCAN_FD_LEASES_LOCK:
            while retry and len(_FAILED_SCAN_FD_LEASES) < _MAX_FAILED_SCAN_FD_LEASES:
                _FAILED_SCAN_FD_LEASES.append(retry.popleft())
            if retry:
                raise RuntimeError("janitor scandir lease retention invariant exceeded")


@dataclass(slots=True)
class _JanitorDescriptorOwner:
    """Close one private-directory FD and return its exact governor lease."""

    descriptor: int | None
    lease: object | None
    lock: threading.Lock = dataclass_field(default_factory=threading.Lock)

    def release(self) -> None:
        """Release resources owned by this janitor descriptor owner."""
        with self.lock:
            descriptor = self.descriptor
            lease = self.lease
            if descriptor is None and lease is None:
                return
            self.descriptor = None
        close_error: BaseException | None = None
        if descriptor is not None:
            try:
                os.close(descriptor)
            except BaseException as exc:
                close_error = exc
            else:
                record_physical_file_descriptors_closed(1)
        lease_error: BaseException | None = None
        if close_error is not None and lease is not None:
            retained_as_debt = retain_uncertain_fd_close(lease, label="temporary-janitor")
            if retained_as_debt:
                with self.lock:
                    if self.lease is lease:
                        self.lease = None
        elif lease is not None:
            try:
                lease.release()  # type: ignore[attr-defined]
            except BaseException as exc:
                lease_error = exc
            else:
                with self.lock:
                    if self.lease is lease:
                        self.lease = None
        if close_error is not None:
            raise close_error
        if lease_error is not None:
            raise lease_error


def _close_descriptor_bundle_or_retain(
    descriptors: tuple[int, ...], lease: object | None, *, force_debt: bool = False
) -> None:
    """Best-effort-close a bundle without ever losing its exact reservation."""
    uncertain = bool(force_debt)
    for descriptor in descriptors:
        if descriptor < 0:
            continue
        try:
            os.close(descriptor)
        except BaseException as exc:
            # Do not retry an ambiguously closed numeric descriptor.  Preserve
            # the physical count and exact lease as conservative terminal debt.
            clear_exception_traceback(exc)
            uncertain = True
        else:
            record_physical_file_descriptors_closed(1)
    if lease is None:
        return
    if uncertain:
        try:
            retain_uncertain_fd_close(lease, label="temporary-janitor-bundle")
        except BaseException as exc:
            clear_exception_traceback(exc)
            try:
                adopt_failed_release(lease, retained_bytes=256)
            except BaseException as adopt_error:
                clear_exception_traceback(adopt_error)
        return
    _release_scan_fd_lease(lease)


def _close_or_retain_descriptor(descriptor: int, lease: object | None) -> None:
    """Close or retain descriptor."""
    owner = _JanitorDescriptorOwner(descriptor if descriptor >= 0 else None, lease)
    try:
        owner.release()
    except BaseException:
        if not adopt_failed_release(owner, retained_bytes=256):
            # The descriptor number has already been relinquished before close;
            # only the exact governor lease can remain and is retained centrally.
            if owner.lease is not None:
                _release_scan_fd_lease(owner.lease)


def _pinned_handle_for_path(path: Path) -> _QuarantineRootHandle | None:
    """Return the validated pinned handle only for its exact root pathname."""
    with _ROOT_HANDLE_LOCK:
        current = _ROOT_HANDLE
        matches = bool(current is not None and current.pid == os.getpid() and current.path == path)
    return _root_handle() if matches else None


class _TemporaryArtifactJanitor:
    """Retain leases until failed-cleanup artifacts are actually removed."""

    def __init__(self) -> None:
        """Initialize the temporary artifact janitor and its owned runtime state."""
        self._lock = threading.RLock()
        self._condition = threading.Condition(self._lock)
        self._scan_lock = threading.Lock()
        self._wake = threading.Event()
        self._pending: dict[str, _PendingArtifact] = {}
        self._pending_order: deque[str] = deque()
        self._thread: threading.Thread | None = None
        self._thread_lease: _Releasable | None = None
        self._retiring_thread: threading.Thread | None = None
        self._retry_scheduled = False
        self._availability_registered = False
        self._failed_thread_leases: deque[_Releasable] = deque()
        self._terminal_failed_thread_lease: _Releasable | None = None
        self._closed = False
        # Child synchronization/container banks are constructed during normal
        # runtime, not in the at-fork prepare callback.
        self._fork_banks = (self._new_fork_bank(), self._new_fork_bank())
        self._fork_bank_index = 0
        self._fork_prepared: tuple[object, ...] | None = None
        self._scanned = False
        self._scan_entries: Iterator[Path] | None = None
        self._quarantined = 0
        self._deleted = 0
        self._failed = 0
        self._identity_mismatches = 0
        self._worker_start_failures = 0
        self._quarantine_inflight = 0
        self._protocol_violations = 0
        self._worker_starting = False
        self._stale_private_artifacts = 0
        self._pending_metadata_bytes = 0
        self._quarantine_reserved_metadata_bytes = 0
        self._rejected_artifacts = 0

    @staticmethod
    def _new_fork_bank() -> tuple[object, ...]:
        """Allocate a fresh janitor fork-quarantine bank."""
        lock = threading.RLock()
        return (
            lock,
            threading.Condition(lock),
            threading.Lock(),
            threading.Event(),
            {},
            deque(),
            deque(),
            threading.Lock(),
            deque(),
        )

    @staticmethod
    def root() -> Path:
        """Return the process-generation quarantine path pinned by a live FD."""
        return _root_handle().path

    @staticmethod
    def _private_delete_path(path: Path) -> Path:
        """Return the private delete path."""
        if path.parent.name == ".delete":
            return path
        handle = _pinned_handle_for_path(path.parent)
        if handle is None:
            raise OSError("temporary artifact is outside the pinned quarantine root")
        try:
            os.mkdir(".delete", stat.S_IRWXU, dir_fd=handle.descriptor)
        except FileExistsError:
            pass
        metadata = os.stat(".delete", dir_fd=handle.descriptor, follow_symlinks=False)
        root = handle.path / ".delete"
        if not stat.S_ISDIR(metadata.st_mode):
            raise OSError("temporary quarantine delete root must be a real directory")
        getuid = getattr(os, "geteuid", None)
        if getuid is not None and metadata.st_uid != getuid():
            raise OSError("temporary quarantine delete root must be owned by the current user")
        return root / f"delete-{os.getpid()}-{uuid.uuid4().hex}"

    @classmethod
    def _delete_owned(
        cls, path: Path, is_dir: bool, expected_identity: PathIdentity | None = None
    ) -> tuple[bool, Path, PathIdentity | None, bool]:
        """Privately transfer and delete one exact entry identity."""
        try:
            metadata = path.lstat()
        except FileNotFoundError:
            return True, path, expected_identity, False
        except OSError:
            return False, path, expected_identity, False
        current_identity = lstat_identity(path)
        if expected_identity is not None and current_identity != expected_identity:
            return False, path, current_identity, True
        if current_identity is None:
            return True, path, expected_identity, False
        try:
            private = cls._private_delete_path(path)
            if private != path:
                handle = _pinned_handle_for_path(path.parent)
                if handle is not None:
                    os.replace(
                        path.name,
                        f".delete/{private.name}",
                        src_dir_fd=handle.descriptor,
                        dst_dir_fd=handle.descriptor,
                    )
                else:
                    os.replace(path, private)
        except FileNotFoundError:
            return True, path, expected_identity or current_identity, False
        except OSError:
            return False, path, expected_identity or current_identity, False
        private_identity = lstat_identity(private)
        if not transfer_identity_matches(current_identity, private_identity):
            try:
                if lstat_identity(path) is None and lstat_identity(private) is not None:
                    handle = _pinned_handle_for_path(path.parent)
                    if handle is not None:
                        os.replace(
                            f".delete/{private.name}",
                            path.name,
                            src_dir_fd=handle.descriptor,
                            dst_dir_fd=handle.descriptor,
                        )
                    else:
                        os.replace(private, path)
            except OSError:
                pass
            return False, path, expected_identity or private_identity, True
        try:
            if stat.S_ISLNK(metadata.st_mode):
                private.unlink(missing_ok=True)
            elif is_dir and stat.S_ISDIR(metadata.st_mode):
                shutil.rmtree(private)
            else:
                private.unlink(missing_ok=True)
        except OSError:
            try:
                private.lstat()
            except FileNotFoundError:
                try:
                    private.parent.rmdir()
                except OSError:
                    pass
                return True, private, expected_identity or private_identity, False
            except OSError:
                return False, private, expected_identity or private_identity, False
            return False, private, expected_identity or private_identity, False
        try:
            private.lstat()
        except FileNotFoundError:
            try:
                private.parent.rmdir()
            except OSError:
                pass
            return True, private, expected_identity or private_identity, False
        except OSError:
            return False, private, expected_identity or private_identity, False
        return False, private, expected_identity or private_identity, False

    @staticmethod
    def _metadata_charge(path: Path) -> int:
        """Estimate control-plane bytes retained by a janitor artifact."""
        try:
            encoded = os.fsencode(str(path))
        except BaseException:
            return _PENDING_METADATA_BASE
        return _PENDING_METADATA_BASE + len(encoded)

    def _pending_fits_locked(self, charge: int, *, reserve_slot: bool = True) -> bool:
        """Return whether another janitor entry fits while holding the janitor lock."""
        count = len(self._pending) + (self._quarantine_inflight if reserve_slot else 0)
        if count >= _MAX_PENDING_ARTIFACTS:
            return False
        used = self._pending_metadata_bytes + self._quarantine_reserved_metadata_bytes
        return charge <= _MAX_PENDING_METADATA_BYTES - used

    def _publish_pending_locked(self, key: str, artifact: _PendingArtifact) -> None:
        """Publish dict+FIFO membership transactionally under the janitor lock."""
        try:
            self._pending[key] = artifact
            try:
                self._pending_order.append(key)
            except BaseException:
                self._pending.pop(key, None)
                raise
        except BaseException:
            raise
        self._pending_metadata_bytes += artifact.metadata_bytes

    @staticmethod
    def _retire_pending_control_noexcept(artifact: _PendingArtifact) -> None:
        """Retire a janitor control charge without risking worker liveness."""
        ticket = artifact.control_ticket
        try:
            if ticket is not None and release_control_plane(ticket):
                artifact.control_ticket = None
        except BaseException as exc:
            # Keep the exact capability attached if authentication failed; a
            # later safe point can retry without losing the only owner token.
            clear_exception_traceback(exc)

    @staticmethod
    def _iter_directory(path: Path) -> Iterator[Path]:
        """Stream one directory while charging its live scandir descriptor."""
        _drain_failed_scan_fd_leases()
        handle = _pinned_handle_for_path(path)
        with acquire_file_descriptor_capability(
            1, timeout_seconds=0, teardown=True, label="temporary_janitor_scandir"
        ) as capability:
            if handle is not None:
                # POSIX scandir(fd) duplicates the supplied directory descriptor;
                # the duplicate owns its own physical slot for the iterator life.
                entries_owner = capability.scandir(
                    handle.descriptor, label="temporary_janitor_scandir_pinned"
                )
            else:
                entries_owner = capability.scandir_path(
                    path, label="temporary_janitor_scandir_path"
                )
            with entries_owner as entries:
                for entry in entries:
                    entry_path = Path(entry.path)
                    yield entry_path if entry_path.is_absolute() else path / entry_path

    def _stale_scan_candidates(self) -> Iterator[Path]:
        """Yield crash leftovers while every live directory FD is governed."""
        # Use a streaming scandir iterator whose descriptor owns a governor lease.
        root = self.root()
        for candidate_root in (root, root / ".delete"):
            try:
                yield from self._iter_directory(candidate_root)
            except OSError:
                pass
        base = root.parent
        try:
            for entry_path in self._iter_directory(base):
                try:
                    metadata = entry_path.lstat()
                except OSError:
                    continue
                if not stat.S_ISDIR(metadata.st_mode):
                    continue
                staging_private = entry_path / ".schema-sanitizer-delete"
                try:
                    yield from self._iter_directory(staging_private)
                except OSError:
                    continue
        except OSError:
            return

    def _scan_stale(self) -> None:
        """Delete one bounded crash-leftover batch and retain scan progress."""
        with self._scan_lock:
            if self._scanned:
                return
            try:
                entries = self._scan_entries
                if entries is None:
                    entries = iter(self._stale_scan_candidates())
                    self._scan_entries = entries
                for _index in range(_SWEEP_BATCH_SIZE):
                    with self._lock:
                        if not self._pending_fits_locked(
                            _PENDING_METADATA_BASE, reserve_slot=False
                        ):
                            self._rejected_artifacts += 1
                            return
                    try:
                        child = next(entries)
                    except StopIteration:
                        self._scanned = True
                        self._scan_entries = None
                        return
                    if not child.name.startswith(("artifact-", "delete-")):
                        continue
                    child_charge = self._metadata_charge(child)
                    with self._lock:
                        if not self._pending_fits_locked(child_charge, reserve_slot=False):
                            self._rejected_artifacts += 1
                            return
                    try:
                        metadata = child.lstat()
                        identity = claim_path_identity(child)
                    except OSError:
                        continue
                    if identity is None:
                        continue
                    try:
                        control_ticket = reserve_control_plane("temporary_janitor_artifact", 384)
                    except BaseException:
                        try:
                            release_path_identity(identity)
                        except BaseException as exc:
                            clear_exception_traceback(exc)
                        with self._lock:
                            self._rejected_artifacts += 1
                        return
                    transferred_ticket = False
                    deleted, _path, _identity, _mismatch = self._delete_owned(
                        child,
                        stat.S_ISDIR(metadata.st_mode),
                        identity,
                    )
                    if deleted:
                        try:
                            release_path_identity(identity)
                        except BaseException:
                            # The inode is gone; retain the claim/FD cleanup as a
                            # normal pending owner instead of silently dropping it.
                            key = f"stale-release:{child}:{id(identity)}"
                            artifact = _PendingArtifact(
                                child,
                                stat.S_ISDIR(metadata.st_mode),
                                _STALE_ARTIFACT_LEASE,
                                identity,
                                time.monotonic(),
                                child_charge,
                                control_ticket,
                            )
                            try:
                                with self._lock:
                                    self._publish_pending_locked(key, artifact)
                                    self._stale_private_artifacts += 1
                                transferred_ticket = True
                            except BaseException as exc:
                                clear_exception_traceback(exc)
                                with self._lock:
                                    self._rejected_artifacts += 1
                        # A fully deleted+released stale artifact needs no queue charge.
                    else:
                        key = str(child)
                        artifact = _PendingArtifact(
                            child,
                            stat.S_ISDIR(metadata.st_mode),
                            _STALE_ARTIFACT_LEASE,
                            identity,
                            time.monotonic(),
                            child_charge,
                            control_ticket,
                        )
                        with self._lock:
                            if key not in self._pending:
                                try:
                                    self._publish_pending_locked(key, artifact)
                                except BaseException as exc:
                                    clear_exception_traceback(exc)
                                    self._rejected_artifacts += 1
                                else:
                                    transferred_ticket = True
                            else:
                                try:
                                    release_path_identity(identity)
                                except BaseException as exc:
                                    clear_exception_traceback(exc)
                            self._stale_private_artifacts += 1
                        self._wake.set()
                    if not transferred_ticket:
                        try:
                            release_control_plane(control_ticket)
                        except BaseException as exc:
                            clear_exception_traceback(exc)
            except OSError:
                self._scan_entries = None

    def _finish_quarantine_inflight_locked(self) -> None:
        """Retire one quarantine operation without masking protocol underflow."""
        if self._quarantine_inflight <= 0:
            self._protocol_violations += 1
            return
        self._quarantine_inflight -= 1

    def quarantine(
        self,
        path: str | Path,
        *,
        is_dir: bool,
        lease: "TemporaryStorageLease",
        expected_identity: PathIdentity | None = None,
    ) -> bool:
        """Claim and transfer one artifact without filesystem work under the lock."""
        ensure_runtime_fork_safe()
        source = Path(path)
        metadata_charge = self._metadata_charge(source)
        with self._condition:
            if self._closed:
                return False
            if not self._pending_fits_locked(metadata_charge):
                self._rejected_artifacts += 1
                return False
            self._quarantine_inflight += 1
            self._quarantine_reserved_metadata_bytes += metadata_charge

        try:
            control_ticket = reserve_control_plane("temporary_janitor_artifact", 384)
        except BaseException:
            with self._condition:
                self._finish_quarantine_inflight_locked()
                self._quarantine_reserved_metadata_bytes = max(
                    0, self._quarantine_reserved_metadata_bytes - metadata_charge
                )
                self._rejected_artifacts += 1
                self._condition.notify_all()
            return False

        retained_path = source
        retained_identity: PathIdentity | None = None
        release_identity: PathIdentity | None = None
        release_missing = False
        release_duplicate = False
        accepted = False
        mismatch = False
        claimed_here = False
        source_identity: PathIdentity | None = None
        try:
            with self._condition:
                duplicate_snapshot = self._pending.get(str(source))
            if duplicate_snapshot is not None:
                current_identity = lstat_identity(source)
                with self._condition:
                    duplicate_is_current = self._pending.get(str(source)) is duplicate_snapshot
                if (
                    duplicate_is_current
                    and current_identity is not None
                    and current_identity == duplicate_snapshot.identity
                ):
                    release_duplicate = True
                    source_identity = current_identity
                else:
                    duplicate_snapshot = None
            if release_duplicate:
                pass
            elif expected_identity is None:
                source_identity = claim_path_identity(source)
                claimed_here = source_identity is not None
            else:
                source_identity = lstat_identity(source)
            if source_identity is None:
                release_missing = True
                release_identity = expected_identity
            elif expected_identity is not None and source_identity != expected_identity:
                mismatch = True
            else:
                if expected_identity is not None:
                    source_identity = expected_identity
                retained_identity = source_identity
                try:
                    suffix = source.suffix if not is_dir else ""
                    selected_root = self.root()
                    handle = _pinned_handle_for_path(selected_root)
                    if handle is None:
                        raise OSError("temporary quarantine root is not pinned")
                    target_name = f"artifact-{os.getpid()}-{uuid.uuid4().hex}{suffix}"
                    target = selected_root / target_name
                    _replace_into_root(source, target_name, handle)
                    target_identity = lstat_identity(target)
                    if not transfer_identity_matches(source_identity, target_identity):
                        mismatch = True
                        try:
                            if (
                                lstat_identity(source) is None
                                and lstat_identity(target) is not None
                            ):
                                _replace_from_root(target.name, source, handle)
                        except OSError:
                            pass
                    else:
                        retained_path = target
                except OSError:
                    current = lstat_identity(retained_path)
                    if current != source_identity:
                        mismatch = True
                    else:
                        retained_identity = source_identity

            with self._condition:
                if mismatch:
                    self._identity_mismatches += 1
                elif release_missing or retained_identity is None:
                    release_missing = True
                else:
                    key = str(retained_path)
                    if key in self._pending:
                        release_duplicate = True
                    else:
                        artifact = _PendingArtifact(
                            retained_path,
                            is_dir,
                            lease,
                            retained_identity,
                            time.monotonic(),
                            metadata_charge,
                            control_ticket,
                        )
                        self._publish_pending_locked(key, artifact)
                        self._quarantined += 1
                        accepted = True
        finally:
            with self._condition:
                self._finish_quarantine_inflight_locked()
                self._quarantine_reserved_metadata_bytes = max(
                    0, self._quarantine_reserved_metadata_bytes - metadata_charge
                )
                self._condition.notify_all()
            if not accepted:
                try:
                    release_control_plane(control_ticket)
                except BaseException as exc:
                    clear_exception_traceback(exc)

        if mismatch:
            if claimed_here:
                release_path_identity(retained_identity or source_identity)
            return False
        if release_missing or release_duplicate:
            identity_to_release = release_identity
            if identity_to_release is None and claimed_here:
                identity_to_release = retained_identity
            release_path_identity(identity_to_release)
            lease.release()
            return True
        if accepted:
            # Thread admission and startup use their own claim/work/commit and
            # therefore cannot hold the janitor's global lock while blocking.
            self._ensure_worker()
            self._wake.set()
        return accepted

    def _has_failed_thread_leases_locked(self) -> bool:
        """Return whether failed thread leases remain while holding the janitor lock."""
        return bool(self._failed_thread_leases or self._terminal_failed_thread_lease is not None)

    def _retain_failed_thread_lease_locked(self, lease: _Releasable) -> None:
        """Retain a failed janitor thread lease while holding the janitor lock."""
        if any(item is lease for item in self._failed_thread_leases):
            return
        if self._terminal_failed_thread_lease is lease:
            return
        if len(self._failed_thread_leases) < _MAX_FAILED_THREAD_LEASES:
            self._failed_thread_leases.append(lease)
            return
        if self._terminal_failed_thread_lease is None:
            self._terminal_failed_thread_lease = lease
            self._worker_start_failures += 1
            return
        raise RuntimeError("janitor thread lease ownership invariant exceeded")

    def _take_failed_thread_leases_locked(self) -> deque[_Releasable]:
        """Remove retained janitor thread leases while holding the janitor lock."""
        failed = self._failed_thread_leases
        self._failed_thread_leases = deque()
        terminal = self._terminal_failed_thread_lease
        self._terminal_failed_thread_lease = None
        if terminal is not None:
            failed.append(terminal)
        return failed

    def _ensure_worker(self) -> None:
        """Start one worker through claim/work/commit outside the global lock."""
        with self._condition:
            if (
                (self._closed and not self._pending)
                or self._worker_starting
                or self._has_failed_thread_leases_locked()
                or (self._thread is not None and self._thread.is_alive())
            ):
                return
            self._worker_starting = True
            stale_lease = self._thread_lease

        if stale_lease is not None:
            released = False
            try:
                stale_lease.release()
                released = True
            except BaseException:
                try:
                    released = adopt_failed_release(stale_lease, retained_bytes=256)
                except BaseException:
                    released = False
            if not released:
                with self._condition:
                    self._worker_starting = False
                    self._worker_start_failures += 1
                    self._schedule_retry_locked()
                    self._condition.notify_all()
                return
            with self._condition:
                if self._thread_lease is stale_lease:
                    self._thread_lease = None

        try:
            acquire = acquire_teardown_project_threads if self._closed else acquire_project_threads
            lease = acquire(1, minimum=1)
        except Exception:
            with self._condition:
                self._worker_starting = False
                self._worker_start_failures += 1
                self._schedule_retry_locked()
                self._condition.notify_all()
            return
        except BaseException:
            with self._condition:
                self._worker_starting = False
                self._condition.notify_all()
            raise

        try:
            thread = threading.Thread(
                target=self._run,
                name="schema-sanitizer-temp-janitor",
                daemon=True,
            )
        except BaseException as exc:
            with self._condition:
                self._worker_starting = False
                self._worker_start_failures += 1
                self._schedule_retry_locked()
                self._condition.notify_all()
            self._release_thread_lease_owner(lease)
            if isinstance(exc, Exception):
                return
            raise
        with self._condition:
            if self._closed and not self._pending:
                self._worker_starting = False
                self._condition.notify_all()
                should_start = False
            else:
                self._scanned = False
                self._scan_entries = None
                self._thread_lease = lease
                self._thread = thread
                self._retry_scheduled = False
                cancel_retry(("temporary-janitor", id(self)))
                should_start = True
        if not should_start:
            self._release_thread_lease_owner(lease)
            return
        try:
            start_governed_thread(thread)
        except BaseException as exc:
            with self._condition:
                if self._thread is thread:
                    self._thread = None
                self._worker_starting = False
                self._worker_start_failures += 1
                self._schedule_retry_locked()
                self._condition.notify_all()
            try:
                lease.release()
            except BaseException as cleanup_error:
                try:
                    adopted = adopt_failed_release(lease, retained_bytes=256)
                except BaseException:
                    adopted = False
                with self._condition:
                    if adopted:
                        if self._thread_lease is lease:
                            self._thread_lease = None
                    else:
                        self._thread_lease = lease
                        self._schedule_retry_locked()
                add_bounded_note(
                    exc,
                    f"janitor thread permit cleanup also failed; guardian_adopted={adopted}",
                    cleanup_error,
                )
            else:
                with self._condition:
                    if self._thread_lease is lease:
                        self._thread_lease = None
            if isinstance(exc, Exception):
                return
            raise
        with self._condition:
            self._worker_starting = False
            self._condition.notify_all()
        self._unregister_availability()

    def _schedule_retry_locked(self) -> None:
        """Coalesce janitor startup and failed-lease retries globally."""
        if self._retry_scheduled or (
            not self._pending and not self._has_failed_thread_leases_locked()
        ):
            return
        retry_token = id(self)
        with _JANITOR_RETRY_OWNERS_LOCK:
            if (
                retry_token not in _JANITOR_RETRY_OWNERS
                and len(_JANITOR_RETRY_OWNERS) >= _MAX_JANITOR_RETRY_OWNERS
            ):
                self._retry_scheduled = False
            else:
                _JANITOR_RETRY_OWNERS[retry_token] = self

                def retry_owner(token: int = retry_token) -> None:
                    """Retry terminal cleanup for one retained owner."""
                    _retry_janitor_owner(token)

                self._retry_scheduled = schedule_retry(
                    ("temporary-janitor", retry_token),
                    retry_owner,
                    delay_seconds=_RETRY_SECONDS,
                    retained_bytes=1024,
                    jitter_fraction=0.2,
                )
                if not self._retry_scheduled:
                    _JANITOR_RETRY_OWNERS.pop(retry_token, None)
        if not self._retry_scheduled and not self._availability_registered:
            self._availability_registered = bool(
                register_project_thread_availability(AvailabilityEvent.TEMPORARY_JANITOR)
            )

    def _availability_wakeup(self) -> None:
        # Acknowledgment is owned by the sealed notifier delivery.  Do not
        # unregister before work succeeds or a transient wakeup would be lost.
        """Retry starting the janitor worker when thread capacity becomes available."""
        self._retry_worker_start()

    def _unregister_availability(self) -> None:
        """Remove the registered capacity-availability callback."""
        with self._condition:
            if not self._availability_registered:
                return
            self._availability_registered = False
        unregister_project_thread_availability(AvailabilityEvent.TEMPORARY_JANITOR)

    def _retry_worker_start(self) -> None:
        """Retry starting the governed temporary-janitor worker."""
        self._unregister_availability()
        with self._condition:
            self._retry_scheduled = False
            failed = self._take_failed_thread_leases_locked()
        retry: deque[_Releasable] = deque()
        while failed:
            lease = failed.popleft()
            try:
                lease.release()
            except BaseException:
                try:
                    adopted = adopt_failed_release(lease, retained_bytes=256)
                except BaseException:
                    adopted = False
                if not adopted:
                    retry.append(lease)
        if retry:
            with self._condition:
                existing = self._take_failed_thread_leases_locked()
                retry.extend(existing)
                while retry:
                    self._retain_failed_thread_lease_locked(retry.popleft())
        self._ensure_worker()
        with self._condition:
            if (self._thread is None and self._pending) or self._has_failed_thread_leases_locked():
                self._schedule_retry_locked()
        self._wake.set()

    def _take_batch_locked(self, limit: int) -> tuple[list[tuple[str, _PendingArtifact]], int]:
        """Pop at most one bounded batch from the retry order."""
        batch: list[tuple[str, _PendingArtifact]] = []
        examined = min(max(0, int(limit)), len(self._pending_order))
        for _index in range(examined):
            key = self._pending_order.popleft()
            artifact = self._pending.get(key)
            if artifact is not None:
                batch.append((key, artifact))
        return batch, examined

    def _sweep_cycle(self) -> None:
        """Attempt each artifact present at cycle start without a full-map copy."""
        with self._lock:
            remaining = len(self._pending_order)
        while remaining > 0:
            with self._lock:
                batch, examined = self._take_batch_locked(min(_SWEEP_BATCH_SIZE, remaining))
            if examined == 0:
                return
            remaining -= examined
            for key, artifact in batch:
                if artifact.identity is None:
                    (
                        deleted,
                        retained_path,
                        retained_identity,
                        mismatch,
                    ) = self._delete_owned(artifact.path, artifact.is_dir)
                else:
                    (
                        deleted,
                        retained_path,
                        retained_identity,
                        mismatch,
                    ) = self._delete_owned(artifact.path, artifact.is_dir, artifact.identity)
                if retained_path != artifact.path:
                    with self._lock:
                        if self._pending.get(key) is artifact:
                            artifact.path = retained_path
                            artifact.identity = retained_identity
                if deleted:
                    try:
                        release_path_identity(artifact.identity)
                        artifact.lease.release()
                    except BaseException:
                        # The artifact is already gone, but the storage lease may
                        # still require a journal retry. Keep the release owner in
                        # the queue instead of losing it with a dead worker.
                        with self._lock:
                            if self._pending.get(key) is artifact:
                                self._pending_order.append(key)
                                self._failed += 1
                    else:
                        with self._lock:
                            if self._pending.get(key) is artifact:
                                self._pending.pop(key)
                                self._pending_metadata_bytes = max(
                                    0, self._pending_metadata_bytes - artifact.metadata_bytes
                                )
                                self._retire_pending_control_noexcept(artifact)
                                self._deleted += 1
                else:
                    current_identity = lstat_identity(artifact.path)
                    with self._lock:
                        if self._pending.get(key) is artifact:
                            self._pending_order.append(key)
                            self._failed += 1
                            if mismatch or (
                                current_identity is not None
                                and current_identity != artifact.identity
                            ):
                                self._identity_mismatches += 1

    def _claim_current_thread_retirement_locked(self) -> _Releasable | None:
        """Mark RETIRING and return the exact lease for outside release."""
        if self._thread is not threading.current_thread():
            return None
        if self._retiring_thread is threading.current_thread():
            return None
        lease = self._thread_lease
        self._retiring_thread = threading.current_thread()
        return lease

    def _release_thread_lease_owner(self, lease: _Releasable | None) -> None:
        """Release thread lease owner."""
        current = threading.current_thread()
        thread = self._thread
        if lease is not None and thread is current and thread.is_alive():
            try:
                retired = defer_governed_thread_retirement(thread, lease.release)
            except BaseException:
                retired = False
            with self._condition:
                if retired:
                    if self._thread_lease is lease:
                        self._thread_lease = None
                    if self._thread is current:
                        self._thread = None
                # If bounded retirement publication failed, retain both fields.
                # The next governed path observes a dead stale thread and can
                # release safely; no permit is returned before physical exit.
                if self._retiring_thread is current:
                    self._retiring_thread = None
                self._condition.notify_all()
            return
        try:
            if lease is not None:
                try:
                    lease.release()
                except BaseException:
                    try:
                        adopted = adopt_failed_release(lease, retained_bytes=256)
                    except BaseException:
                        adopted = False
                    if not adopted:
                        with self._condition:
                            self._retain_failed_thread_lease_locked(lease)
                            self._schedule_retry_locked()
                            self._condition.notify_all()
        finally:
            with self._condition:
                if self._retiring_thread is current:
                    self._retiring_thread = None
                if thread is current and not current.is_alive():
                    self._thread = None
                if self._thread_lease is lease and (thread is None or not thread.is_alive()):
                    self._thread_lease = None
                self._condition.notify_all()

    def _run(self) -> None:
        """Retry bounded batches while retaining failed-artifact leases."""
        try:
            # Crash-leftover discovery can be arbitrarily expensive on a large
            # quarantine directory, so it runs on the janitor thread rather than
            # delaying the operation that transfers a new artifact.
            while True:
                self._scan_stale()
                self._wake.wait(_RETRY_SECONDS)
                self._wake.clear()
                with self._lock:
                    if self._closed and not self._pending:
                        lease = self._claim_current_thread_retirement_locked()
                    else:
                        lease = None
                if lease is not None:
                    self._release_thread_lease_owner(lease)
                    return
                self._sweep_cycle()
                with self._lock:
                    if (self._closed and not self._pending) or (
                        not self._pending and self._scanned
                    ):
                        lease = self._claim_current_thread_retirement_locked()
                    else:
                        lease = None
                if lease is not None:
                    self._release_thread_lease_owner(lease)
                    return
        finally:
            with self._lock:
                lease = self._claim_current_thread_retirement_locked()
            self._release_thread_lease_owner(lease)

    def sweep(self) -> None:
        """Synchronously retry each currently pending artifact once."""
        self._sweep_cycle()

    def snapshot(self) -> TemporaryJanitorSnapshot:
        """Return a bounded snapshot of pending and quarantined artifacts."""
        with self._lock:
            pending_without_worker = int(
                bool(self._pending) and (self._thread is None or not self._thread.is_alive())
            )
            now = time.monotonic()
            oldest = max(
                (now - artifact.enqueued_at for artifact in self._pending.values()),
                default=0.0,
            )
            return TemporaryJanitorSnapshot(
                len(self._pending),
                self._quarantined,
                self._deleted,
                self._failed,
                self._identity_mismatches,
                self._worker_start_failures,
                pending_without_worker,
                oldest,
                len(self._failed_thread_leases)
                + int(self._terminal_failed_thread_lease is not None),
                self._stale_private_artifacts,
                self._pending_metadata_bytes,
                self._rejected_artifacts,
                self._scan_entries is not None,
                len(_FAILED_SCAN_FD_LEASES),
                _ROOT_IDENTITY_MISMATCHES,
                bool(self._thread is not None and self._thread.is_alive()),
                self._worker_starting,
                _ROOT_HANDLE is not None,
                self._retiring_thread is not None,
                self._protocol_violations,
            )

    def close(self, *, deadline_seconds: float = 5.0) -> bool:
        """Stop admission and wait for the worker/lease owner to become quiescent."""
        deadline = deadline_ns_from_timeout(
            deadline_seconds, name="temporary janitor shutdown deadline"
        )
        if getattr(sys, "is_finalizing", lambda: False)():
            with self._condition:
                self._closed = True
                thread = self._thread
                return not (
                    self._pending
                    or self._quarantine_inflight
                    or self._worker_starting
                    or self._retiring_thread is not None
                    or self._has_failed_thread_leases_locked()
                    or bool(_FAILED_SCAN_FD_LEASES)
                    or (thread is not None and thread.is_alive())
                    or self._protocol_violations
                )
        with self._condition:
            self._closed = True
            thread = self._thread
            scan_entries = self._scan_entries
            self._scan_entries = None
            self._condition.notify_all()
        cancel_retry(("temporary-janitor", id(self)))
        self._unregister_availability()
        self._wake.set()
        # Close the governed scandir outside janitor locks.
        close_scan = getattr(scan_entries, "close", None)
        if callable(close_scan):
            try:
                close_scan()
            except BaseException:
                pass
        while time.monotonic_ns() < deadline:
            with self._condition:
                thread = self._thread
                quiescent = not (
                    self._quarantine_inflight
                    or self._worker_starting
                    or self._retiring_thread is not None
                    or (thread is not None and thread.is_alive())
                )
                if quiescent:
                    break
                self._condition.notify_all()
            if thread is not None and thread is not threading.current_thread():
                thread.join(timeout=min(0.01, remaining_seconds(deadline)))
            with self._condition:
                self._condition.wait(timeout=min(0.01, remaining_seconds(deadline)))
        # If startup failed, one bounded caller-side cycle may still release
        # artifacts without publishing work into a dispatcher that closes later.
        with self._condition:
            should_sweep = bool(self._pending) and self._thread is None
        if should_sweep and time.monotonic_ns() < deadline:
            self._sweep_cycle()
        with self._condition:
            has_failed_thread_leases = self._has_failed_thread_leases_locked()
        if has_failed_thread_leases and time.monotonic_ns() < deadline:
            self._retry_worker_start()
        if time.monotonic_ns() < deadline:
            try:
                _drain_failed_scan_fd_leases()
            except BaseException:
                # Shutdown is a status-returning boundary.  Failed descriptor
                # owners remain retained by the bounded fallback registry and
                # are reflected in the final quiescence result.
                pass
        with self._condition:
            thread = self._thread
            quiescent = not (
                self._quarantine_inflight
                or self._worker_starting
                or self._retiring_thread is not None
                or self._pending
                or self._has_failed_thread_leases_locked()
                or bool(_FAILED_SCAN_FD_LEASES)
                or (thread is not None and thread.is_alive())
                or self._protocol_violations
            )
        return bool(quiescent and _close_root_handle())

    def prepare_for_fork(self) -> None:
        """Select a complete child bank allocated during normal runtime."""
        self._fork_prepared = self._fork_banks[self._fork_bank_index]

    def clear_fork_preparation(self) -> None:
        """Clear state established while preparing for a fork."""
        self._fork_prepared = None

    def reset_after_fork(self) -> None:
        """Install only preallocated child state; never allocate after fork."""
        from .fork_safety import fork_quarantine_generation

        if fork_quarantine_generation() > 1:
            return
        prepared = self._fork_prepared
        if prepared is None:
            # fork_safety poisons the child; do not touch inherited locks if the
            # prepare phase could not produce a safe bank.
            return
        global _ROOT_HANDLE, _ROOT_HANDLE_LOCK, _CLOSING_ROOT_OWNERS
        (
            lock,
            condition,
            scan_lock,
            wake,
            pending,
            pending_order,
            failed_thread_leases,
            root_lock,
            closing_roots,
        ) = prepared
        self._fork_prepared = None
        self._fork_bank_index = 1 - self._fork_bank_index
        if _ROOT_HANDLE is not None:
            quarantine_inherited_state("janitor-root-handle", _ROOT_HANDLE)
        if _CLOSING_ROOT_OWNERS:
            quarantine_inherited_state("janitor-closing-roots", _CLOSING_ROOT_OWNERS)
        quarantine_inherited_state("temporary-janitor", self.__dict__)
        _CLOSING_ROOT_OWNERS = closing_roots  # type: ignore[assignment]
        _ROOT_HANDLE = None
        _ROOT_HANDLE_LOCK = root_lock  # type: ignore[assignment]
        self._lock = lock  # type: ignore[assignment]
        self._condition = condition  # type: ignore[assignment]
        self._scan_lock = scan_lock  # type: ignore[assignment]
        self._wake = wake  # type: ignore[assignment]
        self._pending = pending  # type: ignore[assignment]
        self._pending_order = pending_order  # type: ignore[assignment]
        self._thread = None
        self._thread_lease = None
        self._retiring_thread = None
        self._retry_scheduled = False
        self._availability_registered = False
        self._failed_thread_leases = failed_thread_leases  # type: ignore[assignment]
        self._terminal_failed_thread_lease = None
        self._closed = False
        self._scanned = False
        self._scan_entries = None
        self._identity_mismatches = 0
        self._worker_start_failures = 0
        self._quarantine_inflight = 0
        self._protocol_violations = 0
        self._worker_starting = False
        self._stale_private_artifacts = 0
        self._pending_metadata_bytes = 0
        self._quarantine_reserved_metadata_bytes = 0
        self._rejected_artifacts = 0


_JANITOR = _TemporaryArtifactJanitor()


def quarantine_temporary_artifact(
    path: str | Path,
    *,
    is_dir: bool,
    lease: "TemporaryStorageLease",
    expected_identity: PathIdentity | None = None,
) -> bool:
    """Transfer one resistant artifact, returning whether ownership was accepted."""
    return _JANITOR.quarantine(
        path,
        is_dir=is_dir,
        lease=lease,
        expected_identity=expected_identity,
    )


def temporary_janitor_snapshot() -> TemporaryJanitorSnapshot:
    """Return the process-wide temporary janitor snapshot."""
    return _JANITOR.snapshot()


def sweep_temporary_quarantine() -> None:
    """Synchronously sweep only in a fresh, non-forked runtime."""
    ensure_runtime_fork_safe()
    _JANITOR.sweep()


def _prepare_janitor_retry_owners_for_fork() -> None:
    """Prepare janitor retry owners for fork."""
    global _JANITOR_RETRY_FORK_PREPARED
    _JANITOR_RETRY_FORK_PREPARED = _JANITOR_RETRY_FORK_BANKS[_JANITOR_RETRY_FORK_BANK_INDEX]


def _clear_janitor_retry_fork_preparation() -> None:
    """Clear janitor retry fork preparation."""
    global _JANITOR_RETRY_FORK_PREPARED
    _JANITOR_RETRY_FORK_PREPARED = None


def _reset_janitor_retry_owners_after_fork() -> None:
    """Reset janitor retry owners after fork."""
    global _JANITOR_RETRY_OWNERS_LOCK, _JANITOR_RETRY_OWNERS, _JANITOR_RETRY_FORK_PREPARED
    global _JANITOR_RETRY_FORK_BANK_INDEX
    prepared = _JANITOR_RETRY_FORK_PREPARED
    _JANITOR_RETRY_FORK_PREPARED = None
    if prepared is None:
        return
    quarantine_inherited_state("janitor-retry-owners", _JANITOR_RETRY_OWNERS)
    _JANITOR_RETRY_OWNERS_LOCK, _JANITOR_RETRY_OWNERS = prepared
    _JANITOR_RETRY_FORK_BANK_INDEX = 1 - _JANITOR_RETRY_FORK_BANK_INDEX


register_fork_handler(
    "temporary-janitor",
    before=_JANITOR.prepare_for_fork,
    after_in_parent=_JANITOR.clear_fork_preparation,
    after_in_child=_JANITOR.reset_after_fork,
)
register_fork_handler(
    "temporary-janitor-retry-owners",
    before=_prepare_janitor_retry_owners_for_fork,
    after_in_parent=_clear_janitor_retry_fork_preparation,
    after_in_child=_reset_janitor_retry_owners_after_fork,
)

atexit.register(_JANITOR.close)


__all__ = [
    "TemporaryJanitorSnapshot",
    "quarantine_temporary_artifact",
    "sweep_temporary_quarantine",
    "temporary_janitor_snapshot",
]
