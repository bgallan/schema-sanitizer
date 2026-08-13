"""No-follow filesystem identity helpers for temporary artifact ownership."""

from __future__ import annotations

import errno
import hashlib
import json
import os
import secrets
import stat
import tempfile
from dataclasses import dataclass, field
from pathlib import Path
from threading import Condition, Lock
from time import monotonic, time_ns
from typing import Any, cast

from .bounded_generation import BoundedGenerationPool
from .durations import normalize_duration
from .finalization import runtime_is_finalizing
from .finalizer_cleanup import (
    PreparedFinalizerCleanup,
    cancel_prepared_finalizer_cleanup,
    defer_owner_finalizer_cleanup,
    reserve_owner_finalizer_cleanup,
)
from .finalizer_escrow import ReservedFinalizerEscrow
from .fork_safety import quarantine_inherited_state
from .process_identity import process_identity_matches, process_start_token
from .process_resources import (
    FileDescriptorCapability,
    acquire_file_descriptor_capability,
    acquire_file_descriptors,
    record_physical_file_descriptors_closed,
    record_physical_file_descriptors_opened,
    retain_uncertain_fd_close,
)
from .resource_lifecycle import _cleanup_with_note
from .rooted_finalizer import (
    RootedFinalizerAuthority,
    arm_rooted_finalizer_authority,
    retire_or_ack_rooted_finalizer_authority,
)
from .safe_errors import add_bounded_note


def prepare_owner_finalizer_cleanup() -> PreparedFinalizerCleanup:
    """Compatibility injection hook backed by the single-capsule safe API."""
    return reserve_owner_finalizer_cleanup()


_OWNER_XATTR = b"user.schema_sanitizer_owner"
_CLAIM_DIRECTORY = "schema-sanitizer-path-claims"
_COORDINATION_ENV = "SCHEMA_SANITIZER_COORDINATION_DIR"
_CLAIM_VERSION = 2
_CLAIM_SWEEP_LIMIT = 32
_CLAIM_SWEEP_CURSOR: str | None = None
_CLAIM_SWEEP_ITERATOR: Any | None = None
_CLAIM_SWEEP_ROOT: str | None = None
_CLAIM_SWEEP_OWNER: Any | None = None
_CLAIM_SWEEP_LOCK = Lock()
_MAX_CLAIM_BYTES = 4096
_ABANDONED_CLAIM_OWNERS: dict[int, Any] = {}
_ABANDONED_CLAIM_LOCK = Lock()
_MAX_LIVE_PATH_CLAIMS = 8192
_MAX_ABANDONED_CLAIM_OWNERS = _MAX_LIVE_PATH_CLAIMS * 2
_PATH_CLAIM_ADMISSIONS = 0
_PATH_CLAIM_ADMISSION_LOCK = Lock()
# Exact preallocated membership is authoritative; the scalar above is a
# diagnostic mirror only. Owner-first generation slots make both construction
# handoff and release retryable without growing a per-claim Python container.
_PATH_CLAIM_ADMISSION_OWNERS = BoundedGenerationPool(_MAX_LIVE_PATH_CLAIMS)
# Child reset must not allocate a fresh 8k-slot namespace from an at-fork
# callback. Keep one pristine COW bank prepared at normal import time.
_PATH_CLAIM_ADMISSION_OWNERS_FORK_FRESH = BoundedGenerationPool(_MAX_LIVE_PATH_CLAIMS)
_ABANDONED_DESCRIPTOR_OWNERS: dict[int, Any] = {}
_ABANDONED_DESCRIPTOR_LOCK = Lock()
_MAX_ABANDONED_DESCRIPTOR_OWNERS = 8192
_OWNER_CLOSE_WAIT_TIMEOUT_SECONDS = 5.0
# Inherited owners must never be destroyed from an after-fork callback: their
# internal locks may have belonged to vanished parent threads.  The child model
# is fork+exec; quarantined references are intentionally retained until exec.
_FORKED_PATH_KEEPALIVE: list[object] = []
_FORKED_PATH_GENERATIONS = 0
_CLAIM_STABILIZATION_NS = 2_000_000_000
_CLAIM_TEMP_STABILIZATION_NS = 300_000_000_000
_RESOURCE_OPEN_ERRNOS = {errno.EMFILE, errno.ENFILE, errno.ENOMEM, errno.EACCES, errno.EPERM}
_HAS_POSIX_PATH_AUTHORITY = os.name != "nt"
_PATH_CLAIM_FINALIZER_ESCROW: ReservedFinalizerEscrow[RootedFinalizerAuthority | PathClaimOwner] = (
    ReservedFinalizerEscrow(_MAX_LIVE_PATH_CLAIMS * 2, static_kind="path_identity_claims")
)
_PATH_CLAIM_FINALIZER_OVERFLOWS = 0
_PATH_CLAIM_FINALIZER_OVERFLOWED = False
_PATH_CLAIM_PROTOCOL_VIOLATIONS = 0


# Rooting is delegated to reserve_rooted_finalizer_authority(), which performs escrow.root_reserved(...).
def _run_path_claim_admission_finalizer(
    authority: RootedFinalizerAuthority,
) -> None:
    """Release a pre-transfer path-claim admission from rooted state."""
    owner_pid = int(cast(int, authority.arg0) or 0)
    if owner_pid != os.getpid():
        return
    _release_path_claim_admission_owner(authority)
    authority.arg1 = False


def _release_path_claim_admission_owner(authority: RootedFinalizerAuthority) -> bool:
    """Idempotently retire one exact path-admission owner."""
    global _PATH_CLAIM_ADMISSIONS
    with _PATH_CLAIM_ADMISSION_LOCK:
        was_live = _PATH_CLAIM_ADMISSION_OWNERS.owns_owner(authority)
        _PATH_CLAIM_ADMISSION_OWNERS.release_for(authority)
        _PATH_CLAIM_ADMISSIONS = _PATH_CLAIM_ADMISSION_OWNERS.exact_active_count()
        return was_live


def _retire_path_claim_finalizer_ticket(
    ticket: int, authority: RootedFinalizerAuthority | None = None
) -> bool:
    """Retire one rooted path-claim generation or publish ACK-only state."""
    global _PATH_CLAIM_FINALIZER_OVERFLOWS, _PATH_CLAIM_FINALIZER_OVERFLOWED
    if ticket < 0:
        return True
    if authority is None:
        # Compatibility for old synthetic callers. Production owns a rooted
        # authority from admission onward.
        try:
            return bool(_PATH_CLAIM_FINALIZER_ESCROW.release_ticket(ticket))
        except BaseException:
            return False
    try:
        retired = retire_or_ack_rooted_finalizer_authority(
            cast(
                ReservedFinalizerEscrow[RootedFinalizerAuthority],
                _PATH_CLAIM_FINALIZER_ESCROW,
            ),
            ticket,
            authority,
        )
        return retired or authority._escrow_armed
    except BaseException:
        _PATH_CLAIM_FINALIZER_OVERFLOWED = True
        try:
            _PATH_CLAIM_FINALIZER_OVERFLOWS += 1
        except MemoryError:
            pass
        return False


def _close_identity_descriptor(descriptor: int) -> None:
    """Close an identity descriptor without masking the ownership result."""
    try:
        os.close(descriptor)
    except OSError:
        pass


@dataclass(slots=True)
class _PathClaimAdmission:
    """Bound live claim ownership with a separately rooted finalizer owner."""

    pid: int
    finalizer_ticket: int
    finalizer_owner: RootedFinalizerAuthority
    counted: bool = False
    released: bool = False
    transferred: bool = False
    lock: Lock = field(default_factory=Lock)

    def transfer(self) -> None:
        with self.lock:
            if self.released:
                raise RuntimeError("path claim admission was already released")
            if not self.counted:
                raise RuntimeError("path claim admission was never committed")
            self.transferred = True

    def release_if_untransferred(self) -> None:
        with self.lock:
            if self.transferred or self.released:
                return
        self.release()

    def release(self) -> None:
        with self.lock:
            if self.released:
                return
            self.released = True
            counted = self.counted
            self.counted = False
            ticket = self.finalizer_ticket
            authority = self.finalizer_owner
        if self.pid != os.getpid():
            return
        retired_counted = _release_path_claim_admission_owner(authority)
        if counted or retired_counted:
            authority.arg1 = False
        if ticket >= 0 and not counted:
            # Construction rollback retains the historical release_ticket fault
            # injection surface, but the separately rooted authority guarantees
            # that a failed retirement can only publish ACK-only ownership.
            authority.make_ack_only()
            try:
                retired = _PATH_CLAIM_FINALIZER_ESCROW.release_ticket(ticket)
            except BaseException:
                retired = False
            if retired:
                authority.ticket = 0
                authority.clear()
                self.finalizer_ticket = -1
            elif _PATH_CLAIM_FINALIZER_ESCROW.publish_rooted(ticket, authority):
                self.finalizer_ticket = -1
            return
        if ticket >= 0:
            if _retire_path_claim_finalizer_ticket(ticket, authority):
                self.finalizer_ticket = -1

    def __del__(self) -> None:
        """Arm only; never take blocking escrow locks from the GC thread."""
        try:
            if self.pid != os.getpid() or self.transferred:
                return
            ticket = self.finalizer_ticket
            authority = self.finalizer_owner
            if ticket < 0:
                return
            if self.released:
                authority.make_ack_only()
            arm_rooted_finalizer_authority(
                cast(
                    ReservedFinalizerEscrow[RootedFinalizerAuthority],
                    _PATH_CLAIM_FINALIZER_ESCROW,
                ),
                ticket,
                authority,
            )
        except BaseException:
            pass


def _acquire_path_claim_admission() -> _PathClaimAdmission:
    global _PATH_CLAIM_ADMISSIONS
    authority = RootedFinalizerAuthority(_run_path_claim_admission_finalizer)
    pid = os.getpid()
    authority.arg0 = pid
    authority.arg1 = False
    admission = _PathClaimAdmission(pid, -1, authority)
    try:
        ticket = _PATH_CLAIM_FINALIZER_ESCROW.reserve_rooted(authority)
        if ticket is None:
            raise RuntimeError("path-claim finalizer escrow exhausted")
        admission.finalizer_ticket = ticket
        with _PATH_CLAIM_ADMISSION_LOCK:
            if _PATH_CLAIM_ADMISSION_OWNERS.exact_active_count() >= _MAX_LIVE_PATH_CLAIMS:
                raise OSError(
                    "process path-claim capacity exhausted; release existing "
                    "PathIdentity owners before claiming more paths"
                )
            owner_token = _PATH_CLAIM_ADMISSION_OWNERS.acquire_for(authority)
            if owner_token is None:
                raise OSError("process path-claim capacity exhausted")
            _PATH_CLAIM_ADMISSIONS = _PATH_CLAIM_ADMISSION_OWNERS.exact_active_count()
            admission.counted = True
            authority.arg1 = True
        return admission
    except BaseException as primary:
        _cleanup_with_note(
            primary,
            admission,
            label="path claim admission rollback also failed",
            method="release",
        )
        raise


@dataclass(slots=True)
class _IdentityDescriptorOwner:
    """Own one no-follow descriptor and linearize close before credit release."""

    descriptor: int | None
    fd_lease: Any | None
    lock: Lock = field(default_factory=Lock)
    _condition: Condition = field(init=False, repr=False)
    _finalizer_ticket: int = field(init=False, repr=False)
    _finalizer_capsule: PreparedFinalizerCleanup = field(init=False, repr=False)
    _physical_opened: bool = field(init=False, default=False, repr=False)
    _state: int = field(init=False, default=0, repr=False)

    _EMPTY = 0
    _ACTIVE = 1
    _CLOSING = 2
    _RELEASE_PENDING = 3
    _TERMINAL_DEBT = 4
    _CLOSED = 5

    def __post_init__(self) -> None:
        # Reserve finalizer capacity while this owner is still empty. Callers
        # using bind_opened() therefore cannot create an unowned descriptor if
        # finalizer admission fails.
        self._condition = Condition(self.lock)
        self._finalizer_capsule = prepare_owner_finalizer_cleanup()
        self._finalizer_ticket = self._finalizer_capsule.ticket
        self._physical_opened = False
        self._state = (
            self._ACTIVE
            if self.descriptor is not None or self.fd_lease is not None
            else self._EMPTY
        )

    def bind_opened(self, descriptor: int, lease: Any) -> None:
        """Attach one already-open descriptor to this prearmed owner."""
        with self._condition:
            if (
                self._state != self._EMPTY
                or self.descriptor is not None
                or self.fd_lease is not None
                or self._physical_opened
            ):
                raise RuntimeError("identity descriptor owner is already bound")
            self.descriptor = descriptor
            self.fd_lease = lease
            # This hook is noexcept for valid positive input; perform it before
            # publishing ACTIVE so every observer sees one coherent transition.
            record_physical_file_descriptors_opened(1)
            self._physical_opened = True
            self._state = self._ACTIVE

    def _retire_finalizer_slot(self) -> None:
        ticket = self._finalizer_ticket
        capsule = self._finalizer_capsule
        if ticket:
            cancel_prepared_finalizer_cleanup(capsule)
            self._finalizer_ticket = 0

    def descriptor_snapshot(self) -> int | None:
        """Return the descriptor after bounded safe-point finalizer progress."""
        try:
            if not runtime_is_finalizing():
                _drain_path_claim_finalizers(limit=8)
        except (NameError, RuntimeError):
            pass
        with self.lock:
            return self.descriptor

    def release(self) -> None:
        """Serialize physical close, debt handoff, and logical lease release."""
        descriptor: int | None = None
        lease: Any | None = None
        debt_only = False
        deadline = monotonic() + _OWNER_CLOSE_WAIT_TIMEOUT_SECONDS
        with self._condition:
            while self._state == self._CLOSING:
                remaining = deadline - monotonic()
                if remaining <= 0:
                    raise TimeoutError("timed out waiting for path-identity close transaction")
                self._condition.wait(timeout=min(0.1, remaining))
            if self._state == self._CLOSED:
                self._retire_finalizer_slot()
                return
            if self._state == self._TERMINAL_DEBT:
                lease = self.fd_lease
                if lease is None:
                    self._retire_finalizer_slot()
                    return
                debt_only = True
                self._state = self._CLOSING
            elif self._state == self._RELEASE_PENDING:
                lease = self.fd_lease
                if lease is None:
                    self._state = self._CLOSED
                    self._condition.notify_all()
                    self._retire_finalizer_slot()
                    return
                self._state = self._CLOSING
            else:
                descriptor = self.descriptor
                lease = self.fd_lease
                if descriptor is None and lease is None:
                    self._state = self._CLOSED
                    self._condition.notify_all()
                    self._retire_finalizer_slot()
                    return
                # Detach before close so EINTR/uncertainty can never lead to a
                # retry against a recycled integer descriptor.  CLOSING keeps
                # every competing releaser from returning the lease meanwhile.
                self.descriptor = None
                self._state = self._CLOSING

        if debt_only:
            retained = bool(
                lease is not None and retain_uncertain_fd_close(lease, label="path-identity")
            )
            with self._condition:
                if retained and self.fd_lease is lease:
                    self.fd_lease = None
                self._state = self._TERMINAL_DEBT
                self._condition.notify_all()
                owns_nothing = self.fd_lease is None
            if owns_nothing:
                self._retire_finalizer_slot()
            if not retained:
                raise RuntimeError("path identity uncertain FD debt could not be retained")
            return

        close_error: BaseException | None = None
        if descriptor is not None:
            try:
                os.close(descriptor)
            except BaseException as exc:
                close_error = exc
            else:
                with self._condition:
                    if self._physical_opened:
                        record_physical_file_descriptors_closed(1)
                        self._physical_opened = False

        if close_error is not None:
            retained = bool(
                lease is not None and retain_uncertain_fd_close(lease, label="path-identity")
            )
            with self._condition:
                if retained and self.fd_lease is lease:
                    self.fd_lease = None
                self._state = self._TERMINAL_DEBT
                self._condition.notify_all()
                owns_nothing = self.fd_lease is None
            if owns_nothing:
                self._retire_finalizer_slot()
            raise close_error

        lease_error: BaseException | None = None
        if lease is not None:
            try:
                lease.release()
            except BaseException as exc:
                lease_error = exc

        with self._condition:
            if lease is not None and lease_error is None and self.fd_lease is lease:
                self.fd_lease = None
            if self.fd_lease is None:
                self._state = self._CLOSED
            else:
                # Physical close committed; only logical-release retry remains.
                self._state = self._RELEASE_PENDING
            self._condition.notify_all()
            owns_nothing = self.fd_lease is None
        if owns_nothing:
            self._retire_finalizer_slot()
        if lease_error is not None:
            raise lease_error

    def __del__(self) -> None:
        """Transfer descriptor cleanup without closing an FD on the GC thread."""
        try:
            if runtime_is_finalizing():
                return
            ticket = getattr(self, "_finalizer_ticket", 0)
            capsule = getattr(self, "_finalizer_capsule", None)
            if ticket and capsule is not None:
                if defer_owner_finalizer_cleanup(self, capsule):
                    self._finalizer_ticket = 0
        except BaseException:
            pass


def _retain_abandoned_descriptor_owner(owner: _IdentityDescriptorOwner) -> bool:
    """Give an uncertain FD/lease cleanup a durable, bounded owner."""
    if owner.descriptor_snapshot() is None and owner.fd_lease is None:
        return True
    try:
        from .retry_scheduler import adopt_failed_release

        if adopt_failed_release(owner, retained_bytes=256):
            return True
    except BaseException:
        pass
    owner_id = id(owner)
    with _ABANDONED_DESCRIPTOR_LOCK:
        if owner_id in _ABANDONED_DESCRIPTOR_OWNERS:
            return True
        # Every entry owns at least one governed descriptor lease.  This ceiling
        # is therefore above the process FD governor's maximum and cannot reject
        # a legitimate unique owner during normal operation.
        if len(_ABANDONED_DESCRIPTOR_OWNERS) >= _MAX_ABANDONED_DESCRIPTOR_OWNERS:
            return False
        _ABANDONED_DESCRIPTOR_OWNERS[owner_id] = owner
    return True


def _drain_abandoned_descriptor_owners(*, limit: int = 8) -> None:
    """Retry a bounded fallback batch without holding the registry lock."""
    with _ABANDONED_DESCRIPTOR_LOCK:
        owners = tuple(_ABANDONED_DESCRIPTOR_OWNERS.values())[: max(0, int(limit))]
    for owner in owners:
        released = False
        try:
            owner.release()
            released = True
        except BaseException:
            try:
                from .retry_scheduler import adopt_failed_release

                released = adopt_failed_release(owner, retained_bytes=256)
            except BaseException:
                released = False
        if released:
            with _ABANDONED_DESCRIPTOR_LOCK:
                if _ABANDONED_DESCRIPTOR_OWNERS.get(id(owner)) is owner:
                    _ABANDONED_DESCRIPTOR_OWNERS.pop(id(owner), None)


@dataclass(slots=True)
class _ScandirCleanupOwner:
    """Keep a persistent scandir handle and FD lease in one close transaction."""

    iterator: Any | None
    lease: Any | None
    lock: Lock = field(default_factory=Lock)
    _condition: Condition = field(init=False, repr=False)
    _finalizer_ticket: int = field(init=False, repr=False)
    _finalizer_capsule: PreparedFinalizerCleanup = field(init=False, repr=False)
    _physical_opened: bool = field(init=False, default=False, repr=False)
    _state: int = field(init=False, default=0, repr=False)

    _EMPTY = 0
    _ACTIVE = 1
    _CLOSING = 2
    _RELEASE_PENDING = 3
    _TERMINAL_DEBT = 4
    _CLOSED = 5

    def __post_init__(self) -> None:
        self._condition = Condition(self.lock)
        self._finalizer_capsule = prepare_owner_finalizer_cleanup()
        self._finalizer_ticket = self._finalizer_capsule.ticket
        self._physical_opened = False
        self._state = (
            self._ACTIVE if self.iterator is not None or self.lease is not None else self._EMPTY
        )

    def bind_opened(self, iterator: Any, lease: Any) -> None:
        """Attach one successfully-created scandir iterator to this owner."""
        with self._condition:
            if (
                self._state != self._EMPTY
                or self.iterator is not None
                or self.lease is not None
                or self._physical_opened
            ):
                raise RuntimeError("scandir cleanup owner is already bound")
            self.iterator = iterator
            self.lease = lease
            record_physical_file_descriptors_opened(1)
            self._physical_opened = True
            self._state = self._ACTIVE

    def _retire_finalizer_slot(self) -> None:
        ticket = self._finalizer_ticket
        if ticket:
            cancel_prepared_finalizer_cleanup(self._finalizer_capsule)
            self._finalizer_ticket = 0

    def release(self) -> None:
        iterator: Any | None = None
        lease: Any | None = None
        debt_only = False
        deadline = monotonic() + _OWNER_CLOSE_WAIT_TIMEOUT_SECONDS
        with self._condition:
            while self._state == self._CLOSING:
                remaining = deadline - monotonic()
                if remaining <= 0:
                    raise TimeoutError("timed out waiting for path-identity close transaction")
                self._condition.wait(timeout=min(0.1, remaining))
            if self._state == self._CLOSED:
                self._retire_finalizer_slot()
                return
            if self._state == self._TERMINAL_DEBT:
                lease = self.lease
                if lease is None:
                    self._retire_finalizer_slot()
                    return
                debt_only = True
                self._state = self._CLOSING
            elif self._state == self._RELEASE_PENDING:
                lease = self.lease
                if lease is None:
                    self._state = self._CLOSED
                    self._condition.notify_all()
                    self._retire_finalizer_slot()
                    return
                self._state = self._CLOSING
            else:
                iterator = self.iterator
                lease = self.lease
                if iterator is None and lease is None:
                    self._state = self._CLOSED
                    self._condition.notify_all()
                    self._retire_finalizer_slot()
                    return
                self.iterator = None
                self._state = self._CLOSING

        if debt_only:
            retained = bool(
                lease is not None
                and retain_uncertain_fd_close(lease, label="path-identity-scandir")
            )
            with self._condition:
                if retained and self.lease is lease:
                    self.lease = None
                self._state = self._TERMINAL_DEBT
                self._condition.notify_all()
                owns_nothing = self.lease is None
            if owns_nothing:
                self._retire_finalizer_slot()
            if not retained:
                raise RuntimeError("scandir uncertain FD debt could not be retained")
            return

        close_error: BaseException | None = None
        if iterator is not None:
            try:
                iterator.close()
            except BaseException as exc:
                close_error = exc
            else:
                with self._condition:
                    if self._physical_opened:
                        record_physical_file_descriptors_closed(1)
                        self._physical_opened = False

        if close_error is not None:
            retained = bool(
                lease is not None
                and retain_uncertain_fd_close(lease, label="path-identity-scandir")
            )
            with self._condition:
                if retained and self.lease is lease:
                    self.lease = None
                self._state = self._TERMINAL_DEBT
                self._condition.notify_all()
                owns_nothing = self.lease is None
            if owns_nothing:
                self._retire_finalizer_slot()
            raise close_error

        lease_error: BaseException | None = None
        if lease is not None:
            try:
                lease.release()
            except BaseException as exc:
                lease_error = exc

        with self._condition:
            if lease is not None and lease_error is None and self.lease is lease:
                self.lease = None
            self._state = self._CLOSED if self.lease is None else self._RELEASE_PENDING
            self._condition.notify_all()
            owns_nothing = self.lease is None
        if owns_nothing:
            self._retire_finalizer_slot()
        if lease_error is not None:
            raise lease_error

    def __del__(self) -> None:
        """Transfer cursor cleanup without closing an FD on the GC thread."""
        try:
            if runtime_is_finalizing():
                return
            ticket = getattr(self, "_finalizer_ticket", 0)
            capsule = getattr(self, "_finalizer_capsule", None)
            if ticket and capsule is not None:
                if defer_owner_finalizer_cleanup(self, capsule):
                    self._finalizer_ticket = 0
        except BaseException:
            pass


def _release_scandir_owner(owner: _ScandirCleanupOwner | None) -> None:
    if owner is None:
        return
    try:
        owner.release()
    except BaseException:
        try:
            from .retry_scheduler import adopt_failed_release

            if adopt_failed_release(owner, retained_bytes=512):
                return
        except BaseException:
            pass
        # The fallback descriptor registry accepts any release-compatible owner.
        with _ABANDONED_DESCRIPTOR_LOCK:
            if len(_ABANDONED_DESCRIPTOR_OWNERS) < _MAX_ABANDONED_DESCRIPTOR_OWNERS:
                _ABANDONED_DESCRIPTOR_OWNERS[id(owner)] = owner


@dataclass(frozen=True, slots=True)
class _ExternalClaim:
    """Versioned process-instance-safe ownership record."""

    pid: int
    process_token: str
    marker: bytes
    created_at_ns: int


def _private_claim_root() -> Path:
    """Return a real, private directory used when xattrs are unavailable."""
    configured_base = os.getenv(_COORDINATION_ENV)
    base = Path(configured_base or tempfile.gettempdir())
    base.mkdir(parents=True, exist_ok=True)
    getuid = getattr(os, "geteuid", None)
    uid = getuid() if getuid is not None else None
    root = base / _CLAIM_DIRECTORY

    # Preserve a securely owned legacy default root so an in-flight process from
    # an earlier version still shares claim authority.  A system-wide temporary
    # directory can also contain the same legacy name owned by another account
    # (for example, root ran first).  That unrelated owner must not deny service
    # to every other UID, so new default roots are isolated by effective UID.
    if configured_base is None and uid is not None:
        try:
            legacy_metadata = os.lstat(root)
        except FileNotFoundError:
            root = base / f"{_CLAIM_DIRECTORY}-{uid}"
        else:
            if not stat.S_ISDIR(legacy_metadata.st_mode) or legacy_metadata.st_uid != uid:
                root = base / f"{_CLAIM_DIRECTORY}-{uid}"

    try:
        os.mkdir(root, 0o700)
    except FileExistsError:
        pass
    metadata = os.lstat(root)
    if not stat.S_ISDIR(metadata.st_mode):
        raise OSError("temporary path claim root must be a real directory")
    if uid is not None and metadata.st_uid != uid:
        raise OSError("temporary path claim root must be owned by the current user")
    try:
        os.chmod(root, 0o700, follow_symlinks=False)
    except (NotImplementedError, TypeError):
        os.chmod(root, 0o700)
    return root


def _claim_key(metadata: os.stat_result) -> str:
    payload = (
        f"{int(metadata.st_dev)}:{int(metadata.st_ino)}:{stat.S_IFMT(metadata.st_mode)}"
    ).encode("ascii")
    return hashlib.blake2b(payload, digest_size=20).hexdigest()


def _claim_path(metadata: os.stat_result) -> Path:
    return _private_claim_root() / f"claim-{_claim_key(metadata)}"


def _read_claim_bytes(path: Path, *, allowed_link_counts: frozenset[int] = frozenset({1})) -> bytes:
    """Read one regular claim record without following links or large allocation."""
    flags = (
        os.O_RDONLY
        | int(getattr(os, "O_NOFOLLOW", 0))
        | int(getattr(os, "O_CLOEXEC", 0))
        | int(getattr(os, "O_NONBLOCK", 0))
    )
    with acquire_file_descriptor_capability(1, label="temporary_claim_read") as capability:
        with capability.open_descriptor(
            lambda: os.open(path, flags), label="temporary_claim_read"
        ) as descriptor:
            metadata = os.fstat(descriptor)
            if not stat.S_ISREG(metadata.st_mode):
                raise OSError("temporary path claim must be a regular file")
            if int(metadata.st_nlink) not in allowed_link_counts:
                raise OSError(
                    "temporary path claim must not have hard-link aliases (unexpected link count)"
                )
            if int(metadata.st_size) > _MAX_CLAIM_BYTES:
                raise OSError("temporary path claim exceeds its size limit")
            chunks: list[bytes] = []
            remaining = _MAX_CLAIM_BYTES + 1
            while remaining > 0:
                chunk = os.read(descriptor, min(remaining, 1024))
                if not chunk:
                    break
                chunks.append(chunk)
                remaining -= len(chunk)
            raw = b"".join(chunks)
            if len(raw) > _MAX_CLAIM_BYTES:
                raise OSError("temporary path claim exceeds its size limit")
            return raw


def _read_claim_at(
    directory_fd: int,
    name: str,
    *,
    allowed_link_counts: frozenset[int] = frozenset({1}),
    capability: FileDescriptorCapability | None = None,
) -> tuple[bytes, os.stat_result]:
    """Read one bounded claim, optionally consuming an already-reserved subcredit."""
    flags = (
        os.O_RDONLY
        | int(getattr(os, "O_NOFOLLOW", 0))
        | int(getattr(os, "O_CLOEXEC", 0))
        | int(getattr(os, "O_NONBLOCK", 0))
    )

    def read_from(descriptor: int) -> tuple[bytes, os.stat_result]:
        metadata = os.fstat(descriptor)
        if not stat.S_ISREG(metadata.st_mode):
            raise OSError("temporary path claim must be a regular file")
        if int(metadata.st_nlink) not in allowed_link_counts:
            raise OSError(
                "temporary path claim must not have hard-link aliases (unexpected link count)"
            )
        if int(metadata.st_size) > _MAX_CLAIM_BYTES:
            raise OSError("temporary path claim exceeds its size limit")
        chunks: list[bytes] = []
        remaining = _MAX_CLAIM_BYTES + 1
        while remaining > 0:
            chunk = os.read(descriptor, min(remaining, 1024))
            if not chunk:
                break
            chunks.append(chunk)
            remaining -= len(chunk)
        raw = b"".join(chunks)
        if len(raw) > _MAX_CLAIM_BYTES:
            raise OSError("temporary path claim exceeds its size limit")
        return raw, metadata

    def opener() -> int:
        return os.open(name, flags, dir_fd=directory_fd)

    if capability is not None:
        with capability.open_descriptor(opener, label="temporary_claim_read") as descriptor:
            return read_from(descriptor)

    with acquire_file_descriptor_capability(1, label="temporary_claim_read") as local_capability:
        with local_capability.open_descriptor(opener, label="temporary_claim_read") as descriptor:
            return read_from(descriptor)


def _validate_open_directory(
    path: Path, descriptor: int, *, require_private: bool = False
) -> os.stat_result:
    """Pin one private directory and reject pathname substitution races."""
    before = os.lstat(path)
    opened = os.fstat(descriptor)
    if not stat.S_ISDIR(before.st_mode) or not stat.S_ISDIR(opened.st_mode):
        raise OSError("temporary path claim root must be a directory")
    if not _same_inode(before, opened):
        raise OSError("temporary path claim root changed while opening")
    uid = getattr(os, "geteuid", lambda: None)()
    if uid is not None and int(opened.st_uid) != int(uid):
        raise OSError("temporary path claim root must be owned by the current user")
    if require_private and int(opened.st_mode) & 0o077:
        raise OSError("temporary path claim root permissions are not private")
    return opened


def _unlink_recovery_alias(path: Path, expected: bytes) -> bool:
    """Remove only a crash-left alias under one atomic two-FD capability."""
    parent = path.parent
    flags = (
        os.O_RDONLY
        | int(getattr(os, "O_DIRECTORY", 0))
        | int(getattr(os, "O_NOFOLLOW", 0))
        | int(getattr(os, "O_CLOEXEC", 0))
    )
    with acquire_file_descriptor_capability(2, label="temporary_claim_recovery") as capability:
        with capability.open_descriptor(
            lambda: os.open(parent, flags), label="temporary_claim_parent"
        ) as descriptor:
            _validate_open_directory(parent, descriptor)
            try:
                current, metadata = _read_claim_at(
                    descriptor,
                    path.name,
                    allowed_link_counts=frozenset({2}),
                    capability=capability,
                )
            except (FileNotFoundError, OSError):
                return False
            if current != expected or int(metadata.st_nlink) != 2:
                return False
            os.unlink(path.name, dir_fd=descriptor)
            os.fsync(descriptor)
            return True


def _remove_claim_if_unchanged(path: Path, expected: bytes) -> bool:
    """Move and unlink one exact claim without recursive FD admission."""
    parent = path.parent
    parent_flags = (
        os.O_RDONLY
        | int(getattr(os, "O_DIRECTORY", 0))
        | int(getattr(os, "O_NOFOLLOW", 0))
        | int(getattr(os, "O_CLOEXEC", 0))
    )
    with acquire_file_descriptor_capability(2, label="temporary_claim_remove") as capability:
        with capability.open_descriptor(
            lambda: os.open(parent, parent_flags), label="temporary_claim_parent"
        ) as parent_fd:
            _validate_open_directory(parent, parent_fd)
            try:
                current, before = _read_claim_at(parent_fd, path.name, capability=capability)
            except FileNotFoundError:
                return True
            except OSError:
                return False
            if current != expected or not stat.S_ISREG(before.st_mode):
                return False
            private_name = f".delete-claim-{path.name}-{secrets.token_hex(8)}"
            try:
                os.rename(
                    path.name,
                    private_name,
                    src_dir_fd=parent_fd,
                    dst_dir_fd=parent_fd,
                )
            except FileNotFoundError:
                return True
            except OSError:
                return False
            try:
                moved_raw, moved = _read_claim_at(parent_fd, private_name, capability=capability)
                if not _same_inode(before, moved) or moved_raw != expected:
                    try:
                        os.replace(
                            private_name,
                            path.name,
                            src_dir_fd=parent_fd,
                            dst_dir_fd=parent_fd,
                        )
                    except OSError:
                        pass
                    return False
                os.unlink(private_name, dir_fd=parent_fd)
                os.fsync(parent_fd)
                return True
            except FileNotFoundError:
                return True
            except OSError:
                return False


def _claim_checksum(payload: dict[str, object]) -> str:
    canonical = json.dumps(payload, sort_keys=True, separators=(",", ":"))
    return hashlib.blake2b(canonical.encode("utf-8"), digest_size=16).hexdigest()


def _serialize_claim(record: _ExternalClaim) -> bytes:
    payload: dict[str, object] = {
        "version": _CLAIM_VERSION,
        "pid": record.pid,
        "process_token": record.process_token,
        "marker": record.marker.hex(),
        "created_at_ns": record.created_at_ns,
    }
    payload["checksum"] = _claim_checksum(payload)
    return (json.dumps(payload, sort_keys=True, separators=(",", ":")) + "\n").encode("utf-8")


def _parse_claim(raw: bytes) -> _ExternalClaim | None:
    """Parse the current record while accepting the pass31 legacy form."""
    try:
        decoded = raw.decode("utf-8").strip()
    except UnicodeError:
        return None
    if decoded.startswith("{"):
        try:
            payload = json.loads(decoded)
            if not isinstance(payload, dict):
                return None
            checksum = str(payload.pop("checksum"))
            if checksum != _claim_checksum(payload):
                return None
            if int(payload.get("version", 0)) != _CLAIM_VERSION:
                return None
            marker = bytes.fromhex(str(payload["marker"]))
            return _ExternalClaim(
                int(payload["pid"]),
                str(payload["process_token"]),
                marker,
                int(payload["created_at_ns"]),
            )
        except (KeyError, TypeError, ValueError, json.JSONDecodeError):
            return None
    try:
        pid_text, marker_hex = decoded.split(":", 1)
        pid = int(pid_text)
        return _ExternalClaim(pid, "unknown", bytes.fromhex(marker_hex), 0)
    except ValueError:
        return None


def _claim_process_alive(record: _ExternalClaim) -> bool:
    if record.pid <= 0:
        return False
    try:
        os.kill(record.pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    except OSError:
        return True
    if record.process_token == "unknown":
        # Legacy pass31 claims cannot distinguish PID reuse, but an actually
        # live legacy writer must never be stolen by a newer process.
        return True
    return process_identity_matches(record.process_token, process_start_token(record.pid))


def _unlink_stale_claim(path: Path, raw: bytes) -> bool:
    """Remove one unchanged stale/corrupt claim without following links."""
    record = _parse_claim(raw)
    if record is not None and _claim_process_alive(record):
        return False
    return _remove_claim_if_unchanged(path, raw)


def _sweep_external_claims(root: Path, *, limit: int = _CLAIM_SWEEP_LIMIT) -> None:
    """Incrementally scan a bounded number of directory entries.

    Only iterator advancement is serialized.  Claim parsing and deletion happen
    after the global cursor lock is released, so a slow or hostile coordination
    filesystem cannot freeze every path-ownership operation in the process.
    """
    global _CLAIM_SWEEP_CURSOR, _CLAIM_SWEEP_ITERATOR, _CLAIM_SWEEP_ROOT
    global _CLAIM_SWEEP_OWNER
    budget = max(0, int(limit))
    if budget == 0:
        return
    root_key = str(root)
    retired_owner: _ScandirCleanupOwner | None = None

    # Detach an iterator for an old coordination root first.  Its potentially
    # blocking close/release is performed outside the cursor lock.
    with _CLAIM_SWEEP_LOCK:
        if _CLAIM_SWEEP_ITERATOR is not None and _CLAIM_SWEEP_ROOT != root_key:
            retired_owner = _CLAIM_SWEEP_OWNER
            _CLAIM_SWEEP_ITERATOR = None
            _CLAIM_SWEEP_OWNER = None
            _CLAIM_SWEEP_ROOT = None
            _CLAIM_SWEEP_CURSOR = None
    _release_scandir_owner(retired_owner)

    # Open one governed persistent scandir handle without blocking while the
    # process-wide cursor lock is held.  Concurrent openers race only at commit;
    # losers close their own handle and lease.
    with _CLAIM_SWEEP_LOCK:
        needs_iterator = _CLAIM_SWEEP_ITERATOR is None
    if needs_iterator:
        candidate_owner: _ScandirCleanupOwner | None = None
        prearmed_owner: _ScandirCleanupOwner | None = None
        lease: Any | None = None
        try:
            prearmed_owner = _ScandirCleanupOwner(None, None)
            lease = acquire_file_descriptors(1)
            iterator = os.scandir(root)
            prearmed_owner.bind_opened(iterator, lease)
            lease = None
            candidate_owner = prearmed_owner
            prearmed_owner = None
        except (OSError, RuntimeError, TimeoutError) as primary:
            if prearmed_owner is not None:
                if lease is not None and prearmed_owner.lease is None:
                    prearmed_owner.lease = lease
                    lease = None
                try:
                    prearmed_owner.release()
                except BaseException as cleanup_error:
                    _release_scandir_owner(prearmed_owner)
                    try:
                        add_bounded_note(primary, "scandir FD cleanup was retained", cleanup_error)
                    except BaseException:
                        pass
            elif lease is not None:
                try:
                    lease.release()
                except BaseException:
                    pass
            candidate_owner = None
        if candidate_owner is not None:
            installed = False
            with _CLAIM_SWEEP_LOCK:
                if _CLAIM_SWEEP_ITERATOR is None:
                    _CLAIM_SWEEP_ITERATOR = candidate_owner.iterator
                    _CLAIM_SWEEP_OWNER = candidate_owner
                    _CLAIM_SWEEP_ROOT = root_key
                    _CLAIM_SWEEP_CURSOR = None
                    installed = True
            if not installed:
                _release_scandir_owner(candidate_owner)

    candidates: list[tuple[Path, bool]] = []
    completed_owner: _ScandirCleanupOwner | None = None
    with _CLAIM_SWEEP_LOCK:
        if _CLAIM_SWEEP_ROOT != root_key:
            return
        examined = 0
        while examined < budget and _CLAIM_SWEEP_ITERATOR is not None:
            try:
                entry = next(_CLAIM_SWEEP_ITERATOR)
            except StopIteration:
                completed_owner = _CLAIM_SWEEP_OWNER
                _CLAIM_SWEEP_ITERATOR = None
                _CLAIM_SWEEP_OWNER = None
                _CLAIM_SWEEP_ROOT = None
                _CLAIM_SWEEP_CURSOR = None
                break
            except OSError:
                completed_owner = _CLAIM_SWEEP_OWNER
                _CLAIM_SWEEP_ITERATOR = None
                _CLAIM_SWEEP_OWNER = None
                _CLAIM_SWEEP_ROOT = None
                break
            # The limit applies to *all* directory entries, not only matching
            # claims.  Unrelated high-cardinality names therefore cannot turn one
            # ownership operation into an unbounded scan.
            examined += 1
            _CLAIM_SWEEP_CURSOR = entry.name
            if entry.name.startswith(("claim-", ".delete-claim-")):
                candidates.append((Path(entry.path), False))
            elif entry.name.startswith(".claim-write-"):
                candidates.append((Path(entry.path), True))
    _release_scandir_owner(completed_owner)

    for path, temporary in candidates:
        try:
            raw = _read_claim_bytes(
                path,
                allowed_link_counts=(frozenset({1, 2}) if temporary else frozenset({1})),
            )
        except OSError:
            continue
        if not temporary:
            _unlink_stale_claim(path, raw)
            continue
        record = _parse_claim(raw)
        if record is not None:
            try:
                link_count = int(path.stat(follow_symlinks=False).st_nlink)
            except OSError:
                link_count = 0
            if link_count == 2:
                _unlink_recovery_alias(path, raw)
            elif link_count == 1 and not _claim_process_alive(record):
                _remove_claim_if_unchanged(path, raw)
            continue
        # Malformed temporary records may still be written by a very slow live
        # publisher.  Use a much longer stabilization period than canonical
        # claims before recovering them.
        try:
            metadata = path.stat(follow_symlinks=False)
        except OSError:
            continue
        if time_ns() - int(metadata.st_mtime_ns) >= _CLAIM_TEMP_STABILIZATION_NS:
            _remove_claim_if_unchanged(path, raw)


def _read_external_claim(metadata: os.stat_result) -> tuple[bytes | None, str | None]:
    path = _claim_path(metadata)
    try:
        raw = _read_claim_bytes(path)
    except FileNotFoundError:
        return None, None
    except OSError:
        return None, None
    parsed = _parse_claim(raw)
    if parsed is None:
        return None, None
    return parsed.marker, str(path)


def _write_claim_payload(descriptor: int, payload: bytes) -> None:
    view = memoryview(payload)
    while view:
        written = os.write(descriptor, view)
        if written <= 0:
            raise OSError("temporary path claim write made no progress")
        view = view[written:]
    os.fsync(descriptor)


def _claim_is_stable_stale(path: Path, raw: bytes) -> bool:
    record = _parse_claim(raw)
    if record is not None:
        return not _claim_process_alive(record)
    try:
        metadata = path.stat(follow_symlinks=False)
    except OSError:
        return False
    return time_ns() - int(metadata.st_mtime_ns) >= _CLAIM_STABILIZATION_NS


def _install_external_claim(
    metadata: os.stat_result,
    marker: bytes,
    claim_admission: _PathClaimAdmission | None = None,
) -> str:
    """Publish a complete claim under an atomic two-descriptor capability."""
    root = _private_claim_root()
    _sweep_external_claims(root)
    claim = root / f"claim-{_claim_key(metadata)}"
    record = _ExternalClaim(os.getpid(), process_start_token(os.getpid()), marker, time_ns())
    payload = _serialize_claim(record)
    root_flags = (
        os.O_RDONLY
        | int(getattr(os, "O_DIRECTORY", 0))
        | int(getattr(os, "O_NOFOLLOW", 0))
        | int(getattr(os, "O_CLOEXEC", 0))
    )
    for _attempt in range(3):
        existing: bytes | None = None
        temporary_name = f".claim-write-{secrets.token_hex(16)}"
        published = False
        with acquire_file_descriptor_capability(2, label="temporary_claim_publish") as capability:
            with capability.open_descriptor(
                lambda: os.open(root, root_flags), label="temporary_claim_root"
            ) as root_fd:
                _validate_open_directory(root, root_fd, require_private=True)
                try:
                    with capability.open_descriptor(
                        lambda: os.open(
                            temporary_name,
                            os.O_WRONLY | os.O_CREAT | os.O_EXCL | int(getattr(os, "O_CLOEXEC", 0)),
                            0o600,
                            dir_fd=root_fd,
                        ),
                        label="temporary_claim_write",
                    ) as descriptor:
                        _write_claim_payload(descriptor, payload)
                    try:
                        os.link(
                            temporary_name,
                            claim.name,
                            src_dir_fd=root_fd,
                            dst_dir_fd=root_fd,
                            follow_symlinks=False,
                        )
                        published = True
                        try:
                            os.fsync(root_fd)
                            os.unlink(temporary_name, dir_fd=root_fd)
                            os.fsync(root_fd)
                        except BaseException as sync_error:
                            try:
                                os.unlink(temporary_name, dir_fd=root_fd)
                            except (FileNotFoundError, OSError):
                                pass
                            rollback_owner = PathClaimOwner(
                                marker,
                                str(claim),
                                None,
                                claim_admission=claim_admission,
                            )
                            if claim_admission is not None:
                                claim_admission.transfer()
                            try:
                                _release_claim_owner(rollback_owner)
                            except BaseException as cleanup_error:
                                try:
                                    claim_still_exists = claim.exists()
                                except OSError:
                                    claim_still_exists = True
                                if claim_still_exists:
                                    _adopt_abandoned_claim_owner(rollback_owner)
                                else:
                                    admission = rollback_owner.claim_admission
                                    rollback_owner.claim_admission = None
                                    rollback_owner.authority_released = True
                                    rollback_owner.descriptor_released = True
                                    rollback_owner.released = True
                                    if admission is not None:
                                        _cleanup_with_note(
                                            sync_error,
                                            admission,
                                            label="path claim admission release failed",
                                            method="release",
                                        )
                                try:
                                    add_bounded_note(
                                        sync_error,
                                        "published claim rollback cleanup reported",
                                        cleanup_error,
                                    )
                                except BaseException:
                                    pass
                            raise
                        return str(claim)
                    except FileExistsError as exc:
                        try:
                            existing, _existing_metadata = _read_claim_at(
                                root_fd, claim.name, capability=capability
                            )
                        except OSError:
                            raise OSError("temporary path is already owned") from exc
                finally:
                    try:
                        os.unlink(temporary_name, dir_fd=root_fd)
                    except FileNotFoundError:
                        pass
                    except OSError:
                        if not published:
                            pass
        # The two-credit capability is fully released before helpers that may
        # acquire their own descriptor bundles, eliminating hold-and-wait.
        if existing is not None:
            if _release_abandoned_claim_for_path(str(claim)):
                continue
            if _claim_is_stable_stale(claim, existing):
                if _unlink_stale_claim(claim, existing):
                    continue
            raise OSError("temporary path is already owned")
    raise OSError("temporary path is already owned")


@dataclass(frozen=True, slots=True, eq=False)
class PathFingerprint:
    """Immutable observation of one directory entry."""

    device: int
    inode: int
    file_type: int
    change_time_ns: int
    owner_marker: bytes | None = None
    external_claim_path: str | None = None

    def _comparison_key(self) -> tuple[object, ...]:
        discriminator: tuple[str, object]
        if self.owner_marker is not None:
            discriminator = ("marker", self.owner_marker)
        else:
            discriminator = ("ctime", self.change_time_ns)
        return (self.device, self.inode, self.file_type, discriminator)

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, PathFingerprint):
            return NotImplemented
        return self._comparison_key() == other._comparison_key()

    def __hash__(self) -> int:
        return hash(self._comparison_key())


def _run_path_claim_finalizer(authority: RootedFinalizerAuthority) -> None:
    """Release a path claim from wrapper-independent exact cleanup state."""
    marker = cast(bytes | None, authority.arg0)
    external_claim_path = cast(str | None, authority.arg1)
    descriptor_owner = cast(_IdentityDescriptorOwner | None, authority.arg2)
    admission = cast(_PathClaimAdmission | None, authority.arg3)
    owner_pid = int(cast(int, authority.arg4) or 0)

    if marker is not None or external_claim_path is not None:
        if owner_pid == os.getpid():
            if external_claim_path is not None:
                claim = Path(str(external_claim_path))
                try:
                    raw = _read_claim_bytes(claim)
                except FileNotFoundError:
                    pass
                else:
                    parsed = _parse_claim(raw)
                    if parsed is None or parsed.marker != marker:
                        raise OSError(
                            "temporary path claim ownership changed before finalizer release"
                        )
                    if not _remove_claim_if_unchanged(claim, raw):
                        raise OSError("temporary path claim changed during finalizer release")
            elif marker is not None and descriptor_owner is not None:
                with descriptor_owner.lock:
                    descriptor = descriptor_owner.descriptor
                if descriptor is not None:
                    _remove_owner_marker(descriptor, marker)
        # A forked child drops only its copied logical authority.
        authority.arg0 = None
        authority.arg1 = None

    descriptor_owner = cast(_IdentityDescriptorOwner | None, authority.arg2)
    if descriptor_owner is not None:
        descriptor_owner.release()
        authority.arg2 = None

    admission = cast(_PathClaimAdmission | None, authority.arg3)
    if admission is not None:
        # process_one already owns retirement of this same finalizer ticket.
        # Prevent admission.release() from trying to retire the generation twice.
        admission.finalizer_ticket = -1
        admission.release()
        authority.arg3 = None


@dataclass(slots=True)
class PathClaimOwner:
    """Exclusive path authority plus independently retryable FD cleanup."""

    owner_marker: bytes | None
    external_claim_path: str | None
    descriptor_owner: _IdentityDescriptorOwner | None
    claim_admission: _PathClaimAdmission | None = None
    owner_pid: int = field(default_factory=os.getpid)
    lock: Lock = field(default_factory=Lock)
    authority_released: bool = False
    descriptor_released: bool = False
    descriptor_releasing: bool = False
    released: bool = False
    finalizer_ticket: int = -1
    finalizer_owner: RootedFinalizerAuthority | None = field(default=None, init=False, repr=False)

    def __post_init__(self) -> None:
        admission = self.claim_admission
        if admission is not None:
            ticket = admission.finalizer_ticket
            authority = admission.finalizer_owner
            if ticket < 0 or authority.ticket != ticket:
                raise RuntimeError("path-claim admission lost finalizer authority")
            # Transfer the already-rooted generation in place; never replace the
            # escrow owner or create a second authority for the same ticket.
            authority.callback = _run_path_claim_finalizer
        else:
            authority = RootedFinalizerAuthority(_run_path_claim_finalizer)
            self.finalizer_owner = authority
            try:
                reserved_ticket = _PATH_CLAIM_FINALIZER_ESCROW.reserve_rooted(authority)
                if reserved_ticket is None:
                    raise RuntimeError("path-claim finalizer escrow exhausted")
                ticket = reserved_ticket
            except BaseException:
                try:
                    _PATH_CLAIM_FINALIZER_ESCROW.release_rooted_owner(authority)
                except BaseException:
                    pass
                raise
        authority.arg0 = self.owner_marker
        authority.arg1 = self.external_claim_path
        authority.arg2 = self.descriptor_owner
        authority.arg3 = admission
        authority.arg4 = self.owner_pid
        self.finalizer_ticket = ticket
        self.finalizer_owner = authority

    def release(self) -> None:
        """Release both logical path authority and its descriptor resources."""
        _release_claim_owner(self)

    def __del__(self) -> None:
        """Arm the pre-rooted claim authority without filesystem I/O."""
        try:
            if runtime_is_finalizing():
                return
            ticket = getattr(self, "finalizer_ticket", -1)
            authority = getattr(self, "finalizer_owner", None)
            if ticket < 0 or not isinstance(authority, RootedFinalizerAuthority):
                return
            if getattr(self, "released", False):
                authority.make_ack_only()
            if _PATH_CLAIM_FINALIZER_ESCROW.publish_rooted(ticket, authority):
                self.finalizer_ticket = -1
                return
            global _PATH_CLAIM_FINALIZER_OVERFLOWS, _PATH_CLAIM_FINALIZER_OVERFLOWED
            _PATH_CLAIM_FINALIZER_OVERFLOWED = True
            try:
                _PATH_CLAIM_FINALIZER_OVERFLOWS += 1
            except MemoryError:
                pass
        except BaseException:
            pass


@dataclass(frozen=True, slots=True, eq=False)
class PathIdentity(PathFingerprint):
    """Fingerprint plus optional exclusive ownership authority."""

    claim_owner: PathClaimOwner | None = field(default=None, compare=False, hash=False, repr=False)

    @property
    def descriptor_owner(self) -> _IdentityDescriptorOwner | None:
        return self.claim_owner.descriptor_owner if self.claim_owner is not None else None

    @property
    def owns_claim(self) -> bool:
        return self.claim_owner is not None and not self.claim_owner.authority_released

    @classmethod
    def from_stat(
        cls,
        metadata: os.stat_result,
        *,
        owner_marker: bytes | None = None,
        external_claim_path: str | None = None,
        descriptor_owner: _IdentityDescriptorOwner | None = None,
        claim_admission: _PathClaimAdmission | None = None,
        owns_claim: bool = False,
    ) -> "PathIdentity":
        claim_owner = None
        if owns_claim:
            claim_owner = PathClaimOwner(
                owner_marker,
                external_claim_path,
                descriptor_owner,
                claim_admission=claim_admission,
            )
        return cls(
            int(metadata.st_dev),
            int(metadata.st_ino),
            stat.S_IFMT(metadata.st_mode),
            int(metadata.st_ctime_ns),
            owner_marker,
            external_claim_path,
            claim_owner,
        )


def _same_inode(left: os.stat_result, right: os.stat_result) -> bool:
    return (
        int(left.st_dev) == int(right.st_dev)
        and int(left.st_ino) == int(right.st_ino)
        and stat.S_IFMT(left.st_mode) == stat.S_IFMT(right.st_mode)
    )


def _open_identity_fd(path: str | Path) -> _IdentityDescriptorOwner | None:
    """Open one governed no-follow descriptor into a prearmed terminal owner."""
    try:
        metadata = os.lstat(path)
    except FileNotFoundError:
        return None

    owner = _IdentityDescriptorOwner(None, None)
    lease: Any | None = None
    try:
        lease = acquire_file_descriptors(1)
        if not _HAS_POSIX_PATH_AUTHORITY:
            lease.release()
            lease = None
            owner.release()
            return None
        common = int(getattr(os, "O_CLOEXEC", 0)) | int(getattr(os, "O_NOFOLLOW", 0))
        file_type = stat.S_IFMT(metadata.st_mode)
        descriptor: int | None = None
        path_flag = getattr(os, "O_PATH", None)
        if path_flag is not None:
            try:
                descriptor = os.open(path, common | int(path_flag))
            except OSError as exc:
                if exc.errno in _RESOURCE_OPEN_ERRNOS:
                    raise
                descriptor = None
        if descriptor is None and file_type in (stat.S_IFREG, stat.S_IFDIR):
            try:
                descriptor = os.open(
                    path,
                    common | int(getattr(os, "O_NONBLOCK", 0)) | os.O_RDONLY,
                )
            except OSError as exc:
                if exc.errno in _RESOURCE_OPEN_ERRNOS:
                    raise
                descriptor = None
        if descriptor is None:
            lease.release()
            lease = None
            owner.release()
            return None
        owner.bind_opened(descriptor, lease)
        lease = None
        return owner
    except BaseException as primary:
        if lease is not None and owner.fd_lease is None:
            owner.fd_lease = lease
        try:
            owner.release()
        except BaseException as cleanup_error:
            _retain_abandoned_descriptor_owner(owner)
            try:
                add_bounded_note(
                    primary,
                    "identity descriptor cleanup failed and was retained",
                    cleanup_error,
                )
            except BaseException:
                pass
        raise


def _read_owner_marker(path: str | Path | int) -> bytes | None:
    getter = getattr(os, "getxattr", None)
    if getter is None:
        return None
    try:
        if isinstance(path, int):
            return bytes(getter(path, _OWNER_XATTR))
        return bytes(getter(path, _OWNER_XATTR, follow_symlinks=False))
    except (OSError, TypeError, NotImplementedError):
        return None


def _set_new_owner_marker(path: str | Path | int, marker: bytes) -> bool:
    setter = getattr(os, "setxattr", None)
    if setter is None:
        return False
    flags = int(getattr(os, "XATTR_CREATE", 1))
    try:
        if isinstance(path, int):
            setter(path, _OWNER_XATTR, marker, flags)
        else:
            setter(path, _OWNER_XATTR, marker, flags, follow_symlinks=False)
        return True
    except OSError as exc:
        if exc.errno == errno.EEXIST:
            raise OSError("temporary path is already owned") from exc
        return False
    except (TypeError, NotImplementedError):
        return False


def _remove_owner_marker(descriptor: int, marker: bytes) -> None:
    remover = getattr(os, "removexattr", None)
    if remover is None:
        raise OSError("filesystem owner marker cannot be removed")
    current = _read_owner_marker(descriptor)
    if current is None:
        return
    if current != marker:
        raise OSError("temporary path xattr ownership changed before release")
    try:
        remover(descriptor, _OWNER_XATTR)
    except OSError as exc:
        if exc.errno in (errno.ENODATA, getattr(errno, "ENOATTR", errno.ENODATA)):
            return
        raise


def _claim_from_metadata(
    metadata: os.stat_result, descriptor_owner: _IdentityDescriptorOwner | None
) -> PathIdentity:
    """Claim the fstat identity while transferring one already-prearmed FD owner."""
    admission = _acquire_path_claim_admission()
    descriptor = descriptor_owner.descriptor_snapshot() if descriptor_owner is not None else None
    try:
        if not _HAS_POSIX_PATH_AUTHORITY:
            identity = PathIdentity.from_stat(
                metadata,
                descriptor_owner=descriptor_owner,
                claim_admission=admission,
                owns_claim=True,
            )
            admission.transfer()
            _arm_path_claim_finalizer_pulse()
            return identity
        candidate = secrets.token_bytes(16)
        installed = descriptor is not None and _set_new_owner_marker(descriptor, candidate)
        marker = (
            candidate
            if installed
            else (_read_owner_marker(descriptor) if descriptor is not None else None)
        )
        claim_path: str | None = None
        if marker is None:
            claim_path = _install_external_claim(metadata, candidate, claim_admission=admission)
            marker = candidate
        identity = PathIdentity.from_stat(
            metadata,
            owner_marker=marker,
            external_claim_path=claim_path,
            descriptor_owner=descriptor_owner,
            claim_admission=admission,
            owns_claim=True,
        )
        admission.transfer()
        _arm_path_claim_finalizer_pulse()
        return identity
    finally:
        admission.release_if_untransferred()


def claim_path_identity(path: str | Path) -> PathIdentity | None:
    """Atomically claim one exact entry without following or blocking on it."""
    _drain_path_claim_finalizers(limit=8)
    _drain_abandoned_descriptor_owners(limit=8)
    _drain_abandoned_claim_owners(limit=8)
    descriptor_owner = _open_identity_fd(path)
    if descriptor_owner is not None:
        identity: PathIdentity | None = None
        transferred = False
        try:
            descriptor = descriptor_owner.descriptor_snapshot()
            if descriptor is None:
                raise RuntimeError("identity descriptor owner lost its descriptor before claim")
            owned = os.fstat(descriptor)
            try:
                current = os.lstat(path)
            except FileNotFoundError as exc:
                raise OSError("temporary path disappeared while claiming ownership") from exc
            if not _same_inode(owned, current):
                raise OSError("temporary path changed while claiming ownership")
            identity = _claim_from_metadata(owned, descriptor_owner)
            transferred = True
            try:
                after = os.lstat(path)
            except FileNotFoundError as exc:
                release_path_identity(identity)
                raise OSError("temporary path disappeared while claiming ownership") from exc
            if not _same_inode(owned, after):
                release_path_identity(identity)
                raise OSError("temporary path changed while claiming ownership")
            return identity
        finally:
            if not transferred:
                try:
                    descriptor_owner.release()
                except BaseException:
                    _retain_abandoned_descriptor_owner(descriptor_owner)

    try:
        before = os.lstat(path)
    except FileNotFoundError:
        return None
    identity = _claim_from_metadata(before, None)
    try:
        after = os.lstat(path)
    except FileNotFoundError as exc:
        release_path_identity(identity)
        raise OSError("temporary path disappeared while claiming ownership") from exc
    if not _same_inode(before, after):
        release_path_identity(identity)
        raise OSError("temporary path changed while claiming ownership")
    return identity


def lstat_identity(path: str | Path) -> PathIdentity | None:
    """Return a no-follow identity for a path, or ``None`` if absent."""
    try:
        metadata = os.lstat(path)
    except FileNotFoundError:
        return None
    marker = _read_owner_marker(path)
    claim_path: str | None = None
    if marker is None and _HAS_POSIX_PATH_AUTHORITY:
        marker, claim_path = _read_external_claim(metadata)
    return PathIdentity.from_stat(metadata, owner_marker=marker, external_claim_path=claim_path)


def _claim_owner_retry_key(owner: PathClaimOwner) -> tuple[str, int]:
    return ("path-claim-owner", id(owner))


def _retain_abandoned_claim_owner(owner: PathClaimOwner, *, delay_seconds: float = 0.01) -> None:
    """Publish a finalizer owner into preallocated storage without blocking.

    This function is intentionally safe to call from ``__del__``: it performs
    only a non-blocking write into the fixed finalizer escrow. Rich adoption,
    scheduling and filesystem work happen later on governed callers.
    """
    del delay_seconds
    global _PATH_CLAIM_FINALIZER_OVERFLOWS, _PATH_CLAIM_FINALIZER_OVERFLOWED
    if owner.released or owner.owner_pid != os.getpid():
        return
    ticket = getattr(owner, "finalizer_ticket", -1)
    authority = getattr(owner, "finalizer_owner", None)
    if type(ticket) is int and ticket >= 0:
        if isinstance(authority, RootedFinalizerAuthority):
            if _PATH_CLAIM_FINALIZER_ESCROW.publish_rooted(ticket, authority):
                owner.finalizer_ticket = -1
                return
        elif _PATH_CLAIM_FINALIZER_ESCROW.publish_reserved(ticket, owner):
            return
    _PATH_CLAIM_FINALIZER_OVERFLOWED = True
    try:
        _PATH_CLAIM_FINALIZER_OVERFLOWS += 1
    except MemoryError:
        pass


def _path_claim_finalizer_pulse() -> None:
    """Drain finalized path owners from a normal retry worker, never GC."""
    _drain_path_claim_finalizers(limit=64)
    _drain_abandoned_descriptor_owners(limit=64)
    _drain_abandoned_claim_owners(limit=64)
    try:
        with _PATH_CLAIM_ADMISSION_LOCK:
            active_claims = _PATH_CLAIM_ADMISSION_OWNERS.exact_active_count() > 0
        with _ABANDONED_CLAIM_LOCK:
            pending_claims = bool(_ABANDONED_CLAIM_OWNERS)
        pending_finalizers = _PATH_CLAIM_FINALIZER_ESCROW.active_count() > 0
    except BaseException:
        return
    if active_claims or pending_claims or pending_finalizers:
        _arm_path_claim_finalizer_pulse()


def _arm_path_claim_finalizer_pulse() -> None:
    """Pre-arm bounded cleanup while live claims can later become abandoned."""
    try:
        from .retry_scheduler import schedule_retry

        schedule_retry(
            ("path-claim-finalizer-pulse", 0),
            _path_claim_finalizer_pulse,
            delay_seconds=0.25,
            retained_bytes=128,
            jitter_fraction=0.1,
        )
    except BaseException:
        # Admission remains fail-closed. The next path operation and runtime
        # shutdown both drain the same authoritative escrow.
        pass


def _adopt_abandoned_claim_owner(owner: PathClaimOwner, *, delay_seconds: float = 0.01) -> None:
    """Keep an abandoned authority reachable until cleanup makes commit.

    Publication is transactional: either the local bounded registry owns the
    object or the shared release guardian accepts it.  No direct filesystem I/O
    is performed while the registry lock (or a Python finalizer) is active.
    """
    if owner.released:
        return
    owner_id = id(owner)
    locally_owned = False
    with _ABANDONED_CLAIM_LOCK:
        if owner_id in _ABANDONED_CLAIM_OWNERS:
            locally_owned = True
        elif len(_ABANDONED_CLAIM_OWNERS) < _MAX_ABANDONED_CLAIM_OWNERS:
            _ABANDONED_CLAIM_OWNERS[owner_id] = owner
            locally_owned = True

    if not locally_owned:
        try:
            from .retry_scheduler import adopt_failed_release

            if adopt_failed_release(owner, retained_bytes=512):
                return
        except BaseException:
            pass
        try:
            from .cleanup_dispatcher import CleanupSubsystem, dispatch_cleanup

            if dispatch_cleanup(
                _retry_abandoned_claim_owner_token,
                owner_id,
                retained_bytes=1024,
                subsystem=CleanupSubsystem.STORAGE,
            ):
                return
        except BaseException:
            pass
        # Normal owners are admitted below half of this registry ceiling, so
        # reaching every bounded fallback simultaneously requires a violated
        # internal invariant or synthetic unadmitted owners.  Fail closed: the
        # marker/claim remains in place rather than releasing somebody else's
        # pathname or allocating an unbounded Python registry.
        return

    scheduled = False
    try:
        from .retry_scheduler import adopt_failed_release, schedule_retry

        normalized_delay = normalize_duration(
            delay_seconds, name="abandoned claim retry delay", allow_zero=True
        )
        if normalized_delay is None:
            raise RuntimeError("normalized abandoned claim retry delay is missing")

        def retry_owner() -> None:
            _retry_abandoned_claim_owner_token(owner_id)

        scheduled = schedule_retry(
            _claim_owner_retry_key(owner),
            retry_owner,
            delay_seconds=normalized_delay,
            retained_bytes=512,
            jitter_fraction=0.2,
        )
        if not scheduled and adopt_failed_release(owner, retained_bytes=512):
            # The guardian now holds a second reference.  Remove the local one
            # so byte/owner accounting has a single authoritative channel.
            with _ABANDONED_CLAIM_LOCK:
                if _ABANDONED_CLAIM_OWNERS.get(owner_id) is owner:
                    _ABANDONED_CLAIM_OWNERS.pop(owner_id, None)
            return
    except BaseException:
        scheduled = False
    # On scheduling failure the bounded local registry remains the durable
    # owner and will be retried by the next path operation.


def _retry_abandoned_claim_owner_token(owner_id: int) -> None:
    """Retry one retained claim using only a compact scheduler/cleanup token."""
    with _ABANDONED_CLAIM_LOCK:
        owner = _ABANDONED_CLAIM_OWNERS.get(owner_id)
    if owner is None:
        return
    _retry_abandoned_claim_owner(owner)


def _retry_abandoned_claim_owner(owner: PathClaimOwner) -> None:
    try:
        _release_claim_owner(owner)
    except BaseException:
        _adopt_abandoned_claim_owner(owner)
        return
    with _ABANDONED_CLAIM_LOCK:
        _ABANDONED_CLAIM_OWNERS.pop(id(owner), None)


def _drain_path_claim_finalizers(*, limit: int = 8) -> int:
    """Move GC-published claim owners without batch-allocation ownership gaps."""
    normalized_limit = max(0, int(limit))
    progressed = 0

    def process(ticket: int, owner: RootedFinalizerAuthority | PathClaimOwner) -> None:
        nonlocal progressed
        if isinstance(owner, RootedFinalizerAuthority):
            owner.ticket = ticket
            owner.run()
            owner.clear()
            progressed += 1
            return
        if isinstance(owner, PathClaimOwner) and not owner.released:
            owner.finalizer_ticket = ticket
            try:
                _release_claim_owner(owner)
            except BaseException:
                _adopt_abandoned_claim_owner(owner)
            progressed += 1

    attempts = min(normalized_limit, _PATH_CLAIM_FINALIZER_ESCROW.active_count())
    for _ in range(attempts):
        try:
            if not _PATH_CLAIM_FINALIZER_ESCROW.process_one(process):
                break
        except BaseException:
            continue
    return progressed


def path_claim_finalizer_snapshot() -> tuple[int, int]:
    return (
        _PATH_CLAIM_FINALIZER_ESCROW.published_count(),
        max(1, _PATH_CLAIM_FINALIZER_OVERFLOWS)
        if (_PATH_CLAIM_FINALIZER_OVERFLOWED or _PATH_CLAIM_FINALIZER_ESCROW.overflowed)
        else _PATH_CLAIM_FINALIZER_OVERFLOWS,
    )


def _drain_abandoned_claim_owners(*, limit: int = 8) -> None:
    with _ABANDONED_CLAIM_LOCK:
        owners = tuple(_ABANDONED_CLAIM_OWNERS.values())[: max(0, int(limit))]
    for owner in owners:
        _retry_abandoned_claim_owner(owner)


def _release_abandoned_claim_for_path(path: str) -> bool:
    with _ABANDONED_CLAIM_LOCK:
        owners = tuple(
            owner for owner in _ABANDONED_CLAIM_OWNERS.values() if owner.external_claim_path == path
        )
    released = False
    for owner in owners:
        try:
            _release_claim_owner(owner)
        except BaseException:
            continue
        released = True
    return released


def _release_claim_owner(owner: PathClaimOwner) -> None:
    """Release path authority once, then independently drain FD resources."""
    descriptor_owner: _IdentityDescriptorOwner | None
    admission: _PathClaimAdmission | None = None
    with owner.lock:
        if owner.released:
            return
        if not owner.authority_released:
            descriptor_owner = owner.descriptor_owner
            if descriptor_owner is not None:
                with descriptor_owner.lock:
                    descriptor = descriptor_owner.descriptor
            else:
                descriptor = None
            if owner.owner_pid != os.getpid():
                # A forked child owns only its copied descriptor, never the
                # parent's pathname marker/claim.  Forget authority locally and
                # leave the parent-visible ownership record untouched.
                owner.authority_released = True
                owner.external_claim_path = None
                owner.owner_marker = None
            else:
                if owner.external_claim_path is not None:
                    claim = Path(owner.external_claim_path)
                    try:
                        raw = _read_claim_bytes(claim)
                    except FileNotFoundError:
                        pass
                    else:
                        parsed = _parse_claim(raw)
                        if parsed is None or parsed.marker != owner.owner_marker:
                            raise OSError("temporary path claim ownership changed before release")
                        if not _remove_claim_if_unchanged(claim, raw):
                            raise OSError("temporary path claim changed during release")
                elif owner.owner_marker is not None and descriptor is not None:
                    _remove_owner_marker(descriptor, owner.owner_marker)
                # This commit is irreversible. A later FD cleanup error must
                # never restore logical authority over the pathname.
                owner.authority_released = True
                owner.external_claim_path = None
                owner.owner_marker = None
            authority = getattr(owner, "finalizer_owner", None)
            if isinstance(authority, RootedFinalizerAuthority):
                authority.arg0 = None
                authority.arg1 = None

        descriptor_owner = owner.descriptor_owner
        if descriptor_owner is None:
            owner.descriptor_released = True
            owner.released = True
            admission = owner.claim_admission
            owner.claim_admission = None
        elif owner.descriptor_releasing:
            return
        else:
            owner.descriptor_releasing = True

    if descriptor_owner is not None:
        try:
            descriptor_owner.release()
        except BaseException:
            with owner.lock:
                owner.descriptor_releasing = False
            _adopt_abandoned_claim_owner(owner)
            raise
        with owner.lock:
            if owner.descriptor_owner is descriptor_owner:
                owner.descriptor_owner = None
            authority = getattr(owner, "finalizer_owner", None)
            if isinstance(authority, RootedFinalizerAuthority):
                authority.arg2 = None
            owner.descriptor_releasing = False
            owner.descriptor_released = True
            owner.released = True
            admission = owner.claim_admission
            owner.claim_admission = None

    authority = getattr(owner, "finalizer_owner", None)
    if admission is not None:
        admission.release()
        if isinstance(authority, RootedFinalizerAuthority):
            authority.arg3 = None
            authority.make_ack_only()
        if admission.finalizer_ticket < 0:
            owner.finalizer_ticket = -1
            if isinstance(authority, RootedFinalizerAuthority):
                authority.clear()
    else:
        ticket = owner.finalizer_ticket
        if isinstance(authority, RootedFinalizerAuthority):
            authority.make_ack_only()
        if ticket >= 0 and _retire_path_claim_finalizer_ticket(ticket, authority):
            owner.finalizer_ticket = -1
            if isinstance(authority, RootedFinalizerAuthority):
                authority.clear()
    with _ABANDONED_CLAIM_LOCK:
        _ABANDONED_CLAIM_OWNERS.pop(id(owner), None)


def release_path_identity(identity: PathIdentity | None) -> None:
    """Release only an identity carrying explicit claim authority."""
    if identity is None:
        return
    owner = identity.claim_owner
    if owner is None:
        raise OSError("path fingerprint does not own a releasable claim")
    _release_claim_owner(owner)


def transfer_identity_matches(before: PathIdentity | None, after: PathIdentity | None) -> bool:
    """Return whether two identities prove the same transferred artifact."""
    if before is None or after is None:
        return False
    if (
        before.device != after.device
        or before.inode != after.inode
        or before.file_type != after.file_type
    ):
        return False
    if before.owner_marker is not None or after.owner_marker is not None:
        return before.owner_marker is not None and before.owner_marker == after.owner_marker
    return True


def identity_matches(path: str | Path, expected: PathIdentity | None) -> bool:
    """Return whether a path still names the expected claimed identity."""
    return expected is not None and lstat_identity(path) == expected


def _reset_path_identity_after_fork() -> None:
    global _CLAIM_SWEEP_CURSOR, _CLAIM_SWEEP_ITERATOR, _CLAIM_SWEEP_ROOT
    global _CLAIM_SWEEP_OWNER, _CLAIM_SWEEP_LOCK, _ABANDONED_CLAIM_LOCK
    global _ABANDONED_DESCRIPTOR_LOCK, _PATH_CLAIM_ADMISSION_LOCK
    global _PATH_CLAIM_ADMISSIONS, _PATH_CLAIM_ADMISSION_OWNERS, _ABANDONED_CLAIM_OWNERS
    global _ABANDONED_DESCRIPTOR_OWNERS, _FORKED_PATH_GENERATIONS
    global _PATH_CLAIM_FINALIZER_OVERFLOWS, _PATH_CLAIM_FINALIZER_OVERFLOWED
    # Do not close iterators, release owners, acquire inherited locks or clear
    # containers here.  Any of those actions can deadlock on a lock owned by a
    # vanished parent thread or execute arbitrary finalizers in atfork context.
    quarantine_inherited_state(
        "path-identity",
        _CLAIM_SWEEP_ITERATOR,
        _CLAIM_SWEEP_OWNER,
        _ABANDONED_CLAIM_OWNERS,
        _ABANDONED_DESCRIPTOR_OWNERS,
        _PATH_CLAIM_ADMISSION_OWNERS,
    )
    _FORKED_PATH_GENERATIONS += 1
    _CLAIM_SWEEP_CURSOR = None
    _CLAIM_SWEEP_ITERATOR = None
    _CLAIM_SWEEP_ROOT = None
    _CLAIM_SWEEP_OWNER = None
    _CLAIM_SWEEP_LOCK = Lock()
    _ABANDONED_CLAIM_OWNERS = {}
    _ABANDONED_CLAIM_LOCK = Lock()
    _ABANDONED_DESCRIPTOR_OWNERS = {}
    _ABANDONED_DESCRIPTOR_LOCK = Lock()
    _PATH_CLAIM_ADMISSION_LOCK = Lock()
    _PATH_CLAIM_ADMISSIONS = 0
    _PATH_CLAIM_ADMISSION_OWNERS = _PATH_CLAIM_ADMISSION_OWNERS_FORK_FRESH
    _PATH_CLAIM_FINALIZER_ESCROW.reset_after_fork()
    _PATH_CLAIM_FINALIZER_OVERFLOWS = 0
    _PATH_CLAIM_FINALIZER_OVERFLOWED = False


from .fork_manager import register_fork_handler as _register_fork_handler  # noqa: E402

_register_fork_handler("path-identity", mode="quarantine_only")


from .finalizer_registry import (  # noqa: E402
    register_finalizer_domain as _register_finalizer_domain,
)

_register_finalizer_domain(
    "path_claim",
    drain=_drain_path_claim_finalizers,
    snapshot=path_claim_finalizer_snapshot,
    escrows=(("path_claim", _PATH_CLAIM_FINALIZER_ESCROW),),
)


__all__ = [
    "PathClaimOwner",
    "PathFingerprint",
    "PathIdentity",
    "claim_path_identity",
    "identity_matches",
    "lstat_identity",
    "release_path_identity",
    "transfer_identity_matches",
]
