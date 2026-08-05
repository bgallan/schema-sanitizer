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
from threading import Lock
from time import time_ns
from typing import Any

from .durations import normalize_duration
from .fork_safety import quarantine_inherited_state
from .process_identity import process_identity_matches, process_start_token
from .process_resources import acquire_file_descriptors, retain_uncertain_fd_close
from .resource_lifecycle import _cleanup_with_note
from .safe_errors import add_bounded_note

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
_ABANDONED_DESCRIPTOR_OWNERS: dict[int, Any] = {}
_ABANDONED_DESCRIPTOR_LOCK = Lock()
_MAX_ABANDONED_DESCRIPTOR_OWNERS = 8192
# Inherited owners must never be destroyed from an after-fork callback: their
# internal locks may have belonged to vanished parent threads.  The child model
# is fork+exec; quarantined references are intentionally retained until exec.
_FORKED_PATH_KEEPALIVE: list[object] = []
_FORKED_PATH_GENERATIONS = 0
_CLAIM_STABILIZATION_NS = 2_000_000_000
_CLAIM_TEMP_STABILIZATION_NS = 300_000_000_000
_RESOURCE_OPEN_ERRNOS = {errno.EMFILE, errno.ENFILE, errno.ENOMEM, errno.EACCES, errno.EPERM}


def _close_identity_descriptor(descriptor: int) -> None:
    """Close an identity descriptor without masking the ownership result."""
    try:
        os.close(descriptor)
    except OSError:
        pass


@dataclass(slots=True)
class _PathClaimAdmission:
    """Bound live claim/control-block ownership before mutating a pathname."""

    pid: int
    released: bool = False
    transferred: bool = False
    lock: Lock = field(default_factory=Lock)

    def transfer(self) -> None:
        with self.lock:
            if self.released:
                raise RuntimeError("path claim admission was already released")
            self.transferred = True

    def release_if_untransferred(self) -> None:
        with self.lock:
            if self.transferred or self.released:
                return
        self.release()

    def release(self) -> None:
        global _PATH_CLAIM_ADMISSIONS
        with self.lock:
            if self.released:
                return
            self.released = True
        # A child process resets its own counter after fork and must not
        # decrement it for an admission inherited from the parent.
        if self.pid != os.getpid():
            return
        with _PATH_CLAIM_ADMISSION_LOCK:
            _PATH_CLAIM_ADMISSIONS = max(0, _PATH_CLAIM_ADMISSIONS - 1)


def _acquire_path_claim_admission() -> _PathClaimAdmission:
    global _PATH_CLAIM_ADMISSIONS
    with _PATH_CLAIM_ADMISSION_LOCK:
        if _PATH_CLAIM_ADMISSIONS >= _MAX_LIVE_PATH_CLAIMS:
            raise OSError(
                "process path-claim capacity exhausted; release existing "
                "PathIdentity owners before claiming more paths"
            )
        _PATH_CLAIM_ADMISSIONS += 1
    return _PathClaimAdmission(os.getpid())


@dataclass(slots=True)
class _IdentityDescriptorOwner:
    """Own one no-follow descriptor together with its process FD lease."""

    descriptor: int | None
    fd_lease: Any | None
    lock: Lock = field(default_factory=Lock)

    def descriptor_snapshot(self) -> int | None:
        """Return the currently owned descriptor without transferring it."""
        with self.lock:
            return self.descriptor

    def release(self) -> None:
        """Relinquish the FD number before an uncertain close can be retried."""
        with self.lock:
            descriptor = self.descriptor
            lease = self.fd_lease
            if descriptor is None and lease is None:
                return
            # POSIX close errors, especially EINTR, do not prove that the kernel
            # kept the descriptor open. Remove the integer from this owner before
            # calling close so a retry can never close a recycled unrelated FD.
            self.descriptor = None
        close_error: BaseException | None = None
        if descriptor is not None:
            try:
                os.close(descriptor)
            except BaseException as exc:
                close_error = exc
        lease_error: BaseException | None = None
        if close_error is not None and lease is not None:
            # The FD integer can no longer be retried safely, but capacity must
            # remain charged because close() did not prove the descriptor gone.
            retained_as_debt = retain_uncertain_fd_close(lease, label="path-identity")
            if retained_as_debt:
                with self.lock:
                    if self.fd_lease is lease:
                        self.fd_lease = None
        elif lease is not None:
            try:
                lease.release()
            except BaseException as exc:
                lease_error = exc
            else:
                with self.lock:
                    if self.fd_lease is lease:
                        self.fd_lease = None
        if close_error is not None:
            raise close_error
        if lease_error is not None:
            raise lease_error

    def __del__(self) -> None:
        """Transfer failed descriptor cleanup instead of silently dropping it."""
        try:
            self.release()
        except BaseException:
            try:
                _retain_abandoned_descriptor_owner(self)
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
    """Keep a persistent scandir handle and its logical FD lease aligned."""

    iterator: Any | None
    lease: Any | None
    lock: Lock = field(default_factory=Lock)

    def release(self) -> None:
        with self.lock:
            iterator = self.iterator
            lease = self.lease
            if iterator is not None:
                iterator.close()
                self.iterator = None
            if lease is not None:
                lease.release()
                self.lease = None

    def __del__(self) -> None:
        """Close a cursor displaced by reset/monkeypatch without leaking its FD."""
        try:
            self.release()
        except BaseException:
            try:
                _release_scandir_owner(self)
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
    lease = acquire_file_descriptors(1)
    descriptor: int | None = None
    try:
        descriptor = os.open(path, flags)
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
    except BaseException as primary:
        if descriptor is not None:
            owner = _IdentityDescriptorOwner(descriptor, lease)
            try:
                owner.release()
            except BaseException as cleanup_error:
                _retain_abandoned_descriptor_owner(owner)
                add_bounded_note(
                    primary,
                    "temporary claim descriptor cleanup also failed and was retained",
                    cleanup_error,
                )
        else:
            lease_owner = _IdentityDescriptorOwner(None, lease)
            try:
                lease_owner.release()
            except BaseException as cleanup_error:
                _retain_abandoned_descriptor_owner(lease_owner)
                add_bounded_note(
                    primary,
                    "temporary claim FD lease cleanup also failed and was retained",
                    cleanup_error,
                )
        raise
    _IdentityDescriptorOwner(descriptor, lease).release()
    return raw


def _read_claim_at(
    directory_fd: int,
    name: str,
    *,
    allowed_link_counts: frozenset[int] = frozenset({1}),
) -> tuple[bytes, os.stat_result]:
    """Read one bounded regular claim relative to a verified parent descriptor."""
    flags = (
        os.O_RDONLY
        | int(getattr(os, "O_NOFOLLOW", 0))
        | int(getattr(os, "O_CLOEXEC", 0))
        | int(getattr(os, "O_NONBLOCK", 0))
    )
    lease = acquire_file_descriptors(1)
    descriptor: int | None = None
    try:
        descriptor = os.open(name, flags, dir_fd=directory_fd)
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
    except BaseException as primary:
        if descriptor is not None:
            owner = _IdentityDescriptorOwner(descriptor, lease)
            try:
                owner.release()
            except BaseException as cleanup_error:
                _retain_abandoned_descriptor_owner(owner)
                add_bounded_note(
                    primary,
                    "temporary claim descriptor cleanup also failed and was retained",
                    cleanup_error,
                )
        else:
            lease_owner = _IdentityDescriptorOwner(None, lease)
            try:
                lease_owner.release()
            except BaseException as cleanup_error:
                _retain_abandoned_descriptor_owner(lease_owner)
                add_bounded_note(
                    primary,
                    "temporary claim FD lease cleanup also failed and was retained",
                    cleanup_error,
                )
        raise
    owner = _IdentityDescriptorOwner(descriptor, lease)
    owner.release()
    return raw, metadata


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
    """Remove only a crash-left private alias with exactly one other hard link."""
    parent = path.parent
    flags = (
        os.O_RDONLY
        | int(getattr(os, "O_DIRECTORY", 0))
        | int(getattr(os, "O_NOFOLLOW", 0))
        | int(getattr(os, "O_CLOEXEC", 0))
    )
    lease = acquire_file_descriptors(1)
    descriptor: int | None = None
    try:
        descriptor = os.open(parent, flags)
        _validate_open_directory(parent, descriptor)
        try:
            current, metadata = _read_claim_at(
                descriptor, path.name, allowed_link_counts=frozenset({2})
            )
        except (FileNotFoundError, OSError):
            return False
        if current != expected or int(metadata.st_nlink) != 2:
            return False
        os.unlink(path.name, dir_fd=descriptor)
        os.fsync(descriptor)
        return True
    finally:
        owner = _IdentityDescriptorOwner(descriptor, lease)
        try:
            owner.release()
        except BaseException:
            _retain_abandoned_descriptor_owner(owner)


def _remove_claim_if_unchanged(path: Path, expected: bytes) -> bool:
    """Move and unlink one exact claim using descriptor-relative operations."""
    parent = path.parent
    parent_flags = (
        os.O_RDONLY
        | int(getattr(os, "O_DIRECTORY", 0))
        | int(getattr(os, "O_NOFOLLOW", 0))
        | int(getattr(os, "O_CLOEXEC", 0))
    )
    parent_lease = acquire_file_descriptors(1)
    parent_fd: int | None = None
    try:
        parent_fd = os.open(parent, parent_flags)
        _validate_open_directory(parent, parent_fd)
        try:
            current, before = _read_claim_at(parent_fd, path.name)
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
            moved_raw, moved = _read_claim_at(parent_fd, private_name)
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
    finally:
        owner = _IdentityDescriptorOwner(parent_fd, parent_lease)
        try:
            owner.release()
        except BaseException:
            _retain_abandoned_descriptor_owner(owner)


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
        try:
            lease = acquire_file_descriptors(1)
            try:
                iterator = os.scandir(root)
            except BaseException as primary:
                lease_owner = _ScandirCleanupOwner(None, lease)
                try:
                    lease_owner.release()
                except BaseException as cleanup_error:
                    _release_scandir_owner(lease_owner)
                    try:
                        add_bounded_note(
                            primary, "scandir FD lease cleanup was retained", cleanup_error
                        )
                    except BaseException:
                        pass
                raise
            candidate_owner = _ScandirCleanupOwner(iterator, lease)
        except (OSError, RuntimeError, TimeoutError):
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
    """Publish a complete claim atomically using a governed temporary FD."""
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
    root_lease = acquire_file_descriptors(1)
    root_fd: int | None = None
    try:
        root_fd = os.open(root, root_flags)
        _validate_open_directory(root, root_fd, require_private=True)
        for _attempt in range(3):
            temporary_name = f".claim-write-{secrets.token_hex(16)}"
            claim_lease: Any | None = acquire_file_descriptors(1)
            descriptor: int | None = None
            published = False
            try:
                descriptor = os.open(
                    temporary_name,
                    os.O_WRONLY | os.O_CREAT | os.O_EXCL | int(getattr(os, "O_CLOEXEC", 0)),
                    0o600,
                    dir_fd=root_fd,
                )
                _write_claim_payload(descriptor, payload)
                _IdentityDescriptorOwner(descriptor, claim_lease).release()
                descriptor = None
                claim_lease = None
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
                        # Persist publication, then persist removal of the private
                        # alias.  Returning before the second directory fsync can
                        # resurrect .claim-write-* after a power loss.
                        os.fsync(root_fd)
                        os.unlink(temporary_name, dir_fd=root_fd)
                        os.fsync(root_fd)
                    except BaseException as sync_error:
                        # Make the canonical name single-linked before rollback.
                        try:
                            os.unlink(temporary_name, dir_fd=root_fd)
                        except FileNotFoundError:
                            pass
                        except OSError:
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
                            # A published claim rollback was deferred only while authority remains.
                            # unlink() may have committed even when the following
                            # directory fsync failed.  In that case no authority
                            # remains to retain; release the admission exactly once
                            # and report the durability failure to the caller.
                            try:
                                claim_still_exists = claim.exists()
                            except OSError:
                                claim_still_exists = True
                            if claim_still_exists:
                                _retain_abandoned_claim_owner(rollback_owner)
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
                        existing = _read_claim_bytes(claim)
                    except OSError:
                        raise OSError("temporary path is already owned") from exc
                    if _release_abandoned_claim_for_path(str(claim)):
                        continue
                    if _claim_is_stable_stale(claim, existing):
                        if _unlink_stale_claim(claim, existing):
                            continue
                    raise OSError("temporary path is already owned") from exc
            finally:
                if descriptor is not None or claim_lease is not None:
                    descriptor_owner = _IdentityDescriptorOwner(descriptor, claim_lease)
                    try:
                        descriptor_owner.release()
                    except BaseException:
                        _retain_abandoned_descriptor_owner(descriptor_owner)
                try:
                    os.unlink(temporary_name, dir_fd=root_fd)
                except FileNotFoundError:
                    pass
                except OSError:
                    if not published:
                        pass
        raise OSError("temporary path is already owned")
    finally:
        root_owner = _IdentityDescriptorOwner(root_fd, root_lease)
        try:
            root_owner.release()
        except BaseException:
            _retain_abandoned_descriptor_owner(root_owner)


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

    def release(self) -> None:
        """Release both logical path authority and its descriptor resources."""
        _release_claim_owner(self)

    def __del__(self) -> None:
        if self.released:
            return
        # Descriptor-backed xattr authority has no pathname fallback: closing
        # its FD before removing the marker would strand the claim forever.
        # Keep that common path synchronous. External claims retain all pathname
        # authority in their record, so the descriptor/FD lease can be returned
        # immediately while the potentially blocking directory transaction is
        # deferred outside GC context.
        if self.external_claim_path is None or self.owner_pid != os.getpid():
            try:
                _release_claim_owner(self)
                return
            except BaseException:
                pass
        else:
            descriptor_owner: _IdentityDescriptorOwner | None = None
            with self.lock:
                if not self.descriptor_releasing:
                    descriptor_owner = self.descriptor_owner
                    if descriptor_owner is not None:
                        self.descriptor_releasing = True
                    else:
                        self.descriptor_released = True
            if descriptor_owner is not None:
                try:
                    descriptor_owner.release()
                except BaseException:
                    with self.lock:
                        self.descriptor_releasing = False
                else:
                    with self.lock:
                        if self.descriptor_owner is descriptor_owner:
                            self.descriptor_owner = None
                        self.descriptor_releasing = False
                        self.descriptor_released = True
        try:
            # Delay asynchronous directory I/O long enough that finalization has
            # a deterministic FD-accounting commit. A subsequent claim still
            # drains the bounded local registry immediately.
            _retain_abandoned_claim_owner(self, delay_seconds=1.0)
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


def _open_identity_fd(path: str | Path) -> tuple[int | None, Any | None]:
    """Open one governed no-follow descriptor without blocking special files."""
    try:
        metadata = os.lstat(path)
    except FileNotFoundError:
        return None, None
    lease = acquire_file_descriptors(1)
    descriptor: int | None = None
    try:
        common = int(getattr(os, "O_CLOEXEC", 0)) | int(getattr(os, "O_NOFOLLOW", 0))
        file_type = stat.S_IFMT(metadata.st_mode)
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
            return None, None
        return descriptor, lease
    except BaseException as primary:
        owner = _IdentityDescriptorOwner(descriptor, lease)
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
    metadata: os.stat_result, descriptor: int | None, fd_lease: Any | None
) -> PathIdentity:
    """Claim the fstat identity without mutating a raced pathname."""
    admission = _acquire_path_claim_admission()
    try:
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
            descriptor_owner=(
                _IdentityDescriptorOwner(descriptor, fd_lease) if descriptor is not None else None
            ),
            claim_admission=admission,
            owns_claim=True,
        )
        admission.transfer()
        return identity
    finally:
        admission.release_if_untransferred()


def claim_path_identity(path: str | Path) -> PathIdentity | None:
    """Atomically claim one exact entry without following or blocking on it."""
    # Opportunistic bounded progress keeps fallback registries from depending
    # solely on timer-thread availability, without making one claim operation
    # responsible for an unbounded cleanup backlog.
    _drain_abandoned_descriptor_owners(limit=8)
    _drain_abandoned_claim_owners(limit=8)
    descriptor, fd_lease = _open_identity_fd(path)
    if descriptor is not None:
        identity: PathIdentity | None = None
        try:
            owned = os.fstat(descriptor)
            try:
                current = os.lstat(path)
            except FileNotFoundError as exc:
                raise OSError("temporary path disappeared while claiming ownership") from exc
            if not _same_inode(owned, current):
                raise OSError("temporary path changed while claiming ownership")
            identity = _claim_from_metadata(owned, descriptor, fd_lease)
            descriptor = None
            fd_lease = None
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
            if descriptor is not None or fd_lease is not None:
                descriptor_owner = _IdentityDescriptorOwner(descriptor, fd_lease)
                try:
                    descriptor_owner.release()
                except BaseException:
                    _retain_abandoned_descriptor_owner(descriptor_owner)

    try:
        before = os.lstat(path)
    except FileNotFoundError:
        return None
    identity = _claim_from_metadata(before, None, None)
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
    if marker is None:
        marker, claim_path = _read_external_claim(metadata)
    return PathIdentity.from_stat(metadata, owner_marker=marker, external_claim_path=claim_path)


def _claim_owner_retry_key(owner: PathClaimOwner) -> tuple[str, int]:
    return ("path-claim-owner", id(owner))


def _retain_abandoned_claim_owner(owner: PathClaimOwner, *, delay_seconds: float = 0.01) -> None:
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
                _retry_abandoned_claim_owner,
                owner,
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

        def retry_retained_claim(retained: PathClaimOwner = owner) -> None:
            _retry_abandoned_claim_owner(retained)

        scheduled = schedule_retry(
            _claim_owner_retry_key(owner),
            retry_retained_claim,
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


def _retry_abandoned_claim_owner(owner: PathClaimOwner) -> None:
    try:
        _release_claim_owner(owner)
    except BaseException:
        _retain_abandoned_claim_owner(owner)
        return
    with _ABANDONED_CLAIM_LOCK:
        _ABANDONED_CLAIM_OWNERS.pop(id(owner), None)


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
            descriptor = (
                descriptor_owner.descriptor_snapshot() if descriptor_owner is not None else None
            )
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
            _retain_abandoned_claim_owner(owner)
            raise
        with owner.lock:
            if owner.descriptor_owner is descriptor_owner:
                owner.descriptor_owner = None
            owner.descriptor_releasing = False
            owner.descriptor_released = True
            owner.released = True
            admission = owner.claim_admission
            owner.claim_admission = None

    if admission is not None:
        admission.release()
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
    global _PATH_CLAIM_ADMISSIONS, _ABANDONED_CLAIM_OWNERS
    global _ABANDONED_DESCRIPTOR_OWNERS, _FORKED_PATH_GENERATIONS
    # Do not close iterators, release owners, acquire inherited locks or clear
    # containers here.  Any of those actions can deadlock on a lock owned by a
    # vanished parent thread or execute arbitrary finalizers in atfork context.
    quarantine_inherited_state(
        "path-identity",
        _CLAIM_SWEEP_ITERATOR,
        _CLAIM_SWEEP_OWNER,
        _ABANDONED_CLAIM_OWNERS,
        _ABANDONED_DESCRIPTOR_OWNERS,
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


if hasattr(os, "register_at_fork"):
    os.register_at_fork(after_in_child=_reset_path_identity_after_fork)


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
