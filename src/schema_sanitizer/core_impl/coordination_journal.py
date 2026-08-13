"""Crash-recoverable in-place updates for interprocess coordination documents.

The main document remains the lock target and is still rewritten in place so
older schema-sanitizer processes can coordinate through the same inode.  A
small sidecar journal makes a crash between ``truncate`` and ``write``
recoverable without switching the lock protocol to rename-based publication.
"""

from __future__ import annotations

import hashlib
import json
import os
import stat
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from time import monotonic, sleep
from typing import BinaryIO, Callable, Iterator

from .durations import normalize_duration

_JOURNAL_VERSION = 1
_PHASE_PREPARED = "prepared"
_PHASE_COMMITTED = "committed"
_MAX_HEADER_BYTES = 4096

try:  # pragma: no cover - exercised on POSIX CI
    import fcntl as _fcntl
except ImportError:  # pragma: no cover - Windows fallback
    _fcntl = None  # type: ignore[assignment]

_COORDINATION_LOCK_TIMEOUT_SECONDS = 30.0
_COORDINATION_LOCK_POLL_SECONDS = 0.05


@dataclass(frozen=True, slots=True)
class _JournalRecord:
    """One bounded before/after transaction stored beside a locked document."""

    phase: str
    pid: int
    start: str
    before: bytes
    after: bytes


def _digest(payload: bytes) -> str:
    """Return a stable bounded identity for one coordination payload."""
    return hashlib.sha256(payload).hexdigest()


def _journal_path(path: Path) -> Path:
    """Return the sidecar path without changing the main lock-file inode."""
    return path.with_name(f"{path.name}.journal")


def _journal_temporary_path(path: Path) -> Path:
    """Return the single reusable staging path protected by the main flock."""
    journal = _journal_path(path)
    return journal.with_name(f".{journal.name}.tmp")


def _validate_owned_regular(metadata: os.stat_result, label: str) -> None:
    """Reject links and foreign/non-regular coordination artifacts."""
    if not stat.S_ISREG(metadata.st_mode):
        raise OSError(f"{label} must be a regular file")
    if metadata.st_nlink != 1:
        raise OSError(f"{label} must not have additional hard links")
    getuid = getattr(os, "geteuid", None)
    if getuid is not None and metadata.st_uid != getuid():
        raise OSError(f"{label} must be owned by the current user")


@contextmanager
def coordination_file_lock(
    handle: BinaryIO,
    *,
    timeout_seconds: float = _COORDINATION_LOCK_TIMEOUT_SECONDS,
) -> Iterator[None]:
    """Acquire one interprocess lock without allowing an unbounded process stall."""
    if _fcntl is None:
        raise OSError("interprocess coordination locks are unsupported")
    timeout = normalize_duration(
        timeout_seconds,
        name="coordination lock timeout",
        allow_zero=True,
    )
    assert timeout is not None
    deadline = monotonic() + timeout
    descriptor = handle.fileno()
    while True:
        try:
            _fcntl.flock(descriptor, _fcntl.LOCK_EX | _fcntl.LOCK_NB)
            break
        except BlockingIOError as exc:
            remaining = deadline - monotonic()
            if remaining <= 0:
                raise TimeoutError(
                    "timed out waiting for the interprocess coordination lock"
                ) from exc
            sleep(min(_COORDINATION_LOCK_POLL_SECONDS, remaining))
        except InterruptedError:
            if monotonic() >= deadline:
                raise TimeoutError(
                    "timed out waiting for the interprocess coordination lock"
                ) from None
    try:
        yield
    finally:
        try:
            _fcntl.flock(descriptor, _fcntl.LOCK_UN)
        except OSError:
            # Closing the descriptor releases the lock even if an explicit
            # unlock is interrupted during interpreter or process shutdown.
            pass


def open_coordination_file(path: Path) -> BinaryIO:
    """Open one predictable lock file under the teardown FD authority."""
    flags = os.O_RDWR | os.O_CREAT
    if hasattr(os, "O_CLOEXEC"):
        flags |= os.O_CLOEXEC
    if hasattr(os, "O_NOFOLLOW"):
        flags |= os.O_NOFOLLOW

    def _opener():
        descriptor = -1
        try:
            descriptor = os.open(path, flags, 0o600)
            _validate_owned_regular(os.fstat(descriptor), "coordination state")
            os.fchmod(descriptor, 0o600)
            handle = os.fdopen(descriptor, "r+b", closefd=True)
            descriptor = -1
            return handle
        except OSError as exc:
            raise OSError("coordination state cannot be opened safely") from exc
        finally:
            if descriptor >= 0:
                os.close(descriptor)

    from .process_resources import open_governed_stream

    return open_governed_stream(_opener, teardown=True)  # type: ignore[return-value]


def _directory_fsync(path: Path) -> None:
    """Durably publish directory-entry changes under teardown FD admission."""
    flags = os.O_RDONLY
    if hasattr(os, "O_DIRECTORY"):
        flags |= os.O_DIRECTORY
    if hasattr(os, "O_CLOEXEC"):
        flags |= os.O_CLOEXEC
    from .process_resources import governed_os_descriptor

    with governed_os_descriptor(
        lambda: os.open(path, flags), teardown=True, label="coordination-directory"
    ) as descriptor:
        os.fsync(descriptor)


def _encode_header(record: _JournalRecord, max_payload_bytes: int) -> bytes:
    """Encode bounded journal metadata without copying its before/after images."""
    if len(record.before) > max_payload_bytes or len(record.after) > max_payload_bytes:
        raise OSError("coordination journal payload exceeds its bounded size")
    header = {
        "version": _JOURNAL_VERSION,
        "phase": record.phase,
        "pid": record.pid,
        "start": record.start[:128],
        "before_length": len(record.before),
        "after_length": len(record.after),
        "before_sha256": _digest(record.before),
        "after_sha256": _digest(record.after),
    }
    encoded = json.dumps(header, sort_keys=True, separators=(",", ":")).encode("utf-8")
    if len(encoded) > _MAX_HEADER_BYTES:
        raise OSError("coordination journal header exceeds its bounded size")
    return encoded


def _publish_record_mode(
    path: Path,
    record: _JournalRecord,
    max_payload_bytes: int,
    *,
    durable: bool,
    require_directory_sync: bool = True,
) -> None:
    """Publish one sidecar, optionally amortizing power-loss durability."""
    journal = _journal_path(path)
    temporary = _journal_temporary_path(path)
    header = _encode_header(record, max_payload_bytes)
    flags = os.O_WRONLY | os.O_CREAT
    if hasattr(os, "O_CLOEXEC"):
        flags |= os.O_CLOEXEC
    if hasattr(os, "O_NOFOLLOW"):
        flags |= os.O_NOFOLLOW

    def _opener():
        descriptor = -1
        try:
            descriptor = os.open(temporary, flags, 0o600)
            _validate_owned_regular(os.fstat(descriptor), "coordination journal staging file")
            os.fchmod(descriptor, 0o600)
            os.ftruncate(descriptor, 0)
            handle = os.fdopen(descriptor, "wb", closefd=True)
            descriptor = -1
            return handle
        except OSError as exc:
            raise OSError(
                f"coordination journal staging file cannot be opened safely: {exc}"
            ) from exc
        finally:
            if descriptor >= 0:
                os.close(descriptor)

    try:
        from .process_resources import open_governed_stream

        with open_governed_stream(_opener, teardown=True) as handle:
            handle.write(header)
            handle.write(b"\n")
            handle.write(record.before)
            handle.write(record.after)
            handle.flush()
            if durable:
                os.fsync(handle.fileno())
        os.replace(temporary, journal)
        if durable:
            try:
                _directory_fsync(path.parent)
            except OSError:
                if require_directory_sync:
                    raise
    finally:
        try:
            temporary.unlink(missing_ok=True)
        except OSError:
            pass


def _publish_record(
    path: Path,
    record: _JournalRecord,
    max_payload_bytes: int,
    *,
    require_directory_sync: bool = True,
) -> None:
    """Atomically publish one durable sidecar journal record."""
    _publish_record_mode(
        path,
        record,
        max_payload_bytes,
        durable=True,
        require_directory_sync=require_directory_sync,
    )


def _publish_record_relaxed(
    path: Path,
    record: _JournalRecord,
    max_payload_bytes: int,
    *,
    require_directory_sync: bool = True,
) -> None:
    """Publish a process-crash-safe sidecar without forcing every fsync."""
    _publish_record_mode(
        path,
        record,
        max_payload_bytes,
        durable=False,
        require_directory_sync=require_directory_sync,
    )


class _ReadRecordStreamAdapter:
    """Expose a journal stream while retaining its validated stat metadata."""

    __slots__ = ("handle", "metadata")

    def __init__(self, pair):
        self.handle, self.metadata = pair

    def __getattr__(self, name):
        return getattr(self.handle, name)

    def close(self):
        return self.handle.close()


def _read_record(path: Path, max_payload_bytes: int) -> _JournalRecord | None:
    """Read and validate a journal without following a hostile symlink."""
    journal = _journal_path(path)
    flags = os.O_RDONLY
    if hasattr(os, "O_CLOEXEC"):
        flags |= os.O_CLOEXEC
    if hasattr(os, "O_NOFOLLOW"):
        flags |= os.O_NOFOLLOW

    def _opener():
        descriptor = -1
        try:
            descriptor = os.open(journal, flags)
            metadata = os.fstat(descriptor)
            _validate_owned_regular(metadata, "coordination journal")
            maximum = _MAX_HEADER_BYTES + 1 + 2 * max_payload_bytes
            if metadata.st_size > maximum:
                raise OSError("coordination journal exceeds its bounded size")
            handle = os.fdopen(descriptor, "rb", closefd=True)
            descriptor = -1
            return handle, metadata
        finally:
            if descriptor >= 0:
                os.close(descriptor)

    try:
        from .process_resources import open_governed_stream

        pair = open_governed_stream(lambda: _ReadRecordStreamAdapter(_opener()), teardown=True)
    except FileNotFoundError:
        return None
    except OSError as exc:
        raise OSError("coordination journal cannot be opened safely") from exc
    with pair as adapter:
        handle = adapter.handle
        metadata = adapter.metadata
        header_line = handle.readline(_MAX_HEADER_BYTES + 2)
        if not header_line.endswith(b"\n") or len(header_line) > _MAX_HEADER_BYTES + 1:
            raise OSError("coordination journal is corrupt")
        header_raw = header_line[:-1]
        try:
            header = json.loads(header_raw.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise OSError("coordination journal is corrupt") from exc
        if not isinstance(header, dict) or header.get("version") != _JOURNAL_VERSION:
            raise OSError("coordination journal has an unsupported version")
        phase = header.get("phase")
        if phase not in {_PHASE_PREPARED, _PHASE_COMMITTED}:
            raise OSError("coordination journal has an invalid phase")
        try:
            pid = int(header.get("pid", -1))
            start = str(header.get("start", "unknown"))
            before_length = int(header.get("before_length", -1))
            after_length = int(header.get("after_length", -1))
        except (TypeError, ValueError) as exc:
            raise OSError("coordination journal is corrupt") from exc
        if (
            pid <= 0
            or before_length < 0
            or after_length < 0
            or before_length > max_payload_bytes
            or after_length > max_payload_bytes
            or metadata.st_size != len(header_line) + before_length + after_length
        ):
            raise OSError("coordination journal is corrupt")
        before = handle.read(before_length)
        after = handle.read(after_length)
        if len(before) != before_length or len(after) != after_length or handle.read(1):
            raise OSError("coordination journal is corrupt")
    if header.get("before_sha256") != _digest(before) or header.get("after_sha256") != _digest(
        after
    ):
        raise OSError("coordination journal checksum mismatch")
    return _JournalRecord(phase, pid, start, before, after)


def _write_main(handle: BinaryIO, payload: bytes) -> None:
    """Replace the locked document in place and durably flush its contents."""
    handle.seek(0)
    handle.truncate()
    handle.write(payload)
    handle.flush()
    os.fsync(handle.fileno())


def _write_main_relaxed(handle: BinaryIO, payload: bytes) -> None:
    """Replace a locked advisory document without forcing a per-write fsync."""
    handle.seek(0)
    handle.truncate()
    handle.write(payload)
    handle.flush()


def _remove_journal(path: Path) -> None:
    """Best-effort removal after a durable commit or recovery."""
    journal = _journal_path(path)
    try:
        journal.unlink(missing_ok=True)
        _directory_fsync(path.parent)
    except OSError:
        # A committed journal is safe to replay; cleanup failure must not turn a
        # durable reservation into an ambiguous caller-visible failure.
        pass


def _remove_journal_relaxed(path: Path) -> None:
    """Remove an advisory sidecar without forcing a directory fsync."""
    try:
        _journal_path(path).unlink(missing_ok=True)
    except OSError:
        pass


def recover_locked_payload(
    path: Path,
    handle: BinaryIO,
    *,
    max_payload_bytes: int,
    validate: Callable[[bytes], object],
    canonicalize: Callable[[object], bytes],
    process_alive: Callable[[int, str], bool],
) -> bytes:
    """Recover an interrupted transaction while the main inode is locked.

    A prepared transaction owned by a still-live process is rolled back because
    its caller may retry after the failed write.  A dead owner's transaction and
    every committed transaction are completed.  A different valid main document
    is treated as a newer write from a journal-unaware process and preserved.
    """
    handle.seek(0)
    current = handle.read(max_payload_bytes + 1)
    record = _read_record(path, max_payload_bytes)
    if record is None:
        return current

    current_is_known = current == record.before or current == record.after
    if not current_is_known:
        try:
            decoded = validate(current)
            current_is_canonical = bool(current) and canonicalize(decoded) == current
        except OSError:
            current_is_canonical = False
        if current_is_canonical:
            _remove_journal(path)
            return current

    owner_alive = process_alive(record.pid, record.start)
    if record.phase == _PHASE_COMMITTED or not owner_alive:
        target = record.after
    else:
        target = record.before
    if current != target:
        _write_main(handle, target)
    _remove_journal(path)
    return target


def commit_locked_payload(
    path: Path,
    handle: BinaryIO,
    *,
    before: bytes,
    after: bytes,
    max_payload_bytes: int,
    process_start: str,
) -> None:
    """Durably commit one bounded in-place update through a two-phase journal."""
    if before == after:
        return
    if len(after) > max_payload_bytes:
        raise OSError("coordination state exceeds its bounded file size")
    prepared = _JournalRecord(
        _PHASE_PREPARED,
        os.getpid(),
        process_start,
        before,
        after,
    )
    _publish_record(path, prepared, max_payload_bytes)
    _write_main(handle, after)
    committed = _JournalRecord(
        _PHASE_COMMITTED,
        prepared.pid,
        prepared.start,
        prepared.before,
        prepared.after,
    )
    try:
        _publish_record(
            path,
            committed,
            max_payload_bytes,
            require_directory_sync=False,
        )
    except BaseException:
        # The caller must be able to retry incremental reservations without
        # counting a durable main-file write twice.  Restore the before image
        # while the inode is still exclusively locked, then surface the error.
        _write_main(handle, before)
        _remove_journal(path)
        raise
    _remove_journal(path)


def commit_locked_payload_relaxed(
    path: Path,
    handle: BinaryIO,
    *,
    before: bytes,
    after: bytes,
    max_payload_bytes: int,
    process_start: str,
) -> None:
    """Commit advisory state safely across process crashes with amortized fsync."""
    if before == after:
        return
    if len(after) > max_payload_bytes:
        raise OSError("coordination state exceeds its bounded file size")
    prepared = _JournalRecord(
        _PHASE_PREPARED,
        os.getpid(),
        process_start,
        before,
        after,
    )
    _publish_record_relaxed(path, prepared, max_payload_bytes)
    _write_main_relaxed(handle, after)
    committed = _JournalRecord(
        _PHASE_COMMITTED,
        prepared.pid,
        prepared.start,
        prepared.before,
        prepared.after,
    )
    try:
        _publish_record_relaxed(
            path,
            committed,
            max_payload_bytes,
            require_directory_sync=False,
        )
    except BaseException:
        _write_main_relaxed(handle, before)
        _remove_journal_relaxed(path)
        raise
    _remove_journal_relaxed(path)


__all__ = [
    "commit_locked_payload",
    "commit_locked_payload_relaxed",
    "open_coordination_file",
    "recover_locked_payload",
]
