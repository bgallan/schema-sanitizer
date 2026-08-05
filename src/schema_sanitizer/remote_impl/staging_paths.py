"""Owned temporary paths used by remote staging and output publication."""

from __future__ import annotations

import os
import shutil
import stat
import tempfile
import uuid
from dataclasses import dataclass
from pathlib import Path
from threading import Condition, Lock
from time import monotonic
from typing import TYPE_CHECKING

from schema_sanitizer.core_impl.safe_errors import add_bounded_note

from ..core_impl.finalization import runtime_is_finalizing
from ..core_impl.path_identity import (
    PathIdentity,
    claim_path_identity,
    lstat_identity,
    release_path_identity,
    transfer_identity_matches,
)
from ..core_impl.process_resources import reserve_file_descriptors
from ..core_impl.temporary_janitor import quarantine_temporary_artifact
from ..core_impl.temporary_storage import TemporaryStorageLease, TemporaryStoragePermitPool

if TYPE_CHECKING:
    from ..api_impl.operation_context import OperationExecutionContext


def _close_descriptor(fd: int, primary: BaseException | None) -> None:
    """Close one descriptor without masking an active traversal error."""
    try:
        os.close(fd)
    except BaseException as cleanup_error:
        if primary is None:
            raise
        add_bounded_note(
            primary,
            "temporary directory descriptor cleanup also failed",
            cleanup_error,
        )


class StagedPath:
    """Own a local temporary path that mirrors a remote input or output."""

    def __init__(
        self,
        path: str,
        *,
        is_dir: bool = False,
        source_file_by_name: dict[str, str] | None = None,
        storage_lease: TemporaryStorageLease | None = None,
    ) -> None:
        """Initialize this helper."""
        self.path = path
        self._pid = os.getpid()
        self.is_dir = is_dir
        self.source_file_by_name = source_file_by_name
        self.storage_lease = storage_lease
        self._identity: PathIdentity | None = claim_path_identity(path)
        self._lock = Lock()
        self._close_condition = Condition(self._lock)
        self._closing = False
        self._accounting_inflight = False
        self._closed = False

    def _wait_idle_locked(self, *, action: str) -> None:
        """Wait boundedly for lifecycle work owned by another thread."""
        deadline = monotonic() + 30.0
        while self._closing or self._accounting_inflight:
            remaining = deadline - monotonic()
            if remaining <= 0 or not self._close_condition.wait(timeout=remaining):
                raise RuntimeError(f"staged path {action} exceeded its deadline")

    def reserve_actual_size(self, pool: TemporaryStoragePermitPool, *, label: str) -> None:
        """Measure and charge outside the lifecycle lock through claim/work/commit."""
        if os.getpid() != self._pid:
            raise RuntimeError("staged path cannot be reused after fork")
        with self._close_condition:
            self._wait_idle_locked(action="accounting wait")
            if self._closed:
                raise RuntimeError("staged path is already closed")
            self._accounting_inflight = True
            existing_lease = self.storage_lease
        acquired: TemporaryStorageLease | None = None
        try:
            size, artifact_count = self._measure_owned_tree()
            if existing_lease is None:
                acquired = pool.acquire(
                    size,
                    label=label,
                    path=self.path,
                    artifact_count=artifact_count,
                )
            else:
                existing_lease.resize(size, path=self.path)
        finally:
            with self._close_condition:
                if acquired is not None:
                    # ``close()`` cannot publish another lease while this
                    # accounting claim is live, so the commit is exclusive.
                    self.storage_lease = acquired
                self._accounting_inflight = False
                self._close_condition.notify_all()

    def _actual_size(self) -> int:
        """Return no-follow bytes while bounding hostile directory traversal."""
        size, _count = self._measure_owned_tree()
        return size

    def _artifact_count(self) -> int:
        """Return no-follow entry count while bounding hostile traversal."""
        _size, count = self._measure_owned_tree()
        return count

    def _measure_owned_tree(self) -> tuple[int, int]:
        """Measure a tree with descriptor-relative, no-follow traversal."""
        root = Path(self.path)
        expected = self._identity
        before = lstat_identity(root)
        if before != expected:
            raise OSError(f"temporary path ownership changed before accounting: {root}")
        if before is None:
            raise FileNotFoundError(root)
        if expected is None:
            raise OSError(f"temporary path has no retained ownership identity: {root}")
        if before.file_type != stat.S_IFDIR:
            metadata = os.lstat(root)
            return int(metadata.st_size), 1

        max_entries = 1_000_000
        max_depth = 128
        total_size = 0
        total_count = 1
        directory_flags = (
            os.O_RDONLY
            | int(getattr(os, "O_DIRECTORY", 0))
            | int(getattr(os, "O_NOFOLLOW", 0))
            | int(getattr(os, "O_CLOEXEC", 0))
        )

        def walk(directory_fd: int, depth: int) -> None:
            nonlocal total_size, total_count
            if depth > max_depth:
                raise OSError("temporary directory accounting exceeded its depth limit")
            with os.scandir(directory_fd) as entries:
                for entry in entries:
                    total_count += 1
                    if total_count > max_entries:
                        raise OSError("temporary directory accounting exceeded its entry limit")
                    metadata = entry.stat(follow_symlinks=False)
                    entry_type = stat.S_IFMT(metadata.st_mode)
                    if entry_type == stat.S_IFREG:
                        total_size += int(metadata.st_size)
                        continue
                    if entry_type != stat.S_IFDIR:
                        continue
                    with reserve_file_descriptors(1, label="temporary_tree_accounting"):
                        child_fd = os.open(entry.name, directory_flags, dir_fd=directory_fd)
                        primary: BaseException | None = None
                        try:
                            opened = os.fstat(child_fd)
                            if (
                                int(opened.st_dev) != int(metadata.st_dev)
                                or int(opened.st_ino) != int(metadata.st_ino)
                                or not stat.S_ISDIR(opened.st_mode)
                            ):
                                raise OSError(
                                    "temporary directory component changed during accounting"
                                )
                            walk(child_fd, depth + 1)
                        except BaseException as exc:
                            primary = exc
                            raise
                        finally:
                            _close_descriptor(child_fd, primary)

        with reserve_file_descriptors(1, label="temporary_tree_accounting"):
            root_fd = os.open(root, directory_flags)
            primary: BaseException | None = None
            try:
                opened_root = os.fstat(root_fd)
                if (
                    int(opened_root.st_dev) != expected.device
                    or int(opened_root.st_ino) != expected.inode
                    or not stat.S_ISDIR(opened_root.st_mode)
                ):
                    raise OSError(f"temporary path ownership changed during accounting: {root}")
                walk(root_fd, 0)
            except BaseException as exc:
                primary = exc
                raise
            finally:
                _close_descriptor(root_fd, primary)
        after = lstat_identity(root)
        if after != expected:
            raise OSError(f"temporary path ownership changed during accounting: {root}")
        return total_size, total_count

    @staticmethod
    def _private_delete_path(source: Path) -> Path:
        """Create one idempotent same-filesystem private transfer location."""
        if source.parent.name == ".schema-sanitizer-delete":
            return source
        root = source.parent / ".schema-sanitizer-delete"
        try:
            os.mkdir(root, 0o700)
        except FileExistsError:
            pass
        metadata = os.lstat(root)
        if not stat.S_ISDIR(metadata.st_mode):
            raise OSError("temporary delete root must be a real directory")
        getuid = getattr(os, "geteuid", None)
        if getuid is not None and metadata.st_uid != getuid():
            raise OSError("temporary delete root must be owned by the current user")
        try:
            os.chmod(root, 0o700, follow_symlinks=False)
        except (NotImplementedError, TypeError):
            os.chmod(root, 0o700)
        return root / f"artifact-{os.getpid()}-{uuid.uuid4().hex}"

    @staticmethod
    def _restore_raced_transfer(source: Path, target: Path) -> None:
        """Best-effort restore an unowned entry captured by a raced rename."""
        try:
            if lstat_identity(source) is None and lstat_identity(target) is not None:
                os.replace(target, source)
        except OSError:
            # Never delete an identity mismatch. A caller-visible retry keeps
            # the lease alive even when another actor prevents restoration.
            pass

    def close(self) -> None:
        """Delete only the entry acquired by this owner, then release its lease."""
        if os.getpid() != self._pid:
            return
        with self._close_condition:
            self._wait_idle_locked(action="close wait")
            if self._closed:
                return
            self._closing = True
            lease = self.storage_lease
            expected_identity = self._identity
            retry_path = Path(self.path)
            retry_identity = expected_identity

        completed = False
        try:
            current_identity = lstat_identity(retry_path)
            if current_identity is not None and current_identity != expected_identity:
                raise OSError(f"temporary path ownership changed before cleanup: {retry_path}")
            if current_identity is not None:
                private_path = self._private_delete_path(retry_path)
                try:
                    if private_path != retry_path:
                        os.replace(retry_path, private_path)
                except FileNotFoundError:
                    current_identity = None
                except OSError as exc:
                    # Never fall back to destructive work through the public
                    # pathname after a failed transfer. A replacement can appear
                    # between the identity check and this failed rename.
                    raise OSError(
                        f"temporary path could not be transferred privately: {retry_path}"
                    ) from exc
                else:
                    private_identity = lstat_identity(private_path)
                    if not transfer_identity_matches(current_identity, private_identity):
                        self._restore_raced_transfer(retry_path, private_path)
                        raise OSError(
                            f"temporary path was replaced during cleanup transfer: {retry_path}"
                        )
                    retry_path = private_path
                    retry_identity = expected_identity or private_identity
                    current_identity = private_identity

            if current_identity is not None:
                try:
                    # The verified private entry is never interpreted through
                    # a symlink. Only a real owned directory is traversed.
                    if current_identity.file_type == stat.S_IFDIR:
                        shutil.rmtree(retry_path)
                    else:
                        retry_path.unlink()
                except FileNotFoundError:
                    pass
                except OSError:
                    pass

            remaining_identity = lstat_identity(retry_path)
            if remaining_identity is None:
                release_path_identity(retry_identity)
                if lease is not None:
                    lease.release()
                completed = True
            elif remaining_identity != retry_identity:
                # Never delete, quarantine, or account-release a replacement
                # that appeared at either the public or private pathname.
                raise OSError(f"temporary path was replaced during cleanup: {retry_path}")
            elif lease is None:
                raise OSError(f"temporary path could not be deleted: {retry_path}")
            else:
                accepted = quarantine_temporary_artifact(
                    retry_path,
                    is_dir=remaining_identity.file_type == stat.S_IFDIR,
                    lease=lease,
                    expected_identity=retry_identity,
                )
                # Legacy internal callbacks returned ``None`` after accepting
                # ownership. Only an explicit ``False`` means shutdown rejected
                # the handoff and the caller must retain its retry handle.
                if accepted is not False:
                    completed = True
                else:
                    raise RuntimeError(
                        "temporary-artifact janitor is closed; staged path cleanup is retryable"
                    )
        finally:
            with self._close_condition:
                if completed:
                    self.storage_lease = None
                    self._identity = None
                    self._closed = True
                else:
                    self.path = str(retry_path)
                    self._identity = retry_identity
                self._closing = False
                self._close_condition.notify_all()

    def __del__(self) -> None:
        """Release an abandoned path unless interpreter teardown has begun."""
        try:
            if runtime_is_finalizing():
                return
            self.close()
        except BaseException:
            pass


@dataclass(slots=True)
class RemoteOutputTarget:
    """Local output target plus optional remote upload destination."""

    local_path: str
    remote_uri: str | None = None
    temp: StagedPath | None = None
    memory_limit_bytes: int | None = None
    threading_mode: str = "single"
    operation_context: OperationExecutionContext | None = None

    def close(self) -> None:
        """Implement the internal close helper."""
        if self.temp is not None:
            self.temp.close()
            self.temp = None


def create_temp_file_path(
    *, suffix: str, storage_lease: TemporaryStorageLease | None = None
) -> StagedPath:
    """Create an owned temporary file while respecting the FD governor."""
    with reserve_file_descriptors(label="temporary_file_create"):
        fd, path = tempfile.mkstemp(prefix="schema-sanitizer-", suffix=suffix)
        os.close(fd)
    return StagedPath(path, storage_lease=storage_lease)


def create_temp_directory_path(*, storage_lease: TemporaryStorageLease | None = None) -> StagedPath:
    """Create an owned temporary directory path."""
    path = tempfile.mkdtemp(prefix="schema-sanitizer-")
    return StagedPath(path, is_dir=True, storage_lease=storage_lease)


__all__ = [
    "RemoteOutputTarget",
    "StagedPath",
    "create_temp_directory_path",
    "create_temp_file_path",
]
