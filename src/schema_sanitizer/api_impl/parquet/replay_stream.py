"""Replayable Arrow stream storage for safe Parquet fallback.

It spools Arrow batches under memory and temporary-storage budgets so a safe native
decline can be replayed once through the fallback route.
"""

from __future__ import annotations

import os
import tempfile
from collections.abc import Iterable
from threading import Condition, Lock
from time import monotonic
from typing import Any, cast

from ...core_impl.dependencies import ensure_pyarrow
from ...core_impl.finalization import runtime_is_finalizing
from ...core_impl.finalizer_cleanup import (
    PreparedFinalizerCleanup,
    cancel_prepared_finalizer_cleanup,
    defer_prepared_finalizer_cleanup,
    reserve_finalizer_cleanup,
)
from ...core_impl.process_resources import (
    acquire_file_descriptor_capability,
    open_governed_file,
)
from ...core_impl.resource_lifecycle import (
    _close_sequence_retryably,
    _close_suppressing_errors,
)
from ...core_impl.temporary_storage import (
    StreamingStorageReservation,
    TemporaryStoragePermitPool,
)

_REPLAY_CLOSE_WAIT_TIMEOUT_SECONDS = 5.0


def _cleanup_replay_reader_capsule(capsule: PreparedFinalizerCleanup) -> None:
    """Close a detached replay reader and its bounded keepalive tuple."""
    reader = capsule.arg0
    if reader is not None:
        if not _close_suppressing_errors(reader):
            raise RuntimeError("replay reader cleanup remains retryable")
        capsule.arg0 = None
    keepalive = capsule.arg1
    if keepalive is not None:
        items = list(cast(Iterable[Any], keepalive))
        _close_sequence_retryably(items)
        if items:
            capsule.arg1 = tuple(items)
            raise RuntimeError("replay keepalive cleanup remains retryable")
        capsule.arg1 = None


def _cleanup_replay_path_capsule(capsule: PreparedFinalizerCleanup) -> None:
    """Request replay-artifact retirement through its shared owner."""
    owner = capsule.arg0
    if owner is not None:
        cast(_ReplayArtifactOwner, owner).close()
        capsule.arg0 = None


class _ReplayArtifactReaderLease:
    """One exact reader reference that delays storage-credit retirement."""

    __slots__ = ("_owner", "_lock")

    def __init__(self, owner: "_ReplayArtifactOwner") -> None:
        """Retain one reader reference to the shared replay artifact."""
        self._owner: _ReplayArtifactOwner | None = owner
        self._lock = Lock()

    def close(self) -> None:
        """Release this reader reference so replay storage can retire when ready."""
        with self._lock:
            owner = self._owner
            self._owner = None
        if owner is not None:
            owner.release_reader()


class _ReplayArtifactOwner:
    """Refcount pathname, inode and bytes until every replay reader is closed."""

    __slots__ = (
        "_pid",
        "_condition",
        "_path",
        "_lease",
        "_pool",
        "_active_readers",
        "_close_requested",
        "_unlinking",
        "_finalizing",
        "_released",
    )

    def __init__(self, lease: Any, pool: TemporaryStoragePermitPool) -> None:
        """Track spool-path, storage-credit, and active-reader ownership under one condition."""
        self._pid = os.getpid()
        self._condition = Condition(Lock())
        self._path: str | None = None
        self._lease = lease
        self._pool: TemporaryStoragePermitPool | None = pool
        self._active_readers = 0
        self._close_requested = False
        self._unlinking = False
        self._finalizing = False
        self._released = False

    @property
    def path(self) -> str | None:
        """Return the durable spool path owned by this replay store."""
        with self._condition:
            return self._path

    @property
    def active_readers(self) -> int:
        """Return the active readers."""
        with self._condition:
            return self._active_readers

    @property
    def released(self) -> bool:
        """Return whether the resource has been released."""
        with self._condition:
            return self._released

    def bind_path(self, path: str) -> None:
        """Bind the replay store to its durable spool path."""
        with self._condition:
            if self._path is not None or self._close_requested or self._released:
                raise RuntimeError("replay artifact path is already bound or closing")
            self._path = path

    def acquire_reader(self) -> _ReplayArtifactReaderLease:
        """Acquire one active reader reference to the replay store."""
        with self._condition:
            if self._close_requested or self._released or self._path is None:
                raise RuntimeError("Replayable Parquet stream has been closed.")
            self._active_readers += 1
        try:
            return _ReplayArtifactReaderLease(self)
        except BaseException:
            self.release_reader()
            raise

    def release_reader(self) -> None:
        """Release one active reader reference from the replay store."""
        with self._condition:
            if self._active_readers <= 0:
                raise RuntimeError("replay artifact reader over-release")
            self._active_readers -= 1
            self._condition.notify_all()
        self._retire_storage_if_ready()

    def _retire_storage_if_ready(self) -> None:
        """Retire storage if ready."""
        with self._condition:
            if (
                self._released
                or self._finalizing
                or not self._close_requested
                or self._path is not None
                or self._active_readers != 0
            ):
                return
            self._finalizing = True
            lease = self._lease
            pool = self._pool

        primary: BaseException | None = None
        lease_released = lease is None
        pool_closed = pool is None
        if lease is not None:
            try:
                lease.release()
            except BaseException as exc:
                primary = exc
            else:
                lease_released = True
        if lease_released and pool is not None:
            try:
                pool.close()
            except BaseException as exc:
                primary = primary or exc
            else:
                pool_closed = True

        with self._condition:
            if lease_released:
                self._lease = None
            if pool_closed:
                self._pool = None
            self._released = self._lease is None and self._pool is None
            self._finalizing = False
            self._condition.notify_all()
        if primary is not None:
            raise primary

    def close(self) -> None:
        """Unlink the spool and retire storage credits after active readers drain."""
        if os.getpid() != self._pid:
            return
        deadline = monotonic() + _REPLAY_CLOSE_WAIT_TIMEOUT_SECONDS
        with self._condition:
            while self._unlinking:
                remaining = deadline - monotonic()
                if remaining <= 0:
                    raise TimeoutError("timed out waiting for replay artifact unlink transaction")
                self._condition.wait(timeout=min(0.1, remaining))
            if self._released:
                return
            self._close_requested = True
            path = self._path
            if path is not None:
                self._unlinking = True

        if path is not None:
            try:
                os.unlink(path)
            except FileNotFoundError:
                pass
            except BaseException:
                with self._condition:
                    self._unlinking = False
                    self._condition.notify_all()
                raise
            with self._condition:
                if self._path == path:
                    self._path = None
                self._unlinking = False
                self._condition.notify_all()

        self._retire_storage_if_ready()


class _BudgetedReplayFile:
    """File-like adapter that reserves temporary bytes before every Arrow write."""

    __slots__ = ("_handle", "_reservation")

    def __init__(self, handle: Any, reservation: StreamingStorageReservation) -> None:
        """Bind the replay file handle to a reservation charged before every write."""
        self._handle = handle
        self._reservation = reservation

    def write(self, data: Any) -> int:
        """Write bytes through this budgeted replay file."""
        amount = len(data)
        self._reservation.before_write(amount)
        return int(self._handle.write(data))

    def __getattr__(self, name: str) -> Any:
        """Delegate unresolved attributes to the wrapped object."""
        return getattr(self._handle, name)


class _ReplayReader:
    """Keep replay-file resources alive while exposing an Arrow stream reader."""

    def __init__(self, reader: Any, keepalive: tuple[Any, ...] = ()):
        """Retain the Arrow reader and replay keepalives under safe-point finalization."""
        self._pid = os.getpid()
        self._reader = reader
        self._keepalive = keepalive
        self.schema = reader.schema
        self._finalizer_capsule: PreparedFinalizerCleanup | None = reserve_finalizer_cleanup(
            _cleanup_replay_reader_capsule
        )
        self._finalizer_ticket: int | None = self._finalizer_capsule.ticket

    def __iter__(self) -> "_ReplayReader":
        """Return this reader as its own iterator."""
        return self

    def __next__(self) -> Any:
        """Return the next replayed record batch."""
        return next(self._reader)

    def read_next_batch(self) -> Any:
        """Return the next replayed record batch through PyArrow's reader API."""
        return self._reader.read_next_batch()

    def __arrow_c_stream__(self, requested_schema: Any = None) -> Any:
        """Export the replay reader through the Arrow C Stream protocol."""
        return self._reader.__arrow_c_stream__(requested_schema)

    def close(self) -> None:
        """Close reader resources while retaining any cleanup failures."""
        if os.getpid() != getattr(self, "_pid", os.getpid()):
            return
        reader = self._reader
        if reader is not None and not _close_suppressing_errors(reader):
            return
        if self._reader is reader:
            self._reader = None
        keepalive = list(self._keepalive)
        _close_sequence_retryably(keepalive)
        self._keepalive = tuple(keepalive)
        if self._reader is None and not self._keepalive:
            ticket = getattr(self, "_finalizer_ticket", None)
            cleanup = getattr(self, "_finalizer_capsule", None)
            if ticket is not None and cleanup is not None:
                cancel_prepared_finalizer_cleanup(cleanup)
                self._finalizer_ticket = None
                self._finalizer_capsule = None

    def __del__(self) -> None:
        """Detach replay resources into a preallocated safe-point capsule."""
        try:
            if runtime_is_finalizing() or os.getpid() != getattr(self, "_pid", os.getpid()):
                return
            ticket = getattr(self, "_finalizer_ticket", None)
            cleanup = getattr(self, "_finalizer_capsule", None)
            if ticket is None or cleanup is None:
                return
            cleanup.arg0 = getattr(self, "_reader", None)
            cleanup.arg1 = getattr(self, "_keepalive", ()) or None
            if defer_prepared_finalizer_cleanup(cleanup):
                self._reader = None
                self._keepalive = ()
                self._finalizer_ticket = None
                self._finalizer_capsule = None
        except BaseException:
            pass


class ReplayableArrowStream:
    """Spool a one-shot Arrow stream so native Parquet can fail over safely."""

    def __init__(self, stream: Any, *, feature: str, memory_limit_bytes: int | None = None):
        """Initialize one governed, storage-bounded temporary Arrow IPC spool."""
        self._pid = os.getpid()
        self._pa = ensure_pyarrow(feature=feature)
        self._path: str | None = None
        self._storage_pool: TemporaryStoragePermitPool | None = None
        self._storage_lease: Any | None = None
        self._artifact: _ReplayArtifactOwner | None = None
        self.schema: Any
        self._finalizer_capsule: PreparedFinalizerCleanup | None = reserve_finalizer_cleanup(
            _cleanup_replay_path_capsule
        )
        self._finalizer_ticket: int | None = self._finalizer_capsule.ticket
        self._storage_pool = TemporaryStoragePermitPool(memory_limit_bytes)
        self._spool(stream)

    def _reader_or_batches_from_stream(self, stream: Any) -> Any:
        """Return a batch source that can be copied into the replay spool."""
        pa = self._pa
        if isinstance(stream, pa.Table):
            self.schema = stream.schema
            return stream.to_batches()
        if isinstance(stream, pa.RecordBatchReader):
            self.schema = stream.schema
            return stream
        if hasattr(stream, "__arrow_c_stream__"):
            reader = pa.RecordBatchReader.from_stream(stream)
            self.schema = reader.schema
            return reader
        raise TypeError("Parquet output requires a replayable Arrow stream for safe fallback.")

    def _spool(self, stream: Any) -> None:
        """Copy the source stream into a governed, incrementally-budgeted IPC file."""
        source = self._reader_or_batches_from_stream(stream)
        pool = self._storage_pool
        if pool is None:
            raise RuntimeError("replay temporary-storage pool is unavailable")
        temp_root = tempfile.gettempdir()
        lease = pool.acquire(
            0,
            label="parquet_replay_spool",
            path=temp_root,
            artifact_count=1,
        )
        self._storage_lease = lease
        artifact = _ReplayArtifactOwner(lease, pool)
        self._artifact = artifact
        path_box: list[str] = []

        def create_spool_fd() -> int:
            """Create a governed descriptor for the replay spool file."""
            fd, path = tempfile.mkstemp(
                prefix="schema-sanitizer-parquet-replay-",
                suffix=".arrow",
                dir=temp_root,
            )
            path_box.append(path)
            return fd

        try:
            with acquire_file_descriptor_capability(
                1, label="parquet_replay_mkstemp"
            ) as capability:
                with capability.open_descriptor(create_spool_fd, label="parquet_replay_mkstemp"):
                    pass
            if not path_box:
                raise RuntimeError("replay spool creation did not publish a path")
            path = path_box[0]
            artifact.bind_path(path)
            self._path = path
            reservation = StreamingStorageReservation(lease, initial_credit_bytes=0, path=path)
            with open_governed_file(path, "wb") as handle:
                budgeted = _BudgetedReplayFile(handle, reservation)
                with self._pa.output_stream(budgeted) as sink:
                    with self._pa.ipc.new_stream(sink, self.schema) as writer:
                        if isinstance(source, self._pa.RecordBatchReader):
                            self._copy_reader(source, writer)
                        else:
                            for batch in source:
                                writer.write_batch(batch)
            reservation.finalize(os.path.getsize(path))
        except BaseException:
            self.close()
            raise

    @staticmethod
    def _copy_reader(source: Any, writer: Any) -> None:
        """Copy every batch from a reader-like source into an IPC writer."""
        while True:
            try:
                batch = source.read_next_batch()
            except StopIteration:
                return
            writer.write_batch(batch)

    def reader(self) -> _ReplayReader:
        """Return a fresh governed reader retaining artifact bytes until close."""
        artifact = self._artifact
        if artifact is None:
            raise RuntimeError("Replayable Parquet stream has been closed.")
        reader_lease = artifact.acquire_reader()
        path = artifact.path
        if path is None:
            reader_lease.close()
            raise RuntimeError("Replayable Parquet stream has been closed.")
        try:
            handle = open_governed_file(path, "rb")
        except BaseException:
            reader_lease.close()
            raise
        try:
            source = self._pa.input_stream(handle)
            reader = self._pa.ipc.open_stream(source)
        except BaseException:
            handle.close()
            reader_lease.close()
            raise
        # LIFO close order is reader -> source -> handle -> reader lease, so
        # storage bytes cannot be returned before the physical FD is gone.
        keepalive = (reader_lease, handle, source)
        return _ReplayReader(reader, keepalive=keepalive)

    def close(self) -> None:
        """Unlink now but return storage only after every reader FD is closed."""
        if os.getpid() != self._pid:
            return
        artifact = self._artifact
        if artifact is None:
            return
        try:
            artifact.close()
        except BaseException:
            self._path = artifact.path
            return
        self._path = artifact.path
        if artifact.released:
            self._storage_lease = None
            self._storage_pool = None
            self._artifact = None
        ticket = self._finalizer_ticket
        cleanup = self._finalizer_capsule
        if self._artifact is None and ticket is not None and cleanup is not None:
            cancel_prepared_finalizer_cleanup(cleanup)
            self._finalizer_ticket = None
            self._finalizer_capsule = None

    def __del__(self) -> None:
        """Detach the shared replay artifact owner into a safe-point capsule."""
        try:
            if runtime_is_finalizing() or os.getpid() != getattr(self, "_pid", os.getpid()):
                return
            artifact = getattr(self, "_artifact", None)
            ticket = getattr(self, "_finalizer_ticket", None)
            cleanup = getattr(self, "_finalizer_capsule", None)
            if artifact is None or ticket is None or cleanup is None:
                return
            cleanup.arg0 = artifact
            if defer_prepared_finalizer_cleanup(cleanup):
                self._artifact = None
                self._path = None
                self._storage_lease = None
                self._storage_pool = None
                self._finalizer_ticket = None
                self._finalizer_capsule = None
        except BaseException:
            pass


def make_replayable_parquet_stream(
    stream: Any, *, feature: str, memory_limit_bytes: int | None = None
) -> ReplayableArrowStream:
    """Return a governed replay stream used by native Parquet safety fallback."""
    return ReplayableArrowStream(stream, feature=feature, memory_limit_bytes=memory_limit_bytes)
