"""Reusable Parquet Arrow C Stream factory and PyArrow fallback.

It stages inputs when necessary, preflights native reading, constructs replay-safe Arrow
factories, and owns every reader, dataset, and keepalive lease.
"""

from __future__ import annotations

import logging
import os
import tempfile
import weakref
from contextlib import suppress
from dataclasses import dataclass
from pathlib import Path
from threading import Lock
from typing import Any, Callable, cast

from ...core_impl.dependencies import ensure_optional_dependency, ensure_pyarrow
from ...core_impl.finalization import runtime_is_finalizing
from ...core_impl.finalizer_cleanup import (
    PreparedFinalizerCleanup,
    cancel_prepared_finalizer_cleanup,
    defer_prepared_finalizer_cleanup,
    reserve_finalizer_cleanup,
)
from ...core_impl.fork_safety import ensure_runtime_fork_safe
from ...core_impl.native_symbols import PARQUET_STREAM_READ
from ...core_impl.process_resources import (
    acquire_external_file_capability,
    acquire_external_runtime_threads,
    acquire_file_descriptor_capability,
    constrain_external_runtime_worker_pool,
    open_governed_file,
)
from ...core_impl.resource_lifecycle import (
    _close_and_clear_attrs,
    _close_sequence_retryably,
    _close_suppressing_errors,
)
from ...core_impl.temporary_storage import (
    StreamingStorageReservation,
    TemporaryStorageLease,
    TemporaryStoragePermitPool,
)
from ...core_impl.uris import local_path_or_reject_remote
from ..pyarrow.streams import record_batch_reader_from_iterable
from .memory import DEFAULT_PARQUET_BATCH_ROWS, _native_parquet_batch_size_contract_issue
from .native_reader import (
    native_nested_contract_blockers,
    native_writer_detected,
    native_writer_diagnostics,
    try_native_parquet_stream,
)
from .status import native_parquet_stream_preflight_info
from .telemetry import (
    record_parquet_fallback_attempt,
    record_parquet_fallback_failure,
    record_parquet_fallback_success,
)

_LOGGER = logging.getLogger(__name__)


def local_parquet_path_or_none(data: Any, *, source: str, feature: str) -> str | None:
    """Return a local filesystem path when a Parquet source names one."""
    if source == "path":
        return os.fspath(data)
    if source == "uri":
        return local_path_or_reject_remote(
            data,
            remote_error=f"{feature} URI inputs must be staged before Parquet decoding",
        )
    return None


def _parquet_buffer(data: bytes | bytearray | memoryview) -> bytes | bytearray | memoryview:
    """Return a contiguous byte-oriented buffer, copying only when required."""
    if not isinstance(data, memoryview):
        return data
    if not data.contiguous:
        return data.tobytes()
    if data.itemsize != 1 or data.format != "B":
        return data.cast("B")
    return data


def open_parquet_source(data: Any, *, source: str, feature: str, pa: Any) -> tuple[Any, Any | None]:
    """Open a Parquet source and return ``(source, owned_file)``."""
    local_path = local_parquet_path_or_none(data, source=source, feature=feature)
    if local_path is not None:
        opened_file = open_governed_file(local_path, "rb")
        return opened_file, opened_file
    if source == "uri":
        raise ValueError(f"{feature} URI inputs must be staged before Parquet decoding")
    if source == "text":
        if not isinstance(data, (bytes, bytearray, memoryview)):
            raise TypeError(f"{feature} expects bytes for source='text', got {type(data)!r}")
        opened_file = pa.BufferReader(_parquet_buffer(data))
        return opened_file, opened_file
    if source == "stream":
        seek = getattr(data, "seek", None)
        if not callable(seek):
            raise TypeError("Parquet stream inputs require seek(0)")
        seek(0)
        return data, None
    raise TypeError(f"Unsupported Parquet source: {source!r}")


def local_stream_path(data: Any) -> str | None:
    """Return a local path for a file-like object backed by a named file."""
    name = getattr(data, "name", None)
    if not isinstance(name, (str, os.PathLike)):
        return None
    try:
        path = os.fspath(name)
    except TypeError:
        return None
    if not path or path.startswith("<"):
        return None
    try:
        return path if os.path.isfile(path) else None
    except OSError:
        return None


def _cleanup_staged_parquet_capsule(capsule: PreparedFinalizerCleanup) -> None:
    """Delete a detached staged Parquet artifact before returning disk capacity."""
    path = cast(str | None, capsule.arg0)
    if path is not None:
        try:
            os.unlink(path)
        except FileNotFoundError:
            pass
        capsule.arg0 = None
    lease = cast(TemporaryStorageLease | None, capsule.arg1)
    if lease is not None:
        lease.release()
        capsule.arg1 = None
    pool = cast(TemporaryStoragePermitPool | None, capsule.arg2)
    if pool is not None:
        pool.close()
        capsule.arg2 = None


class _StagedParquetArtifact:
    """Prearmed pathname + inode + byte authority for one staged Parquet buffer."""

    __slots__ = (
        "_pid",
        "path",
        "_pool",
        "_lease",
        "_finalizer_ticket",
        "_finalizer_capsule",
    )

    def __init__(
        self, data: bytes | bytearray | memoryview, *, memory_limit_bytes: int | None
    ) -> None:
        """Reserve storage and create the staged Parquet file from the supplied buffer."""
        self._pid = os.getpid()
        self.path: str | None = None
        self._pool: TemporaryStoragePermitPool | None = None
        self._lease: Any | None = None
        self._finalizer_capsule: PreparedFinalizerCleanup | None = reserve_finalizer_cleanup(
            _cleanup_staged_parquet_capsule
        )
        self._finalizer_ticket: int | None = self._finalizer_capsule.ticket
        pool = TemporaryStoragePermitPool(memory_limit_bytes)
        lease = pool.acquire(
            0, label="parquet_buffer_stage", path=tempfile.gettempdir(), artifact_count=1
        )
        self._pool = pool
        self._lease = lease
        path_box: list[str] = []

        def create() -> int:
            """Create and publish the staged Parquet temporary file descriptor."""
            fd, path = tempfile.mkstemp(prefix="schema-sanitizer-parquet-", suffix=".parquet")
            path_box.append(path)
            return fd

        try:
            with acquire_file_descriptor_capability(
                1, label="parquet_buffer_mkstemp"
            ) as capability:
                with capability.open_descriptor(create, label="parquet_buffer_mkstemp"):
                    pass
            if not path_box:
                raise RuntimeError("Parquet staging did not publish its temporary path")
            path = path_box[0]
            self.path = path
            reservation = StreamingStorageReservation(lease, initial_credit_bytes=0, path=path)
            with open_governed_file(path, "wb") as handle:
                payload = _parquet_buffer(data)
                reservation.before_write(len(payload))
                handle.write(payload)
            reservation.finalize(os.path.getsize(path))
        except BaseException:
            self.close()
            raise

    def close(self) -> None:
        """Delete the staged file, release its storage authority, and disarm finalization."""
        if os.getpid() != self._pid:
            return
        path = self.path
        if path is not None:
            try:
                os.unlink(path)
            except FileNotFoundError:
                pass
            except OSError:
                # Retryable cleanup must retain this exact artifact owner; a
                # silent return would let generic attr cleanup clear the owner
                # while bytes/inode authority is still live.
                raise
            self.path = None
        lease = self._lease
        if lease is not None:
            lease.release()
            self._lease = None
        pool = self._pool
        if pool is not None:
            pool.close()
            self._pool = None
        ticket = self._finalizer_ticket
        capsule = self._finalizer_capsule
        if ticket is not None and capsule is not None:
            cancel_prepared_finalizer_cleanup(capsule)
            self._finalizer_ticket = None
            self._finalizer_capsule = None

    def __del__(self) -> None:
        """Schedule best-effort cleanup during garbage collection."""
        try:
            if runtime_is_finalizing() or os.getpid() != getattr(self, "_pid", os.getpid()):
                return
            ticket = getattr(self, "_finalizer_ticket", None)
            capsule = getattr(self, "_finalizer_capsule", None)
            if ticket is None or capsule is None:
                return
            capsule.arg0 = getattr(self, "path", None)
            capsule.arg1 = getattr(self, "_lease", None)
            capsule.arg2 = getattr(self, "_pool", None)
            if defer_prepared_finalizer_cleanup(capsule):
                self.path = None
                self._lease = None
                self._pool = None
                self._finalizer_ticket = None
                self._finalizer_capsule = None
        except BaseException:
            pass


def _stage_parquet_buffer_artifact(
    data: bytes | bytearray | memoryview, *, memory_limit_bytes: int | None
) -> _StagedParquetArtifact:
    """Create a governed temporary Parquet artifact from buffered bytes."""
    return _StagedParquetArtifact(data, memory_limit_bytes=memory_limit_bytes)


def stage_parquet_buffer(data: bytes | bytearray | memoryview) -> str:
    """Stage buffer-backed Parquet bytes under governed inode/FD ownership."""
    path_box: list[str] = []

    def create() -> int:
        """Create and publish the temporary file descriptor used for staged bytes."""
        fd, path = tempfile.mkstemp(prefix="schema-sanitizer-parquet-", suffix=".parquet")
        path_box.append(path)
        return fd

    with acquire_file_descriptor_capability(1, label="parquet_buffer_mkstemp") as capability:
        with capability.open_descriptor(create, label="parquet_buffer_mkstemp"):
            pass
    if not path_box:
        raise RuntimeError("Parquet staging did not publish its temporary path")
    path = path_box[0]
    try:
        with open_governed_file(path, "wb") as handle:
            handle.write(_parquet_buffer(data))
    except BaseException:
        with suppress(OSError):
            Path(path).unlink()
        raise
    return path


def remove_staged_parquet(path: str | None) -> bool:
    """Remove a staged Parquet file and report whether it is gone."""
    if not path:
        return True
    try:
        Path(path).unlink()
    except FileNotFoundError:
        return True
    except OSError:
        return False
    return True


@dataclass(frozen=True, slots=True)
class PreparedParquetFactorySource:
    """Resolved native source details owned by a record-batch factory."""

    local_path: Any | None
    staged_path: str | None
    staged_artifact: _StagedParquetArtifact | None
    native_source_kind: str


def prepare_parquet_factory_source(
    data: Any,
    *,
    source: str,
    feature: str,
    logger: Any,
    memory_limit_bytes: int | None = None,
) -> PreparedParquetFactorySource:
    """Resolve a local path or stage an in-memory Parquet buffer."""
    native_source_kind = source
    local_path = local_parquet_path_or_none(data, source=source, feature=feature)
    staged_path: str | None = None
    staged_artifact: _StagedParquetArtifact | None = None

    if local_path is None and source == "stream":
        local_path = local_stream_path(data)
        if local_path is not None:
            native_source_kind = "stream_path"

    if local_path is None and source == "text" and isinstance(data, (bytes, bytearray, memoryview)):
        try:
            staged_artifact = _stage_parquet_buffer_artifact(
                data, memory_limit_bytes=memory_limit_bytes
            )
            staged_path = staged_artifact.path
        except OSError as exc:
            logger.debug(
                "Native Parquet buffer staging failed; retrying via PyArrow buffer reader: %s",
                exc,
            )
        else:
            local_path = staged_path
            native_source_kind = "staged_text"

    if local_path is not None and source == "path":
        native_source_kind = "path"
    elif local_path is not None and source == "uri":
        native_source_kind = "uri_path"

    return PreparedParquetFactorySource(
        local_path=local_path,
        staged_path=staged_path,
        staged_artifact=staged_artifact,
        native_source_kind=native_source_kind,
    )


def _cleanup_parquet_stream_owner_capsule(capsule: PreparedFinalizerCleanup) -> None:
    """Cleanup parquet stream owner capsule."""
    resources = cast(list[Any] | None, capsule.arg0)
    if resources is None:
        return
    _close_sequence_retryably(resources)
    if resources:
        raise RuntimeError("Parquet stream keepalive cleanup remains retryable")
    capsule.arg0 = None


class _ParquetStreamKeepaliveOwner:
    """Own resources for one Arrow stream, independently of factory lifetime."""

    __slots__ = ("resources", "_finalizer_ticket", "_finalizer_capsule", "_pid", "__weakref__")

    def __init__(self) -> None:
        """Prearm finalization and root an empty list of stream-owned resources."""
        self._pid = os.getpid()
        self.resources: list[Any] = []
        self._finalizer_capsule: PreparedFinalizerCleanup | None = reserve_finalizer_cleanup(
            _cleanup_parquet_stream_owner_capsule
        )
        self._finalizer_ticket = self._finalizer_capsule.ticket
        # Root the already-allocated list directly; GC publication allocates no tuple.
        self._finalizer_capsule.arg0 = self.resources

    def add(self, resource: Any | None) -> Any | None:
        """Add one value to the bounded collection."""
        if resource is not None:
            self.resources.append(resource)
        return resource

    def close(self) -> None:
        """Close retained stream resources retryably, then disarm finalization."""
        _close_sequence_retryably(self.resources)
        if self.resources:
            raise RuntimeError("Parquet stream keepalive cleanup remains retryable")
        ticket = self._finalizer_ticket
        capsule = self._finalizer_capsule
        if ticket and capsule is not None:
            cancel_prepared_finalizer_cleanup(capsule)
            self._finalizer_ticket = 0
            self._finalizer_capsule = None

    def __del__(self) -> None:
        """Schedule best-effort cleanup during garbage collection."""
        try:
            if runtime_is_finalizing() or os.getpid() != getattr(self, "_pid", os.getpid()):
                return
            ticket = getattr(self, "_finalizer_ticket", 0)
            capsule = getattr(self, "_finalizer_capsule", None)
            if ticket and capsule is not None:
                if defer_prepared_finalizer_cleanup(capsule):
                    self._finalizer_ticket = 0
                    self._finalizer_capsule = None
                    self.resources = []
        except BaseException:
            pass


class _OwnedParquetBatchIterator:
    """Tie one owner to the iterable retained by PyArrow's exported reader."""

    __slots__ = ("_iterator", "_owner", "_registry", "_registration", "_closed")

    def __init__(
        self,
        iterator: Any,
        owner: _ParquetStreamKeepaliveOwner,
        registry: list[Any],
        registration: Any,
    ) -> None:
        """Bind the batch iterator to its keepalive owner and root registration."""
        self._iterator = iterator
        self._owner: _ParquetStreamKeepaliveOwner | None = owner
        self._registry = registry
        self._registration = registration
        self._closed = False

    def __iter__(self) -> "_OwnedParquetBatchIterator":
        """Iterate over the retained values."""
        return self

    def __next__(self) -> Any:
        """Return the next retained value."""
        if self._closed:
            raise StopIteration
        try:
            return next(self._iterator)
        except StopIteration:
            self.close()
            raise
        except BaseException:
            try:
                self.close()
            except BaseException:
                pass
            raise

    def close(self) -> None:
        """Close the keepalive owner and remove this iterator from its registry."""
        if self._closed:
            return
        owner = self._owner
        if owner is not None:
            owner.close()
        self._owner = None
        self._closed = True
        with suppress(ValueError):
            self._registry.remove(self._registration)

    def __del__(self) -> None:
        # Dropping the owner triggers its prearmed safe-point finalizer; never
        # perform potentially-blocking runtime teardown on the GC thread.
        """Schedule best-effort cleanup during garbage collection."""
        self._owner = None


@dataclass(slots=True)
class _DatasetLifetimeCleanupState:
    """Named retry state for dataset + FD + staged-storage ownership."""

    dataset: Any | None = None
    fd_capability: Any | None = None
    staged_artifact: Any | None = None


def _cleanup_dataset_lifetime_capsule(capsule: PreparedFinalizerCleanup) -> None:
    """Cleanup dataset lifetime capsule."""
    state = capsule.arg0
    if not isinstance(state, _DatasetLifetimeCleanupState):
        return
    dataset = state.dataset
    if dataset is not None:
        # Dataset destruction must precede returning the FD capability. Clearing
        # the state first prevents retry from destroying the same external owner
        # twice if a later cleanup component fails.
        state.dataset = None
        del dataset
    capability = state.fd_capability
    if capability is not None:
        capability.close()
        state.fd_capability = None
    staged = state.staged_artifact
    if staged is not None:
        staged.close()
        state.staged_artifact = None


def _cleanup_dataset_lifetime_lease_capsule(capsule: PreparedFinalizerCleanup) -> None:
    """Cleanup dataset lifetime lease capsule."""
    owner = cast(_DatasetLifetimeOwner | None, capsule.arg0)
    if owner is not None:
        owner.release()
        capsule.arg0 = None


class _DatasetLifetimeOwner:
    """Keep dataset, external FD admission and staged storage co-owned/retryable."""

    __slots__ = (
        "_lock",
        "_dataset",
        "_fd_capability",
        "_staged_artifact",
        "_refs",
        "_closed",
        "_retiring",
        "_finalizer_ticket",
        "_finalizer_capsule",
        "_cleanup_state",
        "_pid",
    )

    def __init__(self, dataset: Any, fd_capability: Any, staged_artifact: Any | None) -> None:
        """Co-own the dataset, descriptor capability, and staged artifact under one reference count."""
        self._pid = os.getpid()
        self._lock = Lock()
        self._dataset = dataset
        self._fd_capability = fd_capability
        self._staged_artifact = staged_artifact
        self._refs = 1
        self._closed = False
        self._retiring = False
        self._finalizer_capsule: PreparedFinalizerCleanup | None = reserve_finalizer_cleanup(
            _cleanup_dataset_lifetime_capsule
        )
        self._finalizer_ticket = self._finalizer_capsule.ticket
        self._cleanup_state: _DatasetLifetimeCleanupState | None = _DatasetLifetimeCleanupState(
            dataset=dataset, fd_capability=fd_capability, staged_artifact=staged_artifact
        )
        self._finalizer_capsule.arg0 = self._cleanup_state

    def _ensure_owner_process(self) -> None:
        """Reject dataset reuse from a different process."""
        if os.getpid() != self._pid:
            ensure_runtime_fork_safe()
            raise RuntimeError("PyArrow dataset lifetime owner belongs to a different process")

    @property
    def dataset(self) -> Any:
        """Return the PyArrow dataset retained by this lifetime owner."""
        self._ensure_owner_process()
        with self._lock:
            return self._dataset

    def acquire(self) -> "_DatasetLifetimeLease":
        """Acquire governed capacity through this dataset lifetime owner."""
        self._ensure_owner_process()
        with self._lock:
            if self._closed or self._dataset is None:
                raise RuntimeError("PyArrow dataset lifetime is already closed")
            self._refs += 1
        try:
            return _DatasetLifetimeLease(self)
        except BaseException:
            self.release()
            raise

    def release(self) -> None:
        """Drop one reference and retire final dataset, descriptor, and staging ownership in order."""
        self._ensure_owner_process()
        with self._lock:
            if self._closed:
                return
            if self._refs > 0:
                self._refs -= 1
            if self._refs != 0 or self._retiring:
                return
            cleanup_state = self._cleanup_state
            if cleanup_state is None:
                raise RuntimeError("Parquet dataset cleanup state is unavailable")
            # Exactly one closer may retire the external dataset authority at a
            # time.  A failed component remains published and clears this flag
            # in ``finally`` so a later close/finalizer can retry it.
            self._retiring = True
            dataset = self._dataset
            self._dataset = None
            if dataset is not None:
                cleanup_state.dataset = None
        try:
            # Ensure external object destruction precedes authority return.
            del dataset

            capability = self._fd_capability
            if capability is not None:
                capability.close()
                with self._lock:
                    if self._fd_capability is capability:
                        self._fd_capability = None
                        cleanup_state.fd_capability = None

            staged = self._staged_artifact
            if staged is not None:
                staged.close()
                with self._lock:
                    if self._staged_artifact is staged:
                        self._staged_artifact = None
                        cleanup_state.staged_artifact = None
        finally:
            with self._lock:
                self._retiring = False
                self._closed = (
                    self._dataset is None
                    and self._fd_capability is None
                    and self._staged_artifact is None
                )
                if self._closed:
                    ticket = self._finalizer_ticket
                    capsule = self._finalizer_capsule
                    if ticket and capsule is not None:
                        cancel_prepared_finalizer_cleanup(capsule)
                        self._finalizer_ticket = 0
                        self._finalizer_capsule = None
                        self._cleanup_state = None

    close = release

    def __del__(self) -> None:
        """Schedule best-effort cleanup during garbage collection."""
        try:
            if runtime_is_finalizing() or os.getpid() != getattr(self, "_pid", os.getpid()):
                return
            ticket = getattr(self, "_finalizer_ticket", 0)
            capsule = getattr(self, "_finalizer_capsule", None)
            if ticket and capsule is not None:
                if defer_prepared_finalizer_cleanup(capsule):
                    self._finalizer_ticket = 0
                    self._finalizer_capsule = None
                    self._cleanup_state = None
                    self._dataset = None
                    self._fd_capability = None
                    self._staged_artifact = None
        except BaseException:
            pass


class _DatasetLifetimeLease:
    __slots__ = ("_owner", "_lock", "_finalizer_ticket", "_finalizer_capsule", "_pid")

    def __init__(self, owner: _DatasetLifetimeOwner) -> None:
        """Retain one finalizer-backed reference to the dataset lifetime owner."""
        self._pid = os.getpid()
        self._owner: _DatasetLifetimeOwner | None = owner
        self._lock = Lock()
        self._finalizer_capsule: PreparedFinalizerCleanup | None = reserve_finalizer_cleanup(
            _cleanup_dataset_lifetime_lease_capsule
        )
        self._finalizer_ticket = self._finalizer_capsule.ticket
        self._finalizer_capsule.arg0 = owner

    def close(self) -> None:
        """Release the retained dataset reference and disarm this lease finalizer."""
        if os.getpid() != self._pid:
            ensure_runtime_fork_safe()
            raise RuntimeError("PyArrow dataset lifetime lease belongs to a different process")
        with self._lock:
            owner = self._owner
            if owner is None:
                return
            owner.release()
            self._owner = None
            ticket = self._finalizer_ticket
            capsule = self._finalizer_capsule
            if ticket and capsule is not None:
                cancel_prepared_finalizer_cleanup(capsule)
                self._finalizer_ticket = 0
                self._finalizer_capsule = None

    def __del__(self) -> None:
        """Schedule best-effort cleanup during garbage collection."""
        try:
            if runtime_is_finalizing() or os.getpid() != getattr(self, "_pid", os.getpid()):
                return
            ticket = getattr(self, "_finalizer_ticket", 0)
            capsule = getattr(self, "_finalizer_capsule", None)
            if ticket and capsule is not None:
                if defer_prepared_finalizer_cleanup(capsule):
                    self._owner = None
                    self._finalizer_ticket = 0
                    self._finalizer_capsule = None
        except BaseException:
            pass


def close_factory(factory: Any) -> None:
    """Close factory-owned resources without stealing live stream ownership."""
    if os.getpid() != factory._pid:
        return
    _close_and_clear_attrs(factory, "_pending_parquet_file", "_pending_opened_file")
    factory._keepalive.clear()

    # Drop the factory's dataset reference before returning its external FD
    # admission or staged-storage credits. Active streams hold independent refs.
    factory._dataset = None
    _close_and_clear_attrs(factory, "_dataset_owner")
    _close_and_clear_attrs(factory, "_dataset_fd_capability")
    _close_and_clear_attrs(factory, "_staged_artifact")
    staged_path = getattr(factory, "_staged_path", None)
    if remove_staged_parquet(staged_path):
        factory._staged_path = None


def open_parquet_file(factory: Any) -> tuple[Any, Any | None]:
    """Open a ParquetFile and return it with any owned file handle."""
    source, opened_file = open_parquet_source(
        factory._data,
        source=factory._source,
        feature=factory._feature,
        pa=factory._pa,
    )
    return factory._pq.ParquetFile(source), opened_file


def project_schema(factory: Any, schema: Any) -> Any:
    """Return a top-level projected schema when columns were requested."""
    if factory._columns is None:
        return schema
    fields = []
    for column in factory._columns:
        index = schema.get_field_index(column)
        if index < 0:
            raise KeyError(f"Parquet projection column not found: {column!r}")
        fields.append(schema.field(index))
    return factory._pa.schema(fields, metadata=schema.metadata)


def initialize_factory_schema(factory: Any, *, logger: Any) -> None:
    """Read the source schema using dataset or ParquetFile fallback setup."""
    if factory._local_path is not None:
        dataset_fd_capability = acquire_external_file_capability(1, label="pyarrow_dataset_path")
        try:
            factory._ds = ensure_optional_dependency(
                "pyarrow.dataset",
                extra="pyarrow",
                feature=factory._feature,
                dependency_name="pyarrow",
            )
            factory._dataset = factory._ds.dataset(factory._local_path, format="parquet")
        except BaseException as exc:
            dataset_fd_capability.close()
            if not isinstance(exc, Exception):
                raise
            factory._dataset = None
            factory._dataset_error = exc
            logger.debug(
                "PyArrow dataset construction failed for Parquet source; "
                "native reader or ParquetFile fallback may still recover: %s",
                exc,
            )
            factory._pending_parquet_file, factory._pending_opened_file = open_parquet_file(factory)
            schema = factory._pending_parquet_file.schema_arrow
        else:
            try:
                schema = factory._dataset.schema
                staged = getattr(factory, "_staged_artifact", None)
                dataset_owner = _DatasetLifetimeOwner(
                    factory._dataset, dataset_fd_capability, staged
                )
                factory._dataset_owner = dataset_owner
                factory._dataset_fd_capability = None
                if staged is not None:
                    factory._staged_artifact = None
            except BaseException:
                factory._dataset_owner = None
                dataset_fd_capability.close()
                raise
    else:
        factory._dataset = None
        factory._pending_parquet_file, factory._pending_opened_file = open_parquet_file(factory)
        schema = factory._pending_parquet_file.schema_arrow
    factory.schema = project_schema(factory, schema)


def _external_runtime_threads(factory: Any) -> Any:
    """Reserve the complete external worker envelope or degrade to serial."""
    desired = 1
    if factory._use_threads:
        try:
            from ...core_impl.execution_policy import execution_policy

            desired = max(
                2, execution_policy("multi", factory._memory_limit_bytes).effective_workers
            )
        except BaseException:
            desired = max(2, min(32, os.cpu_count() or 2))
    return acquire_external_runtime_threads(
        desired, allow_parallel=factory._use_threads, runtime=factory._pa
    )


_PARQUET_KEEPALIVE_COMPACT_THRESHOLD = 64


def _compact_factory_stream_keepalive(keepalive: list[Any]) -> None:
    """Remove dead weakref tombstones in-place with bounded amortized churn."""
    if len(keepalive) < _PARQUET_KEEPALIVE_COMPACT_THRESHOLD:
        return
    write = 0
    for read, entry in enumerate(keepalive):
        if isinstance(entry, weakref.ReferenceType) and entry() is None:
            continue
        if write != read:
            keepalive[write] = entry
        write += 1
    if write < len(keepalive):
        del keepalive[write:]


def _factory_stream_keepalive(factory: Any) -> list[Any]:
    """Return the per-stream owner list after compacting abandoned tombstones."""
    keepalive = factory._keepalive
    _compact_factory_stream_keepalive(keepalive)
    return keepalive


def _constrain_factory_pyarrow_pool(factory: Any, runtime_lease: Any) -> None:
    """Cap the PyArrow pool to the admitted external-runtime workers."""
    if runtime_lease.workers > 1:
        configured = constrain_external_runtime_worker_pool(factory._pa, runtime_lease.workers)
        runtime_lease.shrink_to(configured)


def pyarrow_fallback_arrow_stream(
    factory: Any,
    *,
    record_batch_reader_from_iterable: Callable[..., Any],
    logger: Any,
) -> Any:
    """Return an Arrow C Stream capsule using PyArrow fallback routes."""
    if factory._filters is not None and factory._dataset is None:
        fallback_route = "pyarrow_dataset_scanner"
        record_parquet_fallback_attempt(fallback_route)
        exc = factory._dataset_error or RuntimeError(
            "Parquet filters require the PyArrow dataset fallback route"
        )
        record_parquet_fallback_failure(fallback_route, exc)
        raise exc

    if factory._dataset is not None:
        fallback_route = "pyarrow_dataset_scanner"
        record_parquet_fallback_attempt(fallback_route)
        runtime_lease = _external_runtime_threads(factory)
        scanner = None
        reader = None
        owner = _ParquetStreamKeepaliveOwner()
        registry = _factory_stream_keepalive(factory)
        registration = weakref.ref(owner)
        registry.append(registration)
        owner.add(runtime_lease)
        dataset_lifetime = None
        try:
            if runtime_lease.parallel:
                _constrain_factory_pyarrow_pool(factory, runtime_lease)
            dataset_owner = factory._dataset_owner
            if dataset_owner is None:
                raise RuntimeError("PyArrow dataset source has no lifetime owner")
            dataset_lifetime = dataset_owner.acquire()
            owner.add(dataset_lifetime)
            dataset = dataset_owner.dataset
            scanner = dataset.scanner(
                columns=None if factory._columns is None else list(factory._columns),
                filter=factory._filters,
                batch_size=factory._batch_size,
                use_threads=runtime_lease.parallel,
            )
            owner.add(scanner)
            reader = scanner.to_reader()
            owner.add(reader)
            owned_batches = _OwnedParquetBatchIterator(iter(reader), owner, registry, registration)
            exported_reader = record_batch_reader_from_iterable(
                factory._pa, factory.schema, owned_batches
            )
            stream = exported_reader.__arrow_c_stream__()
        except BaseException as exc:
            try:
                owner.close()
            finally:
                if not owner.resources:
                    with suppress(ValueError):
                        registry.remove(registration)
            if not isinstance(exc, Exception):
                raise
            record_parquet_fallback_failure(fallback_route, exc)
            if factory._filters is not None:
                raise
            logger.debug(
                "PyArrow dataset fallback failed; trying ParquetFile.iter_batches: %s",
                exc,
            )
        else:
            record_parquet_fallback_success(fallback_route)
            return stream

    dataset_error = factory._dataset_error
    if dataset_error is not None:
        dataset_route = "pyarrow_dataset_scanner"
        record_parquet_fallback_attempt(dataset_route)
        record_parquet_fallback_failure(dataset_route, dataset_error)

    fallback_route = "pyarrow_parquetfile_iter_batches"
    record_parquet_fallback_attempt(fallback_route)
    runtime_lease = _external_runtime_threads(factory)
    owner = _ParquetStreamKeepaliveOwner()
    registry = _factory_stream_keepalive(factory)
    registration = weakref.ref(owner)
    registry.append(registration)
    owner.add(runtime_lease)
    parquet_file = None
    opened_file = None
    reader = None
    try:
        if runtime_lease.parallel:
            _constrain_factory_pyarrow_pool(factory, runtime_lease)
        if factory._pending_parquet_file is not None:
            parquet_file = factory._pending_parquet_file
            opened_file = factory._pending_opened_file
            factory._pending_parquet_file = None
            factory._pending_opened_file = None
        else:
            parquet_file, opened_file = open_parquet_file(factory)
        owner.add(opened_file)
        owner.add(parquet_file)
        batches = parquet_file.iter_batches(
            batch_size=factory._batch_size,
            columns=None if factory._columns is None else list(factory._columns),
            use_threads=runtime_lease.parallel,
        )
        owned_batches = _OwnedParquetBatchIterator(iter(batches), owner, registry, registration)
        reader = record_batch_reader_from_iterable(factory._pa, factory.schema, owned_batches)
        stream = reader.__arrow_c_stream__()
    except BaseException as exc:
        try:
            owner.close()
        finally:
            if not owner.resources:
                with suppress(ValueError):
                    registry.remove(registration)
        if isinstance(exc, Exception):
            record_parquet_fallback_failure(fallback_route, exc)
        raise
    record_parquet_fallback_success(fallback_route)
    return stream


def _cleanup_parquet_factory_capsule(capsule: PreparedFinalizerCleanup) -> None:
    """Close only detached Parquet resources while retaining failed owners."""
    for name in ("arg0", "arg1"):
        resource = getattr(capsule, name)
        if resource is None:
            continue
        if not _close_suppressing_errors(resource):
            raise RuntimeError("Parquet pending resource cleanup remains retryable")
        setattr(capsule, name, None)
    keepalive = cast(tuple[Any, ...] | list[Any] | None, capsule.arg2)
    if keepalive is not None:
        failed = False
        for resource in keepalive:
            if not _close_suppressing_errors(resource):
                failed = True
        if failed:
            raise RuntimeError("Parquet keepalive cleanup remains retryable")
        capsule.arg2 = None
    staged_artifact = capsule.arg3
    if staged_artifact is not None:
        close = getattr(staged_artifact, "close", None)
        if callable(close):
            close()
        elif not remove_staged_parquet(str(staged_artifact)):
            raise RuntimeError("staged Parquet path cleanup remains retryable")
        capsule.arg3 = None
    dataset_owner = capsule.arg5
    if dataset_owner is not None:
        if not _close_suppressing_errors(dataset_owner):
            raise RuntimeError("PyArrow dataset lifetime cleanup remains retryable")
        capsule.arg5 = None
    dataset_fd_capability = capsule.arg4
    if dataset_fd_capability is not None:
        if not _close_suppressing_errors(dataset_fd_capability):
            raise RuntimeError("PyArrow dataset FD admission cleanup remains retryable")
        capsule.arg4 = None


class ParquetRecordBatchStreamFactory:
    """Reusable PyArrow RecordBatchReader factory for direct native ingestion."""

    schema: Any

    def __init__(
        self,
        data: Any,
        *,
        source: str,
        feature: str,
        batch_size: int = DEFAULT_PARQUET_BATCH_ROWS,
        use_threads: bool = False,
        columns: list[str] | tuple[str, ...] | None = None,
        filters: Any | None = None,
        memory_limit_bytes: int | None = None,
    ) -> None:
        """Store the Parquet source and read its schema once, transactionally."""
        self._pid = os.getpid()
        self._data = data
        self._source = source
        self._feature = feature
        self._batch_size = batch_size
        self._use_threads = use_threads
        self._columns = None if columns is None else tuple(columns)
        self._filters = filters
        self._memory_limit_bytes = memory_limit_bytes

        # Arm the only detached cleanup path before staging/opening anything.
        # Every subsequently-published owner is therefore recoverable even if
        # dependency loading or schema initialization raises MemoryError.
        self._local_path = None
        self._staged_path = None
        self._staged_artifact = None
        self._native_source_kind = source
        self.sink = "stream"
        self.diagnostics = None
        self.native_registry_state = None
        self._pa = None
        self._pq = None
        self._ds = None
        self._dataset = None
        self._dataset_error: BaseException | None = None
        self._keepalive: list[Any] = []
        self._pending_parquet_file: Any | None = None
        self._pending_opened_file: Any | None = None
        self._dataset_fd_capability: Any | None = None
        self._dataset_owner: _DatasetLifetimeOwner | None = None
        self._finalizer_capsule: PreparedFinalizerCleanup | None = reserve_finalizer_cleanup(
            _cleanup_parquet_factory_capsule
        )
        self._finalizer_ticket: int | None = self._finalizer_capsule.ticket

        try:
            prepared = prepare_parquet_factory_source(
                data,
                source=source,
                feature=feature,
                logger=_LOGGER,
                memory_limit_bytes=memory_limit_bytes,
            )
            # Publish the staged artifact owner before any dependency import.
            self._staged_artifact = prepared.staged_artifact
            self._local_path = prepared.local_path
            self._staged_path = prepared.staged_path
            self._native_source_kind = prepared.native_source_kind
            if self._filters is not None and self._local_path is None:
                raise ValueError("Parquet filters require a path-backed source")

            self._pa = ensure_pyarrow(feature=feature)
            self._pq = ensure_optional_dependency(
                "pyarrow.parquet",
                extra="pyarrow",
                feature=feature,
                dependency_name="pyarrow",
            )
            initialize_factory_schema(self, logger=_LOGGER)
        except BaseException:
            # Do not mask the primary exception. Successful cleanup cancels the
            # prearmed finalizer; retryable failures stay attached to ``self``
            # and the finalizer capsule remains available for eventual cleanup.
            try:
                close_factory(self)
            except BaseException:
                pass
            if (
                self._pending_parquet_file is None
                and self._pending_opened_file is None
                and not self._keepalive
                and self._staged_artifact is None
                and self._staged_path is None
                and self._dataset_fd_capability is None
                and self._dataset_owner is None
            ):
                ticket = self._finalizer_ticket
                cleanup = self._finalizer_capsule
                if ticket is not None and cleanup is not None:
                    cancel_prepared_finalizer_cleanup(cleanup)
                    self._finalizer_ticket = None
                    self._finalizer_capsule = None
            raise

    def _native_batch_size_blocker(self, info: dict[str, Any]) -> str | None:
        """Return a native-reader blocker for the configured batch size."""
        return _native_parquet_batch_size_contract_issue(info, self._batch_size)

    @staticmethod
    def _native_writer_detected(info: dict[str, Any]) -> bool:
        """Return whether footer metadata identifies the native writer."""
        return native_writer_detected(info)

    def _native_writer_diagnostics(self, info: dict[str, Any]) -> dict[str, Any]:
        """Return native-writer diagnostics derived from footer metadata."""
        return native_writer_diagnostics(info)

    @staticmethod
    def _native_nested_contract_blockers(info: dict[str, Any]) -> list[str]:
        """Return blockers for unsafe nested native-reader layouts."""
        return native_nested_contract_blockers(info)

    def _try_native_stream(self) -> Any | None:
        """Attempt to open the source through the native Parquet reader."""
        return try_native_parquet_stream(
            self,
            native_stream_read_hook=PARQUET_STREAM_READ,
            footer_info=native_parquet_stream_preflight_info,
            logger=_LOGGER,
        )

    def __arrow_c_stream__(self, requested_schema: Any = None) -> Any:
        """Return a fresh Arrow C Stream capsule for native ingestion."""
        self._ensure_owner_process()
        del requested_schema
        native_capsule = self._try_native_stream()
        if native_capsule is not None:
            return native_capsule
        return pyarrow_fallback_arrow_stream(
            self,
            record_batch_reader_from_iterable=record_batch_reader_from_iterable,
            logger=_LOGGER,
        )

    def __arrow_c_schema__(self) -> Any:
        """Export the cached schema without opening a data-bearing stream."""
        self._ensure_owner_process()
        return self.schema.__arrow_c_schema__()

    def _ensure_owner_process(self) -> None:
        """Reject use of PyArrow/native state inherited by a forked child."""
        if os.getpid() != getattr(self, "_pid", os.getpid()):
            raise RuntimeError("Parquet stream factory cannot be reused after fork")

    def close(self) -> None:
        """Close pending Parquet resources in the owning process only."""
        close_factory(self)
        if (
            self._pending_parquet_file is None
            and self._pending_opened_file is None
            and not self._keepalive
            and self._staged_artifact is None
            and self._staged_path is None
            and self._dataset_fd_capability is None
            and self._dataset_owner is None
        ):
            ticket = getattr(self, "_finalizer_ticket", None)
            cleanup = getattr(self, "_finalizer_capsule", None)
            if ticket is not None and cleanup is not None:
                cancel_prepared_finalizer_cleanup(cleanup)
                self._finalizer_ticket = None
                self._finalizer_capsule = None

    def __del__(self) -> None:
        """Detach only pending Parquet cleanup resources into a reserved capsule."""
        try:
            if runtime_is_finalizing() or os.getpid() != getattr(self, "_pid", os.getpid()):
                return
            ticket = getattr(self, "_finalizer_ticket", None)
            cleanup = getattr(self, "_finalizer_capsule", None)
            if ticket is None or cleanup is None:
                return
            cleanup.arg0 = getattr(self, "_pending_parquet_file", None)
            cleanup.arg1 = getattr(self, "_pending_opened_file", None)
            cleanup.arg2 = getattr(self, "_keepalive", ()) or None
            cleanup.arg3 = getattr(self, "_staged_artifact", None) or getattr(
                self, "_staged_path", None
            )
            cleanup.arg4 = getattr(self, "_dataset_fd_capability", None)
            cleanup.arg5 = getattr(self, "_dataset_owner", None)
            if defer_prepared_finalizer_cleanup(cleanup):
                self._pending_parquet_file = None
                self._pending_opened_file = None
                self._keepalive = []
                self._staged_artifact = None
                self._staged_path = None
                self._dataset_fd_capability = None
                self._dataset_owner = None
                self._data = None
                self._dataset = None
                self._finalizer_ticket = None
                self._finalizer_capsule = None
        except BaseException:
            pass


def open_parquet_record_batch_stream_factory(
    data: Any,
    *,
    source: str,
    feature: str,
    batch_size: int = DEFAULT_PARQUET_BATCH_ROWS,
    use_threads: bool = False,
    columns: list[str] | tuple[str, ...] | None = None,
    filters: Any | None = None,
    memory_limit_bytes: int | None = None,
) -> ParquetRecordBatchStreamFactory:
    """Open Parquet input as a reusable Arrow C Stream factory."""
    return ParquetRecordBatchStreamFactory(
        data,
        source=source,
        feature=feature,
        batch_size=batch_size,
        use_threads=use_threads,
        columns=columns,
        filters=filters,
        memory_limit_bytes=memory_limit_bytes,
    )
