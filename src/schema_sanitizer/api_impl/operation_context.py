"""Whole-operation concurrency policy and lazy remote-I/O ownership."""

from __future__ import annotations

from collections.abc import Awaitable, Callable
from concurrent.futures import Future
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from threading import Lock
from time import time_ns
from typing import TypeVar

from ..core_impl.execution_policy import ExecutionPolicy, execution_policy
from ..core_impl.temporary_storage import TemporaryStoragePermitPool
from ..remote_impl.io_coordinator import RemoteIoCoordinator
from ..remote_impl.provider_session_pool import RemoteProviderSessionPool

T = TypeVar("T")

_UNIX_EPOCH = datetime(1970, 1, 1, tzinfo=UTC)


@dataclass(frozen=True, slots=True)
class OperationTimestamps:
    """One wall-clock sample represented for Arrow and registry metadata."""

    ingestion_timestamp_micros: int
    detected_at: str


def capture_operation_timestamps() -> OperationTimestamps:
    """Capture one UTC timestamp before operation work is scheduled."""
    micros = time_ns() // 1_000
    detected_at = (_UNIX_EPOCH + timedelta(microseconds=micros)).strftime("%Y-%m-%dT%H:%M:%S.%fZ")
    return OperationTimestamps(micros, detected_at)


class _OperationExecutionResources:
    """Reference-count shared policy, permits, and remote coordinator."""

    def __init__(
        self,
        policy: ExecutionPolicy,
        memory_limit_bytes: int | None,
    ) -> None:
        """Create one resource domain for related operation contexts."""
        self.policy = policy
        self.memory_limit_bytes = memory_limit_bytes
        self.temporary_storage = TemporaryStoragePermitPool(memory_limit_bytes)
        self._lock = Lock()
        self._remote_coordinator: RemoteIoCoordinator | None = None
        self._references = 1
        self._closed = False

    def retain(self) -> None:
        """Retain this domain for one additional operation context."""
        with self._lock:
            if self._closed:
                raise RuntimeError("operation execution resources are closed")
            self._references += 1

    def ensure_open(self) -> None:
        """Reject work after the final owning context has closed."""
        with self._lock:
            if self._closed:
                raise RuntimeError("operation execution resources are closed")

    def remote_coordinator(self) -> RemoteIoCoordinator | None:
        """Return the lazy multi-mode coordinator shared by every child context."""
        if self.policy.is_single:
            return None
        with self._lock:
            if self._closed:
                raise RuntimeError("operation execution resources are closed")
            if self._remote_coordinator is None:
                self._remote_coordinator = RemoteIoCoordinator(RemoteProviderSessionPool)
            return self._remote_coordinator

    def release(self) -> None:
        """Release one reference and close resources after the final owner."""
        with self._lock:
            if self._references <= 0:
                return
            self._references -= 1
            if self._references != 0:
                return
            self._closed = True
            coordinator = self._remote_coordinator
            self._remote_coordinator = None
        try:
            if coordinator is not None:
                coordinator.close()
        finally:
            self.temporary_storage.close()


class OperationExecutionContext:
    """Own per-operation metadata plus shareable bounded execution resources.

    Normal public operations create an isolated resource domain. Partition
    pipelines may fork contexts so adjacent partitions retain distinct timestamps
    while sharing one temporary-storage budget and one remote-I/O coordinator.
    Single mode remains strictly inline and never constructs a coordinator.
    """

    def __init__(
        self,
        *,
        threading_mode: str,
        memory_limit_bytes: int | None,
        _resources: _OperationExecutionResources | None = None,
        _resources_already_retained: bool = False,
    ) -> None:
        """Capture one timestamp and attach to an execution resource domain."""
        if _resources is None:
            policy = execution_policy(threading_mode, memory_limit_bytes)
            resources = _OperationExecutionResources(policy, memory_limit_bytes)
        else:
            resources = _resources
            policy = resources.policy
            if policy.requested_mode != threading_mode:
                raise ValueError("forked operation context threading mode mismatch")
            if resources.memory_limit_bytes != memory_limit_bytes:
                raise ValueError("forked operation context memory limit mismatch")
            if not _resources_already_retained:
                resources.retain()
        try:
            timestamps = capture_operation_timestamps()
        except BaseException:
            resources.release()
            raise
        self.policy = policy
        self.threading_mode = policy.requested_mode
        self.memory_limit_bytes = memory_limit_bytes
        self.ingestion_timestamp_micros = timestamps.ingestion_timestamp_micros
        self.detected_at = timestamps.detected_at
        self._resources = resources
        self._lock = Lock()
        self._closed = False

    @property
    def is_single(self) -> bool:
        """Return whether project-owned work must remain inline."""
        return self.policy.is_single

    @property
    def temporary_storage(self) -> TemporaryStoragePermitPool:
        """Return the shared permit pool, including post-close diagnostics."""
        return self._resources.temporary_storage

    @property
    def remote_coordinator(self) -> RemoteIoCoordinator | None:
        """Return the shared multi-mode remote coordinator, creating it lazily."""
        self._ensure_open()
        return self._resources.remote_coordinator()

    def fork(self) -> OperationExecutionContext:
        """Create a timestamp-distinct child sharing permits and remote resources."""
        self._ensure_open()
        self._resources.retain()
        try:
            return OperationExecutionContext(
                threading_mode=self.threading_mode,
                memory_limit_bytes=self.memory_limit_bytes,
                _resources=self._resources,
                _resources_already_retained=True,
            )
        except BaseException:
            self._resources.release()
            raise

    def submit_remote(self, operation: Callable[[], Awaitable[T]]) -> Future[T]:
        """Submit one multi-mode coroutine through the operation-owned backend."""
        if self.is_single:
            raise RuntimeError(
                "strict single-mode remote work must use run_remote_sync() and a "
                "blocking provider operation"
            )
        coordinator = self.remote_coordinator
        if coordinator is None:
            raise RuntimeError("multi remote operation coordinator was not created")
        return coordinator.submit(lambda _context: operation())

    def run_remote(self, operation: Callable[[], Awaitable[T]]) -> T:
        """Run one multi-mode remote coroutine and return its result."""
        return self.submit_remote(operation).result()

    def run_remote_sync(self, operation: Callable[[], T]) -> T:
        """Run one strict single-mode provider operation on the caller thread."""
        if not self.is_single:
            raise RuntimeError("blocking remote backend is reserved for threading_mode='single'")
        self._ensure_open()
        return operation()

    def close(self) -> None:
        """Release this context and close shared resources after the final owner."""
        with self._lock:
            if self._closed:
                return
            self._closed = True
        self._resources.release()

    def _ensure_open(self) -> None:
        """Reject work through an already-closed context."""
        with self._lock:
            if self._closed:
                raise RuntimeError("operation execution context is closed")
        self._resources.ensure_open()

    def __enter__(self) -> OperationExecutionContext:
        """Return this operation context."""
        return self

    def __exit__(self, *_exc: object) -> None:
        """Close this context reference."""
        self.close()


__all__ = [
    "OperationExecutionContext",
    "OperationTimestamps",
    "capture_operation_timestamps",
]
