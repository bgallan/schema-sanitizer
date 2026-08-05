"""Whole-operation concurrency policy and lazy remote-I/O ownership."""

from __future__ import annotations

import os
from collections.abc import Awaitable, Callable
from concurrent.futures import Future
from concurrent.futures import TimeoutError as FutureTimeoutError
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from itertools import count
from threading import Condition, Lock
from time import monotonic, time_ns
from typing import Any, TypeVar, cast

from schema_sanitizer.core_impl.safe_errors import add_bounded_note

from ..core_impl.cancellation import (
    activate_operation_cancellation_token,
    bounded_wait_timeout,
    check_operation_cancelled,
    current_operation_cancellation_token,
)
from ..core_impl.execution_policy import ExecutionPolicy, execution_policy
from ..core_impl.finalization import runtime_is_finalizing
from ..core_impl.memory_budget import (
    OperationMemoryLedger,
    activate_operation_memory_ledger,
    memory_budget,
)
from ..core_impl.operation_diagnostics import complete_operation, register_operation
from ..core_impl.process_resources import acquire_project_threads
from ..core_impl.resource_lifecycle import _cleanup_with_note
from ..core_impl.temporary_storage import TemporaryStoragePermitPool
from ..input_impl.directory_inputs import (
    DirectoryMetadataBudget,
    activate_directory_metadata_budget,
)
from ..remote_impl.io_coordinator import RemoteIoCoordinator
from ..remote_impl.provider_session_pool import RemoteProviderSessionPool
from .operation_resource_diagnostics import (
    build_operation_resource_diagnostic_snapshot,
)

T = TypeVar("T")

_UNIX_EPOCH = datetime(1970, 1, 1, tzinfo=UTC)
_OPERATION_IDS = count(1)


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
        *,
        thread_lease: Any | None = None,
    ) -> None:
        """Create one resource domain for related operation contexts."""
        self.policy = policy
        self.pid = os.getpid()
        self.operation_id = f"{self.pid}:{next(_OPERATION_IDS)}"
        self.cancellation_token = current_operation_cancellation_token()
        self.thread_lease = thread_lease
        self.memory_limit_bytes = memory_limit_bytes
        self.remote_timeout_seconds = memory_budget(memory_limit_bytes).async_timeout_seconds
        self._lock = Lock()
        self._close_condition = Condition(self._lock)
        self._remote_coordinator: RemoteIoCoordinator | None = None
        self._remote_coordinator_building = False
        self._references = 1
        self._close_started: bool = False
        self._closing = False
        self._closed = False

        memory_ledger: OperationMemoryLedger | None = None
        temporary_storage: TemporaryStoragePermitPool | None = None
        directory_metadata: DirectoryMetadataBudget | None = None
        try:
            memory_ledger = OperationMemoryLedger(memory_limit_bytes)
            temporary_storage = TemporaryStoragePermitPool(memory_limit_bytes)
            directory_metadata = DirectoryMetadataBudget(
                memory_limit_bytes,
                operation_memory_ledger=memory_ledger,
            )
            self.memory_ledger = memory_ledger
            self.temporary_storage = temporary_storage
            self.directory_metadata = directory_metadata
            register_operation(self.operation_id, self.diagnostic_snapshot)
        except BaseException as exc:
            for label, resource in (
                ("directory metadata", directory_metadata),
                ("temporary storage", temporary_storage),
                ("operation memory", memory_ledger),
            ):
                if resource is None:
                    continue
                try:
                    resource.close()
                except BaseException as cleanup_error:
                    add_bounded_note(
                        exc,
                        f"{label} rollback also failed during operation setup",
                        cleanup_error,
                    )
            raise

    def diagnostic_snapshot(self) -> dict[str, object]:
        """Return one cross-resource snapshot for this operation domain."""
        return build_operation_resource_diagnostic_snapshot(self)

    def retain(self) -> None:
        """Retain this domain for one additional operation context."""
        if os.getpid() != self.pid:
            raise RuntimeError("operation resources cannot be retained after fork")
        with self._lock:
            if self._close_started:
                raise RuntimeError("operation execution resources are closing")
            self._references += 1

    def ensure_open(self) -> None:
        """Reject closed, cancelled, or post-fork inherited resources."""
        if os.getpid() != self.pid:
            raise RuntimeError(
                "operation resources cannot be reused after fork; "
                "create a new operation in the child"
            )
        with activate_operation_cancellation_token(self.cancellation_token):
            check_operation_cancelled(stage="operation")
        with self._lock:
            if self._close_started:
                raise RuntimeError("operation execution resources are closing")

    def remote_coordinator(self) -> RemoteIoCoordinator | None:
        """Return the lazy multi-mode coordinator shared by every child context."""
        if os.getpid() != self.pid:
            raise RuntimeError("operation resources cannot be reused after fork")
        if self.policy.is_single:
            return None
        with self._close_condition:
            while self._remote_coordinator_building:
                self._close_condition.wait()
            if self._close_started:
                raise RuntimeError("operation execution resources are closing")
            existing = self._remote_coordinator
            if existing is not None:
                return existing
            self._remote_coordinator_building = True

        try:
            coordinator = RemoteIoCoordinator(
                RemoteProviderSessionPool,
                permit_capacity=self.policy.async_concurrency,
                operation_id=self.operation_id,
                thread_slot_reserved=True,
            )
        except BaseException:
            with self._close_condition:
                self._remote_coordinator_building = False
                self._close_condition.notify_all()
            raise

        reject = False
        with self._close_condition:
            if bool(getattr(cast(Any, self), "_close_started")):
                reject = True
            else:
                self._remote_coordinator = coordinator
            self._remote_coordinator_building = False
            self._close_condition.notify_all()
        if reject:
            coordinator.close()
            raise RuntimeError("operation execution resources closed during remote startup")
        return coordinator

    def release(self) -> None:
        """Release one reference and retry final cleanup after transient failures."""
        if os.getpid() != self.pid:
            return
        with self._close_condition:
            if not self._close_started:
                if self._references <= 0:
                    return
                self._references -= 1
                if self._references != 0:
                    return
                self._close_started = True
            while self._closing:
                self._close_condition.wait()
            deadline = monotonic() + max(0.001, self.remote_timeout_seconds)
            while self._remote_coordinator_building:
                remaining = deadline - monotonic()
                if remaining <= 0 or not self._close_condition.wait(timeout=remaining):
                    raise RuntimeError(
                        "remote coordinator construction exceeded operation close deadline"
                    )
            if self._closed:
                return
            self._closing = True

        try:
            coordinator = self._remote_coordinator
            if coordinator is not None:
                coordinator.close()
                with self._close_condition:
                    if self._remote_coordinator is coordinator:
                        self._remote_coordinator = None
            self.directory_metadata.close()
            self.temporary_storage.close()
            self.memory_ledger.close()
            thread_lease = self.thread_lease
            if thread_lease is not None:
                thread_lease.release()
                self.thread_lease = None
        except BaseException:
            with self._close_condition:
                self._closing = False
                self._close_condition.notify_all()
            raise

        with self._close_condition:
            self._closed = True
            self._closing = False
            self._close_condition.notify_all()

        # Diagnostics are advisory after ownership has been committed closed.
        try:
            complete_operation(self.operation_id, self.diagnostic_snapshot())
        except Exception:
            try:
                complete_operation(
                    self.operation_id,
                    {"operation_id": self.operation_id, "pid": self.pid, "state": "closed"},
                )
            except Exception:
                pass

    def __del__(self) -> None:
        """Retry abandoned ownership unless interpreter teardown has begun."""
        try:
            if runtime_is_finalizing():
                return
            self.release()
        except BaseException:
            pass


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
        check_operation_cancelled(stage="operation_start")
        if _resources is None:
            policy = execution_policy(threading_mode, memory_limit_bytes)
            thread_lease = None
            if not policy.is_single:
                desired_threads = max(2, policy.effective_workers + 1)
                thread_lease = acquire_project_threads(desired_threads, minimum=2)
                available_workers = max(1, thread_lease.amount - 1)
                if available_workers < policy.effective_workers:
                    policy = execution_policy(
                        threading_mode,
                        memory_limit_bytes,
                        available_cpus=available_workers,
                    )
            try:
                resources = _OperationExecutionResources(
                    policy, memory_limit_bytes, thread_lease=thread_lease
                )
            except BaseException as exc:
                if thread_lease is not None:
                    _cleanup_with_note(
                        exc,
                        thread_lease,
                        label="operation thread-lease rollback also failed",
                        method="release",
                    )
                raise
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
        except BaseException as exc:
            _cleanup_with_note(
                exc,
                resources,
                label="operation resource rollback also failed",
                method="release",
            )
            raise
        self.policy = policy
        self.threading_mode = policy.requested_mode
        self.memory_limit_bytes = memory_limit_bytes
        self.ingestion_timestamp_micros = timestamps.ingestion_timestamp_micros
        self.detected_at = timestamps.detected_at
        self._resources = resources
        self._pid = os.getpid()
        self._lock = Lock()
        self._close_condition = Condition(self._lock)
        self._close_started = False
        self._closing = False
        self._closed = False

    @property
    def is_single(self) -> bool:
        """Return whether project-owned work must remain inline."""
        return self.policy.is_single

    @property
    def memory_ledger(self) -> OperationMemoryLedger:
        """Return the shared Python/native resident-memory ledger."""
        return self._resources.memory_ledger

    @property
    def temporary_storage(self) -> TemporaryStoragePermitPool:
        """Return the shared permit pool, including post-close diagnostics."""
        return self._resources.temporary_storage

    @property
    def directory_metadata(self) -> DirectoryMetadataBudget:
        """Return the shared retained-directory-metadata budget."""
        return self._resources.directory_metadata

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
        except BaseException as exc:
            _cleanup_with_note(
                exc,
                self._resources,
                label="forked operation resource rollback also failed",
                method="release",
            )
            raise

    def submit_remote(
        self,
        operation: Callable[[], Awaitable[T]],
        *,
        permit_weight: int = 1,
        permit_label: str = "remote_operation",
    ) -> Future[T]:
        """Submit weighted multi-mode work through the operation-owned backend."""
        if self.is_single:
            raise RuntimeError(
                "strict single-mode remote work must use run_remote_sync() and a "
                "blocking provider operation"
            )
        coordinator = self.remote_coordinator
        if coordinator is None:
            raise RuntimeError("multi remote operation coordinator was not created")

        async def invoke() -> T:
            """Run remote work inside operation budgets and cancellation scope."""
            with activate_operation_cancellation_token(self._resources.cancellation_token):
                check_operation_cancelled(stage="remote_operation")
                with activate_operation_memory_ledger(self.memory_ledger):
                    with activate_directory_metadata_budget(self.directory_metadata):
                        result = await operation()
                check_operation_cancelled(stage="remote_operation")
                return result

        return coordinator.submit(
            lambda _context: invoke(),
            permit_weight=permit_weight,
            permit_label=permit_label,
        )

    def run_remote(
        self,
        operation: Callable[[], Awaitable[T]],
        *,
        permit_weight: int = 1,
        permit_label: str = "remote_operation",
    ) -> T:
        """Run weighted multi-mode remote work within the operation deadline."""
        future = self.submit_remote(
            operation,
            permit_weight=permit_weight,
            permit_label=permit_label,
        )
        timeout = bounded_wait_timeout(self._resources.remote_timeout_seconds)
        try:
            return future.result(timeout=timeout)
        except FutureTimeoutError:
            future.cancel()
            with activate_operation_cancellation_token(self._resources.cancellation_token):
                check_operation_cancelled(stage="remote_operation")
            raise TimeoutError("remote operation exceeded its bounded transport deadline") from None

    def run_remote_transfer(
        self,
        operation: Callable[[], Awaitable[T]],
        *,
        estimated_bytes: int,
        permit_label: str,
    ) -> T:
        """Run a transfer weighted by its estimated number of I/O chunks."""
        budget = memory_budget(self.memory_limit_bytes)
        chunk_bytes = max(1, budget.io_chunk_bytes)
        weight = max(1, (max(0, estimated_bytes) + chunk_bytes - 1) // chunk_bytes)
        return self.run_remote(
            operation,
            permit_weight=min(self.policy.async_concurrency, weight),
            permit_label=permit_label,
        )

    def run_remote_sync(self, operation: Callable[[], T]) -> T:
        """Run one strict single-mode provider operation on the caller thread."""
        if not self.is_single:
            raise RuntimeError("blocking remote backend is reserved for threading_mode='single'")
        self._ensure_open()
        with activate_operation_cancellation_token(self._resources.cancellation_token):
            check_operation_cancelled(stage="remote_sync")
            with activate_operation_memory_ledger(self.memory_ledger):
                with activate_directory_metadata_budget(self.directory_metadata):
                    result = operation()
            check_operation_cancelled(stage="remote_sync")
            return result

    def close(self) -> None:
        """Release this context, retaining a retry path after cleanup faults."""
        if os.getpid() != self._pid:
            return
        with self._close_condition:
            while self._closing:
                self._close_condition.wait()
            if self._closed:
                return
            self._close_started = True
            self._closing = True
        try:
            self._resources.release()
        except BaseException:
            with self._close_condition:
                self._closing = False
                self._close_condition.notify_all()
            raise
        with self._close_condition:
            self._closed = True
            self._closing = False
            self._close_condition.notify_all()

    def _ensure_open(self) -> None:
        """Reject work after close begins, including a retryable failed close."""
        if os.getpid() != self._pid:
            raise RuntimeError("operation execution context cannot be reused after fork")
        with self._lock:
            if self._close_started:
                raise RuntimeError("operation execution context is closing")
        self._resources.ensure_open()

    def __enter__(self) -> OperationExecutionContext:
        """Return this operation context."""
        return self

    def __exit__(self, *_exc: object) -> None:
        """Close this context reference."""
        self.close()

    def __del__(self) -> None:
        """Clean an abandoned context unless interpreter teardown has begun."""
        try:
            if runtime_is_finalizing():
                return
            self.close()
        except BaseException:
            pass


__all__ = [
    "OperationExecutionContext",
    "OperationTimestamps",
    "capture_operation_timestamps",
]
