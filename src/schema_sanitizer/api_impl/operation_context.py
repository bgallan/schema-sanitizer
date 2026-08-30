"""Whole-operation concurrency policy and lazy remote-I/O ownership.

It derives worker, memory, and admission policy; lazily owns remote coordinators and
sessions; and drains finalizers into resource diagnostics.
"""

from __future__ import annotations

import os
from collections.abc import Awaitable, Callable
from concurrent.futures import Future
from concurrent.futures import TimeoutError as FutureTimeoutError
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from threading import Condition, Lock
from time import monotonic, time_ns
from typing import Any, Protocol, TypeVar, cast

from schema_sanitizer.core_impl.safe_errors import add_bounded_note

from ..core_impl.cancellation import (
    activate_operation_cancellation_token,
    bounded_wait_timeout,
    check_operation_cancelled,
    current_operation_cancellation_token,
)
from ..core_impl.execution_policy import ExecutionPolicy, execution_policy
from ..core_impl.finalization import runtime_is_finalizing
from ..core_impl.finalizer_cleanup import drain_finalizer_cleanup
from ..core_impl.finalizer_escrow import ReservedFinalizerEscrow
from ..core_impl.memory_budget import (
    OperationMemoryLedger,
    activate_operation_memory_ledger,
    memory_budget,
)
from ..core_impl.operation_diagnostics import complete_operation, register_operation
from ..core_impl.process_resources import acquire_file_descriptors, acquire_project_threads
from ..core_impl.resource_lifecycle import _cleanup_with_note
from ..core_impl.rooted_finalizer import RootedFinalizerAuthority
from ..core_impl.temporary_storage import TemporaryStoragePermitPool
from ..input_impl.directory_inputs import (
    DirectoryMetadataBudget,
    activate_directory_metadata_budget,
)
from ..remote_impl.io_coordinator import RemoteIoCoordinator
from ..remote_impl.io_footprint import (
    ActiveRemoteIoFootprint,
    RemoteIoFootprint,
    activate_remote_io_footprint,
)
from ..remote_impl.io_permits import shared_remote_io_permit_governor
from ..remote_impl.provider_session_pool import RemoteProviderSessionPool
from .operation_resource_diagnostics import (
    build_operation_resource_diagnostic_snapshot,
)

T = TypeVar("T")

_UNIX_EPOCH = datetime(1970, 1, 1, tzinfo=UTC)
_OPERATION_ID_LOCK = Lock()
_OPERATION_ID_SEQUENCE = 0
_MAX_OPERATION_ID = (1 << 63) - 1


def _next_operation_id() -> int:
    """Return next operation id retained by this operation."""
    global _OPERATION_ID_SEQUENCE
    with _OPERATION_ID_LOCK:
        if _OPERATION_ID_SEQUENCE >= _MAX_OPERATION_ID:
            raise RuntimeError("operation id generation exhausted")
        _OPERATION_ID_SEQUENCE += 1
        return _OPERATION_ID_SEQUENCE


_MAX_OPERATION_FINALIZER_OWNERS = 8192
_RESOURCE_FINALIZER_ESCROW: ReservedFinalizerEscrow[object] = ReservedFinalizerEscrow(
    _MAX_OPERATION_FINALIZER_OWNERS, static_kind="operation_resource"
)
_CONTEXT_FINALIZER_ESCROW: ReservedFinalizerEscrow[object] = ReservedFinalizerEscrow(
    _MAX_OPERATION_FINALIZER_OWNERS, static_kind="operation_context"
)
_OPERATION_FINALIZER_OVERFLOWS = 0
_OPERATION_FINALIZER_OVERFLOWED = False


class _Releasable(Protocol):
    def release(self) -> object:
        """Release the exact owned runtime capability."""


def _run_operation_resource_finalizer(authority: RootedFinalizerAuthority) -> None:
    """Close each rooted operation resource and clear its finalizer authority slot."""
    coordinator = cast(RemoteIoCoordinator | None, authority.arg0)
    if coordinator is not None:
        coordinator.close()
        authority.arg0 = None
    directory_metadata = cast(DirectoryMetadataBudget | None, authority.arg1)
    if directory_metadata is not None:
        directory_metadata.close()
        authority.arg1 = None
    temporary_storage = cast(TemporaryStoragePermitPool | None, authority.arg2)
    if temporary_storage is not None:
        temporary_storage.close()
        authority.arg2 = None
    memory_ledger = cast(OperationMemoryLedger | None, authority.arg3)
    if memory_ledger is not None:
        memory_ledger.close()
        authority.arg3 = None
    thread_lease = cast(_Releasable | None, authority.arg4)
    if thread_lease is not None:
        thread_lease.release()
        authority.arg4 = None
    operation_id = authority.arg5
    if operation_id is not None:
        try:
            complete_operation(
                str(operation_id), {"operation_id": str(operation_id), "state": "closed"}
            )
        # Finalizer diagnostics cannot block cleanup.
        except Exception as ignored_error:
            del ignored_error
        authority.arg5 = None


def _run_operation_context_finalizer(authority: RootedFinalizerAuthority) -> None:
    """Run operation context finalizer."""
    resources = cast(_Releasable | None, authority.arg0)
    if resources is not None:
        resources.release()
        authority.arg0 = None


def _publish_operation_finalizer(
    escrow: ReservedFinalizerEscrow[object], ticket: int | None, owner: object
) -> None:
    """Publish operation-context cleanup to its reserved finalizer slot."""
    global _OPERATION_FINALIZER_OVERFLOWS, _OPERATION_FINALIZER_OVERFLOWED
    if ticket is None:
        return
    publish = (
        escrow.publish_rooted
        if isinstance(owner, RootedFinalizerAuthority)
        else escrow.publish_reserved
    )
    if not publish(ticket, owner):
        _OPERATION_FINALIZER_OVERFLOWED = True
        try:
            _OPERATION_FINALIZER_OVERFLOWS += 1
        except MemoryError:
            pass


def drain_operation_finalizers() -> int:
    """Run abandoned operation cleanup one rooted slot at a time."""
    drained = 0
    for escrow, method in (
        (_CONTEXT_FINALIZER_ESCROW, "close"),
        (_RESOURCE_FINALIZER_ESCROW, "release"),
    ):

        def process(ticket: int, owner: object, *, _method: str = method) -> None:
            """Process one retained work item."""
            nonlocal drained
            if isinstance(owner, RootedFinalizerAuthority):
                owner.ticket = ticket
                owner.run()
                owner.clear()
                drained += 1
                return
            try:
                owner._finalizer_ticket = ticket  # type: ignore[attr-defined]
            except BaseException:
                pass
            getattr(owner, _method)()
            try:
                owner._finalizer_ticket = None  # type: ignore[attr-defined]
            except BaseException:
                pass
            drained += 1

        attempts = escrow.active_count()
        for _ in range(attempts):
            try:
                if not escrow.process_one(process):
                    break
            except BaseException:
                continue
    return drained


def operation_finalizer_snapshot() -> tuple[int, int, int]:
    """Return reserved slots, published owners, and irreversible overflows."""
    return (
        _RESOURCE_FINALIZER_ESCROW.reserved_count() + _CONTEXT_FINALIZER_ESCROW.reserved_count(),
        _RESOURCE_FINALIZER_ESCROW.published_count() + _CONTEXT_FINALIZER_ESCROW.published_count(),
        max(1, _OPERATION_FINALIZER_OVERFLOWS)
        if (
            _OPERATION_FINALIZER_OVERFLOWED
            or _RESOURCE_FINALIZER_ESCROW.overflowed
            or _CONTEXT_FINALIZER_ESCROW.overflowed
        )
        else _OPERATION_FINALIZER_OVERFLOWS,
    )


def _reset_operation_finalizers_after_fork() -> None:
    """Rebind finalizer escrows without touching inherited owners/locks."""
    global _OPERATION_FINALIZER_OVERFLOWS, _OPERATION_FINALIZER_OVERFLOWED
    _RESOURCE_FINALIZER_ESCROW.reset_after_fork()
    _CONTEXT_FINALIZER_ESCROW.reset_after_fork()
    _OPERATION_FINALIZER_OVERFLOWS = 0
    _OPERATION_FINALIZER_OVERFLOWED = False


from ..core_impl.fork_manager import register_fork_handler as _register_fork_handler  # noqa: E402

_register_fork_handler("operation-context", mode="quarantine_only")


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
        drain_finalizer_cleanup()
        drain_operation_finalizers()
        self._finalizer_owner = RootedFinalizerAuthority(_run_operation_resource_finalizer)
        self._finalizer_ticket = None
        try:
            ticket = _RESOURCE_FINALIZER_ESCROW.reserve_rooted(self._finalizer_owner)
            if ticket is None:
                raise RuntimeError("operation-resource finalizer escrow exhausted")
            self._finalizer_ticket = ticket
        except BaseException:
            try:
                _RESOURCE_FINALIZER_ESCROW.release_rooted_owner(self._finalizer_owner)
            except BaseException:
                pass
            raise
        self.policy = policy
        self.pid = os.getpid()
        self.operation_id = f"{self.pid}:{_next_operation_id()}"
        self.cancellation_token = current_operation_cancellation_token()
        self.thread_lease = thread_lease
        self._finalizer_owner.arg4 = thread_lease
        self._finalizer_owner.arg5 = self.operation_id
        self.memory_limit_bytes = memory_limit_bytes
        try:
            self.remote_timeout_seconds = memory_budget(memory_limit_bytes).async_timeout_seconds
        except BaseException:
            self._finalizer_owner.arg4 = None
            self._finalizer_owner.arg5 = None
            self._finalizer_owner.make_ack_only()
            ticket = self._finalizer_ticket
            if ticket is not None and _RESOURCE_FINALIZER_ESCROW.release_rooted_ticket(
                ticket, self._finalizer_owner
            ):
                self._finalizer_ticket = None
                self._finalizer_owner.clear()
            elif ticket is not None:
                _RESOURCE_FINALIZER_ESCROW.publish_rooted(ticket, self._finalizer_owner)
            raise
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
            self._finalizer_owner.arg3 = memory_ledger
            temporary_storage = TemporaryStoragePermitPool(memory_limit_bytes)
            self._finalizer_owner.arg2 = temporary_storage
            directory_metadata = DirectoryMetadataBudget(
                memory_limit_bytes,
                operation_memory_ledger=memory_ledger,
            )
            self._finalizer_owner.arg1 = directory_metadata
            self.memory_ledger = memory_ledger
            self.temporary_storage = temporary_storage
            self.directory_metadata = directory_metadata
            register_operation(self.operation_id, self.diagnostic_snapshot)
        except BaseException as exc:
            for label, resource, owner_arg in (
                ("directory metadata", directory_metadata, "arg1"),
                ("temporary storage", temporary_storage, "arg2"),
                ("operation memory", memory_ledger, "arg3"),
            ):
                if resource is None:
                    continue
                try:
                    resource.close()
                    setattr(self._finalizer_owner, owner_arg, None)
                except BaseException as cleanup_error:
                    add_bounded_note(
                        exc,
                        f"{label} rollback also failed during operation setup",
                        cleanup_error,
                    )
            # The caller retains construction-time thread-lease rollback authority.
            self._finalizer_owner.arg4 = None
            self._finalizer_owner.arg5 = None
            ticket = getattr(self, "_finalizer_ticket", None)
            live = any(
                getattr(self._finalizer_owner, name) is not None
                for name in ("arg0", "arg1", "arg2", "arg3")
            )
            if live and ticket is not None:
                _RESOURCE_FINALIZER_ESCROW.publish_rooted(ticket, self._finalizer_owner)
            elif ticket is not None:
                self._finalizer_owner.make_ack_only()
                if _RESOURCE_FINALIZER_ESCROW.release_rooted_ticket(ticket, self._finalizer_owner):
                    self._finalizer_ticket = None
                    self._finalizer_owner.clear()
                else:
                    _RESOURCE_FINALIZER_ESCROW.publish_rooted(ticket, self._finalizer_owner)
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
        deadline = monotonic() + max(0.001, float(getattr(self, "remote_timeout_seconds", 5.0)))
        with self._close_condition:
            while self._remote_coordinator_building:
                remaining = deadline - monotonic()
                if remaining <= 0 or not self._close_condition.wait(timeout=remaining):
                    raise RuntimeError(
                        "remote coordinator construction exceeded operation deadline"
                    )
            if self._close_started:
                raise RuntimeError("operation execution resources are closing")
            existing = self._remote_coordinator
            if existing is not None:
                return existing
            self._remote_coordinator_building = True

        try:
            configured_memory_limit = getattr(self, "memory_limit_bytes", None)
            stage_bytes_per_permit = (
                max(1, memory_budget(configured_memory_limit).io_chunk_bytes)
                if hasattr(self, "memory_limit_bytes")
                else 64 * 1024
            )
            coordinator = RemoteIoCoordinator(
                lambda: RemoteProviderSessionPool(
                    default_descriptor_weight=max(1, self.policy.async_concurrency)
                ),
                permit_capacity=self.policy.async_concurrency,
                operation_id=self.operation_id,
                thread_slot_reserved=True,
                operation_memory_ledger=getattr(self, "memory_ledger", None),
                stage_bytes_per_permit=stage_bytes_per_permit,
            )
            # Root the freshly constructed coordinator before any later
            # close-race validation can reject it.  A failed reject cleanup then
            # remains recoverable from the resource finalizer authority.
            self._finalizer_owner.arg0 = coordinator
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
                self._finalizer_owner.arg0 = coordinator
            self._remote_coordinator_building = False
            self._close_condition.notify_all()
        if reject:
            coordinator.close()
            self._finalizer_owner.arg0 = None
            raise RuntimeError("operation execution resources closed during remote startup")
        return coordinator

    def release(self) -> None:
        """Release one reference and retry final cleanup after transient failures."""
        if os.getpid() != self.pid:
            return
        deadline = monotonic() + max(0.001, float(getattr(self, "remote_timeout_seconds", 5.0)))
        with self._close_condition:
            if not self._close_started:
                if self._references <= 0:
                    return
                self._references -= 1
                if self._references != 0:
                    return
                self._close_started = True
            while self._closing:
                remaining = deadline - monotonic()
                if remaining <= 0 or not self._close_condition.wait(timeout=remaining):
                    raise RuntimeError("concurrent operation-resource close exceeded its deadline")
            while self._remote_coordinator_building:
                remaining = deadline - monotonic()
                if remaining <= 0 or not self._close_condition.wait(timeout=remaining):
                    raise RuntimeError(
                        "remote coordinator construction exceeded operation close deadline"
                    )
            if self._closed:
                ticket = getattr(self, "_finalizer_ticket", None)
                if ticket is not None:
                    self._finalizer_owner.make_ack_only()
                    if _RESOURCE_FINALIZER_ESCROW.release_rooted_ticket(
                        ticket, self._finalizer_owner
                    ):
                        self._finalizer_ticket = None
                        self._finalizer_owner.clear()
                    else:
                        _RESOURCE_FINALIZER_ESCROW.publish_rooted(ticket, self._finalizer_owner)
                return
            self._closing = True

        try:
            coordinator = self._remote_coordinator
            if coordinator is not None:
                coordinator.close()
                with self._close_condition:
                    if self._remote_coordinator is coordinator:
                        self._remote_coordinator = None
                self._finalizer_owner.arg0 = None
            self.directory_metadata.close()
            self._finalizer_owner.arg1 = None
            self.temporary_storage.close()
            self._finalizer_owner.arg2 = None
            self.memory_ledger.close()
            self._finalizer_owner.arg3 = None
            thread_lease = self.thread_lease
            if thread_lease is not None:
                thread_lease.release()
                self.thread_lease = None
                self._finalizer_owner.arg4 = None
        except BaseException:
            with self._close_condition:
                self._closing = False
                self._close_condition.notify_all()
            raise

        with self._close_condition:
            self._closed = True
            self._closing = False
            self._close_condition.notify_all()
        ticket = getattr(self, "_finalizer_ticket", None)
        if ticket is not None:
            self._finalizer_owner.arg5 = None
            self._finalizer_owner.make_ack_only()
            if _RESOURCE_FINALIZER_ESCROW.release_rooted_ticket(ticket, self._finalizer_owner):
                self._finalizer_ticket = None
                self._finalizer_owner.clear()
            else:
                _RESOURCE_FINALIZER_ESCROW.publish_rooted(ticket, self._finalizer_owner)
                raise RuntimeError("operation-resource finalizer slot retirement did not commit")

        # Diagnostics are advisory after ownership has been committed closed.
        try:
            complete_operation(self.operation_id, self.diagnostic_snapshot())
        except Exception:
            try:
                complete_operation(
                    self.operation_id,
                    {"operation_id": self.operation_id, "pid": self.pid, "state": "closed"},
                )
            # Finalizer diagnostics are best effort.
            except Exception as ignored_error:
                del ignored_error

    def __del__(self) -> None:
        """Arm the pre-rooted resource authority without blocking."""
        try:
            if runtime_is_finalizing() or os.getpid() != getattr(self, "pid", os.getpid()):
                return
            ticket = getattr(self, "_finalizer_ticket", None)
            owner = getattr(self, "_finalizer_owner", None)
            if ticket is None and isinstance(owner, RootedFinalizerAuthority):
                ticket = owner.ticket or None
            if ticket is None or not isinstance(owner, RootedFinalizerAuthority):
                return
            if getattr(self, "_closed", True):
                owner.make_ack_only()
            if _RESOURCE_FINALIZER_ESCROW.publish_rooted(ticket, owner):
                self._finalizer_ticket = None
            else:
                _publish_operation_finalizer(_RESOURCE_FINALIZER_ESCROW, ticket, owner)
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
        external_runtime_workers: int = 0,
        exact_external_runtime_workers: int = 0,
        _resources: _OperationExecutionResources | None = None,
        _resources_already_retained: bool = False,
    ) -> None:
        """Capture one timestamp and attach to an execution resource domain."""
        drain_finalizer_cleanup()
        drain_operation_finalizers()
        self._finalizer_owner = RootedFinalizerAuthority(_run_operation_context_finalizer)
        self._finalizer_ticket = None
        try:
            ticket = _CONTEXT_FINALIZER_ESCROW.reserve_rooted(self._finalizer_owner)
            if ticket is None:
                raise RuntimeError("operation-context finalizer escrow exhausted")
            self._finalizer_ticket = ticket
        except BaseException:
            try:
                _CONTEXT_FINALIZER_ESCROW.release_rooted_owner(self._finalizer_owner)
            except BaseException:
                pass
            raise
        try:
            check_operation_cancelled(stage="operation_start")
        except BaseException:
            self._finalizer_owner.make_ack_only()
            ticket = self._finalizer_ticket
            if ticket is not None and _CONTEXT_FINALIZER_ESCROW.release_rooted_ticket(
                ticket, self._finalizer_owner
            ):
                self._finalizer_ticket = None
                self._finalizer_owner.clear()
            elif ticket is not None:
                _CONTEXT_FINALIZER_ESCROW.publish_rooted(ticket, self._finalizer_owner)
            raise
        if _resources is None:
            policy = execution_policy(threading_mode, memory_limit_bytes)
            thread_lease = None
            if not policy.is_single:
                # An exact-width external pool must subdivide the operation's
                # lease rather than reacquire global logical capacity later.
                # Reserve that child width up front even when a small memory
                # limit has degraded the native pipeline itself to one worker.
                external_envelope = max(
                    0,
                    int(external_runtime_workers),
                    int(exact_external_runtime_workers),
                )
                desired_threads = max(
                    2,
                    policy.effective_workers + external_envelope + 1,
                )
                thread_lease = acquire_project_threads(desired_threads, minimum=2)
                thread_lease.reserve_exact_external_runtime_threads(
                    max(0, int(exact_external_runtime_workers))
                )
                # Keep the external handoff's envelope outside the source/native
                # worker plan. Child runtime borrows still subdivide the same
                # parent authority; this subtraction prevents the producer from
                # consuming credits needed by a concurrent analytical adapter.
                available_workers = max(
                    1,
                    thread_lease.amount - external_envelope - 1,
                )
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
                ticket = self._finalizer_ticket
                if ticket is not None and _CONTEXT_FINALIZER_ESCROW.release_rooted_ticket(
                    ticket, self._finalizer_owner
                ):
                    self._finalizer_ticket = None
                raise
        else:
            resources = _resources
            policy = resources.policy
            try:
                if policy.requested_mode != threading_mode:
                    raise ValueError("forked operation context threading mode mismatch")
                if resources.memory_limit_bytes != memory_limit_bytes:
                    raise ValueError("forked operation context memory limit mismatch")
                if not _resources_already_retained:
                    resources.retain()
            except BaseException:
                ticket = self._finalizer_ticket
                if ticket is not None and _CONTEXT_FINALIZER_ESCROW.release_rooted_ticket(
                    ticket, self._finalizer_owner
                ):
                    self._finalizer_ticket = None
                raise
        self._finalizer_owner.arg0 = resources
        try:
            timestamps = capture_operation_timestamps()
        except BaseException as exc:
            _cleanup_with_note(
                exc,
                resources,
                label="operation resource rollback also failed",
                method="release",
            )
            ticket = self._finalizer_ticket
            if ticket is not None:
                _CONTEXT_FINALIZER_ESCROW.publish_rooted(ticket, self._finalizer_owner)
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
    def execution_lease(self) -> object | None:
        """Return the operation-owned physical-thread capability for stage composition."""
        return self._resources.thread_lease

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
        footprint: RemoteIoFootprint | None = None,
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
            footprint=footprint,
        )

    def run_remote(
        self,
        operation: Callable[[], Awaitable[T]],
        *,
        permit_weight: int = 1,
        permit_label: str = "remote_operation",
        footprint: RemoteIoFootprint | None = None,
    ) -> T:
        """Run weighted multi-mode remote work within the operation deadline."""
        future = self.submit_remote(
            operation,
            permit_weight=permit_weight,
            permit_label=permit_label,
            footprint=footprint,
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
        network_fds: int = 0,
        local_file_fds: int = 0,
    ) -> T:
        """Run a transfer under independent logical and descriptor weights."""
        budget = memory_budget(self.memory_limit_bytes)
        chunk_bytes = max(1, budget.io_chunk_bytes)
        weight = max(1, (max(0, estimated_bytes) + chunk_bytes - 1) // chunk_bytes)
        remote_weight = min(self.policy.async_concurrency, weight)
        footprint = RemoteIoFootprint(
            remote_weight=remote_weight,
            network_fds=network_fds,
            local_file_fds=local_file_fds,
        )
        return self.run_remote(
            operation,
            permit_label=permit_label,
            footprint=footprint,
        )

    def run_remote_sync(
        self,
        operation: Callable[[], T],
        *,
        permit_label: str = "remote_sync",
        footprint: RemoteIoFootprint | None = None,
    ) -> T:
        """Run strict single mode under the same process-wide remote authority."""
        if not self.is_single:
            raise RuntimeError("blocking remote backend is reserved for threading_mode='single'")
        self._ensure_open()
        footprint = footprint or RemoteIoFootprint(network_fds=0)
        # Synchronous SDK owners reserve their network descriptor in a terminal
        # cleanup escrow before construction. The operation footprint therefore
        # carries only local-file descriptors; charging the socket here would
        # double-count it and could not transfer that credit after close failure.
        if footprint.network_fds:
            footprint = RemoteIoFootprint(
                remote_weight=footprint.remote_weight,
                network_fds=0,
                local_file_fds=footprint.local_file_fds,
            )
        descriptor_lease = None
        permit = None
        primary_error: BaseException | None = None
        with activate_operation_cancellation_token(self._resources.cancellation_token):
            check_operation_cancelled(stage="remote_sync")
            try:
                if footprint.total_file_descriptors:
                    descriptor_lease = acquire_file_descriptors(footprint.total_file_descriptors)
                try:
                    permit = shared_remote_io_permit_governor().acquire_sync(
                        footprint.remote_weight,
                        label=permit_label,
                        operation_id=self._resources.operation_id,
                    )
                except BaseException as permit_error:
                    if descriptor_lease is not None:
                        try:
                            descriptor_lease.release()
                            descriptor_lease = None
                        except BaseException as cleanup_error:
                            add_bounded_note(
                                permit_error,
                                "remote sync descriptor admission rollback also failed",
                                cleanup_error,
                            )
                    raise
                footprint_owner = ActiveRemoteIoFootprint(footprint)
                with activate_operation_memory_ledger(self.memory_ledger):
                    with activate_directory_metadata_budget(self.directory_metadata):
                        with activate_remote_io_footprint(footprint_owner):
                            result = operation()
                check_operation_cancelled(stage="remote_sync")
                return result
            except BaseException as exc:
                primary_error = exc
                raise
            finally:
                cleanup_error_first: BaseException | None = None
                for label, owner in (
                    ("remote permit", permit),
                    ("descriptor footprint", descriptor_lease),
                ):
                    if owner is None:
                        continue
                    try:
                        owner.release()
                    except BaseException as cleanup_error:
                        if primary_error is not None:
                            add_bounded_note(
                                primary_error, f"{label} cleanup also failed", cleanup_error
                            )
                        elif cleanup_error_first is None:
                            cleanup_error_first = cleanup_error
                        else:
                            add_bounded_note(
                                cleanup_error_first, f"{label} cleanup also failed", cleanup_error
                            )
                if primary_error is None and cleanup_error_first is not None:
                    raise cleanup_error_first

    def close(self) -> None:
        """Release this context, retaining a retry path after cleanup faults."""
        if os.getpid() != self._pid:
            return
        deadline = monotonic() + max(
            0.001, float(getattr(self._resources, "remote_timeout_seconds", 5.0))
        )
        with self._close_condition:
            while self._closing:
                remaining = deadline - monotonic()
                if remaining <= 0 or not self._close_condition.wait(timeout=remaining):
                    raise RuntimeError("concurrent operation close exceeded its deadline")
            if self._closed:
                ticket = getattr(self, "_finalizer_ticket", None)
                if ticket is not None:
                    self._finalizer_owner.make_ack_only()
                    if _CONTEXT_FINALIZER_ESCROW.release_rooted_ticket(
                        ticket, self._finalizer_owner
                    ):
                        self._finalizer_ticket = None
                        self._finalizer_owner.clear()
                    else:
                        _CONTEXT_FINALIZER_ESCROW.publish_rooted(ticket, self._finalizer_owner)
                return
            self._close_started = True
            self._closing = True
        try:
            self._resources.release()
            self._finalizer_owner.arg0 = None
        except BaseException:
            with self._close_condition:
                self._closing = False
                self._close_condition.notify_all()
            raise
        with self._close_condition:
            self._closed = True
            self._closing = False
            self._close_condition.notify_all()
        ticket = getattr(self, "_finalizer_ticket", None)
        if ticket is not None:
            self._finalizer_owner.make_ack_only()
            if _CONTEXT_FINALIZER_ESCROW.release_rooted_ticket(ticket, self._finalizer_owner):
                self._finalizer_ticket = None
                self._finalizer_owner.clear()
            else:
                _CONTEXT_FINALIZER_ESCROW.publish_rooted(ticket, self._finalizer_owner)
                raise RuntimeError("operation-context finalizer slot retirement did not commit")

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
        """Arm the pre-rooted context authority without blocking."""
        try:
            if runtime_is_finalizing() or os.getpid() != getattr(self, "_pid", os.getpid()):
                return
            ticket = getattr(self, "_finalizer_ticket", None)
            owner = getattr(self, "_finalizer_owner", None)
            if ticket is None and isinstance(owner, RootedFinalizerAuthority):
                ticket = owner.ticket or None
            if ticket is None or not isinstance(owner, RootedFinalizerAuthority):
                return
            if getattr(self, "_closed", True):
                owner.make_ack_only()
            if _CONTEXT_FINALIZER_ESCROW.publish_rooted(ticket, owner):
                self._finalizer_ticket = None
            else:
                _publish_operation_finalizer(_CONTEXT_FINALIZER_ESCROW, ticket, owner)
        except BaseException:
            pass


from ..core_impl.finalizer_registry import (  # noqa: E402
    register_finalizer_domain as _register_finalizer_domain,
)

_register_finalizer_domain(
    "operation_context",
    drain=drain_operation_finalizers,
    snapshot=operation_finalizer_snapshot,
    escrows=(
        ("operation_resource", _RESOURCE_FINALIZER_ESCROW),
        ("operation_context", _CONTEXT_FINALIZER_ESCROW),
    ),
)


__all__ = [
    "OperationExecutionContext",
    "OperationTimestamps",
    "capture_operation_timestamps",
    "drain_operation_finalizers",
    "operation_finalizer_snapshot",
]
