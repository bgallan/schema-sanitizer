"""One operation-owned event-loop thread for bounded remote I/O.

RemoteIoCoordinator serializes one operation's provider coroutines on an owned loop thread while
integrating permits, sessions, diagnostics, and terminal cleanup.
"""

from __future__ import annotations

import asyncio
import os
import threading
from collections import deque
from collections.abc import Awaitable, Callable
from concurrent.futures import CancelledError, Future
from concurrent.futures import TimeoutError as FutureTimeoutError
from contextlib import asynccontextmanager, suppress
from dataclasses import dataclass, field
from time import monotonic
from typing import Any, AsyncContextManager, TypeVar, cast

from ..core_impl.cancellation import cancellable_async_sleep, check_operation_cancelled
from ..core_impl.cleanup_dispatcher import CleanupSubsystem, dispatch_cleanup
from ..core_impl.durations import normalize_duration
from ..core_impl.governed_thread import (
    defer_governed_thread_retirement,
    reap_governed_thread_retirements,
    start_governed_thread,
)
from ..core_impl.memory_budget import (
    OperationMemoryLedger,
    StageConcurrencyAdmission,
    acquire_stage_concurrency_admission,
)
from ..core_impl.process_resources import (
    acquire_file_descriptors,
    acquire_project_threads,
    process_file_descriptor_snapshot,
)
from ..core_impl.retry_scheduler import (
    adopt_failed_release,
    cancel_retry,
    schedule_retry,
)
from ..core_impl.runtime_registry import RuntimeServiceRegistration, reserve_runtime_service
from ..core_impl.safe_errors import add_bounded_note, clear_exception_traceback
from ..core_impl.terminal_hosts import TerminalHostMarkers
from ..core_impl.terminal_ownership import publish_terminal_owner, retire_terminal_owner
from ..errors import SchemaSanitizerResourceError
from .io_footprint import (
    ActiveRemoteIoFootprint,
    RemoteIoFootprint,
    activate_remote_io_footprint,
)
from .io_permits import (
    RemoteIoCapacityRegistration,
    RemoteIoPermitGovernor,
    RemoteIoSubmissionReservation,
    _bounded_metadata,
    default_remote_io_permit_capacity,
    shared_remote_io_permit_governor,
)
from .io_shutdown import RemoteIoCleanupOwner, shutdown_remote_io
from .provider_session_pool import activate_provider_session_pool

T = TypeVar("T")
_DEFAULT_SHUTDOWN_TIMEOUT_SECONDS = 30.0
_ORPHANED_STARTUPS = TerminalHostMarkers(128, category="remote_startup_terminal")
_ORPHANED_STARTUPS_LOCK = threading.Lock()
_MAX_HOST_RESOURCE_CAPSULES = 256
_HOST_RESOURCE_CAPSULES_LOCK = threading.Lock()
_HOST_RESOURCE_CAPSULES: dict[int, "_RemoteHostResourceCapsule"] = {}
_HOST_RESOURCE_CAPSULE_SEQUENCE = 0
_MAX_TERMINAL_RETRY_COORDINATORS = 1024
_TERMINAL_RETRY_COORDINATORS_LOCK = threading.Lock()
_TERMINAL_RETRY_COORDINATORS: dict[int, "RemoteIoCoordinator"] = {}
_TERMINAL_RETRY_OVERFLOWS = 0
_TERMINAL_RETRY_OVERFLOWED = False


def _retain_terminal_retry_coordinator(coordinator: "RemoteIoCoordinator") -> bool:
    """Keep terminal callback ownership in a bounded subsystem registry."""
    global _TERMINAL_RETRY_OVERFLOWS, _TERMINAL_RETRY_OVERFLOWED
    token = id(coordinator)
    with _TERMINAL_RETRY_COORDINATORS_LOCK:
        current = _TERMINAL_RETRY_COORDINATORS.get(token)
        if current is coordinator:
            return True
        if (
            current is not None
            or len(_TERMINAL_RETRY_COORDINATORS) >= _MAX_TERMINAL_RETRY_COORDINATORS
        ):
            _TERMINAL_RETRY_OVERFLOWED = True
            try:
                _TERMINAL_RETRY_OVERFLOWS += 1
            except MemoryError:
                pass
            publish_terminal_owner("remote_terminal_retry_overflow", token, retained_bytes=256)
            return False
        _TERMINAL_RETRY_COORDINATORS[token] = coordinator
    publish_terminal_owner("remote_terminal_retry", token, retained_bytes=512)
    return True


def _discard_terminal_retry_coordinator(coordinator: "RemoteIoCoordinator") -> None:
    """Discard terminal retry coordinator."""
    token = id(coordinator)
    with _TERMINAL_RETRY_COORDINATORS_LOCK:
        if _TERMINAL_RETRY_COORDINATORS.get(token) is coordinator:
            _TERMINAL_RETRY_COORDINATORS.pop(token, None)
            retire_terminal_owner("remote_terminal_retry", token)


def _retry_remote_terminal_callbacks_token(token: int) -> None:
    """Retry remote terminal callbacks token."""
    with _TERMINAL_RETRY_COORDINATORS_LOCK:
        coordinator = _TERMINAL_RETRY_COORDINATORS.get(token)
    if coordinator is not None:
        coordinator._retry_terminal_callbacks()


@dataclass(slots=True)
class _RemoteHostResourceCapsule:
    """Compact terminal ownership detached from the coordinator graph."""

    permit_registration: Any | None
    thread_lease: Any | None
    runtime_registration: Any | None

    def release(self) -> bool:
        """Release exact resource owners before unregistering the host."""
        for attribute in ("permit_registration", "thread_lease"):
            owner = getattr(self, attribute)
            if owner is None:
                continue
            try:
                owner.release()
            except BaseException:
                try:
                    adopted = adopt_failed_release(owner, retained_bytes=256)
                except BaseException:
                    adopted = False
                if not adopted:
                    return False
            setattr(self, attribute, None)
        registration = self.runtime_registration
        if registration is not None:
            try:
                registration.close()
            except BaseException:
                return False
            self.runtime_registration = None
        return True


def _retry_remote_host_resource_capsule(token: int) -> None:
    """Retry remote host resource capsule."""
    with _HOST_RESOURCE_CAPSULES_LOCK:
        capsule = _HOST_RESOURCE_CAPSULES.get(token)
    if capsule is None:
        return
    if not capsule.release():
        raise RuntimeError("remote host resource capsule is not quiescent")
    with _HOST_RESOURCE_CAPSULES_LOCK:
        if _HOST_RESOURCE_CAPSULES.get(token) is capsule:
            _HOST_RESOURCE_CAPSULES.pop(token, None)
    retire_terminal_owner("remote_host_resource_capsule", token)


@asynccontextmanager
async def _empty_async_context() -> Any:
    """Provide a contextless event loop for operation-wide remote work."""
    yield None


@dataclass(slots=True)
class _RemoteIoSubmission:
    """Retain one submission until the real asyncio task has terminated."""

    reservation: RemoteIoSubmissionReservation
    lock: threading.Lock = field(default_factory=threading.Lock)
    terminal: threading.Event = field(default_factory=threading.Event)
    started: threading.Event = field(default_factory=threading.Event)
    real_ready: threading.Event = field(default_factory=threading.Event)
    registration_complete: threading.Event = field(default_factory=threading.Event)
    callback_seen: threading.Event = field(default_factory=threading.Event)
    future: Future[Any] | None = None
    operation_error: BaseException | None = None
    task_error: BaseException | None = None
    permit_cleanup_error: BaseException | None = None
    finalized: bool = False
    callback_pending: bool = True
    terminal_callbacks: list[Callable[[Future[Any]], None]] = field(default_factory=list)
    callback_quiescent: threading.Event = field(default_factory=threading.Event)
    callbacks_pending: int = 0
    callback_errors: list[BaseException] = field(default_factory=list)
    callback_failure_count: int = 0
    callback_protocol_violations: int = 0
    callback_protocol_reported: bool = False
    first_callback_error: BaseException | None = None
    last_callback_error: BaseException | None = None
    callback_dispatch: (
        Callable[["_RemoteIoSubmission", Callable[[Future[Any]], None]], bool] | None
    ) = field(default=None, repr=False)

    def __post_init__(self) -> None:
        """Validate and normalize the initialized instance state."""
        self.callback_quiescent.set()

    def claim_finalization(self) -> bool:
        """Return whether this caller owns the real-terminal commit."""
        with self.lock:
            if self.finalized:
                return False
            self.finalized = True
            return True

    def claim_callback(self) -> bool:
        """Return whether this caller owns callback-barrier completion."""
        with self.lock:
            if not self.callback_pending:
                return False
            self.callback_pending = False
            return True

    def add_terminal_callback(self, callback: Callable[[Future[Any]], None]) -> bool:
        """Register one callback without executing it in a lifecycle caller."""
        dispatch: Callable[["_RemoteIoSubmission", Callable[[Future[Any]], None]], bool] | None = (
            None
        )
        with self.lock:
            self.callbacks_pending += 1
            self.callback_quiescent.clear()
            if self.terminal.is_set():
                dispatch = self.callback_dispatch
            else:
                self.terminal_callbacks.append(callback)
                return True
        if dispatch is None:
            with self.lock:
                self.terminal_callbacks.append(callback)
            return False
        dispatch(self, callback)
        return True

    def _complete_callback(self, callback: Callable[[Future[Any]], None]) -> BaseException | None:
        """Execute one callback and publish callback-generation quiescence."""
        future = self.future
        error: BaseException | None = None
        try:
            if future is not None:
                callback(future)
        except BaseException as exc:
            error = exc
            with self.lock:
                self.callback_failure_count += 1
                if self.first_callback_error is None:
                    self.first_callback_error = exc
                self.last_callback_error = exc
                self.callback_errors[:] = [
                    item
                    for item in (self.first_callback_error, self.last_callback_error)
                    if item is not None
                ]
        finally:
            with self.lock:
                # A failed callback remains the owner of this pending slot
                # until the retained retry succeeds.  Retiring the slot on a
                # transient failure made the retry look like an underflow and
                # converted an otherwise recoverable cleanup error into a
                # protocol violation at close().
                if error is None:
                    if self.callbacks_pending <= 0:
                        self.callback_protocol_violations += 1
                    else:
                        self.callbacks_pending -= 1
                if self.callbacks_pending == 0:
                    self.callback_quiescent.set()
        return error


@dataclass(slots=True)
class _RemotePermitStageDomain:
    """Own remote concurrency and the matching composite FD footprint."""

    coordinator: "RemoteIoCoordinator"
    permit: Any | None
    descriptor_lease: Any | None
    footprint_owner: ActiveRemoteIoFootprint

    def release(self) -> None:
        """Retire both domains without publishing a false cleanup commit.

        The provider pool accounts persistent/control-plane descriptors.  This
        lease accounts sockets that may be active for this submitted operation,
        so SDK connection fanout can never exceed process FD admission merely by
        bypassing provider-session ownership.
        """
        first_error: BaseException | None = None
        permit = self.permit
        if permit is not None:
            try:
                permit.release()
            except BaseException as primary:
                try:
                    self.coordinator._retain_failed_permit(permit)
                except BaseException as transfer_error:
                    add_bounded_note(
                        primary,
                        "remote-I/O failed-permit ownership transfer also failed",
                        transfer_error,
                    )
                else:
                    self.permit = None
                first_error = primary
            else:
                self.permit = None

        descriptor_lease = self.descriptor_lease
        if descriptor_lease is not None:
            try:
                self.footprint_owner.release_descriptor_lease()
            except BaseException as descriptor_error:
                if first_error is None:
                    first_error = descriptor_error
                else:
                    add_bounded_note(
                        first_error,
                        "remote-I/O active descriptor cleanup also failed",
                        descriptor_error,
                    )
            else:
                # The footprint owner may have terminally retained this exact
                # composite lease after an uncertain physical close.
                self.descriptor_lease = None
        if first_error is not None:
            raise first_error


class RemoteIoCoordinator:
    """Run remote coroutines on one owned event loop and context."""

    def __init__(
        self,
        context_factory: Callable[[], AsyncContextManager[Any]] | None = None,
        *,
        thread_name: str = "schema-sanitizer-remote-io",
        shutdown_timeout_seconds: float = _DEFAULT_SHUTDOWN_TIMEOUT_SECONDS,
        permit_governor: RemoteIoPermitGovernor | None = None,
        permit_capacity: int | None = None,
        operation_id: str | None = None,
        thread_slot_reserved: bool = False,
        operation_memory_ledger: OperationMemoryLedger | None = None,
        stage_bytes_per_permit: int = 4096,
    ) -> None:
        """Start the coordinator and enter its shared async context."""
        normalized_shutdown_timeout = normalize_duration(
            shutdown_timeout_seconds,
            name="shutdown_timeout_seconds",
            allow_zero=False,
        )
        assert normalized_shutdown_timeout is not None
        self._context_factory = context_factory or _empty_async_context
        self._pid = os.getpid()
        self._operation_id = _bounded_metadata(
            operation_id or f"coordinator:{id(self)}", kind="operation"
        )
        if type(stage_bytes_per_permit) is not int or stage_bytes_per_permit <= 0:
            raise ValueError("stage_bytes_per_permit must be a positive exact integer")
        self._operation_memory_ledger = operation_memory_ledger
        self._stage_bytes_per_permit = stage_bytes_per_permit
        uses_shared_governor = permit_governor is None
        self._permit_governor = permit_governor or shared_remote_io_permit_governor()
        self._permit_registration: RemoteIoCapacityRegistration | None = None
        requested_capacity = (
            default_remote_io_permit_capacity()
            if permit_capacity is None and uses_shared_governor
            else permit_capacity
        )
        if requested_capacity is not None:
            self._permit_registration = self._permit_governor.register_capacity(requested_capacity)
        self._shutdown_timeout_seconds = normalized_shutdown_timeout
        self._ready = threading.Event()
        self._lock = threading.Lock()
        self._release_lock = threading.Lock()
        self._loop: asyncio.AbstractEventLoop | None = None
        self._context_manager: AsyncContextManager[Any] | None = None
        self._context: Any = None
        self._cleanup_owner = RemoteIoCleanupOwner()
        self._startup_error: BaseException | None = None
        self._startup_task: asyncio.Task[Any] | None = None
        self._startup_terminal = threading.Event()
        self._startup_decision = threading.Event()
        self._startup_accepted = False
        self._startup_abandoned = False
        self._shutdown_future: Future[Any] | None = None
        self._futures: set[Future[Any]] = set()
        self._submissions: dict[Future[Any], _RemoteIoSubmission] = {}
        self._failed_submissions: deque[RemoteIoSubmissionReservation] = deque()
        self._failed_permits: deque[Any] = deque()
        self._submission_callbacks_inflight = 0
        self._protocol_violations = 0
        self._callbackless_submissions: dict[Future[Any], _RemoteIoSubmission] = {}
        self._deferred_terminal_callbacks: deque[
            tuple[_RemoteIoSubmission, Callable[[Future[Any]], None]]
        ] = deque()
        self._terminal_callback_owners: set[int] = set()
        self._failed_terminal_callbacks: deque[
            tuple[_RemoteIoSubmission, Callable[[Future[Any]], None]]
        ] = deque()
        self._terminal_retry_scheduled = False
        self._terminal_retry_attempt = 0
        self._resource_retry_scheduled = False
        self._resource_retry_attempt = 0
        self._closed = False
        self._closing = False
        self._close_complete = threading.Event()
        self._close_condition = threading.Condition(self._lock)
        self._close_generation = 0
        self._completed_close_generation = 0
        self._close_results: dict[int, BaseException | None] = {}
        self._close_waiters: dict[int, int] = {}
        self._close_error: BaseException | None = None
        self._thread_lease: Any | None = None
        self._runtime_registration: RuntimeServiceRegistration | None = None
        if not thread_slot_reserved:
            try:
                self._thread_lease = acquire_project_threads(1, minimum=1)
            except BaseException as exc:
                try:
                    self._release_permit_registration()
                except BaseException as cleanup_error:
                    add_bounded_note(
                        exc, "remote-I/O capacity cleanup after thread admission", cleanup_error
                    )
                raise
        started = False
        try:
            self._runtime_registration = reserve_runtime_service(
                self, kind="remote_io_coordinator", close_name="_runtime_shutdown"
            )
            self._thread = threading.Thread(
                target=self._run_with_thread_lease,
                name=thread_name,
                daemon=True,
            )
            registration = self._runtime_registration
            start_governed_thread(self._thread, registration=registration)
            started = True
        except BaseException as exc:
            self._closed = True
            if not started:
                try:
                    started = bool(self._thread.is_alive())
                except BaseException:
                    started = False
            if started:
                # The live host owns its permit and provider startup. Preserve the
                # reserved registry entry and let the host finally release them.
                with self._lock:
                    self._startup_abandoned = True
                    self._startup_accepted = False
                with _ORPHANED_STARTUPS_LOCK:
                    _ORPHANED_STARTUPS.add(self)
                self._startup_decision.set()
                add_bounded_note(
                    exc,
                    "remote-I/O host retained after registration activation failure",
                    RuntimeError("live host ownership preserved"),
                )
                raise
            rollback_registration = self._runtime_registration
            self._runtime_registration = None
            cleanup_errors: list[BaseException] = []
            if rollback_registration is not None:
                try:
                    rollback_registration.close()
                except BaseException as registration_cleanup_error:
                    cleanup_errors.append(registration_cleanup_error)

            for release in (
                self._release_permit_registration,
                self._release_thread_lease,
            ):
                try:
                    release()
                except BaseException as cleanup_error:
                    cleanup_errors.append(cleanup_error)
            for retained_cleanup_error in cleanup_errors:
                add_bounded_note(
                    exc,
                    "remote-I/O host-resource cleanup after startup",
                    retained_cleanup_error,
                )
            if cleanup_errors:
                self._retain_host_resource_cleanup()
            raise
        if not self._ready.wait(timeout=self._shutdown_timeout_seconds):
            with self._lock:
                self._closed = True
                self._startup_abandoned = True
                self._startup_accepted = False
                loop = cast(
                    asyncio.AbstractEventLoop | None,
                    getattr(cast(Any, self), "_loop", None),
                )
                startup_task = self._startup_task
            with _ORPHANED_STARTUPS_LOCK:
                _ORPHANED_STARTUPS.add(self)
            if loop is not None:

                def cancel_startup() -> None:
                    """Cancel startup and retire any partially acquired resources."""
                    task = startup_task or self._startup_task
                    if task is not None and not task.done():
                        task.cancel()

                with suppress(RuntimeError):
                    loop.call_soon_threadsafe(cancel_startup)
            self._startup_decision.set()
            # The constructor may fail publicly, but registration and thread
            # capacity remain owned by the orphan control block until the real
            # startup task and provider cleanup reach their finally blocks.
            raise RuntimeError("remote I/O coordinator startup exceeded its deadline")
        with self._lock:
            self._startup_accepted = self._startup_error is None
        self._startup_decision.set()
        try:
            self._raise_startup_error()
        except BaseException:
            self._release_permit_registration()
            raise

    def _raise_startup_error(self) -> None:
        """Raise an error captured while the coordinator thread was starting."""
        startup_error = self._startup_error
        if startup_error is None:
            return
        self._thread.join(timeout=self._shutdown_timeout_seconds)
        if not self._thread.is_alive():
            reap_governed_thread_retirements()
        if self._thread.is_alive():
            add_bounded_note(
                startup_error,
                "remote I/O coordinator startup thread remained alive after its bounded join",
                "daemon and thread permit remain owned until exit",
            )
        raise startup_error

    def submit(
        self,
        operation: Callable[[Any], Awaitable[T]],
        *,
        permit_weight: int = 1,
        permit_label: str = "remote_operation",
        footprint: RemoteIoFootprint | None = None,
    ) -> Future[T]:
        """Schedule one operation under one atomic remote/FD footprint."""
        self._ensure_owner_process()
        if footprint is None:
            footprint = RemoteIoFootprint(remote_weight=permit_weight, network_fds=0)
        elif permit_weight != 1 and permit_weight != footprint.remote_weight:
            raise ValueError("permit_weight conflicts with remote I/O footprint")
        permit_weight = footprint.remote_weight
        bounded_label = _bounded_metadata(permit_label, kind="label")
        with self._lock:
            if self._closed:
                raise RuntimeError("remote I/O coordinator is closed")
            loop = self._loop
            if loop is None:
                raise RuntimeError("remote I/O coordinator did not start")
            submission_owner = _RemoteIoSubmission(
                self._permit_governor.reserve_submission(),
                callback_dispatch=self._dispatch_terminal_callback,
            )

            async def invoke() -> T:
                """Invoke one submitted operation under one composed stage admission."""
                submission_owner.started.set()
                stage_admission: StageConcurrencyAdmission | None = None
                primary_error: BaseException | None = None
                try:
                    stage_admission = acquire_stage_concurrency_admission(
                        1,
                        per_slot_bytes=max(4096, permit_weight * self._stage_bytes_per_permit),
                        stage=f"remote_io:{bounded_label}",
                        reserve_bytes=0,
                        memory_ledger=self._operation_memory_ledger,
                        physical_threads=False,
                    )
                    descriptor_lease = await self._acquire_active_descriptor_lease(
                        footprint.total_file_descriptors
                    )
                    try:
                        permit = await self._permit_governor.acquire(
                            footprint.remote_weight,
                            label=bounded_label,
                            operation_id=self._operation_id,
                        )
                    except BaseException as permit_error:
                        if descriptor_lease is not None:
                            try:
                                descriptor_lease.release()
                            except BaseException as cleanup_error:
                                add_bounded_note(
                                    permit_error,
                                    "remote descriptor admission rollback also failed",
                                    cleanup_error,
                                )
                        raise
                    footprint_owner = ActiveRemoteIoFootprint(footprint, descriptor_lease)
                    domain = _RemotePermitStageDomain(
                        self, permit, descriptor_lease, footprint_owner
                    )
                    try:
                        stage_admission.attach_domain("remote_io", domain)
                    except BaseException as attach_error:
                        try:
                            domain.release()
                        except BaseException as cleanup_error:
                            submission_owner.permit_cleanup_error = cleanup_error
                            add_bounded_note(
                                attach_error,
                                "remote-I/O domain rollback also failed",
                                cleanup_error,
                            )
                            try:
                                adopted = adopt_failed_release(domain, retained_bytes=512)
                            except BaseException:
                                adopted = False
                            if not adopted:
                                # The domain still owns the permit when retry
                                # publication fails; keep the coordinator itself
                                # terminally reachable as the final ownership root.
                                _retain_terminal_retry_coordinator(self)
                        raise
                    with activate_provider_session_pool(self._context):
                        with activate_remote_io_footprint(footprint_owner):
                            return await operation(self._context)
                except BaseException as exc:
                    primary_error = exc
                    submission_owner.task_error = exc
                    submission_owner.operation_error = exc
                    raise
                finally:
                    if stage_admission is not None:
                        try:
                            stage_admission.close()
                        except BaseException as exc:
                            submission_owner.permit_cleanup_error = exc
                            try:
                                adopted = adopt_failed_release(stage_admission, retained_bytes=1024)
                            except BaseException:
                                adopted = False
                            if primary_error is not None:
                                add_bounded_note(
                                    primary_error,
                                    "remote stage-admission cleanup also failed",
                                    exc,
                                )
                            elif not adopted:
                                submission_owner.task_error = exc
                                submission_owner.operation_error = exc
                                raise
                    self._finish_submission_real(submission_owner)

            invoke_coro: Any | None = None
            try:
                invoke_coro = invoke()
                future = asyncio.run_coroutine_threadsafe(invoke_coro, loop)
            except BaseException as exc:
                if invoke_coro is not None:
                    try:
                        invoke_coro.close()
                    except BaseException as cleanup_error:
                        add_bounded_note(
                            exc,
                            "remote-I/O coroutine cleanup also failed after submission",
                            cleanup_error,
                        )
                try:
                    submission_owner.reservation.release()
                except BaseException as cleanup_error:
                    add_bounded_note(
                        exc,
                        "remote-I/O submission rollback also failed",
                        cleanup_error,
                    )
                raise
            submission_owner.future = future
            self._futures.add(future)
            self._submissions[future] = submission_owner
            self._submission_callbacks_inflight += 1
            try:
                setattr(future, "_schema_sanitizer_remote_submission", submission_owner)
            except BaseException:
                pass

        # A Future may already be complete. Register outside the lifecycle lock
        # because concurrent.futures invokes callbacks synchronously in that case.
        try:

            def bridge_done(
                done: Future[Any], owner: _RemoteIoSubmission = submission_owner
            ) -> None:
                """Handle completion of the asynchronous bridge operation."""
                self._bridge_submission_done(done, owner)

            future.add_done_callback(bridge_done)
            submission_owner.registration_complete.set()
            if submission_owner.real_ready.is_set():
                self._finish_submission_real(submission_owner)
            if submission_owner.callback_seen.is_set():
                self._complete_submission_callback_barrier(submission_owner)
        except BaseException:
            with self._close_condition:
                self._callbackless_submissions[future] = submission_owner
                submission_owner.registration_complete.set()
                self._close_condition.notify_all()
            if submission_owner.real_ready.is_set():
                self._finish_submission_real(submission_owner)
            future.cancel()
            if future.done() and (not future.cancelled() or not submission_owner.started.is_set()):
                self._complete_callbackless_submissions()
            raise
        return future

    def permit_snapshot(self):
        """Return process-wide remote admission diagnostics."""
        self._ensure_owner_process()
        return self._permit_governor.snapshot()

    def close(self) -> None:
        """Cancel and drain one generation, with generation-scoped waiters."""
        self._ensure_owner_process()
        if threading.current_thread() is self._thread:
            raise RuntimeError("remote I/O coordinator cannot close from its owned thread")

        condition = self._close_condition
        with condition:
            if self._closing:
                generation = self._close_generation
                self._close_waiters[generation] = self._close_waiters.get(generation, 0) + 1
                wait_for_owner = True
                loop = None
                futures: tuple[Future[Any], ...] = ()
                submissions: tuple[_RemoteIoSubmission, ...] = ()
            elif self._closed:
                existing_close_error = self._close_error
                if existing_close_error is None:
                    return
                if not self._has_retryable_release_owners_locked():
                    raise RuntimeError(str(existing_close_error)) from existing_close_error
                if self._close_generation >= (1 << 63) - 1:
                    raise RuntimeError("remote I/O close generation exhausted")
                self._close_generation += 1
                generation = self._close_generation
                wait_for_owner = False
                self._closing = True
                self._close_complete.clear()
                loop = self._loop if self._thread.is_alive() else None
                futures = tuple(self._futures)
                submissions = tuple(self._submissions.values())
            else:
                if self._close_generation >= (1 << 63) - 1:
                    raise RuntimeError("remote I/O close generation exhausted")
                self._close_generation += 1
                generation = self._close_generation
                wait_for_owner = False
                self._closing = True
                self._closed = True
                self._close_complete.clear()
                loop = self._loop
                futures = tuple(self._futures)
                submissions = tuple(self._submissions.values())

        if wait_for_owner:
            deadline = monotonic() + self._shutdown_timeout_seconds
            with condition:
                try:
                    while self._completed_close_generation < generation:
                        remaining = deadline - monotonic()
                        if remaining <= 0 or not condition.wait(timeout=remaining):
                            raise RuntimeError(
                                "remote I/O coordinator concurrent close exceeded its deadline"
                            )
                    concurrent_close_error = self._close_results.get(generation)
                finally:
                    remaining_waiters = self._close_waiters.get(generation, 1) - 1
                    if remaining_waiters <= 0:
                        self._close_waiters.pop(generation, None)
                        if generation != self._completed_close_generation:
                            self._close_results.pop(generation, None)
                    else:
                        self._close_waiters[generation] = remaining_waiters
            if concurrent_close_error is not None:
                raise RuntimeError(str(concurrent_close_error)) from concurrent_close_error
            return

        deadline = monotonic() + self._shutdown_timeout_seconds
        close_error: BaseException | None = None
        host_must_remain_live = False
        loop_stop_requested = False
        try:
            for future in futures:
                future.cancel()
            for submission in submissions:
                remaining = deadline - monotonic()
                if remaining <= 0:
                    close_error = RuntimeError(
                        "remote I/O coordinator shutdown exceeded its deadline"
                    )
                    host_must_remain_live = True
                    break
                while not submission.terminal.is_set():
                    submission_future = submission.future
                    if (
                        submission_future is not None
                        and submission_future.done()
                        and (not submission_future.cancelled() or not submission.started.is_set())
                    ):
                        if not submission_future.cancelled():
                            try:
                                submission_future.result(timeout=0)
                            except BaseException as exc:
                                if submission.operation_error is None:
                                    submission.operation_error = exc
                        self._finish_submission_real(submission)
                    remaining = deadline - monotonic()
                    if remaining <= 0 or not submission.terminal.wait(timeout=min(0.01, remaining)):
                        if monotonic() >= deadline:
                            close_error = RuntimeError(
                                "remote I/O coordinator shutdown exceeded its deadline "
                                "while waiting for task termination"
                            )
                            host_must_remain_live = True
                            break
                if close_error is not None:
                    break
                error = submission.operation_error
                if error is not None and not isinstance(
                    error, (CancelledError, asyncio.CancelledError)
                ):
                    close_error = close_error or error

            # Drain callbacks already published by terminal submissions while
            # the loop host is still alive. Besides reducing close latency, this
            # makes the off-loop guarantee observable even on platforms that
            # immediately recycle thread identifiers after host termination.
            try:
                self._drain_terminal_callback_work(deadline)
            except BaseException as exc:
                close_error = close_error or exc

            if (
                not host_must_remain_live
                and loop is not None
                and not loop.is_closed()
                and (loop.is_running() or self._thread.is_alive())
            ):
                try:
                    remaining = deadline - monotonic()
                    with self._lock:
                        close_future = self._shutdown_future
                        if close_future is None or close_future.done():
                            if close_future is not None:
                                try:
                                    close_future.result(timeout=0)
                                except BaseException:
                                    close_future = None
                            if close_future is None:
                                close_future = asyncio.run_coroutine_threadsafe(
                                    self._shutdown(max(0.0, remaining)), loop
                                )
                                self._shutdown_future = close_future
                    if remaining <= 0:
                        close_error = close_error or RuntimeError(
                            "remote I/O coordinator shutdown exceeded its deadline"
                        )
                        host_must_remain_live = True
                    else:
                        try:
                            close_future.result(timeout=remaining)
                        except FutureTimeoutError:
                            close_error = close_error or RuntimeError(
                                "remote I/O coordinator shutdown exceeded its deadline"
                            )
                            host_must_remain_live = True
                        except BaseException as exc:
                            close_error = close_error or exc
                            host_must_remain_live = True
                            with self._lock:
                                if self._shutdown_future is close_future:
                                    self._shutdown_future = None
                        else:
                            with self._lock:
                                if self._shutdown_future is close_future:
                                    self._shutdown_future = None
                            with suppress(RuntimeError):
                                loop.call_soon_threadsafe(loop.stop)
                                loop_stop_requested = True
                except BaseException as exc:
                    close_error = close_error or exc
                    host_must_remain_live = True

            if loop_stop_requested or not self._thread.is_alive():
                self._thread.join(timeout=max(0.0, deadline - monotonic()))
                if not self._thread.is_alive():
                    reap_governed_thread_retirements()
                if self._thread.is_alive() and close_error is None:
                    close_error = RuntimeError(
                        "remote I/O coordinator host thread did not stop before its deadline"
                    )

            try:
                self._drain_callbackless_submissions(deadline)
            except BaseException as exc:
                close_error = close_error or exc
            try:
                self._drain_terminal_callback_work(deadline)
            except BaseException as exc:
                close_error = close_error or exc

            # A concurrent Future can become done before add_done_callback()
            # returns, and result waiters can wake before its callback finishes.
            # The close generation cannot commit until every publisher that can
            # retain a failed reservation has left its callback.
            with condition:
                while self._submission_callbacks_inflight:
                    remaining = deadline - monotonic()
                    if remaining <= 0 or not condition.wait(timeout=remaining):
                        close_error = close_error or RuntimeError(
                            "remote I/O submission callbacks exceeded their deadline"
                        )
                        break
        finally:
            try:
                self._retry_failed_submissions()
            except BaseException as exc:
                close_error = close_error or exc
            try:
                self._retry_failed_permits()
            except BaseException as exc:
                close_error = close_error or exc
            try:
                self._release_permit_registration()
            except BaseException as exc:
                close_error = close_error or exc
            if not self._thread.is_alive():
                try:
                    self._release_thread_lease()
                except BaseException as exc:
                    close_error = close_error or exc
            if self._protocol_violations and close_error is None:
                close_error = RuntimeError(
                    "remote I/O callback protocol violation prevents clean close"
                )
            with condition:
                self._closing = False
                self._close_error = close_error
                self._completed_close_generation = max(self._completed_close_generation, generation)
                self._close_results[generation] = close_error
                # Results with no possible waiter are not retained indefinitely.
                for old_generation in tuple(self._close_results):
                    if (
                        old_generation != generation
                        and self._close_waiters.get(old_generation, 0) == 0
                    ):
                        self._close_results.pop(old_generation, None)
                self._close_complete.set()
                condition.notify_all()

        if close_error is not None:
            raise close_error

    def _runtime_shutdown(self, *, deadline_seconds: float) -> bool:
        """Participate in the process-wide absolute-deadline shutdown."""
        remaining = normalize_duration(
            deadline_seconds,
            name="remote coordinator runtime shutdown deadline",
            allow_zero=True,
        )
        assert remaining is not None
        previous = self._shutdown_timeout_seconds
        self._shutdown_timeout_seconds = min(previous, remaining)
        try:
            self.close()
        except BaseException:
            return False
        finally:
            self._shutdown_timeout_seconds = previous
        with self._lock:
            retryable_owners = self._has_retryable_release_owners_locked()
        stopped = not self._thread.is_alive() and not retryable_owners
        if stopped:
            registration = self._runtime_registration
            self._runtime_registration = None
            if registration is not None:
                registration.close()
        return stopped

    def __enter__(self) -> RemoteIoCoordinator:
        """Return the started coordinator."""
        return self

    def __exit__(self, *_exc: object) -> None:
        """Close the coordinator."""
        self.close()

    @property
    def thread_ident(self) -> int | None:
        """Return the owned host thread identifier for diagnostics/tests."""
        return self._thread.ident

    @property
    def shutdown_timeout_seconds(self) -> float:
        """Return the shared close/drain deadline used by owned resources."""
        return self._shutdown_timeout_seconds

    async def _acquire_active_descriptor_lease(self, amount: int) -> Any | None:
        """Acquire the whole composite FD footprint with operation cancellation."""
        if amount <= 0:
            return None
        deadline = monotonic() + min(30.0, self._shutdown_timeout_seconds)
        delay = 0.001
        last_error: SchemaSanitizerResourceError | None = None
        while True:
            check_operation_cancelled(stage="remote_fd_admission")
            try:
                return acquire_file_descriptors(amount, timeout_seconds=0.0)
            except SchemaSanitizerResourceError as exc:
                last_error = exc
                snapshot = process_file_descriptor_snapshot()
                if snapshot.admission_closed or amount > snapshot.external_capacity:
                    raise
            remaining = deadline - monotonic()
            if remaining <= 0:
                check_operation_cancelled(stage="remote_fd_admission")
                assert last_error is not None
                raise last_error
            # Keep the event-loop thread available to tasks that can release the
            # composite credit, while the operation token/deadline can wake us.
            await cancellable_async_sleep(min(delay, remaining), stage="remote_fd_admission")
            delay = min(0.05, delay * 2.0)

    def _ensure_owner_process(self) -> None:
        """Reject inherited event-loop, thread, and lock state before touching it."""
        if os.getpid() != self._pid:
            raise RuntimeError(
                "remote I/O coordinator cannot be reused after fork; create it in the child"
            )

    def _has_retryable_release_owners_locked(self) -> bool:
        """Return whether a completed close still owns cleanup work."""
        return bool(
            self._permit_registration is not None
            or self._thread_lease is not None
            or self._failed_submissions
            or self._failed_permits
            or self._submission_callbacks_inflight
            or self._callbackless_submissions
            or self._submissions
            or self._shutdown_future is not None
            or self._terminal_callback_owners
            or self._deferred_terminal_callbacks
            or self._failed_terminal_callbacks
            or self._protocol_violations
            or self._thread.is_alive()
        )

    def _finish_submission_callback_locked(self) -> None:
        """Retire one submission callback barrier without masking underflow."""
        current = self._submission_callbacks_inflight
        if current <= 0:
            self._protocol_violations += 1
            return
        self._submission_callbacks_inflight = current - 1

    def _release_permit_registration(self, *, transfer_on_failure: bool = False) -> bool:
        """Return shared capacity, optionally transferring a failed owner.

        Explicit ``close()`` generations propagate the first release error
        and the coordinator remains the retry owner. Terminal worker paths can
        request a bounded guardian handoff because no caller remains to retry.
        """
        with self._release_lock:
            with self._lock:
                registration = self._permit_registration
            if registration is None:
                return True
            try:
                registration.release()
            except BaseException:
                if not transfer_on_failure:
                    raise
                try:
                    adopted = adopt_failed_release(registration, retained_bytes=256)
                except BaseException:
                    adopted = False
                if not adopted:
                    raise
                with self._lock:
                    if self._permit_registration is registration:
                        self._permit_registration = None
                return False
            with self._lock:
                if self._permit_registration is registration:
                    self._permit_registration = None
            return True

    def _release_thread_lease(self, *, transfer_on_failure: bool = False) -> bool:
        """Return the host-thread slot, optionally transferring failed cleanup."""
        with self._release_lock:
            with self._lock:
                lease = self._thread_lease
            if lease is None:
                return True
            try:
                lease.release()
            except BaseException:
                if not transfer_on_failure:
                    raise
                try:
                    adopted = adopt_failed_release(lease, retained_bytes=256)
                except BaseException:
                    adopted = False
                if not adopted:
                    raise
                with self._lock:
                    if self._thread_lease is lease:
                        self._thread_lease = None
                return False
            with self._lock:
                if self._thread_lease is lease:
                    self._thread_lease = None
            return True

    def _detach_host_resource_capsule(self) -> int | None:
        """Move terminal resource ownership out of the coordinator graph."""
        global _HOST_RESOURCE_CAPSULE_SEQUENCE
        with self._release_lock:
            with self._lock:
                permit_registration = self._permit_registration
                thread_lease = self._thread_lease
                runtime_registration = self._runtime_registration
                if (
                    permit_registration is None
                    and thread_lease is None
                    and runtime_registration is None
                ):
                    return None
                capsule = _RemoteHostResourceCapsule(
                    permit_registration, thread_lease, runtime_registration
                )
                with _HOST_RESOURCE_CAPSULES_LOCK:
                    if len(_HOST_RESOURCE_CAPSULES) >= _MAX_HOST_RESOURCE_CAPSULES:
                        return None
                    if _HOST_RESOURCE_CAPSULE_SEQUENCE >= (1 << 63) - 1:
                        return None
                    _HOST_RESOURCE_CAPSULE_SEQUENCE += 1
                    token = _HOST_RESOURCE_CAPSULE_SEQUENCE
                    _HOST_RESOURCE_CAPSULES[token] = capsule
                self._permit_registration = None
                self._thread_lease = None
                self._runtime_registration = None
        publish_terminal_owner("remote_host_resource_capsule", token, retained_bytes=1024)
        return token

    def _retain_host_resource_cleanup(self) -> None:
        """Transfer terminal resources to a compact bounded cleanup capsule."""
        token = self._detach_host_resource_capsule()
        if token is None:
            # Capacity exhaustion is fail-closed: retain the coordinator through
            # the terminal registry rather than dropping resource ownership.
            with _ORPHANED_STARTUPS_LOCK:
                _ORPHANED_STARTUPS.add(self)
            return
        capsule_token: int = token
        accepted = dispatch_cleanup(
            _retry_remote_host_resource_capsule,
            capsule_token,
            retained_bytes=1024,
            subsystem=CleanupSubsystem.REMOTE,
        )
        if accepted:
            return
        # Retry callbacks retain only the integer capsule token, never ``self``.

        def retry_capsule(token: int = capsule_token) -> None:
            """Retry cleanup for the retained resource capsule."""
            _retry_remote_host_resource_capsule(token)

        scheduled = schedule_retry(
            ("remote-host-resource-capsule", capsule_token),
            retry_capsule,
            delay_seconds=0.05,
            retained_bytes=256,
            jitter_fraction=0.2,
        )
        if not scheduled:
            # The bounded capsule registry remains the durable owner and the
            # central terminal ledger makes shutdown fail closed.
            return

    def _retry_host_resource_cleanup(self) -> None:
        """Retry host resource cleanup."""
        with self._lock:
            self._resource_retry_scheduled = False
        errors: list[BaseException] = []
        for release in (
            self._release_permit_registration,
            self._release_thread_lease,
        ):
            try:
                release(transfer_on_failure=True)
            except BaseException as exc:
                errors.append(exc)
        if errors:
            self._retain_host_resource_cleanup()
            return
        cancel_retry(("remote-host-resource-release", id(self)))
        with self._lock:
            self._resource_retry_attempt = 0
        with _ORPHANED_STARTUPS_LOCK:
            _ORPHANED_STARTUPS.discard(self)

    def _run_with_thread_lease(self) -> None:
        """Retain host resources through the physical host-thread lifetime."""
        try:
            self._run()
        finally:
            errors: list[BaseException] = []
            try:
                self._release_permit_registration(transfer_on_failure=True)
            except BaseException as exc:
                errors.append(exc)

            lease = self._thread_lease
            registration = self._runtime_registration
            retired = lease is None
            if lease is not None:
                try:
                    retired = defer_governed_thread_retirement(
                        self._thread, lease.release, registration=registration
                    )
                except BaseException as exc:
                    clear_exception_traceback(exc)
                    retired = False
                if retired:
                    with self._lock:
                        if self._thread_lease is lease:
                            self._thread_lease = None
                        if self._runtime_registration is registration:
                            self._runtime_registration = None
            elif registration is not None:
                try:
                    registration.close()
                except BaseException as exc:
                    errors.append(exc)
                else:
                    if self._runtime_registration is registration:
                        self._runtime_registration = None

            if errors or not retired:
                # Do not transfer a still-live thread permit into a cleanup
                # capsule: keeping the coordinator registered is the safe
                # fail-closed owner until a later post-exit shutdown pass.
                with _ORPHANED_STARTUPS_LOCK:
                    _ORPHANED_STARTUPS.add(self)
            else:
                cancel_retry(("remote-host-resource-release", id(self)))
                cancel_retry(("remote-terminal-callback", id(self)))
                _discard_terminal_retry_coordinator(self)
                with _ORPHANED_STARTUPS_LOCK:
                    _ORPHANED_STARTUPS.discard(self)

    def _finish_submission_real(self, owner: _RemoteIoSubmission) -> None:
        """Commit cleanup only after the actual asyncio task reaches ``finally``."""
        owner.real_ready.set()
        if not owner.registration_complete.is_set():
            return
        if not owner.claim_finalization():
            return
        try:
            owner.reservation.release()
        except BaseException:
            with self._lock:
                self._failed_submissions.append(owner.reservation)
        condition = self._close_condition
        callbacks: tuple[Callable[[Future[Any]], None], ...] = ()
        with condition:
            future = owner.future
            if future is not None:
                self._futures.discard(future)
                self._submissions.pop(future, None)
                was_callbackless = self._callbackless_submissions.pop(future, None) is not None
            else:
                was_callbackless = False
            owner.terminal.set()
            with owner.lock:
                callbacks = tuple(owner.terminal_callbacks)
                owner.terminal_callbacks.clear()
            if was_callbackless and owner.claim_callback():
                self._finish_submission_callback_locked()
            condition.notify_all()
        if callbacks:
            with condition:
                self._terminal_callback_owners.add(id(owner))
            for callback in callbacks:
                self._dispatch_terminal_callback(owner, callback)

    def _run_terminal_callback(
        self,
        owner: _RemoteIoSubmission,
        callback: Callable[[Future[Any]], None],
    ) -> None:
        """Execute one retained callback and update authoritative ownership."""
        error = owner._complete_callback(callback)
        condition = self._close_condition
        with condition:
            if owner.callback_protocol_violations and not owner.callback_protocol_reported:
                self._protocol_violations += owner.callback_protocol_violations
                owner.callback_protocol_reported = True
            if error is not None:
                self._failed_terminal_callbacks.append((owner, callback))
                self._schedule_terminal_retry_locked()
            elif owner.callback_quiescent.is_set():
                self._terminal_callback_owners.discard(id(owner))
            condition.notify_all()

    def _dispatch_terminal_callback(
        self,
        owner: _RemoteIoSubmission,
        callback: Callable[[Future[Any]], None],
    ) -> bool:
        """Publish callback work using only a compact coordinator token."""
        condition = self._close_condition
        with condition:
            self._terminal_callback_owners.add(id(owner))
            self._deferred_terminal_callbacks.append((owner, callback))
            retained = _retain_terminal_retry_coordinator(self)
            condition.notify_all()
        if not retained:
            return False
        token = id(self)
        if dispatch_cleanup(
            _retry_remote_terminal_callbacks_token,
            token,
            retained_bytes=512,
            subsystem=CleanupSubsystem.REMOTE,
        ):
            return True
        with condition:
            self._schedule_terminal_retry_locked()
            condition.notify_all()
        return False

    def _schedule_terminal_retry_locked(self) -> None:
        """Retry retained callbacks through a compact process-scheduler token."""
        if self._terminal_retry_scheduled:
            return
        if not self._deferred_terminal_callbacks and not self._failed_terminal_callbacks:
            _discard_terminal_retry_coordinator(self)
            return
        if not _retain_terminal_retry_coordinator(self):
            return
        delay = min(30.0, 0.05 * (2 ** min(self._terminal_retry_attempt, 9)))
        token = id(self)

        def retry_callbacks(token: int = token) -> None:
            """Retry the retained terminal callbacks."""
            _retry_remote_terminal_callbacks_token(token)

        self._terminal_retry_scheduled = schedule_retry(
            ("remote-terminal-callback", token),
            retry_callbacks,
            delay_seconds=delay,
            retained_bytes=512,
            jitter_fraction=0.2,
        )
        if self._terminal_retry_scheduled:
            self._terminal_retry_attempt += 1

    def _retry_terminal_callbacks(self) -> None:
        """Retry terminal callbacks retained by the coordinator."""
        with self._close_condition:
            self._terminal_retry_scheduled = False
            if self._failed_terminal_callbacks:
                owner, callback = self._failed_terminal_callbacks.popleft()
            elif self._deferred_terminal_callbacks:
                owner, callback = self._deferred_terminal_callbacks.popleft()
            else:
                self._terminal_retry_attempt = 0
                _discard_terminal_retry_coordinator(self)
                return
        self._run_terminal_callback(owner, callback)
        with self._close_condition:
            if not self._deferred_terminal_callbacks and not self._failed_terminal_callbacks:
                self._terminal_retry_attempt = 0
                _discard_terminal_retry_coordinator(self)
            self._schedule_terminal_retry_locked()

    def _drain_terminal_callback_work(self, deadline: float) -> None:
        """Wait boundedly while retained callbacks execute off-thread."""
        while True:
            with self._close_condition:
                if self._failed_terminal_callbacks:
                    owner, callback = self._failed_terminal_callbacks.popleft()
                elif self._deferred_terminal_callbacks:
                    owner, callback = self._deferred_terminal_callbacks.popleft()
                elif not self._terminal_callback_owners:
                    return
                else:
                    remaining = deadline - monotonic()
                    if remaining <= 0:
                        raise RuntimeError("remote I/O terminal callbacks exceeded their deadline")
                    self._close_condition.wait(timeout=min(0.02, remaining))
                    continue
            self._dispatch_terminal_callback(owner, callback)
            if monotonic() >= deadline:
                raise RuntimeError("remote I/O terminal cleanup callbacks exceeded their deadline")

    def _bridge_submission_done(self, future: Future[Any], owner: _RemoteIoSubmission) -> None:
        """Use bridge completion only as notification, never as cancel proof."""
        owner.callback_seen.set()
        try:
            if not owner.terminal.is_set() and (
                not future.cancelled() or not owner.started.is_set()
            ):
                # A non-cancelled run_coroutine_threadsafe Future becomes done
                # only after the task has produced its terminal outcome.
                try:
                    future.result(timeout=0)
                except BaseException as exc:
                    if owner.operation_error is None:
                        owner.operation_error = exc
                self._finish_submission_real(owner)
        finally:
            self._complete_submission_callback_barrier(owner)

    def _complete_submission_callback_barrier(self, owner: _RemoteIoSubmission) -> None:
        """Leave the callback barrier only after registration itself returned."""
        if not owner.registration_complete.is_set() or not owner.callback_seen.is_set():
            return
        if owner.claim_callback():
            with self._close_condition:
                self._finish_submission_callback_locked()
                self._close_condition.notify_all()

    def _complete_callbackless_submissions(self) -> None:
        """Finalize callbackless owners only after a safe terminal proof."""
        with self._lock:
            pending = tuple(
                (future, owner)
                for future, owner in self._callbackless_submissions.items()
                if owner.terminal.is_set()
                or (future.done() and (not future.cancelled() or not owner.started.is_set()))
            )
        for future, owner in pending:
            if not owner.terminal.is_set():
                try:
                    future.result(timeout=0)
                except BaseException as exc:
                    if owner.operation_error is None:
                        owner.operation_error = exc
                self._finish_submission_real(owner)
            else:
                with self._close_condition:
                    self._callbackless_submissions.pop(future, None)
                    if owner.claim_callback():
                        self._finish_submission_callback_locked()
                    self._close_condition.notify_all()

    def _drain_callbackless_submissions(self, deadline: float) -> None:
        """Cancel callbackless bridges but wait for real task termination."""
        while True:
            self._complete_callbackless_submissions()
            with self._lock:
                pending = tuple(self._callbackless_submissions.items())
            if not pending:
                return
            for future, owner in pending:
                future.cancel()
                remaining = deadline - monotonic()
                if remaining <= 0 or not owner.terminal.wait(timeout=remaining):
                    raise RuntimeError(
                        "remote I/O callbackless task termination exceeded its deadline"
                    )
            self._complete_callbackless_submissions()

    def _retain_failed_permit(self, permit: Any) -> None:
        """Keep one failed permit release reachable for a later close generation."""
        with self._close_condition:
            self._failed_permits.append(permit)
            self._close_condition.notify_all()

    def _retry_failed_permits(self) -> None:
        """Retry failed permit owners without changing operation outcomes."""
        with self._lock:
            pending = self._failed_permits
            self._failed_permits = deque()
        failed: deque[Any] = deque()
        primary: BaseException | None = None
        while pending:
            permit = pending.popleft()
            try:
                permit.release()
            except BaseException as exc:
                failed.append(permit)
                if primary is None:
                    primary = exc
                else:
                    add_bounded_note(
                        primary,
                        "another remote-I/O permit release also failed",
                        exc,
                    )
        if failed:
            with self._lock:
                failed.extend(self._failed_permits)
                self._failed_permits = failed
        if primary is not None:
            raise primary

    def _retry_failed_submissions(self) -> None:
        """Retry every submission owner retained by failed done callbacks."""
        with self._lock:
            pending = self._failed_submissions
            self._failed_submissions = deque()
        failed: deque[RemoteIoSubmissionReservation] = deque()
        primary: BaseException | None = None
        while pending:
            submission = pending.popleft()
            try:
                submission.release()
            except BaseException as exc:
                failed.append(submission)
                if primary is None:
                    primary = exc
                else:
                    add_bounded_note(
                        primary,
                        "another remote-I/O submission release also failed",
                        exc,
                    )
        if failed:
            with self._lock:
                failed.extend(self._failed_submissions)
                self._failed_submissions = failed
        if primary is not None:
            raise primary

    def _run(self) -> None:
        """Own the loop until startup and every auxiliary task terminate for real."""
        loop: asyncio.AbstractEventLoop | None = None
        try:
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
        except BaseException as exc:
            self._startup_error = exc
            self._startup_terminal.set()
            self._ready.set()
            if loop is not None:
                loop.close()
            return
        with self._lock:
            self._loop = loop
            startup_already_abandoned = self._startup_abandoned or self._closed
        enter_task: asyncio.Task[Any] | None = None
        try:
            manager = self._context_factory()
            self._context_manager = manager
            enter_task = loop.create_task(manager.__aenter__())
            with self._lock:
                self._startup_task = enter_task
                startup_already_abandoned = startup_already_abandoned or self._startup_abandoned
            if startup_already_abandoned:
                enter_task.cancel()
            self._context = loop.run_until_complete(enter_task)
            with self._lock:
                startup_abandoned = self._startup_abandoned or self._closed
            if startup_abandoned:
                exit_task = loop.create_task(manager.__aexit__(None, None, None))
                # Never destroy a cancellation-resistant provider cleanup. The
                # daemon host and its governed thread slot remain live until the
                # provider's actual finally path returns.
                loop.run_until_complete(exit_task)
                self._context_manager = None
                self._context = None
                self._ready.set()
                self._drain_and_close_loop(loop)
                return
        except BaseException as exc:
            self._startup_error = exc
            self._ready.set()
            self._drain_and_close_loop(loop)
            return
        finally:
            with self._lock:
                self._startup_task = None
            self._startup_terminal.set()

        self._ready.set()
        # The host must observe the constructor's accept/abandon decision before
        # entering run_forever(). This closes the timeout handoff window where
        # the constructor abandons immediately after the host's last check.
        startup_decided = self._startup_decision.wait(timeout=self._shutdown_timeout_seconds)
        with self._lock:
            if not startup_decided:
                self._startup_abandoned = True
                self._startup_accepted = False
            startup_accepted = self._startup_accepted and not self._startup_abandoned
        if not startup_accepted:
            exit_task = loop.create_task(manager.__aexit__(None, None, None))
            loop.run_until_complete(exit_task)
            self._context_manager = None
            self._context = None
            self._drain_and_close_loop(loop)
            return
        try:
            loop.run_forever()
        finally:
            self._drain_and_close_loop(loop)

    @staticmethod
    def _drain_and_close_loop(loop: asyncio.AbstractEventLoop) -> None:
        """Drain host Tasks to quiescence, including Tasks spawned by cleanup."""
        while True:
            pending = tuple(task for task in asyncio.all_tasks(loop) if not task.done())
            if not pending:
                break
            for task in pending:
                task.cancel()

            async def drain_round() -> None:
                """Drain one bounded round of pending cleanup work."""
                await asyncio.gather(*pending, return_exceptions=True)

            # A cancelled Task may create another Task from its finally block.
            # Repeat until the loop has no live Task; cancellation resistance
            # keeps the governed daemon alive rather than destroying cleanup.
            loop.run_until_complete(drain_round())
        loop.close()

    async def _shutdown(self, timeout_seconds: float) -> None:
        """Transfer provider ownership only after shutdown starts on its loop."""
        manager = self._context_manager
        await shutdown_remote_io(manager, timeout_seconds, cleanup_owner=self._cleanup_owner)
        if self._context_manager is manager:
            self._context_manager = None
            self._context = None


def _reset_remote_terminal_retry_registry_after_fork() -> None:
    """Reset remote terminal retry registry after fork."""
    global _TERMINAL_RETRY_COORDINATORS_LOCK, _TERMINAL_RETRY_COORDINATORS
    global _TERMINAL_RETRY_OVERFLOWS, _TERMINAL_RETRY_OVERFLOWED
    _TERMINAL_RETRY_COORDINATORS_LOCK = threading.Lock()
    _TERMINAL_RETRY_COORDINATORS = {}
    _TERMINAL_RETRY_OVERFLOWS = 0
    _TERMINAL_RETRY_OVERFLOWED = False


from ..core_impl.fork_manager import register_fork_handler as _register_fork_handler  # noqa: E402

_register_fork_handler("remote-io-coordinator", mode="quarantine_only")


def _orphaned_startup_snapshot() -> object:
    """Return a bounded snapshot of orphaned startup."""
    return _ORPHANED_STARTUPS.snapshot()


from ..core_impl.shutdown_observers import (  # noqa: E402
    register_shutdown_observer as _register_shutdown_observer,
)

_register_shutdown_observer("orphaned_remote_startups", _orphaned_startup_snapshot)
