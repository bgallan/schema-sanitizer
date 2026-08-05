"""One operation-owned event-loop thread for bounded remote I/O."""

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

from ..core_impl.cleanup_dispatcher import CleanupSubsystem, dispatch_cleanup
from ..core_impl.durations import normalize_duration
from ..core_impl.process_resources import acquire_project_threads
from ..core_impl.retry_scheduler import (
    adopt_failed_release,
    cancel_retry,
    schedule_retry,
)
from ..core_impl.runtime_registry import RuntimeServiceRegistration, reserve_runtime_service
from ..core_impl.safe_errors import add_bounded_note, clear_exception_traceback
from ..core_impl.terminal_hosts import TerminalHostMarkers
from .io_permits import (
    RemoteIoCapacityRegistration,
    RemoteIoPermitGovernor,
    RemoteIoSubmissionReservation,
    _bounded_metadata,
    default_remote_io_permit_capacity,
    shared_remote_io_permit_governor,
)
from .io_shutdown import shutdown_remote_io
from .provider_session_pool import activate_provider_session_pool
from .staged_ownership import StagedResultOwnership as StagedResultOwnership

T = TypeVar("T")
_DEFAULT_SHUTDOWN_TIMEOUT_SECONDS = 30.0
_ORPHANED_STARTUPS = TerminalHostMarkers(128)
_ORPHANED_STARTUPS_LOCK = threading.Lock()


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
    first_callback_error: BaseException | None = None
    last_callback_error: BaseException | None = None
    callback_dispatch: (
        Callable[["_RemoteIoSubmission", Callable[[Future[Any]], None]], bool] | None
    ) = field(default=None, repr=False)

    def __post_init__(self) -> None:
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
                self.callbacks_pending = max(0, self.callbacks_pending - 1)
                if self.callbacks_pending == 0:
                    self.callback_quiescent.set()
        return error


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
            start_thread = getattr(cast(Any, registration), "start_thread", None)
            if callable(start_thread):
                start_thread(self._thread)
                started = True
            else:  # compatibility for narrow test/control doubles
                self._thread.start()
                started = True
                registration.activate()
        except BaseException as exc:
            self._closed = True
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
    ) -> Future[T]:
        """Schedule one operation and retain admission until real task termination."""
        self._ensure_owner_process()
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
                """Invoke one submitted operation under process-wide I/O admission."""
                submission_owner.started.set()
                permit: Any | None = None
                primary_error: BaseException | None = None
                try:
                    permit = await self._permit_governor.acquire(
                        permit_weight,
                        label=bounded_label,
                        operation_id=self._operation_id,
                    )
                    with activate_provider_session_pool(self._context):
                        return await operation(self._context)
                except BaseException as exc:
                    primary_error = exc
                    submission_owner.task_error = exc
                    submission_owner.operation_error = exc
                    raise
                finally:
                    if permit is not None:
                        try:
                            permit.release()
                        except BaseException as exc:
                            submission_owner.permit_cleanup_error = exc
                            if primary_error is not None:
                                add_bounded_note(
                                    primary_error,
                                    "remote-I/O permit cleanup also failed",
                                    exc,
                                )
                            self._retain_failed_permit(permit)
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
            submissions = getattr(self, "_submissions", None)
            if submissions is None:
                submissions = {}
                self._submissions = submissions
            submissions[future] = submission_owner
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

        condition = getattr(self, "_close_condition", None)
        if condition is None:
            with self._lock:
                condition = getattr(self, "_close_condition", None)
                if condition is None:
                    condition = threading.Condition(self._lock)
                    self._close_condition = condition
        with condition:
            if not hasattr(self, "_terminal_callback_owners"):
                self._terminal_callback_owners = set()
            if not hasattr(self, "_deferred_terminal_callbacks"):
                self._deferred_terminal_callbacks = deque()
            if not hasattr(self, "_shutdown_future"):
                self._shutdown_future = None
            if not hasattr(self, "_failed_terminal_callbacks"):
                self._failed_terminal_callbacks = deque()
            if not hasattr(self, "_failed_permits"):
                self._failed_permits = deque()
            if not hasattr(self, "_terminal_retry_scheduled"):
                self._terminal_retry_scheduled = False
                self._terminal_retry_attempt = 0
            if not hasattr(self, "_close_generation"):
                self._close_generation = 0
                self._completed_close_generation = 0
                self._close_results = {}
                self._close_waiters = {}
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
                self._close_generation += 1
                generation = self._close_generation
                wait_for_owner = False
                self._closing = True
                self._close_complete.clear()
                loop = self._loop if self._thread.is_alive() else None
                futures = tuple(self._futures)
                submissions = tuple(getattr(self, "_submissions", {}).values())
            else:
                self._close_generation += 1
                generation = self._close_generation
                wait_for_owner = False
                self._closing = True
                self._closed = True
                self._close_complete.clear()
                loop = self._loop
                futures = tuple(self._futures)
                submissions = tuple(getattr(self, "_submissions", {}).values())

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
                and not bool(getattr(loop, "is_closed", lambda: False)())
                and (bool(getattr(loop, "is_running", lambda: False)()) or self._thread.is_alive())
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
                while getattr(self, "_submission_callbacks_inflight", 0):
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

    def _ensure_owner_process(self) -> None:
        """Reject inherited event-loop, thread, and lock state before touching it."""
        if os.getpid() != getattr(self, "_pid", os.getpid()):
            raise RuntimeError(
                "remote I/O coordinator cannot be reused after fork; create it in the child"
            )

    def _has_retryable_release_owners_locked(self) -> bool:
        """Return whether a completed close still owns cleanup work."""
        return bool(
            self._permit_registration is not None
            or self._thread_lease is not None
            or getattr(self, "_failed_submissions", ())
            or getattr(self, "_failed_permits", ())
            or getattr(self, "_submission_callbacks_inflight", 0)
            or getattr(self, "_callbackless_submissions", {})
            or getattr(self, "_submissions", {})
            or getattr(self, "_shutdown_future", None) is not None
            or getattr(self, "_terminal_callback_owners", set())
            or getattr(self, "_deferred_terminal_callbacks", ())
            or getattr(self, "_failed_terminal_callbacks", ())
            or self._thread.is_alive()
        )

    def _release_permit_registration(self, *, transfer_on_failure: bool = False) -> bool:
        """Return shared capacity, optionally transferring a failed owner.

        The default preserves the historical commit-after-release contract used
        by explicit ``close()`` generations: the first release error propagates
        and the coordinator remains the retry owner. Terminal worker paths can
        request a bounded guardian handoff because no caller remains to retry.
        """
        release_lock = getattr(self, "_release_lock", None)
        if release_lock is None:
            release_lock = threading.Lock()
            self._release_lock = release_lock
        with release_lock:
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
        release_lock = getattr(self, "_release_lock", None)
        if release_lock is None:
            release_lock = threading.Lock()
            self._release_lock = release_lock
        with release_lock:
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

    def _retain_host_resource_cleanup(self) -> None:
        """Retain terminal host resources if both bounded transfer paths reject."""
        with _ORPHANED_STARTUPS_LOCK:
            _ORPHANED_STARTUPS.add(self)
        with self._lock:
            if self._resource_retry_scheduled:
                return
            delay = min(
                30.0,
                0.05 * (2 ** min(self._resource_retry_attempt, 9)),
            )
            self._resource_retry_scheduled = schedule_retry(
                ("remote-host-resource-release", id(self)),
                self._retry_host_resource_cleanup,
                delay_seconds=delay,
                retained_bytes=1024,
                jitter_fraction=0.2,
            )
            if self._resource_retry_scheduled:
                self._resource_retry_attempt += 1
                return
        # The cleanup dispatcher is an independent bounded liveness path.  If it
        # is also saturated, the orphan registry remains the durable owner and a
        # caller-driven close/retry can make progress later.
        dispatch_cleanup(
            self._retry_host_resource_cleanup,
            retained_bytes=1024,
            subsystem=CleanupSubsystem.REMOTE,
        )

    def _retry_host_resource_cleanup(self) -> None:
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
        """Retain host resources until every startup/task finally has executed."""
        try:
            self._run()
        finally:
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
            else:
                cancel_retry(("remote-host-resource-release", id(self)))
                cancel_retry(("remote-terminal-callback", id(self)))
                registration = self._runtime_registration
                if registration is not None:
                    try:
                        registration.close()
                    except BaseException as exc:
                        clear_exception_traceback(exc)
                    else:
                        if self._runtime_registration is registration:
                            self._runtime_registration = None
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
                failed = getattr(self, "_failed_submissions", None)
                if failed is None:
                    failed = deque()
                    self._failed_submissions = failed
                failed.append(owner.reservation)
        condition = getattr(self, "_close_condition", None)
        if condition is None:
            condition = threading.Condition(self._lock)
            self._close_condition = condition
        callbacks: tuple[Callable[[Future[Any]], None], ...] = ()
        with condition:
            future = owner.future
            if future is not None:
                self._futures.discard(future)
                getattr(self, "_submissions", {}).pop(future, None)
                callbackless = getattr(self, "_callbackless_submissions", {})
                was_callbackless = callbackless.pop(future, None) is not None
            else:
                was_callbackless = False
            owner.terminal.set()
            with owner.lock:
                callbacks = tuple(owner.terminal_callbacks)
                owner.terminal_callbacks.clear()
            if was_callbackless and owner.claim_callback():
                self._submission_callbacks_inflight = max(
                    0, getattr(self, "_submission_callbacks_inflight", 1) - 1
                )
            condition.notify_all()
        if callbacks:
            with condition:
                self._terminal_callback_owners.add(id(owner))
            for callback in callbacks:
                self._dispatch_terminal_callback(owner, callback)

    def _dispatch_terminal_callback(
        self,
        owner: _RemoteIoSubmission,
        callback: Callable[[Future[Any]], None],
    ) -> bool:
        """Publish cleanup away from lifecycle threads or retain it durably."""

        def run_callback() -> None:
            error = owner._complete_callback(callback)
            condition = self._close_condition
            with condition:
                if error is not None:
                    self._failed_terminal_callbacks.append((owner, callback))
                    self._schedule_terminal_retry_locked()
                elif owner.callback_quiescent.is_set():
                    self._terminal_callback_owners.discard(id(owner))
                condition.notify_all()

        if dispatch_cleanup(
            run_callback,
            retained_bytes=2048,
            subsystem=CleanupSubsystem.REMOTE,
        ):
            return True
        condition = self._close_condition
        with condition:
            self._terminal_callback_owners.add(id(owner))
            self._deferred_terminal_callbacks.append((owner, callback))
            self._schedule_terminal_retry_locked()
            condition.notify_all()
        return False

    def _schedule_terminal_retry_locked(self) -> None:
        """Retry retained callbacks through the governed process scheduler."""
        if self._terminal_retry_scheduled:
            return
        if not self._deferred_terminal_callbacks and not self._failed_terminal_callbacks:
            return
        delay = min(30.0, 0.05 * (2 ** min(self._terminal_retry_attempt, 9)))
        self._terminal_retry_scheduled = schedule_retry(
            ("remote-terminal-callback", id(self)),
            self._retry_terminal_callbacks,
            delay_seconds=delay,
            retained_bytes=2048,
            jitter_fraction=0.2,
        )
        if self._terminal_retry_scheduled:
            self._terminal_retry_attempt += 1

    def _retry_terminal_callbacks(self) -> None:
        with self._close_condition:
            self._terminal_retry_scheduled = False
            if self._failed_terminal_callbacks:
                owner, callback = self._failed_terminal_callbacks.popleft()
            elif self._deferred_terminal_callbacks:
                owner, callback = self._deferred_terminal_callbacks.popleft()
            else:
                self._terminal_retry_attempt = 0
                return
        self._dispatch_terminal_callback(owner, callback)
        with self._close_condition:
            if not self._deferred_terminal_callbacks and not self._failed_terminal_callbacks:
                self._terminal_retry_attempt = 0
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
                # only after the task has produced its terminal outcome. This is
                # also the safe fallback for non-standard test Futures whose
                # coroutine body was never driven.
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
            condition = getattr(self, "_close_condition", None)
            if condition is None:
                with self._lock:
                    self._submission_callbacks_inflight = max(
                        0,
                        getattr(self, "_submission_callbacks_inflight", 1) - 1,
                    )
            else:
                with condition:
                    self._submission_callbacks_inflight = max(
                        0,
                        getattr(self, "_submission_callbacks_inflight", 1) - 1,
                    )
                    condition.notify_all()

    def _complete_submission(
        self,
        future: Future[Any],
        cleanup: _RemoteIoSubmission | RemoteIoSubmissionReservation,
    ) -> None:
        """Compatibility callback for older tests and internal doubles."""
        owner = (
            cleanup
            if isinstance(cleanup, _RemoteIoSubmission)
            else _RemoteIoSubmission(cleanup, future=future)
        )
        if not isinstance(cleanup, _RemoteIoSubmission):
            owner.registration_complete.set()
            owner.callback_seen.set()
            self._finish_submission_real(owner)
            self._complete_submission_callback_barrier(owner)
            return
        self._bridge_submission_done(future, owner)

    def _complete_callbackless_submissions(self) -> None:
        """Finalize callbackless owners only after a safe terminal proof."""
        with self._lock:
            pending = tuple(
                (future, owner)
                for future, owner in getattr(self, "_callbackless_submissions", {}).items()
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
                    getattr(self, "_callbackless_submissions", {}).pop(future, None)
                    if owner.claim_callback():
                        self._submission_callbacks_inflight = max(
                            0,
                            getattr(self, "_submission_callbacks_inflight", 1) - 1,
                        )
                    self._close_condition.notify_all()

    def _drain_callbackless_submissions(self, deadline: float) -> None:
        """Cancel callbackless bridges but wait for real task termination."""
        while True:
            self._complete_callbackless_submissions()
            with self._lock:
                pending = tuple(getattr(self, "_callbackless_submissions", {}).items())
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
            pending: deque[RemoteIoSubmissionReservation] = getattr(
                self, "_failed_submissions", deque()
            )
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
        self._startup_decision.wait()
        with self._lock:
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
                await asyncio.gather(*pending, return_exceptions=True)

            # A cancelled Task may create another Task from its finally block.
            # Repeat until the loop has no live Task; cancellation resistance
            # keeps the governed daemon alive rather than destroying cleanup.
            loop.run_until_complete(drain_round())
        loop.close()

    async def _shutdown(self, timeout_seconds: float) -> None:
        """Transfer provider ownership only after shutdown starts on its loop."""
        manager = self._context_manager
        await shutdown_remote_io(manager, timeout_seconds)
        if self._context_manager is manager:
            self._context_manager = None
            self._context = None
