"""Bounded synchronous bridge for coroutines invoked from an active loop.

It runs a coroutine on a bounded bridge runner when the caller already owns an event loop and
retains failed runners for governed cleanup.
"""

from __future__ import annotations

import asyncio
import os
from concurrent.futures import Future, InvalidStateError
from concurrent.futures import TimeoutError as FutureTimeoutError
from contextvars import Context, copy_context
from threading import Lock, current_thread
from time import monotonic_ns
from typing import Any

from ..core_impl.cancellation import bounded_wait_timeout
from ..core_impl.durations import deadline_ns_from_timeout, remaining_seconds
from ..core_impl.execution_policy import normalize_threading_mode
from ..core_impl.finalization import runtime_is_finalizing
from ..core_impl.finalizer_cleanup import (
    PreparedFinalizerCleanup,
    cancel_prepared_finalizer_cleanup,
    defer_owner_finalizer_cleanup,
    reserve_owner_finalizer_cleanup,
)
from ..core_impl.fork_safety import ensure_runtime_fork_safe
from ..core_impl.governed_thread import (
    RetirementAwareThread,
    defer_governed_thread_retirement,
    start_governed_thread,
)
from ..core_impl.process_resources import acquire_project_threads
from ..core_impl.resource_lifecycle import _cleanup_with_note
from ..core_impl.retry_scheduler import (
    adopt_failed_release,
    cancel_retry,
    schedule_retry,
)
from ..core_impl.runtime_registry import reserve_runtime_service
from ..core_impl.safe_errors import add_bounded_note
from ..core_impl.terminal_hosts import TerminalHostMarkers

_DEFAULT_ASYNC_BRIDGE_TIMEOUT_SECONDS = 300.0
_FAILED_BRIDGE_RUNNERS = TerminalHostMarkers(128, category="async_bridge_terminal")
_FAILED_BRIDGE_RUNNERS_LOCK = Lock()
_FAILED_BRIDGE_RUNNER_OWNERS: dict[int, "_BridgeRunner"] = {}
_MAX_FAILED_BRIDGE_RUNNER_OWNERS = 128


def _retain_failed_bridge_runner(runner: "_BridgeRunner") -> bool:
    """Retain a bridge strongly while retry callbacks carry only an integer token."""
    token = id(runner)
    with _FAILED_BRIDGE_RUNNERS_LOCK:
        if token not in _FAILED_BRIDGE_RUNNER_OWNERS:
            if len(_FAILED_BRIDGE_RUNNER_OWNERS) >= _MAX_FAILED_BRIDGE_RUNNER_OWNERS:
                _FAILED_BRIDGE_RUNNERS.add(runner)
                return False
            _FAILED_BRIDGE_RUNNER_OWNERS[token] = runner
        _FAILED_BRIDGE_RUNNERS.add(runner)
    return True


def _discard_failed_bridge_runner(runner: "_BridgeRunner") -> None:
    """Discard failed bridge runner."""
    with _FAILED_BRIDGE_RUNNERS_LOCK:
        _FAILED_BRIDGE_RUNNER_OWNERS.pop(id(runner), None)
        _FAILED_BRIDGE_RUNNERS.discard(runner)


def _retry_failed_bridge_runner_token(token: int) -> None:
    """Retry failed bridge runner token."""
    with _FAILED_BRIDGE_RUNNERS_LOCK:
        runner = _FAILED_BRIDGE_RUNNER_OWNERS.get(token)
    if runner is not None:
        runner._retry_thread_lease()


def _close_coroutine(coro: Any) -> None:
    """Close one coroutine-like input when ownership was never transferred."""
    close = getattr(coro, "close", None)
    if callable(close):
        close()


class _BridgeRunner:
    """Own one helper event loop, task, thread permit, and result publication."""

    def __init__(self, coro: Any, context: Context, lease: Any) -> None:
        """Capture resources before the host thread is started."""
        try:
            capsule = reserve_owner_finalizer_cleanup()
            ticket = capsule.ticket
        except BaseException as exc:
            try:
                _close_coroutine(coro)
            except BaseException as cleanup_error:
                add_bounded_note(exc, "async bridge coroutine cleanup also failed", cleanup_error)
            _cleanup_with_note(
                exc,
                lease,
                label="async bridge thread permit cleanup also failed",
                method="release",
            )
            raise
        self._finalizer_ticket = ticket
        self._finalizer_capsule: PreparedFinalizerCleanup | None = capsule
        self._coro = coro
        self._context = context
        self._lease = lease
        self._result: Future[Any] = Future()
        self._lock = Lock()
        self._loop: asyncio.AbstractEventLoop | None = None
        self._task: asyncio.Task[Any] | None = None
        self._cancel_requested = False
        self._lease_retry_attempt = 0
        self._runtime_registration = None
        self._drain_deadline_ns = 0
        self._terminal_loop: asyncio.AbstractEventLoop | None = None
        self._terminal_tasks: tuple[asyncio.Task[Any], ...] = ()
        self._terminal_non_cooperative = False
        self._thread = RetirementAwareThread(
            target=self._run,
            name="schema-sanitizer-async",
            daemon=True,
        )
        try:
            self._runtime_registration = reserve_runtime_service(
                self, kind="async_bridge", close_name="close"
            )
        except BaseException:
            try:
                _close_coroutine(self._coro)
            finally:
                try:
                    self._lease.release()
                finally:
                    self._lease = None
            self._retire_finalizer_slot()
            raise

    def _retire_finalizer_slot(self) -> None:
        """Retire the finalizer escrow slot owned by this bridge runner."""
        ticket = self._finalizer_ticket
        capsule = self._finalizer_capsule
        if ticket and capsule is not None:
            cancel_prepared_finalizer_cleanup(capsule)
            self._finalizer_ticket = 0
            self._finalizer_capsule = None

    @property
    def result(self) -> Future[Any]:
        """Return the cross-thread terminal-result future."""
        return self._result

    def start(self) -> None:
        """Publish the host transactionally without releasing a live thread permit."""
        ensure_runtime_fork_safe()
        started = False
        try:
            registration = self._runtime_registration
            start_governed_thread(self._thread, registration=registration)
            started = True
        except BaseException as exc:
            if not started:
                try:
                    started = bool(self._thread.is_alive())
                except BaseException:
                    started = False
            if started:
                # The thread owns the coroutine and permit now. Keep both visible
                # through the reserved registry entry until its terminal finally.
                _retain_failed_bridge_runner(self)
                try:
                    self.cancel()
                except BaseException as cancel_error:
                    add_bounded_note(
                        exc, "async-bridge cancellation after publication failure", cancel_error
                    )
                add_bounded_note(
                    exc,
                    "async-bridge host remains retained after registration activation failure",
                    RuntimeError("live host ownership preserved"),
                )
                raise
            registration = self._runtime_registration
            self._runtime_registration = None
            if registration is not None:
                try:
                    registration.close()
                except BaseException as cleanup_error:
                    add_bounded_note(
                        exc, "async-bridge registry rollback also failed", cleanup_error
                    )
            try:
                _close_coroutine(self._coro)
            except BaseException as cleanup_error:
                add_bounded_note(
                    exc, "async-bridge coroutine cleanup after startup failure", cleanup_error
                )
            if not self._release_thread_lease():
                add_bounded_note(
                    exc,
                    "async-bridge thread permit retained after startup failure",
                    RuntimeError("permit release pending"),
                )
            raise

    def cancel(self) -> None:
        """Cancel the bridge result while retaining the host until real cleanup."""
        ensure_runtime_fork_safe()
        self._result.cancel()
        default_deadline = deadline_ns_from_timeout(
            5.0, name="async bridge cancellation drain deadline"
        )
        with self._lock:
            if self._drain_deadline_ns == 0:
                self._drain_deadline_ns = default_deadline
            self._cancel_requested = True
            loop = self._loop
            task = self._task
        if loop is None:
            return

        def cancel_task() -> None:
            """Request cooperative cancellation without destroying a live loop."""
            if task is not None and not task.done():
                task.cancel()

        try:
            loop.call_soon_threadsafe(cancel_task)
        except RuntimeError:
            pass

    def _release_thread_lease(self) -> bool:
        """Return the helper-thread permit only after release commits."""
        with self._lock:
            lease = self._lease
        if lease is None:
            cancel_retry(("async-bridge-thread-lease", id(self)))
            _discard_failed_bridge_runner(self)
            return True
        try:
            lease.release()
        except BaseException:
            # Failed release ownership is centralized so one broken lease cannot
            # create one retry thread (or one immortal BridgeRunner) of its own.
            try:
                adopted = adopt_failed_release(lease, retained_bytes=256)
            except BaseException:
                adopted = False
            if adopted:
                with self._lock:
                    if self._lease is lease:
                        self._lease = None
                    self._lease_retry_attempt = 0
                cancel_retry(("async-bridge-thread-lease", id(self)))
                _discard_failed_bridge_runner(self)
                return False
            with self._lock:
                self._lease_retry_attempt += 1
                attempt = self._lease_retry_attempt
            token = id(self)
            _retain_failed_bridge_runner(self)

            def retry_runner(token: int = token) -> None:
                """Retry cleanup of a bridge runner that failed to terminate."""
                _retry_failed_bridge_runner_token(token)

            scheduled = schedule_retry(
                ("async-bridge-thread-lease", token),
                retry_runner,
                delay_seconds=min(30.0, 0.05 * (2 ** min(attempt, 10))),
                retained_bytes=512,
                jitter_fraction=0.2,
            )
            if not scheduled:
                # The process-wide fallback set remains the durable owner.  A
                # finalizer and subsequent explicit cleanup attempts can retry;
                # no resource is silently forgotten on queue rejection.
                return False
            return False
        with self._lock:
            if self._lease is lease:
                self._lease = None
            self._lease_retry_attempt = 0
        cancel_retry(("async-bridge-thread-lease", id(self)))
        _discard_failed_bridge_runner(self)
        return True

    def _retry_thread_lease(self) -> None:
        """Retry one failed helper-thread lease without losing its owner."""
        self._release_thread_lease()

    def _publish_result(self, value: Any) -> None:
        """Publish a value unless timeout/cancellation already won the race."""
        try:
            self._result.set_result(value)
        except InvalidStateError:
            pass

    def _publish_error(self, error: BaseException) -> None:
        """Publish an error unless timeout/cancellation already won the race."""
        try:
            self._result.set_exception(error)
        except InvalidStateError:
            pass

    def _run(self) -> None:
        """Run the captured coroutine on one explicitly owned event loop."""
        loop: asyncio.AbstractEventLoop | None = None
        task: asyncio.Task[Any] | None = None
        try:
            try:
                loop = asyncio.new_event_loop()
                asyncio.set_event_loop(loop)
                task = self._context.run(loop.create_task, self._coro)
            except BaseException as exc:
                if task is None:
                    try:
                        _close_coroutine(self._coro)
                    except BaseException as cleanup_error:
                        add_bounded_note(
                            exc, "async-bridge coroutine cleanup during loop setup", cleanup_error
                        )
                self._publish_error(exc)
                return
            with self._lock:
                self._loop = loop
                self._task = task
                cancel_requested = self._cancel_requested
            if cancel_requested:
                task.cancel()
            try:
                value = loop.run_until_complete(task)
            except asyncio.CancelledError as exc:
                self._publish_error(exc)
            except BaseException as exc:
                self._publish_error(exc)
            else:
                self._publish_result(value)
        finally:
            try:
                if loop is not None:
                    # Cancellation cleanup can create more Tasks from finally
                    # blocks. Drain to a fixed point instead of trusting one
                    # all_tasks() snapshot. A resistant Task deliberately keeps
                    # this governed daemon and its lease alive.
                    with self._lock:
                        if self._drain_deadline_ns == 0:
                            self._drain_deadline_ns = deadline_ns_from_timeout(
                                5.0, name="async bridge terminal drain deadline"
                            )
                        drain_deadline_ns = self._drain_deadline_ns
                    drain_round = 0
                    while monotonic_ns() < drain_deadline_ns:
                        pending = tuple(task for task in asyncio.all_tasks(loop) if not task.done())
                        if not pending:
                            break
                        for pending_task in pending:
                            pending_task.cancel()
                        remaining = remaining_seconds(drain_deadline_ns)
                        loop.run_until_complete(
                            asyncio.wait(
                                pending,
                                timeout=min(0.05, max(0.001, remaining)),
                            )
                        )
                        drain_round += 1
                        if drain_round >= 32:
                            # A chain of cancellation finalizers can repeatedly
                            # create successor tasks. Yield with bounded
                            # exponential backoff to avoid a CPU-spin shutdown
                            # while retaining the governed host and lease.
                            delay = min(
                                0.05,
                                0.001 * (2 ** min((drain_round - 32) // 8, 6)),
                            )
                            loop.run_until_complete(asyncio.sleep(delay))
                    pending_after_deadline = tuple(
                        pending_task
                        for pending_task in asyncio.all_tasks(loop)
                        if not pending_task.done()
                    )
                    if pending_after_deadline:
                        # Hard deadline means hard stop: never enter another
                        # unbounded cancellation loop. Retain the loop/task graph
                        # in bounded fail-closed parking for diagnostics.
                        for pending_task in pending_after_deadline:
                            pending_task.cancel()
                        with self._lock:
                            self._terminal_loop = loop
                            self._terminal_tasks = pending_after_deadline
                            self._terminal_non_cooperative = True
                        _retain_failed_bridge_runner(self)
                    else:
                        loop.close()
            finally:
                with self._lock:
                    self._task = None
                    self._loop = None
                    terminal = self._terminal_non_cooperative
                lease = self._lease
                registration = self._runtime_registration
                if lease is not None:
                    try:
                        retired = defer_governed_thread_retirement(
                            self._thread,
                            lease.release,
                            registration=None if terminal else registration,
                        )
                    except BaseException:
                        retired = False
                    if retired:
                        with self._lock:
                            if self._lease is lease:
                                self._lease = None
                            if not terminal and self._runtime_registration is registration:
                                self._runtime_registration = None
                        cancel_retry(("async-bridge-thread-lease", id(self)))
                    else:
                        _retain_failed_bridge_runner(self)
                elif registration is not None and not terminal:
                    try:
                        registration.close()
                    except BaseException:
                        pass
                    else:
                        if self._runtime_registration is registration:
                            self._runtime_registration = None

    def close(self, *, deadline_seconds: float = 5.0) -> bool:
        """Cancel and join the bridge host within one validated deadline."""
        ensure_runtime_fork_safe()
        deadline_ns = deadline_ns_from_timeout(
            deadline_seconds, name="async bridge shutdown deadline"
        )
        with self._lock:
            # Normal execution never consumes teardown budget. Every explicit
            # close receives a fresh absolute deadline.
            self._drain_deadline_ns = deadline_ns
        self.cancel()
        if self._thread is current_thread():
            return False
        self._thread.join(timeout=remaining_seconds(deadline_ns))
        with self._lock:
            terminal_non_cooperative = self._terminal_non_cooperative
        stopped = (
            not self._thread.is_alive() and self._lease is None and not terminal_non_cooperative
        )
        if stopped:
            registration = self._runtime_registration
            self._runtime_registration = None
            if registration is not None:
                registration.close()
            self._retire_finalizer_slot()
        return stopped

    def __del__(self) -> None:
        """Transfer bridge shutdown without joins or permit locks during GC."""
        try:
            if runtime_is_finalizing() or os.getpid() != getattr(self, "_pid", os.getpid()):
                return
            ticket = getattr(self, "_finalizer_ticket", 0)
            capsule = getattr(self, "_finalizer_capsule", None)
            if ticket and capsule is not None:
                if defer_owner_finalizer_cleanup(self, capsule):
                    self._finalizer_ticket = 0
                    self._finalizer_capsule = None
        except BaseException:
            pass


def run_sync(coro: Any, *, threading_mode: str = "single") -> Any:
    """Run a coroutine without an unbounded or process-blocking helper thread."""
    mode = normalize_threading_mode(threading_mode)
    try:
        asyncio.get_running_loop()
    except RuntimeError:
        return asyncio.run(coro)
    if mode == "single":
        _close_coroutine(coro)
        raise RuntimeError(
            "A synchronous schema-sanitizer API cannot run inside an active "
            "asyncio loop with threading_mode='single' because doing so would "
            "require a helper host thread. Call it outside the loop or use "
            "threading_mode='multi'."
        )
    lease = acquire_project_threads(1, minimum=1)
    runner = _BridgeRunner(coro, copy_context(), lease)
    runner.start()
    try:
        return runner.result.result(
            timeout=bounded_wait_timeout(_DEFAULT_ASYNC_BRIDGE_TIMEOUT_SECONDS)
        )
    except FutureTimeoutError:
        runner.cancel()
        raise TimeoutError(
            "async bridge exceeded its bounded wait or the operation deadline"
        ) from None
    finally:
        # A completed result can be published a few instructions before the
        # host thread reaches its terminal retirement path. Because this is the
        # synchronous bridge, join that already-terminal host before returning
        # to the caller so the physical-thread permit and runtime registration
        # are observably retired exactly once. A timed-out/non-cooperative host
        # remains retained by the existing fail-closed terminal path.
        if runner.result.done():
            try:
                runner.close(deadline_seconds=5.0)
            except BaseException:
                pass


def _failed_bridge_runner_snapshot() -> object:
    """Return a bounded snapshot of failed bridge runner."""
    return _FAILED_BRIDGE_RUNNERS.snapshot()


from ..core_impl.shutdown_observers import (  # noqa: E402
    register_shutdown_observer as _register_shutdown_observer,
)

_register_shutdown_observer("failed_bridge_runners", _failed_bridge_runner_snapshot)


__all__ = ["run_sync"]


def _reset_failed_bridge_registry_after_fork() -> None:
    """Reset failed bridge registry after fork."""
    global _FAILED_BRIDGE_RUNNERS_LOCK, _FAILED_BRIDGE_RUNNER_OWNERS
    _FAILED_BRIDGE_RUNNERS_LOCK = Lock()
    _FAILED_BRIDGE_RUNNER_OWNERS = {}


try:
    from ..core_impl.fork_manager import register_fork_handler as _register_fork_handler

    _register_fork_handler("async-bridge", mode="quarantine_only")
except BaseException:
    pass
