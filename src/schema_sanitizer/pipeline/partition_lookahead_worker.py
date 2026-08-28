"""Bounded daemon worker used by partition source lookahead.

Its one-slot queue and single governed daemon provide speculative preparation with bounded
backlog, fork rejection, and deadline-aware shutdown.
"""

from __future__ import annotations

import os
from collections.abc import Callable
from concurrent.futures import Future
from dataclasses import dataclass
from queue import Empty, Full, Queue
from threading import Lock, Thread, current_thread
from typing import Any

from ..core_impl.durations import deadline_ns_from_timeout, remaining_seconds
from ..core_impl.finalization import runtime_is_finalizing
from ..core_impl.finalizer_cleanup import (
    PreparedFinalizerCleanup,
    cancel_prepared_finalizer_cleanup,
    defer_owner_finalizer_cleanup,
    reserve_owner_finalizer_cleanup,
)
from ..core_impl.fork_safety import ensure_runtime_fork_safe
from ..core_impl.governed_thread import (
    reap_governed_thread_retirements,
    retire_governed_runtime_thread,
    start_governed_thread,
)
from ..core_impl.process_resources import acquire_project_threads
from ..core_impl.retry_scheduler import adopt_failed_release
from ..core_impl.runtime_registry import reserve_runtime_service
from ..core_impl.safe_errors import add_bounded_note, clear_exception_traceback


@dataclass(slots=True)
class _DaemonTask:
    """One bounded task owned by the daemon lookahead host."""

    future: Future[Any]
    function: Callable[..., Any]
    args: tuple[Any, ...]
    kwargs: dict[str, Any]


class ThreadPoolExecutor:
    """One-slot daemon executor with bounded and reentrant shutdown semantics."""

    def __init__(
        self,
        *,
        max_workers: int,
        thread_name_prefix: str,
        permit_factory: Callable[..., Any] = acquire_project_threads,
    ) -> None:
        """Start exactly one daemon host with one pending-task slot."""
        if max_workers != 1:
            raise ValueError("partition lookahead supports exactly one worker")
        self._finalizer_capsule: PreparedFinalizerCleanup | None = reserve_owner_finalizer_cleanup()
        self._finalizer_ticket = self._finalizer_capsule.ticket
        self._pid = os.getpid()
        self._queue: Queue[_DaemonTask] = Queue(maxsize=1)
        self._lock = Lock()
        self._closed = False
        self._runtime_registration = None
        self._thread_lease: Any | None = permit_factory(1, minimum=1)
        started = False
        try:
            self._runtime_registration = reserve_runtime_service(
                self, kind="partition_lookahead", close_name="_runtime_shutdown"
            )
            self._thread = Thread(
                target=self._run,
                name=f"{thread_name_prefix}_0",
                daemon=True,
            )
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
                # The worker has consumed the permit. Preserve its reserved
                # registry slot and request terminal drain instead of releasing
                # capacity while the thread is still alive.
                with self._lock:
                    self._closed = True
                add_bounded_note(
                    exc,
                    "partition-lookahead host retained after registration activation failure",
                    RuntimeError("live host ownership preserved"),
                )
                raise
            rollback_registration = self._runtime_registration
            self._runtime_registration = None
            if rollback_registration is not None:
                rollback_registration.close()
            lease = self._thread_lease
            if lease is not None:
                try:
                    lease.release()
                except BaseException as cleanup_error:
                    add_bounded_note(
                        exc, "partition-lookahead thread permit rollback", cleanup_error
                    )
                else:
                    self._thread_lease = None
            self._retire_finalizer_slot()
            raise

    def _retire_finalizer_slot(self) -> None:
        """Retire the finalizer escrow slot owned by this thread pool executor."""
        ticket = self._finalizer_ticket
        capsule = self._finalizer_capsule
        if ticket and capsule is not None:
            cancel_prepared_finalizer_cleanup(capsule)
            self._finalizer_ticket = 0
            self._finalizer_capsule = None

    def submit(
        self,
        function: Callable[..., Any],
        /,
        *args: Any,
        **kwargs: Any,
    ) -> Future[Any]:
        """Queue one task without allowing an unbounded pending backlog."""
        ensure_runtime_fork_safe()
        if os.getpid() != getattr(self, "_pid", os.getpid()):
            raise RuntimeError("partition lookahead worker cannot be reused after fork")
        future: Future[Any] = Future()
        task = _DaemonTask(future, function, args, kwargs)
        with self._lock:
            if self._closed:
                raise RuntimeError("partition lookahead worker is closed")
            try:
                self._queue.put_nowait(task)
            except Full:
                raise RuntimeError("partition lookahead worker queue is full") from None
        return future

    def shutdown(
        self,
        *,
        wait: bool = True,
        cancel_futures: bool = False,
    ) -> None:
        """Close worker admission and optionally wait for queued work."""
        deadline = 5.0 if wait else 0.0
        self._close(
            deadline_seconds=deadline,
            cancel_futures=cancel_futures,
        )

    def _runtime_shutdown(self, *, deadline_seconds: float) -> bool:
        """Participate in the process-wide absolute-deadline shutdown."""
        return self._close(
            deadline_seconds=deadline_seconds,
            cancel_futures=True,
        )

    def _close(
        self,
        *,
        deadline_seconds: float,
        cancel_futures: bool,
    ) -> bool:
        """Close this thread pool executor and release its retained resources."""
        if os.getpid() != getattr(self, "_pid", os.getpid()):
            return False
        ensure_runtime_fork_safe()
        deadline_ns = deadline_ns_from_timeout(
            deadline_seconds, name="partition lookahead shutdown deadline"
        )
        with self._lock:
            self._closed = True
        if cancel_futures:
            while True:
                try:
                    task = self._queue.get_nowait()
                except Empty:
                    break
                task.future.cancel()
                del task
        thread = getattr(self, "_thread", None)
        if thread is not None and thread is not current_thread():
            thread.join(timeout=remaining_seconds(deadline_ns))
        # The worker's terminal finally transfers the permit into retirement
        # debt before returning. Once join proves physical death, that debt is
        # the sole authority allowed to release/adopt the permit; a direct
        # retry here would race the debt and can double-release the same lease.
        reap_governed_thread_retirements()
        stopped = bool(thread is not None and not thread.is_alive() and self._thread_lease is None)
        if stopped:
            registration = self._runtime_registration
            self._runtime_registration = None
            if registration is not None:
                registration.close()
            self._retire_finalizer_slot()
        return stopped

    def _release_thread_lease_owner(self) -> bool:
        """Release or transfer the exact worker permit without dropping it."""
        lease = self._thread_lease
        if lease is None:
            return True
        try:
            lease.release()
        except BaseException:
            try:
                adopted = adopt_failed_release(lease, retained_bytes=256)
            except BaseException:
                adopted = False
            if not adopted:
                return False
        if self._thread_lease is lease:
            self._thread_lease = None
        return True

    def _retry_stopped_thread_lease(self) -> None:
        """Return a retained permit only after its worker has actually exited."""
        if os.getpid() != getattr(self, "_pid", os.getpid()):
            return
        thread = getattr(self, "_thread", None)
        if thread is not None and thread.is_alive():
            return
        self._release_thread_lease_owner()

    def _run(self) -> None:
        """Execute queued work until closed and fully drained."""
        try:
            while True:
                try:
                    task = self._queue.get(timeout=0.05)
                except Empty:
                    with self._lock:
                        if self._closed:
                            return
                    continue
                try:
                    if not task.future.set_running_or_notify_cancel():
                        continue
                    try:
                        result = task.function(*task.args, **task.kwargs)
                    except BaseException as exc:
                        task.future.set_exception(exc)
                    else:
                        task.future.set_result(result)
                finally:
                    del task
        finally:
            registration = self._runtime_registration
            try:
                retired = retire_governed_runtime_thread(
                    getattr(self, "_thread", None),
                    registration,
                    self._release_thread_lease_owner,
                    terminal_from_current=True,
                )
            except BaseException as exc:
                clear_exception_traceback(exc)
                retired = False
            if retired and self._runtime_registration is registration:
                self._runtime_registration = None

    def _finalizer_cleanup_from_escrow(self) -> None:
        """Retry a stopped worker permit from a governed cleanup path."""
        self._retry_stopped_thread_lease()

    def __del__(self) -> None:
        """Transfer stopped-worker cleanup without taking locks during GC."""
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


__all__ = ["ThreadPoolExecutor"]
