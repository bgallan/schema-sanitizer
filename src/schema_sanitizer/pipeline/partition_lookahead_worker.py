"""Bounded daemon worker used by partition source lookahead."""

from __future__ import annotations

import os
from collections.abc import Callable
from concurrent.futures import Future
from dataclasses import dataclass
from queue import Empty, Full, Queue
from threading import Lock, Thread, current_thread
from typing import Any, cast

from ..core_impl.durations import deadline_ns_from_timeout, remaining_seconds
from ..core_impl.finalization import runtime_is_finalizing
from ..core_impl.fork_safety import ensure_runtime_fork_safe
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
            start_thread = getattr(cast(Any, registration), "start_thread", None)
            if callable(start_thread):
                start_thread(self._thread)
                started = True
            else:  # compatibility for narrow test/control doubles
                self._thread.start()
                started = True
                registration.activate()
        except BaseException as exc:
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
            raise

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
        """Close admission while preserving the historical executor API."""
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
        self._retry_stopped_thread_lease()
        stopped = bool(thread is not None and not thread.is_alive() and self._thread_lease is None)
        if stopped:
            registration = self._runtime_registration
            self._runtime_registration = None
            if registration is not None:
                registration.close()
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
            released = self._release_thread_lease_owner()
            if released:
                registration = self._runtime_registration
                if registration is not None:
                    try:
                        registration.close()
                    except BaseException as exc:
                        clear_exception_traceback(exc)
                    else:
                        if self._runtime_registration is registration:
                            self._runtime_registration = None

    def __del__(self) -> None:
        """Retry a stopped worker permit outside child/finalization teardown."""
        try:
            if runtime_is_finalizing() or os.getpid() != getattr(self, "_pid", os.getpid()):
                return
            self._retry_stopped_thread_lease()
        except BaseException:
            pass


__all__ = ["ThreadPoolExecutor"]
