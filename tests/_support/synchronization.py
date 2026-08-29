"""Provide deterministic synchronization primitives for runner-independent concurrency tests.

Observable conditions and events establish scheduling handshakes while retaining a bounded
timeout only as a deadlock fuse.
"""

from __future__ import annotations

import os
import signal
from threading import Condition, Event, Lock
from time import monotonic
from typing import Any, Protocol

# Deadlock fuse for scheduling another test thread on a contended CI host.
# Correctness must still be established by an Event/Condition/Barrier handshake.
SCHEDULER_TIMEOUT_SECONDS = 10.0
_WAIT_NOHANG = getattr(os, "WNOHANG", None)


class _WaitableSignal(Protocol):
    """Describe an event-like synchronization signal used by tests."""

    def wait(self, timeout: float | None = None) -> bool:
        """Wait for the signal and report whether it arrived."""
        ...


class _JoinableThread(Protocol):
    """Describe a thread-like worker with bounded join support."""

    def join(self, timeout: float | None = None) -> None:
        """Wait up to the requested timeout for worker termination."""
        ...

    def is_alive(self) -> bool:
        """Report whether the worker is still running."""
        ...


class _JoinableProcess(_JoinableThread, Protocol):
    """Describe a child process that can be stopped after a missed deadline."""

    def terminate(self) -> None:
        """Request termination of the child process."""
        ...


def wait_event_or_fail(
    event: _WaitableSignal,
    *,
    timeout: float = SCHEDULER_TIMEOUT_SECONDS,
) -> None:
    """Fail when an expected synchronization event misses its deadlock fuse."""
    if not event.wait(timeout):
        raise TimeoutError(f"test event was not signaled within {timeout:g} seconds")


def join_thread_or_fail(
    thread: _JoinableThread,
    *,
    timeout: float = SCHEDULER_TIMEOUT_SECONDS,
) -> None:
    """Join a test thread within the deadlock fuse and require its termination."""
    thread.join(timeout=timeout)
    if thread.is_alive():
        raise TimeoutError(f"test thread did not exit within {timeout:g} seconds")


def join_process_or_fail(
    process: _JoinableProcess,
    *,
    timeout: float = SCHEDULER_TIMEOUT_SECONDS,
) -> None:
    """Join a test child by its deadline and reap it after emergency termination."""
    process.join(timeout=timeout)
    if not process.is_alive():
        return
    try:
        process.terminate()
    except ProcessLookupError:
        pass
    process.join(timeout=timeout)
    if process.is_alive():
        kill = getattr(process, "kill", None)
        if kill is not None:
            try:
                kill()
            except ProcessLookupError:
                pass
            process.join(timeout=timeout)
    if process.is_alive():
        raise TimeoutError(f"test child did not exit or terminate within {timeout * 3:g} seconds")
    raise TimeoutError(f"test child did not exit within {timeout:g} seconds")


def wait_for_process_exit(pid: int) -> int:
    """Reap one test child or terminate it after the shared deadlock fuse."""
    if _WAIT_NOHANG is None:
        raise RuntimeError("nonblocking process reaping requires POSIX waitpid support")
    deadline = monotonic() + SCHEDULER_TIMEOUT_SECONDS
    while True:
        waited_pid, status = os.waitpid(pid, _WAIT_NOHANG)
        if waited_pid == pid:
            return status
        remaining = deadline - monotonic()
        if remaining <= 0:
            try:
                os.kill(pid, getattr(signal, "SIGKILL", signal.SIGTERM))
            except ProcessLookupError:
                pass
            reap_deadline = monotonic() + SCHEDULER_TIMEOUT_SECONDS
            while True:
                waited_pid, _status = os.waitpid(pid, _WAIT_NOHANG)
                if waited_pid == pid:
                    break
                reap_remaining = reap_deadline - monotonic()
                if reap_remaining <= 0:
                    raise TimeoutError(
                        f"terminated test child {pid} could not be reaped within "
                        f"{SCHEDULER_TIMEOUT_SECONDS:g} seconds"
                    )
                Event().wait(min(0.01, reap_remaining))
            raise TimeoutError(
                f"test child {pid} did not exit within {SCHEDULER_TIMEOUT_SECONDS:g} seconds"
            )
        Event().wait(min(0.01, remaining))


class WaitObservedCondition(Condition):
    """Expose when another test thread enters or waits on a condition."""

    def __init__(self, lock: Any | None = None) -> None:
        """Initialize the wait observed condition test double."""
        super().__init__(lock)
        self.enter_observed = Event()
        self.wait_entered = Event()

    def __enter__(self) -> bool:
        """Enter the context managed by the wait observed condition test double."""
        entered = super().__enter__()
        self.enter_observed.set()
        return entered

    def wait(self, timeout: float | None = None) -> bool:
        """Wait for the wait observed condition test double to reach its terminal state."""
        self.wait_entered.set()
        return super().wait(timeout)


class ContentionObservedLock:
    """Lock wrapper exposing the first blocking acquisition attempt."""

    def __init__(self) -> None:
        """Initialize the contention observed lock test double."""
        self._lock = Lock()
        self.contention_entered = Event()

    def acquire(self, blocking: bool = True, timeout: float = -1) -> bool:
        """Acquire the resource represented by the contention observed lock test double."""
        if not blocking:
            return self._lock.acquire(False)
        if self._lock.acquire(False):
            return True
        self.contention_entered.set()
        if timeout == -1:
            return self._lock.acquire()
        return self._lock.acquire(timeout=timeout)

    def release(self) -> None:
        """Release the resource held by the contention observed lock test double."""
        self._lock.release()

    def __enter__(self) -> ContentionObservedLock:
        """Enter the context managed by the contention observed lock test double."""
        self.acquire()
        return self

    def __exit__(self, *_exc: object) -> None:
        """Exit the context managed by the contention observed lock test double and run cleanup."""
        self.release()


__all__ = [
    "ContentionObservedLock",
    "SCHEDULER_TIMEOUT_SECONDS",
    "WaitObservedCondition",
    "join_process_or_fail",
    "join_thread_or_fail",
    "wait_event_or_fail",
    "wait_for_process_exit",
]
