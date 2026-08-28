"""Provide deterministic synchronization primitives for runner-independent concurrency tests.

Observable conditions and events establish scheduling handshakes while retaining a bounded
timeout only as a deadlock fuse.
"""

from __future__ import annotations

from threading import Condition, Event, Lock
from typing import Any

# Deadlock fuse for scheduling another test thread on a contended CI host.
# Correctness must still be established by an Event/Condition/Barrier handshake.
SCHEDULER_TIMEOUT_SECONDS = 10.0


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
]
