"""Provide small strict resource owners for lifecycle and finalization tests.

The fakes expose exact capsule, lease, and callback protocols while recording releases and
injected failures.
"""

from __future__ import annotations

from typing import Any


class CapsuleStream:
    """Expose exactly one owned Arrow C Stream capsule."""

    def __init__(self, capsule: Any) -> None:
        """Initialize the capsule stream test double."""
        self._capsule = capsule

    def __arrow_c_stream__(self, requested_schema: Any = None) -> Any:
        """Export the owned Arrow C Stream capsule."""
        del requested_schema
        return self._capsule


class CountingLease:
    """Count releases and optionally fail a fixed number of attempts."""

    def __init__(self, *, failures: int = 0, amount: int = 1) -> None:
        """Initialize the counting lease test double."""
        if failures < 0:
            raise ValueError("failures must be non-negative")
        self.amount = amount
        self.attempts = 0
        self.released = False
        self._failures = failures

    def release(self) -> None:
        """Release the resource held by the counting lease test double."""
        self.attempts += 1
        if self.attempts <= self._failures:
            raise RuntimeError("injected release failure")
        self.released = True
        self.amount = 0


class DeadThread:
    """Minimal completed-thread object for shutdown paths."""

    ident = None

    def is_alive(self) -> bool:
        """Report whether the dead thread test double is active."""
        return False

    def join(self, timeout: float | None = None) -> None:
        """Wait for the dead thread test double to finish."""
        del timeout
