"""Small strict resource owners shared by lifecycle tests."""

from __future__ import annotations

from typing import Any


class CapsuleStream:
    """Expose exactly one owned Arrow C Stream capsule."""

    def __init__(self, capsule: Any) -> None:
        self._capsule = capsule

    def __arrow_c_stream__(self, requested_schema: Any = None) -> Any:
        del requested_schema
        return self._capsule


class CountingLease:
    """Count releases and optionally fail a fixed number of attempts."""

    def __init__(self, *, failures: int = 0, amount: int = 1) -> None:
        if failures < 0:
            raise ValueError("failures must be non-negative")
        self.amount = amount
        self.attempts = 0
        self.released = False
        self._failures = failures

    def release(self) -> None:
        self.attempts += 1
        if self.attempts <= self._failures:
            raise RuntimeError("injected release failure")
        self.released = True
        self.amount = 0


class DeadThread:
    """Minimal completed-thread object for shutdown paths."""

    ident = None

    def is_alive(self) -> bool:
        return False

    def join(self, timeout: float | None = None) -> None:
        del timeout
