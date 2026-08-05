"""Retryable ownership transfer for asynchronously staged remote results."""

from __future__ import annotations

import os
import threading
from time import monotonic
from typing import Any

from ..core_impl.finalization import runtime_is_finalizing
from ..core_impl.resource_lifecycle import _close_suppressing_errors

_CLEANUP_WAIT_SECONDS = 30.0


class StagedResultOwnership:
    """Transfer one staged result exactly once without concurrent double-close."""

    def __init__(self) -> None:
        """Create an empty, live ownership slot."""
        self._pid = os.getpid()
        self._condition = threading.Condition()
        # Compatibility alias retained for older fault-injection tests.
        self._lock = self._condition
        self._staged: Any | None = None
        self._abandoned = False
        self._cleanup_inflight = False
        self._cleanup_generation = 0
        self._cleanup_succeeded = True

    def _assert_owner_process(self) -> None:
        """Reject inherited direct use before touching the parent lock."""
        if os.getpid() != getattr(self, "_pid", os.getpid()):
            raise RuntimeError("staged-result ownership cannot be reused after fork")

    def _wait_for_cleanup_locked(self) -> None:
        """Wait boundedly for one earlier cleanup generation."""
        deadline = monotonic() + _CLEANUP_WAIT_SECONDS
        while self._cleanup_inflight:
            remaining = deadline - monotonic()
            if remaining <= 0 or not self._condition.wait(timeout=remaining):
                raise RuntimeError("staged-result cleanup generation exceeded its deadline")

    def _claim_cleanup_locked(self) -> tuple[Any | None, int]:
        """Claim the published object after any earlier cleanup generation ends."""
        self._wait_for_cleanup_locked()
        staged = self._staged
        if staged is None:
            return None, self._cleanup_generation
        self._cleanup_inflight = True
        self._cleanup_generation += 1
        return staged, self._cleanup_generation

    def _finish_cleanup(self, staged: Any, generation: int, succeeded: bool) -> bool:
        """Commit one cleanup generation while retaining failed ownership."""
        with self._condition:
            if generation != self._cleanup_generation:
                raise RuntimeError("staged-result cleanup generation changed unexpectedly")
            if succeeded and self._staged is staged:
                self._staged = None
            elif not succeeded and self._staged is None:
                self._staged = staged
            self._cleanup_succeeded = succeeded
            self._cleanup_inflight = False
            self._condition.notify_all()
        return succeeded

    def _cleanup_published(self) -> bool:
        """Close the currently published result through one serialized generation."""
        with self._condition:
            staged, generation = self._claim_cleanup_locked()
        if staged is None:
            return True
        succeeded = _close_suppressing_errors(staged)
        return self._finish_cleanup(staged, generation, succeeded)

    def publish(self, staged: Any | None) -> Any | None:
        """Publish a completed result or self-clean after abandonment."""
        self._assert_owner_process()
        with self._condition:
            self._wait_for_cleanup_locked()
            if self._staged is not None:
                raise RuntimeError("staged-result ownership already contains a result")
            self._staged = staged
            abandoned = self._abandoned
        if not abandoned:
            return staged
        self._cleanup_published()
        return None

    def consume(self, staged: Any | None) -> Any | None:
        """Transfer exactly the published result to the iterator consumer."""
        self._assert_owner_process()
        with self._condition:
            self._wait_for_cleanup_locked()
            if not self._abandoned:
                if self._staged is not staged:
                    raise RuntimeError(
                        "staged-result consumer did not present the published object"
                    )
                self._staged = None
                return staged
        self._cleanup_published()
        return None

    def abandon(self) -> bool:
        """Abandon delivery and serialize every cleanup attempt."""
        if os.getpid() != self._pid:
            return True
        with self._condition:
            self._abandoned = True
        return self._cleanup_published()

    def __del__(self) -> None:
        """Retry abandoned cleanup outside fork children and interpreter teardown."""
        try:
            if runtime_is_finalizing() or os.getpid() != getattr(self, "_pid", os.getpid()):
                return
            self.abandon()
        except BaseException:
            pass


__all__ = ["StagedResultOwnership"]
