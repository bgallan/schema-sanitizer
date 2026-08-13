"""Retryable ownership transfer for asynchronously staged remote results."""

from __future__ import annotations

import os
import threading
from time import monotonic
from typing import Any

from ..core_impl.finalization import runtime_is_finalizing
from ..core_impl.finalizer_cleanup import (
    PreparedFinalizerCleanup,
    cancel_prepared_finalizer_cleanup,
    defer_prepared_finalizer_cleanup,
    reserve_detached_resources_finalizer_cleanup,
)
from ..core_impl.resource_lifecycle import _close_suppressing_errors

_CLEANUP_WAIT_SECONDS = 30.0


class StagedResultOwnership:
    """Transfer one staged result exactly once without concurrent double-close."""

    def __init__(self) -> None:
        """Create an empty, live ownership slot."""
        capsule = reserve_detached_resources_finalizer_cleanup()
        ticket = capsule.ticket
        self._finalizer_ticket = ticket
        self._finalizer_capsule: PreparedFinalizerCleanup | None = capsule
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

    def _claim_cleanup_locked(self) -> Any | None:
        """Claim the published object without allocating after inflight commit."""
        self._wait_for_cleanup_locked()
        staged = self._staged
        if staged is None:
            return None
        if self._cleanup_generation >= (1 << 63) - 1:
            raise RuntimeError("staged-result cleanup generation exhausted")
        # Prepare the allocating PyLong before publishing the inflight latch.
        next_generation = self._cleanup_generation + 1
        try:
            self._cleanup_generation = next_generation
            self._cleanup_inflight = True
        except BaseException:
            self._cleanup_inflight = False
            raise
        return staged

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
            staged = self._claim_cleanup_locked()
            generation = self._cleanup_generation
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

    def _retire_terminal_finalizer(self) -> None:
        """Return the prepared finalizer slot once no staged owner remains."""
        capsule = self._finalizer_capsule
        if capsule is None or self._staged is not None:
            return
        try:
            cancel_prepared_finalizer_cleanup(capsule)
        except BaseException:
            # Keep exact capsule authority for an explicit retry / GC fallback.
            return
        self._finalizer_ticket = 0
        self._finalizer_capsule = None

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
                consumed = staged
            else:
                consumed = None
        if consumed is not None or staged is None:
            self._retire_terminal_finalizer()
            return consumed
        self._cleanup_published()
        self._retire_terminal_finalizer()
        return None

    def abandon(self) -> bool:
        """Abandon delivery and serialize every cleanup attempt."""
        if os.getpid() != self._pid:
            return True
        with self._condition:
            self._abandoned = True
        succeeded = self._cleanup_published()
        if succeeded:
            self._retire_terminal_finalizer()
        return succeeded

    def __del__(self) -> None:
        """Retry abandoned cleanup outside fork children and interpreter teardown."""
        try:
            if runtime_is_finalizing() or os.getpid() != getattr(self, "_pid", os.getpid()):
                return
            ticket = getattr(self, "_finalizer_ticket", 0)
            capsule = getattr(self, "_finalizer_capsule", None)
            if ticket and capsule is not None:
                capsule.arg0 = getattr(self, "_staged", None)
                if defer_prepared_finalizer_cleanup(capsule):
                    self._staged = None
                    self._finalizer_ticket = 0
                    self._finalizer_capsule = None
        except BaseException:
            pass


__all__ = ["StagedResultOwnership"]
