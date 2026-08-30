"""Deadline-aware task and provider shutdown for remote I/O loops.

It cancels outstanding tasks to a deadline, closes provider owners and the loop, and
retains failed cleanup under one retryable owner.
"""

from __future__ import annotations

import asyncio
from typing import Any

from ..core_impl.durations import normalize_duration


class RemoteIoCleanupOwner:
    """Own exactly one provider ``__aexit__`` Task across bounded retries.

    A timeout never loses the Task identity.  A later shutdown attempt observes
    the same physical cleanup first and starts another exit only when the prior
    Task was definitively cancelled/failed without committing cleanup.
    """

    __slots__ = ("task", "manager", "generation")

    def __init__(self) -> None:
        """Start without a retained provider-exit task or manager generation."""
        self.task: asyncio.Task[Any] | None = None
        self.manager: Any | None = None
        self.generation = 0

    async def close(self, manager: Any, *, deadline: float) -> None:
        """Start or resume the exact provider-exit task within the shared shutdown deadline."""
        loop = asyncio.get_running_loop()
        task = self.task
        if task is not None and self.manager is not manager:
            if not task.done():
                raise RuntimeError("provider cleanup owner already owns another context")
            self.task = None
            self.manager = None
            task = None
        if task is None:
            self.manager = manager
            self.generation += 1
            task = loop.create_task(manager.__aexit__(None, None, None))
            self.task = task

        remaining = max(0.0, deadline - loop.time())
        if remaining <= 0:
            raise TimeoutError("provider cleanup deadline expired")
        done, _ = await asyncio.wait({task}, timeout=remaining)
        if task not in done:
            # Cancellation is advisory only.  Keep the exact task rooted because
            # provider cleanup may ignore CancelledError and still commit later.
            task.cancel()
            raise TimeoutError("provider cleanup deadline expired")

        try:
            await task
        except asyncio.CancelledError:
            # Cancellation reached the provider before a successful exit commit.
            # Retain manager ownership but allow a future attempt to create a
            # fresh Task rather than treating cancellation as quiescence.
            self.task = None
            self.manager = manager
            raise
        except BaseException:
            self.task = None
            self.manager = manager
            raise
        else:
            self.task = None
            self.manager = None


async def shutdown_remote_io(
    context_manager: Any | None,
    timeout_seconds: float,
    *,
    cleanup_owner: RemoteIoCleanupOwner | None = None,
) -> None:
    """Cancel loop work and close its provider within one shared deadline."""
    loop = asyncio.get_running_loop()
    timeout = normalize_duration(
        timeout_seconds, name="remote I/O shutdown timeout", allow_zero=True
    )
    if timeout is None:
        raise AssertionError("validated remote I/O shutdown timeout cannot be absent")
    deadline = loop.time() + timeout
    current = asyncio.current_task()
    pending = [task for task in asyncio.all_tasks() if task is not current]
    # Do not cancel the provider cleanup Task retained from a previous bounded
    # attempt; it is the authoritative physical-cleanup owner.
    retained_task = cleanup_owner.task if cleanup_owner is not None else None
    pending = [task for task in pending if task is not retained_task]
    for task in pending:
        task.cancel()
    stubborn: set[asyncio.Task[Any]] = set()
    if pending:
        remaining = max(0.0, deadline - loop.time())
        _done, stubborn = await asyncio.wait(pending, timeout=remaining * 0.6)

    if stubborn:
        raise RuntimeError(
            "remote I/O coordinator shutdown exceeded its deadline because "
            "tasks ignored cancellation"
        )

    if context_manager is not None:
        owner = cleanup_owner or RemoteIoCleanupOwner()
        await owner.close(context_manager, deadline=deadline)


__all__ = ["RemoteIoCleanupOwner", "shutdown_remote_io"]
