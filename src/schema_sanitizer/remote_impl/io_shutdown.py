"""Deadline-aware task and provider shutdown for remote I/O loops."""

from __future__ import annotations

import asyncio
from typing import Any

from ..core_impl.durations import normalize_duration


async def shutdown_remote_io(
    context_manager: Any | None,
    timeout_seconds: float,
) -> None:
    """Cancel loop work and close its provider within one shared deadline."""
    loop = asyncio.get_running_loop()
    timeout = normalize_duration(
        timeout_seconds, name="remote I/O shutdown timeout", allow_zero=True
    )
    assert timeout is not None
    deadline = loop.time() + timeout
    current = asyncio.current_task()
    pending = [task for task in asyncio.all_tasks() if task is not current]
    for task in pending:
        task.cancel()
    stubborn: set[asyncio.Task[Any]] = set()
    if pending:
        remaining = max(0.0, deadline - loop.time())
        _done, stubborn = await asyncio.wait(pending, timeout=remaining * 0.6)

    if stubborn:
        # Keep the provider context owned and the loop alive. Closing it while
        # cancellation-resistant tasks still run can invalidate resources that
        # those tasks continue to use and makes a later retry double-exit it.
        raise RuntimeError(
            "remote I/O coordinator shutdown exceeded its deadline because "
            "tasks ignored cancellation"
        )

    if context_manager is not None:
        remaining = max(0.0, deadline - loop.time())
        if remaining <= 0:
            raise TimeoutError("provider cleanup deadline expired")
        await asyncio.wait_for(context_manager.__aexit__(None, None, None), timeout=remaining)
