"""Provide cooperative operation cancellation and monotonic deadlines.

Context-scoped tokens combine explicit cancellation with optional deadlines and make sleeps and
future waits interruptible without exposing wall-clock drift.
"""

from __future__ import annotations

import os
from contextlib import contextmanager
from contextvars import ContextVar
from dataclasses import dataclass, field
from threading import Event
from time import monotonic, sleep
from typing import Iterator, Protocol

from ..errors import SchemaSanitizerCancelledError
from .durations import normalize_duration
from .fork_safety import quarantine_inherited_state


class _EventLike(Protocol):
    """Minimal event contract accepted by the public cancellation scope."""

    def is_set(self) -> bool:
        """Return whether cancellation was requested."""


@dataclass(slots=True)
class OperationCancellationToken:
    """Thread-safe cooperative cancellation token with an optional deadline."""

    deadline: float | None = None
    external_event: _EventLike | None = None
    _event: Event = field(default_factory=Event, init=False, repr=False)
    _parent: "OperationCancellationToken | None" = field(default=None, repr=False)

    def cancel(self) -> None:
        """Request cancellation from any thread."""
        self._event.set()

    def _cancellation_reason(self) -> str | None:
        """Walk parent state iteratively and fail closed on malformed cycles."""
        current: OperationCancellationToken | None = self
        slow: OperationCancellationToken | None = self
        fast: OperationCancellationToken | None = self
        while current is not None:
            if current._event.is_set():
                return "event"
            if current.external_event is not None and current.external_event.is_set():
                return "event"
            if current.deadline is not None and monotonic() >= current.deadline:
                return "deadline"
            current = current._parent

            slow = None if slow is None else slow._parent
            if fast is not None and fast._parent is not None:
                fast = fast._parent._parent
            else:
                fast = None
            if fast is not None and fast is slow:
                return "event"
        return None

    def cancelled(self) -> bool:
        """Return whether cancellation or the deadline has fired."""
        return self._cancellation_reason() is not None

    def remaining_seconds(self, default: float | None = None) -> float | None:
        """Return the bounded remaining duration for one blocking wait."""
        if self.deadline is None:
            return default
        remaining = max(0.0, self.deadline - monotonic())
        return remaining if default is None else min(max(0.0, default), remaining)

    def raise_if_cancelled(self, *, stage: str = "operation") -> None:
        """Raise the stable public cancellation error when cancellation fired."""
        reason = self._cancellation_reason()
        if reason is None:
            return
        raise SchemaSanitizerCancelledError(
            f"schema-sanitizer operation cancelled during {stage}",
            detail={"stage": stage, "reason": reason},
        )


_CURRENT_TOKEN: ContextVar[OperationCancellationToken | None] = ContextVar(
    "schema_sanitizer_operation_cancellation", default=None
)


@contextmanager
def operation_cancellation(
    *,
    timeout_seconds: float | None = None,
    cancellation_event: _EventLike | None = None,
) -> Iterator[OperationCancellationToken]:
    """Apply one cooperative deadline/event to all nested public operations."""
    timeout = normalize_duration(
        timeout_seconds,
        name="timeout_seconds",
        allow_none=True,
        allow_zero=False,
    )
    parent = _CURRENT_TOKEN.get()
    deadline = None if timeout is None else monotonic() + timeout
    if parent is not None and parent.deadline is not None:
        deadline = parent.deadline if deadline is None else min(deadline, parent.deadline)
    token = OperationCancellationToken(
        deadline=deadline, external_event=cancellation_event, _parent=parent
    )
    owner_pid = os.getpid()
    context_token = _CURRENT_TOKEN.set(token)
    try:
        token.raise_if_cancelled(stage="operation_start")
        yield token
    finally:
        # Work spawned inside the scope may outlive the caller. Mark the token
        # cancelled before restoring the parent context so leaked workers observe
        # scope exit instead of continuing detached from the operation lifetime.
        token.cancel()
        if os.getpid() == owner_pid:
            _CURRENT_TOKEN.reset(context_token)
        else:
            _CURRENT_TOKEN.set(None)


@contextmanager
def activate_operation_cancellation_token(
    token: OperationCancellationToken | None,
) -> Iterator[OperationCancellationToken | None]:
    """Propagate one captured token into a worker thread or event loop."""
    owner_pid = os.getpid()
    context_token = _CURRENT_TOKEN.set(token)
    try:
        yield token
    finally:
        if os.getpid() == owner_pid:
            _CURRENT_TOKEN.reset(context_token)
        else:
            _CURRENT_TOKEN.set(None)


_FORK_CURRENT_TOKEN_BANKS: tuple[ContextVar[OperationCancellationToken | None], ...] = (
    ContextVar[OperationCancellationToken | None](
        "schema_sanitizer_cancellation_token_child_0", default=None
    ),
    ContextVar[OperationCancellationToken | None](
        "schema_sanitizer_cancellation_token_child_1", default=None
    ),
)
_FORK_CURRENT_TOKEN_BANK_INDEX = 0
_FORK_PREPARED_CURRENT_TOKEN: ContextVar[OperationCancellationToken | None] | None = None


def _prepare_cancellation_token_for_fork() -> None:
    """Prepare cancellation token for fork."""
    global _FORK_PREPARED_CURRENT_TOKEN
    _FORK_PREPARED_CURRENT_TOKEN = _FORK_CURRENT_TOKEN_BANKS[_FORK_CURRENT_TOKEN_BANK_INDEX]


def _clear_cancellation_token_fork_preparation() -> None:
    """Clear cancellation token fork preparation."""
    global _FORK_PREPARED_CURRENT_TOKEN
    _FORK_PREPARED_CURRENT_TOKEN = None


def _reset_current_token_after_fork() -> None:
    """Swap to a preallocated empty child ContextVar without decrefing owners."""
    global _CURRENT_TOKEN, _FORK_PREPARED_CURRENT_TOKEN, _FORK_CURRENT_TOKEN_BANK_INDEX
    prepared = _FORK_PREPARED_CURRENT_TOKEN
    if prepared is None:
        return
    inherited = _CURRENT_TOKEN.get()
    if inherited is not None:
        quarantine_inherited_state("cancellation-token", inherited, _CURRENT_TOKEN)
    _CURRENT_TOKEN = prepared
    _FORK_PREPARED_CURRENT_TOKEN = None
    _FORK_CURRENT_TOKEN_BANK_INDEX = 1 - _FORK_CURRENT_TOKEN_BANK_INDEX


from .fork_manager import register_fork_handler as _register_fork_handler  # noqa: E402

_register_fork_handler(
    "cancellation-token",
    before=_prepare_cancellation_token_for_fork,
    after_in_parent=_clear_cancellation_token_fork_preparation,
    after_in_child=_reset_current_token_after_fork,
)


def current_operation_cancellation_token() -> OperationCancellationToken | None:
    """Return the active cooperative cancellation token, if any."""
    return _CURRENT_TOKEN.get()


def check_operation_cancelled(*, stage: str) -> None:
    """Raise when cancelled and publish that a real cancellation checkpoint ran."""
    token = _CURRENT_TOKEN.get()
    if token is not None:
        token.raise_if_cancelled(stage=stage)
    try:
        from .concurrency_contracts import observe_runtime_concurrency_contract_noexcept

        observe_runtime_concurrency_contract_noexcept("operation_cancellation_checkpoint")
    except BaseException:
        pass


from .concurrency_contracts import (  # noqa: E402
    register_runtime_concurrency_contract as _register_runtime_concurrency_contract,
)

_register_runtime_concurrency_contract(
    "operation_cancellation_checkpoint", check_operation_cancelled
)


async def cancellable_async_sleep(seconds: float, *, stage: str) -> None:
    """Sleep in short cancellable slices bounded by the active deadline."""
    import asyncio

    normalized = normalize_duration(seconds, name=f"{stage} sleep duration", allow_zero=True)
    assert normalized is not None
    remaining = normalized
    while remaining > 0:
        check_operation_cancelled(stage=stage)
        slice_seconds = bounded_wait_timeout(min(0.1, remaining))
        if slice_seconds is not None and slice_seconds <= 0:
            check_operation_cancelled(stage=stage)
        sleep_seconds = 0.1 if slice_seconds is None else slice_seconds
        started = monotonic()
        await asyncio.sleep(sleep_seconds)
        # Subtract at least the requested slice. Spurious event loops may return
        # immediately; they must not turn cancellation polling
        # into an unbounded busy loop. Real scheduling delays are also honoured.
        remaining = max(0.0, remaining - max(sleep_seconds, monotonic() - started))
    check_operation_cancelled(stage=stage)


def cancellable_sleep(seconds: float, *, stage: str) -> None:
    """Block in short cancellable slices bounded by the active deadline."""
    normalized = normalize_duration(seconds, name=f"{stage} sleep duration", allow_zero=True)
    assert normalized is not None
    remaining = normalized
    while remaining > 0:
        check_operation_cancelled(stage=stage)
        slice_seconds = bounded_wait_timeout(min(0.1, remaining))
        if slice_seconds is not None and slice_seconds <= 0:
            check_operation_cancelled(stage=stage)
        sleep_seconds = 0.1 if slice_seconds is None else slice_seconds
        started = monotonic()
        sleep(sleep_seconds)
        remaining = max(0.0, remaining - max(sleep_seconds, monotonic() - started))
    check_operation_cancelled(stage=stage)


async def await_cancellable_future(future, *, stage: str, poll_seconds: float = 0.05, on_poll=None):
    """Await one Future while deadlines/events remain able to wake the waiter.

    ``Future`` completion alone cannot wake a task when the operation deadline
    expires or an external cancellation event fires.  Poll only the cancellation
    token at a short bounded cadence without cancelling the underlying Future;
    the caller retains authoritative rollback of any queued/granted resource.
    """
    import asyncio

    token = _CURRENT_TOKEN.get()
    if token is None and on_poll is None:
        return await future
    while True:
        if token is not None:
            token.raise_if_cancelled(stage=stage)
        if on_poll is not None:
            on_poll()
        if future.done():
            return await future
        remaining = None if token is None else token.remaining_seconds(poll_seconds)
        if remaining is not None and remaining <= 0:
            assert token is not None
            token.raise_if_cancelled(stage=stage)
        timeout = poll_seconds if remaining is None else max(0.001, remaining)
        done, _pending = await asyncio.wait(
            (future,), timeout=timeout, return_when=asyncio.FIRST_COMPLETED
        )
        if done:
            return await future


def bounded_wait_timeout(default_seconds: float | None) -> float | None:
    """Clamp one validated wait timeout to the active operation deadline."""
    normalized = normalize_duration(
        default_seconds,
        name="wait timeout",
        allow_none=True,
        allow_zero=True,
    )
    token = _CURRENT_TOKEN.get()
    if token is None:
        return normalized
    token.raise_if_cancelled(stage="wait")
    return token.remaining_seconds(normalized)


__all__ = [
    "OperationCancellationToken",
    "activate_operation_cancellation_token",
    "await_cancellable_future",
    "bounded_wait_timeout",
    "cancellable_async_sleep",
    "cancellable_sleep",
    "check_operation_cancelled",
    "current_operation_cancellation_token",
    "operation_cancellation",
]
