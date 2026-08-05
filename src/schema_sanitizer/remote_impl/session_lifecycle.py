"""Bounded lifecycle helpers for shared asynchronous download sessions."""

from __future__ import annotations

import asyncio
import os
from concurrent.futures import CancelledError, Future
from concurrent.futures import TimeoutError as FutureTimeoutError
from dataclasses import dataclass, field
from threading import Event, Lock
from time import monotonic
from typing import Any

from ..core_impl.durations import normalize_duration
from ..core_impl.safe_errors import add_bounded_note


@dataclass(slots=True)
class _SessionCloseAttempt:
    """One bridge submission plus separately committed close phases."""

    terminal: Event = field(default_factory=Event)
    exit_committed: Event = field(default_factory=Event)
    future: Future[Any] | None = None
    error: BaseException | None = None


@dataclass(slots=True)
class _SessionEntryAttempt:
    """One entry task plus caller decision and acknowledgement barriers."""

    entered: Event = field(default_factory=Event)
    decision: Future[bool] = field(default_factory=Future)
    decision_observed: Event = field(default_factory=Event)
    return_decision: Future[bool] = field(default_factory=Future)
    return_observed: Event = field(default_factory=Event)
    return_abandoned: Event = field(default_factory=Event)
    abandoned: Event = field(default_factory=Event)
    state_lock: Lock = field(default_factory=Lock)
    ownership_committed: bool = False
    return_ownership_committed: bool = False
    terminal: Event = field(default_factory=Event)
    error: BaseException | None = None


class SharedDownloadSessionCloser:
    """Schedule one session exit exactly once and retain it across timeouts."""

    def __init__(self, coordinator: Any, download_session: Any, futures: tuple[Any, ...]) -> None:
        self._pid = os.getpid()
        self._coordinator = coordinator
        self._download_session = download_session
        self._futures = futures
        self._lock = Lock()
        self._attempt: _SessionCloseAttempt | None = None
        self._future: Future[Any] | None = None
        self._closed = False

    async def _run_attempt(self, attempt: _SessionCloseAttempt, _context: Any) -> None:
        """Drain staging, commit ``__aexit__`` once, then publish real termination."""
        primary_error: BaseException | None = None
        try:
            wrapped = [asyncio.wrap_future(future) for future in self._futures]
            if wrapped:
                await asyncio.gather(*wrapped, return_exceptions=True)
            # Bridge cancellation is not a terminal proof. Keep the session
            # alive until every coordinator-owned asyncio task has published
            # its real-terminal event.
            for future in self._futures:
                submission = getattr(future, "_schema_sanitizer_remote_submission", None)
                terminal = getattr(submission, "terminal", None)
                while terminal is not None and not terminal.is_set():
                    await asyncio.sleep(0.001)
        except BaseException as exc:
            primary_error = exc

        try:
            await self._download_session.__aexit__(None, None, None)
        except BaseException as cleanup_error:
            if primary_error is None:
                primary_error = cleanup_error
            else:
                add_bounded_note(
                    primary_error,
                    "remote download session cleanup also failed after drain",
                    cleanup_error,
                )
        else:
            attempt.exit_committed.set()

        try:
            if primary_error is not None:
                attempt.error = primary_error
                raise primary_error
        finally:
            attempt.terminal.set()

    def _commit_closed(self, attempt: _SessionCloseAttempt) -> None:
        """Forget all session owners only after the exit phase committed."""
        with self._lock:
            if self._attempt is not attempt and not self._closed:
                return
            self._closed = True
            self._attempt = None
            self._future = None
            self._futures = ()
            self._download_session = None
            self._coordinator = None

    def close(self, *, timeout_seconds: float) -> bool:
        """Return whether session exit committed within the caller deadline."""
        if os.getpid() != getattr(self, "_pid", os.getpid()):
            return True
        timeout = normalize_duration(
            timeout_seconds, name="remote session close timeout", allow_zero=True
        )
        assert timeout is not None
        deadline = monotonic() + timeout
        with self._lock:
            if self._closed:
                return True
            attempt = self._attempt
            if attempt is None:
                attempt = _SessionCloseAttempt()
                future = self._coordinator.submit(
                    lambda context, current=attempt: self._run_attempt(current, context)
                )
                attempt.future = future
                self._attempt = attempt
                self._future = future
                future.add_done_callback(
                    lambda done, current=attempt: (
                        current.terminal.set() if not done.cancelled() else None
                    )
                )
            future = attempt.future
        assert future is not None

        try:
            future.result(timeout=max(0.0, deadline - monotonic()))
        except FutureTimeoutError:
            return False
        except CancelledError:
            if not attempt.terminal.wait(timeout=max(0.0, deadline - monotonic())):
                return False
        except BaseException:
            if not attempt.terminal.wait(timeout=max(0.0, deadline - monotonic())):
                return False

        if not attempt.terminal.is_set() and not attempt.terminal.wait(
            timeout=max(0.0, deadline - monotonic())
        ):
            return False

        # A successful __aexit__ is the ownership commit. A cancellation or
        # drain failure reported afterwards must never schedule __aexit__ again.
        if attempt.exit_committed.is_set() or attempt.error is None:
            self._commit_closed(attempt)
            error = attempt.error
            if error is not None and not isinstance(error, asyncio.CancelledError):
                raise error
            return True

        error = attempt.error
        with self._lock:
            if self._attempt is attempt and attempt.terminal.is_set():
                self._attempt = None
                self._future = None
        if isinstance(error, asyncio.CancelledError):
            return False
        if error is not None:
            raise error


def enter_shared_download_session(
    coordinator: Any,
    download_session: Any,
    *,
    timeout_seconds: float,
) -> None:
    """Enter one shared session through an acknowledged accept/abandon handshake."""
    attempt = _SessionEntryAttempt()

    async def enter_session(_context: Any) -> Any:
        entered = False
        try:
            value = await download_session.__aenter__()
            entered = True
            attempt.entered.set()
            accepted = False
            try:
                accepted = await asyncio.wrap_future(attempt.decision)
                state_lock = getattr(attempt, "state_lock", None)
                if accepted and state_lock is not None:
                    with state_lock:
                        if attempt.abandoned.is_set():
                            accepted = False
                        else:
                            attempt.ownership_committed = True
                elif accepted and getattr(attempt, "abandoned", None) is not None:
                    accepted = not attempt.abandoned.is_set()
                attempt.decision_observed.set()
            except BaseException as exc:
                attempt.decision_observed.set()
                committed = bool(getattr(attempt, "ownership_committed", False))
                if committed:
                    # The caller owns the entered session from this point. A
                    # late bridge cancellation must not close it underneath
                    # the acknowledged caller.
                    return value
                try:
                    await download_session.__aexit__(None, None, None)
                except BaseException as cleanup_error:
                    add_bounded_note(
                        exc,
                        "remote staging session cleanup also failed after entry cancellation",
                        cleanup_error,
                    )
                raise
            if accepted:
                return_accepted = False
                try:
                    return_accepted = await asyncio.wrap_future(attempt.return_decision)
                    with attempt.state_lock:
                        if attempt.return_abandoned.is_set():
                            return_accepted = False
                        elif return_accepted:
                            attempt.return_ownership_committed = True
                    attempt.return_observed.set()
                except BaseException as exc:
                    attempt.return_observed.set()
                    if attempt.return_ownership_committed:
                        return value
                    try:
                        await download_session.__aexit__(None, None, None)
                    except BaseException as cleanup_error:
                        add_bounded_note(
                            exc,
                            "remote staging session cleanup also failed after return cancellation",
                            cleanup_error,
                        )
                    raise
                if return_accepted:
                    return value
            await download_session.__aexit__(None, None, None)
            return None
        except BaseException as exc:
            attempt.error = exc
            raise
        finally:
            attempt.terminal.set()
            if not entered:
                attempt.entered.set()
                attempt.decision_observed.set()
                attempt.return_observed.set()

    enter_future = coordinator.submit(enter_session)
    timeout = normalize_duration(
        timeout_seconds, name="remote session startup timeout", allow_zero=True
    )
    assert timeout is not None
    deadline = monotonic() + timeout
    try:
        if not attempt.entered.wait(timeout=max(0.0, deadline - monotonic())):
            if not attempt.decision.done():
                attempt.decision.set_result(False)
            raise TimeoutError(
                "remote staging session startup exceeded its bounded deadline"
            ) from None

        if attempt.error is not None:
            if enter_future.done():
                enter_future.result(timeout=0)
            raise attempt.error

        if monotonic() >= deadline and not enter_future.done():
            if not attempt.decision.done():
                attempt.decision.set_result(False)
            raise TimeoutError(
                "remote staging session startup exceeded its bounded deadline"
            ) from None

        attempt.decision.set_result(True)
        # Ownership does not transfer until the coroutine itself has observed
        # the acceptance. This closes the cancellation window between set_result
        # and asyncio.wrap_future resuming on the coordinator loop.
        if not attempt.decision_observed.wait(timeout=max(0.0, deadline - monotonic())):
            state_lock = getattr(attempt, "state_lock", None)
            committed = bool(getattr(attempt, "ownership_committed", False))
            if state_lock is not None:
                with state_lock:
                    committed = bool(attempt.ownership_committed)
                    if not committed:
                        attempt.abandoned.set()
            if not committed:
                raise TimeoutError(
                    "remote staging session acceptance was not acknowledged before "
                    "its bounded deadline"
                ) from None
        if attempt.error is not None:
            raise attempt.error
        attempt.return_decision.set_result(True)
        if not attempt.return_observed.wait(timeout=max(0.0, deadline - monotonic())):
            with attempt.state_lock:
                committed = attempt.return_ownership_committed
                if not committed:
                    attempt.return_abandoned.set()
            if not committed:
                raise TimeoutError(
                    "remote staging session return was not acknowledged before its bounded deadline"
                ) from None
        if attempt.error is not None:
            raise attempt.error
        if enter_future.done():
            enter_future.result(timeout=0)
    except BaseException:
        state_lock = getattr(attempt, "state_lock", None)
        committed = bool(getattr(attempt, "ownership_committed", False))
        if state_lock is not None:
            with state_lock:
                committed = bool(attempt.ownership_committed)
                if not committed:
                    attempt.abandoned.set()
        elif not committed:
            abandoned = getattr(attempt, "abandoned", None)
            if abandoned is not None:
                abandoned.set()
        if not attempt.decision.done():
            attempt.decision.set_result(False)
        return_lock = getattr(attempt, "state_lock", None)
        return_committed = bool(getattr(attempt, "return_ownership_committed", False))
        if return_lock is not None:
            with return_lock:
                return_committed = bool(getattr(attempt, "return_ownership_committed", False))
                if not return_committed:
                    return_abandoned = getattr(attempt, "return_abandoned", None)
                    if return_abandoned is not None:
                        return_abandoned.set()
        return_decision = getattr(attempt, "return_decision", None)
        if return_decision is not None and not return_decision.done():
            return_decision.set_result(False)
        raise


def close_shared_download_session(
    coordinator: Any,
    download_session: Any,
    futures: tuple[Any, ...],
    *,
    timeout_seconds: float,
) -> bool:
    """Close one shared download session and all retained futures."""
    return SharedDownloadSessionCloser(coordinator, download_session, futures).close(
        timeout_seconds=timeout_seconds
    )


__all__ = [
    "SharedDownloadSessionCloser",
    "close_shared_download_session",
    "enter_shared_download_session",
]
