"""Close ingest runtime wrappers while preserving useful failure context.

Wrapper attributes and owner sequences are retried in acquisition-safe order, primary exceptions
receive bounded cleanup notes, and failed owners remain retained for later release.
"""

from __future__ import annotations

from typing import Any

from .safe_errors import add_bounded_note


def _close_suppressing_errors(obj: Any, *, main_stream_only: bool = False) -> bool:
    """Best-effort close an object and report whether ownership may be cleared."""
    if obj is None:
        return True
    fn = None
    if main_stream_only:
        fn = getattr(obj, "close_main_stream", None)
    if fn is None:
        fn = getattr(obj, "close", None)
    if not callable(fn):
        return True
    try:
        fn()
    except BaseException:
        return False
    return True


def _cleanup_with_note(
    primary: BaseException,
    obj: Any,
    *,
    label: str,
    method: str = "close",
) -> None:
    """Attempt cleanup and attach a bounded secondary failure to ``primary``."""
    cleanup = getattr(obj, method, None)
    if not callable(cleanup):
        return
    try:
        cleanup()
    except BaseException as cleanup_error:
        add_bounded_note(primary, label, cleanup_error)


def _close_keepalive_attr(owner: Any) -> None:
    """Close and remove an owner's keepalive only after cleanup succeeds."""
    keepalive = getattr(owner, "_keepalive", None)
    if keepalive is None:
        return
    if _close_suppressing_errors(keepalive):
        try:
            delattr(owner, "_keepalive")
        # Missing optional keepalive metadata is harmless.
        except Exception as ignored_error:
            del ignored_error


def _close_and_clear_attrs(owner: Any, *attrs: str) -> None:
    """Close unique resources and retain attributes whose cleanup failed."""
    results: dict[int, bool] = {}
    for attr in attrs:
        obj = getattr(owner, attr, None)
        if obj is None:
            try:
                object.__setattr__(owner, attr, None)
            # Cleanup tolerates immutable foreign wrappers.
            except Exception as ignored_error:
                del ignored_error
            continue
        ident = id(obj)
        succeeded = results.get(ident)
        if succeeded is None:
            succeeded = _close_suppressing_errors(obj)
            results[ident] = succeeded
        if succeeded:
            try:
                object.__setattr__(owner, attr, None)
            # Cleanup tolerates immutable foreign wrappers.
            except Exception as ignored_error:
                del ignored_error


def _close_resource_owner_attr(owner: Any) -> None:
    """Close and remove a retained resource owner only after success."""
    resource_owner = getattr(owner, "_resource_owner", None)
    if resource_owner is None:
        return
    if _close_suppressing_errors(resource_owner):
        try:
            delattr(owner, "_resource_owner")
        # Missing optional owner metadata is harmless.
        except Exception as ignored_error:
            del ignored_error


def _close_sequence_retryably(items: list[Any]) -> None:
    """Close unique items in LIFO order and retain failures for a later retry."""
    outcomes: dict[int, bool] = {}
    failed: list[Any] = []
    failed_ids: set[int] = set()
    while items:
        item = items.pop()
        if item is None:
            continue
        ident = id(item)
        succeeded = outcomes.get(ident)
        if succeeded is None:
            succeeded = _close_suppressing_errors(item)
            outcomes[ident] = succeeded
        if not succeeded and ident not in failed_ids:
            failed.append(item)
            failed_ids.add(ident)
    items.extend(reversed(failed))


def _close_sequence_with_error(items: list[Any]) -> BaseException | None:
    """Close unique items in LIFO order, retaining failures and one primary error."""
    outcomes: dict[int, tuple[bool, BaseException | None]] = {}
    failed: list[Any] = []
    failed_ids: set[int] = set()
    first_error: BaseException | None = None
    while items:
        item = items.pop()
        if item is None:
            continue
        ident = id(item)
        outcome = outcomes.get(ident)
        if outcome is None:
            try:
                item.close()
            except BaseException as close_error:
                outcome = (False, close_error)
            else:
                outcome = (True, None)
            outcomes[ident] = outcome
        succeeded, outcome_error = outcome
        if not succeeded and ident not in failed_ids:
            failed.append(item)
            failed_ids.add(ident)
            if outcome_error is not None:
                if first_error is None:
                    first_error = outcome_error
                else:
                    add_bounded_note(first_error, "additional cleanup failure", outcome_error)
    items.extend(reversed(failed))
    return first_error
