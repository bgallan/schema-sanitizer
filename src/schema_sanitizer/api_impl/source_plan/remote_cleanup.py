"""Retryable cleanup helpers for lazy remote source-plan ownership.

It combines primary and cleanup failures and retries close or abandonment across lazy
remote owners without losing the authoritative error.
"""

from __future__ import annotations

from collections import deque
from typing import Any

from ...core_impl.safe_errors import add_bounded_note


def combine_cleanup_error(
    primary: BaseException | None,
    secondary: BaseException | None,
) -> BaseException | None:
    """Retain one primary cleanup error and attach later failures as notes."""
    if secondary is None:
        return primary
    if primary is None:
        return secondary
    add_bounded_note(primary, "additional remote cleanup failure", secondary)
    return primary


def close_one_retryably(obj: Any) -> BaseException | None:
    """Close one object and return its failure without discarding ownership."""
    if obj is None:
        return None
    close = getattr(obj, "close", None)
    if not callable(close):
        return None
    try:
        close()
    except BaseException as exc:
        return exc
    return None


def close_deque_retryably(items: deque[Any]) -> BaseException | None:
    """Close a deque in LIFO order while retaining failed unique owners."""
    failed: deque[Any] = deque()
    failed_ids: set[int] = set()
    outcomes: dict[int, BaseException | None] = {}
    primary: BaseException | None = None
    while items:
        item = items.pop()
        ident = id(item)
        if ident not in outcomes:
            outcomes[ident] = close_one_retryably(item)
        error = outcomes[ident]
        if error is not None and ident not in failed_ids:
            failed.appendleft(item)
            failed_ids.add(ident)
            primary = combine_cleanup_error(primary, error)
    items.extend(failed)
    return primary


def close_list_retryably(items: list[Any]) -> BaseException | None:
    """Close a list in LIFO order while retaining failed unique owners."""
    failed: list[Any] = []
    failed_ids: set[int] = set()
    outcomes: dict[int, BaseException | None] = {}
    primary: BaseException | None = None
    while items:
        item = items.pop()
        ident = id(item)
        if ident not in outcomes:
            outcomes[ident] = close_one_retryably(item)
        error = outcomes[ident]
        if error is not None and ident not in failed_ids:
            failed.append(item)
            failed_ids.add(ident)
            primary = combine_cleanup_error(primary, error)
    items.extend(reversed(failed))
    return primary


def staged_file_count(staged: Any) -> int:
    """Return the bounded source count represented by one staged chunk."""
    try:
        return max(0, len(staged.manifest.source_batch.sources))
    except BaseException:
        return 0


def take_prefetched_chunks(manifest: Any) -> tuple[list[Any], int]:
    """Take an optional bounded lookahead prefix from one remote manifest."""
    take = getattr(manifest, "take_prefetched_chunks", None)
    if not callable(take):
        return [], 0
    chunks, file_count = take()
    return list(chunks), max(0, int(file_count))


__all__ = [
    "close_deque_retryably",
    "close_list_retryably",
    "close_one_retryably",
    "combine_cleanup_error",
    "staged_file_count",
    "take_prefetched_chunks",
]
