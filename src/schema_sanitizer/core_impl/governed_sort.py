"""Memory-governed in-place sorting for large retained metadata collections."""

from __future__ import annotations

from collections.abc import Callable
from contextlib import contextmanager
from typing import Any, Iterator, TypeVar

from .control_plane_budget import release_control_plane, reserve_control_plane
from .memory_budget import acquire_operation_memory, current_operation_memory_ledger
from .safe_errors import add_bounded_note

T = TypeVar("T")

# CPython's keyed list.sort can transiently retain a key array and a merge
# workspace.  Three pointer-sized words per element is deliberately
# conservative on 64-bit runtimes while remaining a hard, deterministic
# bound for admission purposes.
_SORT_SCRATCH_BYTES_PER_ITEM = 24
_MIN_SORT_SCRATCH_BYTES = 4096


def estimated_sort_scratch_bytes(item_count: int) -> int:
    """Return a conservative transient scratch reservation for one list sort."""
    if type(item_count) is not int:
        raise TypeError("sort item count must be an exact integer")
    if item_count < 0:
        raise ValueError("sort item count must be >= 0")
    if item_count <= 1:
        return 0
    return max(_MIN_SORT_SCRATCH_BYTES, item_count * _SORT_SCRATCH_BYTES_PER_ITEM)


@contextmanager
def reserve_sort_scratch(item_count: int, *, stage: str) -> Iterator[None]:
    """Reserve transient sort workspace before CPython may allocate it."""
    amount = estimated_sort_scratch_bytes(item_count)
    if amount == 0:
        yield
        return

    lease = None
    control_ticket = None
    if current_operation_memory_ledger() is not None:
        lease = acquire_operation_memory(amount, stage=stage)
    else:
        control_ticket = reserve_control_plane(f"sort_scratch:{stage}", amount)
    try:
        yield
    except BaseException as primary:
        try:
            if lease is not None:
                lease.close()
            elif control_ticket is not None:
                release_control_plane(control_ticket)
        except BaseException as cleanup_error:
            add_bounded_note(primary, "sort scratch cleanup also failed", cleanup_error)
        raise
    else:
        if lease is not None:
            lease.close()
        elif control_ticket is not None:
            release_control_plane(control_ticket)


def governed_sort(
    values: list[T],
    *,
    key: Callable[[T], Any] | None = None,
    reverse: bool = False,
    stage: str,
) -> None:
    """Sort in place while charging the transient key/merge workspace first."""
    with reserve_sort_scratch(len(values), stage=stage):
        values.sort(key=key, reverse=reverse)


__all__ = ["estimated_sort_scratch_bytes", "governed_sort", "reserve_sort_scratch"]
