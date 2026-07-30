"""Internal handoff of prepared partition resources into public converters."""

from __future__ import annotations

import os
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from contextvars import ContextVar
from dataclasses import dataclass, field
from threading import Lock
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from ..input_impl.prepared import PreparedPublicInput
    from .operation_context import OperationExecutionContext


@dataclass(slots=True)
class BorrowedPartitionResources:
    """One prepared input and operation context owned by a partition runner."""

    input_path: str
    prepared_input: PreparedPublicInput
    operation_context: OperationExecutionContext
    allow_early_lookahead: bool
    lookahead_trigger: Callable[[], None]
    _lock: Lock = field(init=False, repr=False)
    _consumed: bool = field(init=False, repr=False)
    _triggered: bool = field(init=False, repr=False)

    def __post_init__(self) -> None:
        """Initialize one-shot consumption and trigger state."""
        self._lock = Lock()
        self._consumed = False
        self._triggered = False

    def take(
        self,
        input_path: str | os.PathLike[str],
        *,
        threading_mode: str,
        memory_limit_bytes: int | None,
    ) -> tuple[PreparedPublicInput, OperationExecutionContext] | None:
        """Transfer the prepared input to its matching public conversion once."""
        if os.fspath(input_path) != self.input_path:
            return None
        context = self.operation_context
        if context.threading_mode != threading_mode:
            raise RuntimeError("partition lookahead threading mode does not match conversion")
        if context.memory_limit_bytes != memory_limit_bytes:
            raise RuntimeError("partition lookahead memory limit does not match conversion")
        with self._lock:
            if self._consumed:
                raise RuntimeError("partition lookahead input was already consumed")
            self._consumed = True
        return self.prepared_input, context

    def close_if_unconsumed(self) -> None:
        """Release a handoff rejected before the converter took ownership."""
        with self._lock:
            if self._consumed:
                return
            self._consumed = True
        self.prepared_input.close()
        self.operation_context.close()

    def trigger(self) -> None:
        """Schedule the next source exactly once at a safe conversion boundary."""
        with self._lock:
            if self._triggered:
                return
            self._triggered = True
        self.lookahead_trigger()


_CURRENT_PARTITION_RESOURCES: ContextVar[BorrowedPartitionResources | None] = ContextVar(
    "schema_sanitizer_partition_resources",
    default=None,
)


@contextmanager
def borrowed_partition_resources(
    resources: BorrowedPartitionResources,
) -> Iterator[BorrowedPartitionResources]:
    """Expose one internal prepared-input handoff to a public converter."""
    token = _CURRENT_PARTITION_RESOURCES.set(resources)
    try:
        yield resources
    finally:
        _CURRENT_PARTITION_RESOURCES.reset(token)


def take_borrowed_partition_resources(
    input_path: str | os.PathLike[str],
    *,
    threading_mode: str,
    memory_limit_bytes: int | None,
) -> tuple[PreparedPublicInput, OperationExecutionContext, BorrowedPartitionResources] | None:
    """Take the current matching handoff, if a partition runner supplied one."""
    resources = _CURRENT_PARTITION_RESOURCES.get()
    if resources is None:
        return None
    taken = resources.take(
        input_path,
        threading_mode=threading_mode,
        memory_limit_bytes=memory_limit_bytes,
    )
    if taken is None:
        return None
    prepared_input, operation_context = taken
    return prepared_input, operation_context, resources


__all__ = [
    "BorrowedPartitionResources",
    "borrowed_partition_resources",
    "take_borrowed_partition_resources",
]
