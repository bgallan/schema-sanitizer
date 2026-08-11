"""Internal handoff of prepared partition resources into public converters."""

from __future__ import annotations

import os
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from contextvars import ContextVar
from dataclasses import dataclass, field
from threading import Lock
from typing import TYPE_CHECKING

from schema_sanitizer.core_impl.fork_safety import quarantine_inherited_state

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
_FORKED_PARTITION_RESOURCES_KEEPALIVE: list[BorrowedPartitionResources] = []


@contextmanager
def borrowed_partition_resources(
    resources: BorrowedPartitionResources,
) -> Iterator[BorrowedPartitionResources]:
    """Expose one internal prepared-input handoff to a public converter."""
    owner_pid = os.getpid()
    token = _CURRENT_PARTITION_RESOURCES.set(resources)
    try:
        yield resources
    finally:
        if os.getpid() == owner_pid:
            _CURRENT_PARTITION_RESOURCES.reset(token)
        else:
            _reset_partition_resources_after_fork()


_FORK_CURRENT_PARTITION_RESOURCES_BANKS = (
    ContextVar[BorrowedPartitionResources | None](
        "schema_sanitizer_partition_resources_child_0", default=None
    ),
    ContextVar[BorrowedPartitionResources | None](
        "schema_sanitizer_partition_resources_child_1", default=None
    ),
)
_FORK_CURRENT_PARTITION_RESOURCES_BANK_INDEX = 0
_FORK_PREPARED_CURRENT_PARTITION_RESOURCES: ContextVar[BorrowedPartitionResources | None] | None = (
    None
)


def _prepare_partition_resources_for_fork() -> None:
    global _FORK_PREPARED_CURRENT_PARTITION_RESOURCES
    _FORK_PREPARED_CURRENT_PARTITION_RESOURCES = _FORK_CURRENT_PARTITION_RESOURCES_BANKS[
        _FORK_CURRENT_PARTITION_RESOURCES_BANK_INDEX
    ]


def _clear_partition_resources_fork_preparation() -> None:
    global _FORK_PREPARED_CURRENT_PARTITION_RESOURCES
    _FORK_PREPARED_CURRENT_PARTITION_RESOURCES = None


def _reset_partition_resources_after_fork() -> None:
    """Swap to a preallocated empty child ContextVar without decrefing owners."""
    global _CURRENT_PARTITION_RESOURCES, _FORK_PREPARED_CURRENT_PARTITION_RESOURCES
    global _FORK_CURRENT_PARTITION_RESOURCES_BANK_INDEX
    prepared = _FORK_PREPARED_CURRENT_PARTITION_RESOURCES
    if prepared is None:
        return
    inherited = _CURRENT_PARTITION_RESOURCES.get()
    if inherited is not None:
        quarantine_inherited_state("partition-resources", inherited, _CURRENT_PARTITION_RESOURCES)
    _CURRENT_PARTITION_RESOURCES = prepared
    _FORK_PREPARED_CURRENT_PARTITION_RESOURCES = None
    _FORK_CURRENT_PARTITION_RESOURCES_BANK_INDEX = 1 - _FORK_CURRENT_PARTITION_RESOURCES_BANK_INDEX


from ..core_impl.fork_manager import register_fork_handler as _register_fork_handler  # noqa: E402

_register_fork_handler(
    "partition-resources",
    before=_prepare_partition_resources_for_fork,
    after_in_parent=_clear_partition_resources_fork_preparation,
    after_in_child=_reset_partition_resources_after_fork,
)


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
