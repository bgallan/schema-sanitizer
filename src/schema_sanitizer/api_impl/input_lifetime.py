"""Operation-scoped ownership helpers for prepared input data."""

from __future__ import annotations

from typing import Any

from ..input_impl.prepared import ChainedKeepalive
from .input.memory_limits import materialized_input_size_bytes
from .operation_context import OperationExecutionContext


def operation_context_for_source_plan(
    source_plan: Any,
    *,
    threading_mode: str,
    memory_limit_bytes: int | None,
) -> tuple[OperationExecutionContext, bool]:
    """Return a borrowed plan context or create one owned by this call."""
    if source_plan is not None:
        for item in getattr(source_plan, "close_items", ()):
            if isinstance(item, OperationExecutionContext):
                return item, False
    return (
        OperationExecutionContext(
            threading_mode=threading_mode,
            memory_limit_bytes=memory_limit_bytes,
        ),
        True,
    )


def reserve_materialized_input(
    operation_context: OperationExecutionContext,
    data: Any,
    format_name: str,
    *,
    source_name: str,
) -> Any | None:
    """Charge retained materialized input bytes to the operation ledger."""
    materialized_bytes = materialized_input_size_bytes(
        data,
        format_name,
        source=source_name,
    )
    if not materialized_bytes:
        return None
    return operation_context.memory_ledger.acquire(
        materialized_bytes,
        stage="materialized_input",
    )


def operation_input_keepalive(
    operation_context: OperationExecutionContext,
    *,
    owns_operation_context: bool,
    input_reservation: Any | None,
) -> Any | None:
    """Build the keepalive that closes only resources owned by this call."""
    if owns_operation_context:
        if input_reservation is not None:
            return ChainedKeepalive(operation_context, input_reservation)
        return operation_context
    return input_reservation
