"""Shared operation-memory scopes for public source discovery APIs.

It creates a bounded operation context around public discovery calls and bridges
synchronous or asynchronous execution with guaranteed cleanup.
"""

from __future__ import annotations

from collections.abc import Awaitable, Callable
from typing import Any

from ..core_impl.memory_budget import (
    OperationMemoryLedger,
    activate_operation_memory_ledger,
    normalize_memory_limit,
)
from ..input_impl.directory_inputs import (
    DirectoryMetadataBudget,
    directory_metadata_budget_scope,
)


async def run_async_discovery_with_budget(
    discover: Callable[..., Awaitable[Any]],
    plans: Any,
    *,
    input_mode: str,
    input_format: str,
    source_file_extension: str | None,
    memory_limit_bytes: int | None,
    threading_mode: str,
) -> Any:
    """Run asynchronous discovery under one cross-runtime memory ledger."""
    normalized_limit = normalize_memory_limit(memory_limit_bytes)
    ledger = OperationMemoryLedger(normalized_limit)
    metadata_budget = DirectoryMetadataBudget(
        normalized_limit,
        operation_memory_ledger=ledger,
    )
    retained = False
    try:
        with activate_operation_memory_ledger(ledger):
            with directory_metadata_budget_scope(normalized_limit, budget=metadata_budget):
                result = await discover(
                    plans,
                    input_mode=input_mode,
                    input_format=input_format,
                    source_file_extension=source_file_extension,
                    memory_limit_bytes=normalized_limit,
                    threading_mode=threading_mode,
                )
        metadata_budget.retain()
        retained = True
        return result
    finally:
        if not retained:
            metadata_budget.close()
        ledger.close()


def run_sync_discovery_with_budget(
    discover: Callable[..., Any],
    plans: Any,
    *,
    input_mode: str,
    input_format: str,
    source_file_extension: str | None,
    memory_limit_bytes: int | None,
) -> Any:
    """Run synchronous discovery under one cross-runtime memory ledger."""
    normalized_limit = normalize_memory_limit(memory_limit_bytes)
    ledger = OperationMemoryLedger(normalized_limit)
    metadata_budget = DirectoryMetadataBudget(
        normalized_limit,
        operation_memory_ledger=ledger,
    )
    retained = False
    try:
        with activate_operation_memory_ledger(ledger):
            with directory_metadata_budget_scope(normalized_limit, budget=metadata_budget):
                result = discover(
                    plans,
                    input_mode=input_mode,
                    input_format=input_format,
                    source_file_extension=source_file_extension,
                    memory_limit_bytes=normalized_limit,
                )
        metadata_budget.retain()
        retained = True
        return result
    finally:
        if not retained:
            metadata_budget.close()
        ledger.close()


def run_public_source_discovery(
    async_discover: Callable[..., Awaitable[Any]],
    plans: Any,
    *,
    input_mode: str,
    input_format: str,
    source_file_extension: str | None,
    memory_limit_bytes: int | None,
    threading_mode: str,
) -> Any:
    """Run the public sync facade through the appropriate budgeted path."""
    from ..core_impl.execution_policy import normalize_threading_mode

    mode = normalize_threading_mode(threading_mode)
    if mode == "single":
        from .source_discovery_sync import discover_existing_source_plans_sync

        return run_sync_discovery_with_budget(
            discover_existing_source_plans_sync,
            plans,
            input_mode=input_mode,
            input_format=input_format,
            source_file_extension=source_file_extension,
            memory_limit_bytes=memory_limit_bytes,
        )
    from ..remote_impl.async_bridge import run_sync

    return run_sync(
        async_discover(
            plans,
            input_mode=input_mode,
            input_format=input_format,
            source_file_extension=source_file_extension,
            memory_limit_bytes=memory_limit_bytes,
            threading_mode=mode,
        ),
        threading_mode=mode,
    )
