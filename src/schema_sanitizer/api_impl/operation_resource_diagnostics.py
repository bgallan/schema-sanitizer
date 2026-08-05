"""Composition of cross-resource operation diagnostic snapshots."""

from __future__ import annotations

import os
from dataclasses import asdict
from typing import Any

from ..core_impl.operation_diagnostics import operation_diagnostic_registry_snapshot
from ..core_impl.process_resources import (
    process_file_descriptor_snapshot,
    process_thread_snapshot,
)
from ..core_impl.system_pressure import system_pressure_snapshot
from ..core_impl.temporary_janitor import temporary_janitor_snapshot
from ..core_impl.temporary_storage import process_temporary_storage_snapshot
from ..remote_impl.provider_throttle import process_provider_throttle_snapshot


def build_operation_resource_diagnostic_snapshot(resources: Any) -> dict[str, object]:
    """Return one bounded cross-resource snapshot for an operation domain."""
    if os.getpid() != resources.pid:
        return {
            "operation_id": resources.operation_id,
            "pid": resources.pid,
            "state": "inherited_after_fork",
        }
    memory = resources.memory_ledger.snapshot()
    storage = resources.temporary_storage.snapshot()
    with resources._lock:
        state = (
            "closed" if resources._closed else "closing" if resources._close_started else "running"
        )
        coordinator = resources._remote_coordinator
        references = resources._references
    payload: dict[str, object] = {
        "operation_id": resources.operation_id,
        "pid": resources.pid,
        "state": state,
        "references": references,
        "threading_mode": resources.policy.requested_mode,
        "effective_workers": resources.policy.effective_workers,
        "memory_limit_bytes": memory.limit_bytes,
        "memory_reserved_bytes": memory.reserved_bytes,
        "memory_peak_bytes": memory.peak_reserved_bytes,
        "temporary_reserved_bytes": storage.reserved_bytes,
        "temporary_peak_bytes": storage.peak_reserved_bytes,
        "temporary_active_leases": storage.active_leases,
        "process_threads": asdict(process_thread_snapshot()),
        "process_file_descriptors": asdict(process_file_descriptor_snapshot()),
        "process_temporary_storage": asdict(process_temporary_storage_snapshot()),
        "temporary_janitor": asdict(temporary_janitor_snapshot()),
        "provider_throttle": asdict(process_provider_throttle_snapshot()),
        "operation_registry": asdict(operation_diagnostic_registry_snapshot()),
        "system_pressure": asdict(system_pressure_snapshot()),
    }
    for key, owner in (
        ("operation_memory_diagnostics", resources.memory_ledger),
        ("operation_temporary_storage_diagnostics", resources.temporary_storage),
    ):
        diagnostics = getattr(owner, "diagnostics", None)
        if not callable(diagnostics):
            continue
        try:
            payload[key] = asdict(diagnostics())
        except Exception:
            pass
    if coordinator is not None:
        try:
            payload["remote_io"] = asdict(coordinator.permit_snapshot())
        except Exception:
            pass
    return payload


__all__ = ["build_operation_resource_diagnostic_snapshot"]
