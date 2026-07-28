"""Immutable execution policy shared by Python orchestration and native work."""

from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import Any

from .memory_budget import memory_budget, normalize_memory_limit
from .native_options import ThreadingMode, coerce_enum_member
from .native_runtime import native_core as _native

_FALLBACK_REASONS = {
    0: None,
    1: "single_requested",
    2: "cpu_limited",
    3: "memory_limited",
}


def normalize_threading_mode(value: Any) -> str:
    """Return the canonical internal threading mode name."""
    member = coerce_enum_member(ThreadingMode, value, label="option 'threading_mode'")
    return member.name.lower()


def threading_mode_from_multi_threading(value: Any) -> str:
    """Translate the public boolean concurrency switch to the internal mode."""
    if not isinstance(value, bool):
        raise TypeError("Option 'multi_threading' must be a bool")
    return "multi" if value else "single"


@dataclass(frozen=True, slots=True)
class ExecutionPolicy:
    """All concurrency controls for one operation."""

    requested_mode: str
    available_cpus: int
    effective_workers: int
    task_queue_capacity: int
    reorder_capacity: int
    worker_arena_bytes: int
    materialization_packet_target_bytes: int
    materialization_packet_max_rows: int
    async_concurrency: int
    async_prefetch_files: int
    remote_chunk_prefetch: int
    source_discovery_concurrency: int
    temporary_storage_limit_bytes: int
    pyarrow_use_threads: bool
    fallback_to_one_worker_reason: str | None

    @property
    def is_single(self) -> bool:
        """Return whether project-owned execution must stay inline."""
        return self.requested_mode == "single"

    def to_dict(self) -> dict[str, Any]:
        """Return a stable diagnostics representation."""
        return asdict(self)


def execution_policy(
    threading_mode: Any = "single",
    memory_limit_bytes: int | None = None,
    *,
    available_cpus: int | None = None,
) -> ExecutionPolicy:
    """Derive the canonical execution policy through the native implementation."""
    mode = coerce_enum_member(
        ThreadingMode,
        threading_mode,
        label="option 'threading_mode'",
    )
    requested_memory = (
        -1 if memory_limit_bytes is None else normalize_memory_limit(memory_limit_bytes)
    )
    args: tuple[int, ...] = (int(mode.value), requested_memory)
    if available_cpus is not None:
        if isinstance(available_cpus, bool) or not isinstance(available_cpus, int):
            raise TypeError("available_cpus must be an integer or None")
        if available_cpus <= 0:
            raise ValueError("available_cpus must be > 0")
        args += (available_cpus,)
    values = _native.execution_policy(*args)
    if not isinstance(values, tuple) or len(values) != 14:
        raise RuntimeError("native execution policy returned an invalid contract")
    (
        requested_mode,
        detected_cpus,
        effective_workers,
        task_queue_capacity,
        reorder_capacity,
        worker_arena_bytes,
        materialization_packet_target_bytes,
        materialization_packet_max_rows,
        async_concurrency,
        async_prefetch_files,
        remote_chunk_prefetch,
        source_discovery_concurrency,
        pyarrow_use_threads,
        fallback_reason,
    ) = values
    return ExecutionPolicy(
        requested_mode=ThreadingMode(int(requested_mode)).name.lower(),
        available_cpus=int(detected_cpus),
        effective_workers=int(effective_workers),
        task_queue_capacity=int(task_queue_capacity),
        reorder_capacity=int(reorder_capacity),
        worker_arena_bytes=int(worker_arena_bytes),
        materialization_packet_target_bytes=int(materialization_packet_target_bytes),
        materialization_packet_max_rows=int(materialization_packet_max_rows),
        async_concurrency=int(async_concurrency),
        async_prefetch_files=int(async_prefetch_files),
        remote_chunk_prefetch=int(remote_chunk_prefetch),
        source_discovery_concurrency=int(source_discovery_concurrency),
        temporary_storage_limit_bytes=memory_budget(memory_limit_bytes).replay_spool_bytes,
        pyarrow_use_threads=bool(pyarrow_use_threads),
        fallback_to_one_worker_reason=_FALLBACK_REASONS.get(int(fallback_reason)),
    )


__all__ = [
    "ExecutionPolicy",
    "execution_policy",
    "normalize_threading_mode",
    "threading_mode_from_multi_threading",
]
