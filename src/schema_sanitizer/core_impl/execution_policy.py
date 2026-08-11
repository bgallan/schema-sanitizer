"""Immutable execution policy shared by Python orchestration and native work."""

from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import Any

from .memory_budget import memory_budget, normalize_memory_limit
from .native_options import ThreadingMode, coerce_enum_member
from .native_runtime import native_core as _native
from .system_pressure import pressure_adjusted_target

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
    requested_memory = normalize_memory_limit(memory_limit_bytes)
    args: tuple[int, ...] = (int(mode.value), requested_memory)
    if available_cpus is not None:
        if isinstance(available_cpus, bool) or not isinstance(available_cpus, int):
            raise TypeError("available_cpus must be an integer or None")
        if available_cpus <= 0:
            raise ValueError("available_cpus must be > 0")
        args += (available_cpus,)
    values = _native.execution_policy(*args)
    if available_cpus is None and isinstance(values, tuple) and len(values) == 14:
        detected = max(1, int(values[1]))
        pressured = pressure_adjusted_target(detected)
        if pressured < detected:
            values = _native.execution_policy(int(mode.value), requested_memory, pressured)
    if (
        available_cpus is None
        and isinstance(values, tuple)
        and len(values) == 14
        and int(mode.value) != int(ThreadingMode.SINGLE.value)
    ):
        # The native policy owns per-operation sizing, while the Python control
        # plane owns process-global metadata. Fold the composed payload headroom
        # back into the canonical worker policy so static/dynamic control charges
        # cannot leave the native policy advertising workers that cannot be
        # admitted physically. Serial forward progress always retains one worker.
        from .memory_budget import (
            _raw_process_resident_memory_snapshot,
            process_resident_memory_snapshot,
        )

        resident = process_resident_memory_snapshot()
        raw_resident = _raw_process_resident_memory_snapshot()
        from .control_plane_budget import process_control_plane_snapshot

        control = process_control_plane_snapshot()
        current_workers = max(1, int(values[2]))
        per_worker_bytes = max(1, int(values[5]))
        headroom = max(0, resident.capacity_bytes - resident.reserved_bytes)
        memory_workers = max(1, min(current_workers, headroom // per_worker_bytes))
        # A source-tree/native test can leave the process pool configured below
        # the freshly resolved automatic budget. Real operation-context creation
        # restores that pool to the automatic process envelope. Do not let a
        # stale smaller envelope permanently collapse an automatic policy before
        # the context has had a chance to install its canonical capacity. Explicit
        # user limits remain subject to the currently active process envelope.
        smaller_idle_process_envelope = (
            raw_resident.capacity_bytes < requested_memory and resident.reserved_bytes == 0
        )
        dynamic_pressure = resident.reserved_bytes > 0 or control.reserved_bytes > 0
        if (
            memory_workers < current_workers
            and dynamic_pressure
            and not smaller_idle_process_envelope
        ):
            original_detected = int(values[1])
            constrained = _native.execution_policy(
                int(mode.value), requested_memory, memory_workers
            )
            if isinstance(constrained, tuple) and len(constrained) == 14:
                adjusted = list(constrained)
                adjusted[1] = original_detected
                adjusted[13] = 3  # memory_limited
                values = tuple(adjusted)
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
