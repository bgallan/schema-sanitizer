"""Integrated regressions for wide arenas and process memory governance."""

from __future__ import annotations

import json
import threading
from pathlib import Path
from typing import Any

import pytest
from conftest import require_native

import schema_sanitizer as ss
from schema_sanitizer.core_impl.execution import ExecutionContext
from schema_sanitizer.core_impl.native_runtime import native_core
from schema_sanitizer.options_impl.call_options import normalize_call_options

ROOT = Path(__file__).resolve().parents[2]


def _governor_stats() -> tuple[int, int, int]:
    """Return capacity, active leases, and FIFO waiters."""
    values = native_core.process_memory_governor_stats()
    assert isinstance(values, tuple)
    assert len(values) == 3
    return tuple(int(value) for value in values)


def test_dynamic_worker_bitmaps_schedule_above_32_without_a_hard_cap() -> None:
    """Wide lanes initialize and exercise every requested physical worker."""
    require_native()

    for workers in (33, 64, 96):
        (
            arena_workers,
            peak,
            total_threads,
            _overlap,
            upstream_threads,
            output_threads,
            submitted,
        ) = native_core.operation_task_arena_probe(
            workers,
            workers,
            workers,
            workers * 4,
        )

        assert arena_workers == workers
        # Pass54 separates physical arena width from dynamic runnable CPU
        # credit. A constrained cgroup may legitimately cap simultaneous work
        # well below 16 while all wide physical lanes still participate.
        assert 1 <= peak <= workers
        assert total_threads == workers
        assert 16 < upstream_threads <= workers
        assert 16 < output_threads <= workers
        assert submitted == workers * 8


def test_scheduler_queues_and_retained_output_use_operation_pmr() -> None:
    """The largest scheduler/output containers allocate from the operation pool."""
    arena = (ROOT / "cpp/src/internal/runtime/operation_task_arena.cc").read_text(encoding="utf-8")
    output = (ROOT / "cpp/src/internal/output/ordered_text_output.hh").read_text(encoding="utf-8")

    assert "std::pmr::deque<QueuedTask> tasks;" in arena
    assert "telemetry->memory_pool()" in arena
    assert "std::pmr::string bytes;" in output
    assert "task_arena->memory_resource()" in output
    assert "TextBuffer bytes(output_memory_resource" in output
    assert "std::string bytes;" not in output
    assert "reserved_output_bytes" in output


def test_wide_worker_bitmap_has_a_nonempty_summary_and_numa_local_stealing() -> None:
    """Very wide arenas skip empty shards and prefer local memory domains."""
    bitmap = (ROOT / "cpp/src/internal/runtime/atomic_worker_bitmap.hh").read_text(encoding="utf-8")
    arena = (ROOT / "cpp/src/internal/runtime/operation_task_arena_runtime.cc.inc").read_text(
        encoding="utf-8"
    )
    numa = (ROOT / "cpp/src/internal/runtime/numa_locality.hh").read_text(encoding="utf-8")

    assert "nonempty_words_" in bitmap
    assert "mark_word_empty" in bitmap
    assert "current_locality_domain" in numa
    assert "same_domain" in arena


def test_process_cpu_governor_bounds_two_concurrent_registrations() -> None:
    """Concurrent operation registrations share one fair CPU capacity."""
    require_native()
    capacity, peak, waits, completed = native_core.process_cpu_governor_probe(256)

    assert capacity >= 1
    assert 1 <= peak <= capacity
    assert completed == 256
    if capacity < completed:
        assert waits > 0


def test_text_output_uses_worker_local_governed_scratch() -> None:
    """Parallel encoders recycle private blocks through the operation pool."""
    output = (ROOT / "cpp/src/internal/output/ordered_text_output.hh").read_text(encoding="utf-8")
    resource = (ROOT / "cpp/src/internal/memory/pool_resource.cc").read_text(encoding="utf-8")

    assert "worker_resources" in output
    assert "recycle_exact_blocks=*/true" in output
    assert "pending_packets" in output
    assert "kOutOfMemory" in output
    assert "max_cached_bytes" in resource


def test_skewed_text_row_degrades_to_bounded_serial_encoding(tmp_path: Path) -> None:
    """A row larger than the packet target drains parallel output safely."""
    from schema_sanitizer.api_impl.execution_context import default_pool

    require_native()
    source = tmp_path / "skewed.jsonl"
    output = tmp_path / "skewed-output.jsonl"
    value = "x" * (2 * 1024 * 1024)
    source.write_text(json.dumps({"value": value}) + "\n", encoding="utf-8")

    ss.to_jsonl(
        source,
        output,
        input_format="jsonl",
        multi_threading=True,
        memory_limit_bytes=128 * 1024 * 1024,
    )

    assert json.loads(output.read_text(encoding="utf-8"))["value"] == value
    counters = default_pool().get().performance_stats()["counters"]
    assert counters["output_pressure_serializations"] >= 1


def test_every_context_routes_actual_bytes_through_the_process_pool() -> None:
    """Per-operation quotas are children of one aggregate native pool."""
    execution_context = (ROOT / "cpp/src/planning/execution_context.cc").read_text(encoding="utf-8")
    memory_pool = (ROOT / "cpp/src/internal/memory/memory_pool.cc").read_text(encoding="utf-8")

    assert "shared_process_memory_pool(process_capacity)" in execution_context
    assert "make_governed_operation_memory_pool(" in execution_context
    assert "schema_sanitizer::ProcessMemoryPool" in memory_pool
    assert "pool->SetLimit" in memory_pool
    assert "capacity_bytes_ =" in memory_pool
    assert "kMinimumOperationAdmissionBytes" in memory_pool
    assert "kMaximumOperationAdmissionBytes" in memory_pool


def test_operation_pool_lease_tracks_stream_lifetime() -> None:
    """The global lease remains live with a stream and returns on close."""
    require_native()
    limit = 8 * 1024 * 1024
    options = normalize_call_options(memory_limit_bytes=limit).raw
    output = ExecutionContext().to_sink_text(
        "stream",
        "jsonl",
        '{"value":1}\n',
        options,
    )
    try:
        capacity, leased, waiting = _governor_stats()
        assert capacity >= limit
        assert 0 < leased <= 8 * 1024 * 1024
        assert waiting == 0
    finally:
        output.close()

    capacity, leased, waiting = _governor_stats()
    assert capacity >= limit
    assert leased == 0
    assert waiting == 0


def test_process_governor_allows_suboperations_without_double_reserving_budget() -> None:
    """Full-budget streams coexist; their real bytes share the process pool."""
    require_native()
    capacity, leased, waiting = _governor_stats()
    if capacity == 0:
        capacity = int(native_core.memory_budget(-1)[0])
    assert leased == 0
    assert waiting == 0

    options = normalize_call_options(memory_limit_bytes=capacity).raw
    first = ExecutionContext().to_sink_text(
        "stream",
        "jsonl",
        '{"value":1}\n',
        options,
    )
    result: list[Any] = []
    errors: list[BaseException] = []

    def open_second() -> None:
        """Open and close a competing stream on a separate caller thread."""
        try:
            output = ExecutionContext().to_sink_text(
                "stream",
                "jsonl",
                '{"value":2}\n',
                options,
            )
            result.append(output)
            output.close()
        except BaseException as error:
            errors.append(error)

    thread = threading.Thread(target=open_second, daemon=True)
    thread.start()
    try:
        thread.join(timeout=10.0)
        assert not thread.is_alive()
        _, active_leases, queued = _governor_stats()
        assert 0 < active_leases <= 8 * 1024 * 1024
        assert queued == 0
    finally:
        first.close()

    assert not errors
    assert len(result) == 1
    _, leased, waiting = _governor_stats()
    assert leased == 0
    assert waiting == 0


def test_iter_batches_keeps_the_memory_lease_until_close(tmp_path: Path) -> None:
    """Lazy analytical output owns its operation until the iterator closes."""
    pytest.importorskip("pyarrow")
    require_native()
    source = tmp_path / "lazy.jsonl"
    source.write_text('{"value":1}\n{"value":2}\n', encoding="utf-8")

    stream = ss.iter_batches(
        source,
        input_format="jsonl",
        multi_threading=True,
        memory_limit_bytes=64 * 1024 * 1024,
    )
    try:
        stream_resources = stream._keepalive
        payload_owner = stream_resources._payload_owner
        assert payload_owner.memory_lease.reserved_bytes > 0
        first_batch = next(stream)
        first_rows = first_batch.num_rows
        del first_batch
        assert payload_owner.memory_lease.reserved_bytes > 0
        _capacity, leased, waiting = _governor_stats()
        assert leased > 0
        assert waiting == 0
        assert first_rows + sum(batch.num_rows for batch in stream) == 2
        assert list(stream) == []
        assert stream_resources._payload_owner is None
        assert payload_owner.memory_lease is None
        assert payload_owner.control_ticket is None
        _capacity, leased, waiting = _governor_stats()
        assert leased == 0
        assert waiting == 0
    finally:
        stream.close()

    _capacity, leased, waiting = _governor_stats()
    assert leased == 0
    assert waiting == 0

    # The closed stream can publish its ledger finalizer only after the native
    # reader drops its last buffer.  Starting the next ledger is a safe point
    # that must retire that conservative cross-process contribution.
    from schema_sanitizer.api_impl.operation_context import (
        OperationExecutionContext,
        operation_finalizer_snapshot,
    )
    from schema_sanitizer.core_impl.cross_process_memory import process_cross_memory_snapshot

    successor = OperationExecutionContext(
        threading_mode="single",
        memory_limit_bytes=64 * 1024 * 1024,
    )
    successor.close()
    assert process_cross_memory_snapshot()["logical_contributions"] == 0
    assert operation_finalizer_snapshot()[2] == 0
