"""Regression coverage for concurrency high core mixed lanes drain without extra workers."""

from __future__ import annotations

from conftest import require_native

from schema_sanitizer.core_impl.native_runtime import native_core


def test_high_core_mixed_lanes_drain_without_extra_workers() -> None:
    """Signal coalescing cannot strand compatible work or oversubscribe."""
    require_native()
    _elapsed_ns, stolen, started, peak, finished, queued, submitted = (
        native_core.operation_task_arena_mixed_lane_probe(16, 4_000)
    )

    assert stolen > 0
    assert 1 <= started <= 16
    assert 1 <= peak <= 16
    assert finished == 12_000
    assert queued == 0
    assert submitted == 12_008


def test_output_priority_survives_wake_coalescing() -> None:
    """A running target still allows a real idle high-lane helper to wake."""
    require_native()
    promoted, outputs, broad, stolen, started, queued, submitted = (
        native_core.operation_task_arena_output_steal_probe(16)
    )

    assert 1 <= promoted <= outputs
    assert outputs == 7
    assert broad == 15
    assert stolen > 0
    assert queued == 0
    # The probe blocks only the CPU credits that can run concurrently. Its
    # remaining submissions are the seven output and fifteen broad packets.
    assert 23 <= submitted <= 38
    blocker_count = submitted - outputs - broad
    assert blocker_count <= started <= 16
