"""Define high-core output-steal selection contracts.

The cases keep low-core stealing unchanged, prefer front output ahead of later broad work only
at the high-core gate, and preserve constant-time selection.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from schema_sanitizer.core_impl.native_runtime import native_core


def test_output_steal_preference_is_dormant_through_eight_workers(require_native: None) -> None:
    """The eight-worker stealing path uses the low-worker reverse scan."""
    promoted, outputs, broad, stolen, started, queued, submitted, cpu_capacity = (
        native_core.operation_task_arena_output_steal_probe(8)
    )

    if cpu_capacity < 3:
        pytest.skip("output-steal topology requires at least three runnable CPUs")

    assert 0 <= promoted <= outputs
    assert outputs == 3
    assert broad == 7
    assert stolen > 0
    assert queued == 0
    assert 11 <= submitted <= 18
    blocker_count = submitted - outputs - broad
    assert blocker_count <= started <= 8


def test_idle_high_worker_steals_front_output_before_later_broad_work(require_native: None) -> None:
    """An idle high worker no longer hides front output behind back broad work."""
    promoted, outputs, broad, stolen, started, queued, submitted, cpu_capacity = (
        native_core.operation_task_arena_output_steal_probe(16)
    )

    if cpu_capacity < 3:
        pytest.skip("output-steal topology requires at least three runnable CPUs")

    assert 0 <= promoted <= outputs
    assert outputs == 7
    assert broad == 15
    assert stolen > 0
    assert queued == 0
    assert 23 <= submitted <= 38
    blocker_count = submitted - outputs - broad
    assert blocker_count <= started <= 16


def test_steal_preference_is_constant_time_and_high_core_only() -> None:
    """The extension adds no queue scan, global index, or low-core branch."""
    root = Path(__file__).resolve().parents[2]
    runtime = (root / "cpp/src/internal/runtime/operation_task_arena_runtime.cc.inc").read_text()
    probe = (root / "cpp/src/api/python_abi3/runtime/test_probes.cc").read_text()

    assert "template <bool PreferDedicatedOutput>\nbool steal_compatible" in runtime
    assert "dedicated_high_output(candidate.tasks.front()" in runtime
    assert "selected = candidate.tasks.begin()" in runtime
    assert "worker_loop<true, false>(state, index, stop)" in runtime
    assert "worker_loop<false, false>(state, index, stop)" in runtime
    assert "worker_loop<false, true>(state, index, stop)" in runtime
    assert "global lane index, queue scan" in runtime
    assert "operation_task_arena_output_steal_probe" in probe
