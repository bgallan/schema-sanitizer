"""Regression coverage for concurrency output steal preference is dormant through eight workers."""

from __future__ import annotations

from pathlib import Path

from conftest import require_native

from schema_sanitizer.core_impl.native_runtime import native_core


def test_output_steal_preference_is_dormant_through_eight_workers() -> None:
    """The eight-worker stealing path uses the low-worker reverse scan."""
    require_native()
    promoted, outputs, broad, stolen, started, queued, submitted = (
        native_core.operation_task_arena_output_steal_probe(8)
    )

    assert 0 <= promoted <= outputs
    assert outputs == 3
    assert broad == 7
    assert stolen > 0
    assert queued == 0
    assert 11 <= submitted <= 18
    blocker_count = submitted - outputs - broad
    assert blocker_count <= started <= 8


def test_idle_high_worker_steals_front_output_before_later_broad_work() -> None:
    """An idle high worker no longer hides front output behind back broad work."""
    require_native()
    promoted, outputs, broad, stolen, started, queued, submitted = (
        native_core.operation_task_arena_output_steal_probe(16)
    )

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
