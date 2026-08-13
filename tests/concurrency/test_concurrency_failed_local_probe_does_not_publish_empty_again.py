"""Regression coverage for concurrency failed local probe does not publish empty again."""

from __future__ import annotations

from pathlib import Path

from conftest import require_native

from schema_sanitizer.core_impl.native_runtime import native_core

ROOT = Path(__file__).resolve().parents[2]
RUNTIME = ROOT / "cpp/src/internal/runtime/operation_task_arena_runtime.cc.inc"
ARENA = ROOT / "cpp/src/internal/runtime/operation_task_arena.cc"


def _function(source: str, start: str, end: str) -> str:
    """Return the source slice between two markers."""
    begin = source.index(start)
    return source[begin : source.index(end, begin)]


def test_failed_local_probe_does_not_publish_empty_again() -> None:
    """Verify the named concurrency regression contract."""
    source = RUNTIME.read_text(encoding="utf-8")
    local = _function(source, "bool take_local", "bool steal_compatible")
    empty_branch = local[
        local.index("if (slot.tasks.empty())") : local.index(
            "// Publish initialization", local.index("if (slot.tasks.empty())")
        )
    ]

    assert "return false;" in empty_branch
    assert "mark_empty(" not in empty_branch
    assert "failed local probe" in empty_branch


def test_stale_remote_candidate_does_not_repeat_empty_rmw() -> None:
    """Verify the named concurrency regression contract."""
    source = RUNTIME.read_text(encoding="utf-8")
    steal = _function(source, "bool steal_compatible", "bool take_task")
    empty_branch = steal[
        steal.index("if (candidate.tasks.empty())") : steal.index(
            "auto selected", steal.index("if (candidate.tasks.empty())")
        )
    ]

    assert "return;" in empty_branch
    assert "mark_empty(" not in empty_branch
    assert "A stale mask" in steal


def test_successful_last_packet_removals_still_publish_empty() -> None:
    """Verify the named concurrency regression contract."""
    source = RUNTIME.read_text(encoding="utf-8")
    local = _function(source, "bool take_local", "bool steal_compatible")
    steal = _function(source, "bool steal_compatible", "bool take_task")

    assert "--slot.queued_local;" in local
    assert "if (slot.tasks.empty()) {\n    mark_empty(state, index);" in local
    assert "--candidate.queued_local;" in steal
    assert "if (candidate.tasks.empty()) {\n      mark_empty(state, candidate_index);" in steal
    assert source.count("mark_empty(state") == 3


def test_visibility_transitions_keep_existing_ordering() -> None:
    """Verify the named concurrency regression contract."""
    runtime = RUNTIME.read_text(encoding="utf-8")

    assert "nonempty_mask.fetch_or(" in runtime
    assert "nonempty_mask.fetch_and(" in runtime
    visibility = runtime[
        runtime.index("void mark_nonempty") : runtime.index("[[nodiscard]] bool update_peak")
    ]
    assert visibility.count("std::memory_order_release") == 2
    assert runtime.count("nonempty_mask.load(") >= 2
    assert runtime.count("std::memory_order_acquire") >= 2
    assert "nonempty_mask.load(std::memory_order_relaxed)" not in runtime


def test_native_direct_producers_finish_exactly() -> None:
    """Concurrent direct producers leave no queued arena tasks."""
    require_native()
    for workers in (2, 4, 8, 16):
        elapsed, submitted, finished, queued, started, peak = (
            native_core.operation_task_arena_concurrent_submit_probe(workers, 2, 1_500)
        )
        assert elapsed > 0
        assert submitted == 3_000
        assert finished == 3_000
        assert queued == 0
        assert 1 <= started <= workers
        assert 1 <= peak <= workers
