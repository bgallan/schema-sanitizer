"""Regression coverage for v94 successful-drain-only visibility."""

from __future__ import annotations

from pathlib import Path

from conftest import require_native

from schema_sanitizer.core_impl.concurrency_coverage import (
    concurrency_pair_guarantees,
)
from schema_sanitizer.core_impl.native_runtime import native_core

ROOT = Path(__file__).resolve().parents[2]
RUNTIME = ROOT / "cpp/src/internal/runtime/operation_task_arena_runtime.cc.inc"
ARENA = ROOT / "cpp/src/internal/runtime/operation_task_arena.cc"
STAGE = "successful_drain_only_queue_visibility_publication"


def _function(source: str, start: str, end: str) -> str:
    """Return the source slice between two markers."""
    begin = source.index(start)
    return source[begin : source.index(end, begin)]


def test_v94_failed_local_probe_does_not_publish_empty_again() -> None:
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


def test_v94_stale_remote_candidate_does_not_repeat_empty_rmw() -> None:
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


def test_v94_successful_last_packet_removals_still_publish_empty() -> None:
    """Verify the named concurrency regression contract."""
    source = RUNTIME.read_text(encoding="utf-8")
    local = _function(source, "bool take_local", "bool steal_compatible")
    steal = _function(source, "bool steal_compatible", "bool take_task")

    assert "--slot.queued_local;" in local
    assert "if (slot.tasks.empty()) {\n    mark_empty(state, index);" in local
    assert "--candidate.queued_local;" in steal
    assert "if (candidate.tasks.empty()) {\n      mark_empty(state, candidate_index);" in steal
    assert source.count("mark_empty(state") == 3


def test_v94_visibility_transitions_keep_existing_ordering() -> None:
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


def test_v94_all_56_pairs_inherit_successful_drain_publication() -> None:
    """Verify the named concurrency regression contract."""
    pairs = concurrency_pair_guarantees()

    assert sum(len(outputs) for outputs in pairs.values()) == 56
    for input_name, outputs in pairs.items():
        assert len(outputs) == 7, input_name
        for guarantee in outputs.values():
            assert STAGE in guarantee["shared_parallel_stages"]
            assert guarantee["source_to_sink_parallel_path"] is True
            assert guarantee["eligible_multi_benefit"] is True


def test_v94_native_exact_drain_and_direct_producers() -> None:
    """Verify the named concurrency regression contract."""
    require_native()
    for workers in (2, 4, 5, 8, 16):
        elapsed, completed, checksum, started, peak, queued, submitted = (
            native_core.ordered_executor_arena_completion_probe(workers, 20_000, 1)
        )
        assert elapsed > 0
        assert completed == 20_000
        assert checksum >= 0
        assert 1 <= started <= workers
        assert 1 <= peak <= workers
        assert queued == 0
        assert submitted == 20_000

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
