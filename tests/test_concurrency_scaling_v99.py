"""Regression coverage for v99 initialized-worker admission snapshots."""

from pathlib import Path

from conftest import require_native

from schema_sanitizer.core_impl.concurrency_coverage import concurrency_pair_guarantees
from schema_sanitizer.core_impl.native_runtime import native_core

ROOT = Path(__file__).resolve().parents[1]
RUNTIME = ROOT / "cpp/src/internal/runtime/operation_task_arena_runtime.cc.inc"
ARENA = ROOT / "cpp/src/internal/runtime/operation_task_arena.cc"
DOC = ROOT / "CONCURRENCY_SCALING_V99.md"
BENCH = ROOT / "benchmarks/v99_initialized_worker_snapshot_admission_ab.json"
STAGE = "initialized_worker_snapshot_admission_elision"


def test_v99_idle_selection_uses_initialized_snapshot_without_started_reload() -> None:
    source = RUNTIME.read_text(encoding="utf-8")
    helper = source[
        source.index("[[nodiscard]] std::size_t idle_started_worker") : source.index(
            "void mark_nonempty"
        )
    ]
    assert "std::uint64_t initialized_snapshot" in helper
    assert "initialized_snapshot & allowed" in helper
    assert "started_mask.load" not in helper
    assert "initialized implies started" in helper


def test_v99_one_snapshot_elides_impossible_reservation_and_startup_check() -> None:
    source = ARENA.read_text(encoding="utf-8")
    submit = source[
        source.index("const auto initialized_snapshot") : source.index(
            "auto &slot = *state_->slots[physical]"
        )
    ]
    assert submit.count("initialized_mask.load") == 1
    assert "lane_fully_initialized" in submit
    assert "physical == lane_end && !lane_fully_initialized" in submit
    assert "initialized_snapshot & worker_bit(physical)" in submit
    assert "reserve_unstarted_worker" in submit
    assert "worker_already_started_fast_path" in submit


def test_v99_all_56_pairs_inherit_initialized_admission_snapshot() -> None:
    pairs = concurrency_pair_guarantees()
    assert len(pairs) == 8
    assert sum(len(outputs) for outputs in pairs.values()) == 56
    assert "python" in pairs
    for input_name, outputs in pairs.items():
        assert len(outputs) == 7, input_name
        for guarantee in outputs.values():
            assert STAGE in guarantee["shared_parallel_stages"]
            assert guarantee["source_to_sink_parallel_path"] is True
            assert guarantee["eligible_multi_benefit"] is True


def test_v99_native_order_startup_cancel_and_drain_remain_exact() -> None:
    require_native()
    for workers in (2, 4, 5, 8, 16):
        elapsed, completed, checksum, started, peak, queued, submitted = (
            native_core.ordered_executor_arena_completion_probe(workers, 10_000, 0)
        )
        assert elapsed > 0
        assert completed == 10_000
        assert checksum >= 0
        assert 1 <= started <= workers
        assert 1 <= peak <= workers
        assert queued == 0
        assert submitted == 10_000

    elapsed, active, observed_stop, queued = native_core.operation_task_arena_cancellation_probe()
    assert elapsed >= 0
    assert active == 0
    assert observed_stop >= 1
    assert queued == 0


def test_v99_documentation_and_benchmark_cover_matrix_and_host_limit() -> None:
    text = DOC.read_text(encoding="utf-8")
    benchmark = BENCH.read_text(encoding="utf-8")
    assert "8 x 7 = 56" in text
    assert "pure-Python" in text
    assert "initialized_mask" in text
    assert "five-CPU" in text
    assert "stale snapshot is always conservative" in text
    for workers in ('"2"', '"4"', '"5"', '"8"', '"16"'):
        assert workers in benchmark
