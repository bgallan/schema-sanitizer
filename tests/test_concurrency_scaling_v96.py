"""Regression coverage for v96 authoritative worker-start publication."""

from pathlib import Path

from conftest import require_native

from schema_sanitizer.core_impl.concurrency_coverage import concurrency_pair_guarantees
from schema_sanitizer.core_impl.native_runtime import native_core

ROOT = Path(__file__).resolve().parents[1]
ARENA = ROOT / "cpp/src/internal/runtime/operation_task_arena.cc"
RUNTIME = ROOT / "cpp/src/internal/runtime/operation_task_arena_runtime.cc.inc"
DOC = ROOT / "CONCURRENCY_SCALING_V96.md"
STAGE = "authoritative_started_mask_start_lock_elision"


def test_v96_started_mask_elides_repeated_start_mutex_checks() -> None:
    arena = ARENA.read_text(encoding="utf-8")
    runtime = RUNTIME.read_text(encoding="utf-8")
    helper = runtime[
        runtime.index("worker_already_started_fast_path") : runtime.index(
            "sanitize::Status ensure_worker_started"
        )
    ]
    startup = runtime[runtime.index("sanitize::Status ensure_worker_started") :]

    assert "state->worker_count >= 4U" in helper
    assert "state->started_mask.load(std::memory_order_acquire)" in helper
    assert "worker_already_started_fast_path(state_, physical)" in arena
    assert "state->started_mask.fetch_or(worker_bit(index)" in startup
    assert startup.index("slot.worker = std::make_unique<std::jthread>") < startup.index(
        "state->started_mask.fetch_or(worker_bit(index)"
    )


def test_v96_started_mask_is_the_only_started_worker_authority() -> None:
    arena = ARENA.read_text(encoding="utf-8")
    state_fields = arena[arena.index("const std::size_t worker_count") : arena.index("namespace {")]
    started = arena[
        arena.index("OperationTaskArena::started_workers") : arena.index(
            "OperationTaskArena::wake_epoch_publishes"
        )
    ]

    assert "std::atomic<std::size_t> started" not in state_fields
    assert "state->started.fetch_add" not in arena
    assert "std::popcount" in started
    assert "state_->started_mask.load(std::memory_order_acquire)" in started


def test_v96_all_56_pairs_inherit_started_mask_fast_path() -> None:
    pairs = concurrency_pair_guarantees()
    assert sum(len(outputs) for outputs in pairs.values()) == 56
    for input_name, outputs in pairs.items():
        assert len(outputs) == 7, input_name
        for guarantee in outputs.values():
            assert STAGE in guarantee["shared_parallel_stages"]
            assert guarantee["source_to_sink_parallel_path"] is True
            assert guarantee["eligible_multi_benefit"] is True


def test_v96_native_startup_and_ordered_drain_remain_exact() -> None:
    require_native()
    for workers in (2, 4, 5, 8, 16):
        elapsed, completed, checksum, started, peak, queued, submitted = (
            native_core.ordered_executor_arena_completion_probe(workers, 20_000, 0)
        )
        assert elapsed > 0
        assert completed == 20_000
        assert checksum >= 0
        assert 1 <= started <= workers
        assert 1 <= peak <= workers
        assert queued == 0
        assert submitted == 20_000


def test_v96_documentation_records_matrix_safety_and_mixed_ab() -> None:
    text = DOC.read_text(encoding="utf-8")
    assert "8 x 7" in text
    assert "pure-Python" in text
    assert "start_mutex" in text
    assert "std::popcount" in text
    assert "memory remains bounded" in text
    assert "five-CPU" in text
