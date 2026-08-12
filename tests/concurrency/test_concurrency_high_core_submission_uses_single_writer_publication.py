"""Regression coverage for concurrency high core submission uses single writer publication."""

from pathlib import Path

from conftest import require_native

from schema_sanitizer.core_impl.native_runtime import native_core

ROOT = Path(__file__).resolve().parents[2]
EXECUTOR = ROOT / "cpp/src/internal/runtime/ordered_executor.hh"
SUBMISSION = ROOT / "cpp/src/internal/runtime/ordered_executor_submission.cc.inc"
COMPLETION = ROOT / "cpp/src/internal/runtime/ordered_executor_arena_completion.cc.inc"


def test_high_core_submission_uses_single_writer_publication():
    """Verify the named concurrency regression contract."""
    header = EXECUTOR.read_text()
    submission = SUBMISSION.read_text()
    assert "increment_high_core_in_flight_locked" in header
    helper = header.split("void increment_high_core_in_flight_locked", 1)[1].split("}\n", 1)[0]
    assert "in_flight_.load(std::memory_order_relaxed)" in helper
    assert "in_flight_.store(current + 1U, std::memory_order_release)" in helper
    assert "increment_high_core_in_flight_locked();" in submission
    assert "in_flight_.fetch_add" not in submission


def test_low_core_and_consumption_paths_retain_atomic_rmw():
    """Verify the named concurrency regression contract."""
    header = EXECUTOR.read_text()
    completion = COMPLETION.read_text()
    # Inline, regular arena and local-pool submissions remain atomic RMWs.
    assert header.count("in_flight_.fetch_add(1, std::memory_order_release);") == 3
    assert "in_flight_.fetch_sub(1, std::memory_order_release);" in header
    assert "in_flight_.fetch_sub(1, std::memory_order_release);" in completion
    assert "decrement_arena_in_flight_locked" not in header + completion


def test_native_order_rollback_cancel_and_drain():
    """Verify the named concurrency regression contract."""
    require_native()
    for workers in (2, 4, 5, 8, 16):
        elapsed, completed, _, started, peak, queued, submitted = (
            native_core.ordered_executor_arena_completion_probe(workers, 5000, 0)
        )
        assert elapsed > 0
        assert completed == 5000
        assert 1 <= started <= workers
        assert 1 <= peak <= workers
        assert queued == 0
        assert submitted == 5000
    _, active, observed, queued = native_core.operation_task_arena_cancellation_probe()
    assert active == 0
    assert observed >= 1
    assert queued == 0
