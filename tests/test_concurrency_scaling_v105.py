"""Regression coverage for v105 mutex-owned memory-order tightening."""

import re
from pathlib import Path

from conftest import require_native

from schema_sanitizer.core_impl.concurrency_coverage import concurrency_pair_guarantees
from schema_sanitizer.core_impl.native_runtime import native_core

ROOT = Path(__file__).resolve().parents[1]
EXECUTOR = ROOT / "cpp/src/internal/runtime/ordered_executor.hh"
SUBMISSION = ROOT / "cpp/src/internal/runtime/ordered_executor_submission.cc.inc"
COMPLETION = ROOT / "cpp/src/internal/runtime/ordered_executor_arena_completion.cc.inc"
STAGE = "mutex_owned_memory_order_tightening"


def test_v105_internal_in_flight_reads_are_mutex_owned_and_relaxed():
    """Internal in-flight decisions use relaxed snapshots only under mutex_."""
    header = EXECUTOR.read_text()
    submission = SUBMISSION.read_text()
    assert "std::size_t in_flight_locked() const noexcept" in header
    helper = header.split("std::size_t in_flight_locked() const noexcept", 1)[1]
    helper = helper.split("}\n", 1)[0]
    assert "in_flight_.load(std::memory_order_relaxed)" in helper
    assert len(re.findall(r"(?<![A-Za-z0-9_])in_flight_locked\(\)", header)) == 4
    assert len(re.findall(r"(?<![A-Za-z0-9_])in_flight_locked\(\)", submission)) == 1
    assert "return in_flight_.load(std::memory_order_acquire);" in header


def test_v105_completion_slot_orders_are_minimal_and_safe():
    """Slot reuse keeps acquire publication while dropping redundant barriers."""
    completion = COMPLETION.read_text()
    claim = completion.split("compare_exchange_strong", 1)[1].split(") {", 1)[0]
    assert "ArenaSlotState::kPublishing" in claim
    assert re.search(r"std::memory_order_acquire,\s+std::memory_order_acquire", claim)
    assert "std::memory_order_acq_rel" not in claim
    assert "state = slot.state.load(std::memory_order_relaxed);" in completion
    assert "slot.state.store(published_state, std::memory_order_release);" in completion


def test_v105_dead_completed_counter_is_removed():
    """The unread completed counter and all of its writes remain absent."""
    source = EXECUTOR.read_text()
    assert "completed_count_" not in source


def test_v105_all_56_pairs_inherit_stage():
    """Every supported input/output pair inherits the shared v105 stage."""
    pairs = concurrency_pair_guarantees()
    assert len(pairs) == 8
    assert sum(map(len, pairs.values())) == 56
    assert "python" in pairs
    for outputs in pairs.values():
        assert len(outputs) == 7
        for guarantee in outputs.values():
            assert STAGE in guarantee["shared_parallel_stages"]


def test_v105_native_order_cancel_and_drain():
    """Native order, cancellation, worker bounds, and final drain stay exact."""
    require_native()
    for workers in (2, 4, 5, 8, 16):
        elapsed, completed, checksum, started, peak, queued, submitted = (
            native_core.ordered_executor_arena_completion_probe(workers, 6000, 0)
        )
        assert elapsed > 0
        assert completed == 6000
        assert checksum >= 0
        assert 1 <= started <= workers
        assert 1 <= peak <= workers
        assert queued == 0
        assert submitted == 6000
    _, active, observed, queued = native_core.operation_task_arena_cancellation_probe()
    assert active == 0
    assert observed >= 1
    assert queued == 0
