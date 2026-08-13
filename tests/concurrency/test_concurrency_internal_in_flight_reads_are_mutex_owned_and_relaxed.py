"""Regression coverage for concurrency internal in flight reads are mutex owned and relaxed."""

import re
from pathlib import Path

from conftest import require_native

from schema_sanitizer.core_impl.native_runtime import native_core

ROOT = Path(__file__).resolve().parents[2]
EXECUTOR = ROOT / "cpp/src/internal/runtime/ordered_executor.hh"
SUBMISSION = ROOT / "cpp/src/internal/runtime/ordered_executor_submission.cc.inc"
COMPLETION = ROOT / "cpp/src/internal/runtime/ordered_executor_arena_completion.cc.inc"


def test_internal_in_flight_reads_are_mutex_owned_and_relaxed():
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


def test_dead_completed_counter_is_removed():
    """The unread completed counter and all of its writes remain absent."""
    source = EXECUTOR.read_text()
    assert "completed_count_" not in source


def test_native_cancellation_drain_stays_exact():
    """Native cancellation leaves no active or queued arena work."""
    require_native()
    drained, active, observed, queued = native_core.operation_task_arena_cancellation_probe()
    assert drained is True
    assert active == 0
    assert observed >= 1
    assert queued == 0
