"""Regression coverage for v88 slot-authoritative arena consumption."""

from __future__ import annotations

from pathlib import Path

from conftest import require_native

from schema_sanitizer.core_impl.concurrency_coverage import (
    concurrency_pair_guarantees,
)
from schema_sanitizer.core_impl.native_runtime import native_core

ROOT = Path(__file__).resolve().parents[1]
EXECUTOR = ROOT / "cpp/src/internal/runtime/ordered_executor.hh"
COMPLETION = ROOT / "cpp/src/internal/runtime/ordered_executor_arena_completion.cc.inc"


def _take_next_arena_source() -> str:
    """Return the arena-backed TakeNext implementation for structural checks."""
    source = COMPLETION.read_text(encoding="utf-8")
    start = source.index("sanitize::Result<Outcome> take_next_arena()")
    end = source.index("void close_empty_arena_slot", start)
    return source[start:end]


def test_v88_arena_wait_has_no_preliminary_executor_mutex() -> None:
    """The slot wait occurs before the one authoritative executor lock."""
    take = _take_next_arena_source()
    wait = take.index("WaitOnAtomic(slot.state")
    first_executor_lock = take.index("std::lock_guard lock(mutex_)")

    assert "const auto expected_ordinal = next_take_ordinal_;" in take[:wait]
    assert "std::lock_guard lock(mutex_)" not in take[:wait]
    assert first_executor_lock > wait
    assert take.count("std::lock_guard lock(mutex_)") == 1


def test_v88_local_pool_keeps_v87_outcome_validation() -> None:
    """Only arena completion changes; local/inline fallback keeps ordinal checks."""
    executor = EXECUTOR.read_text(encoding="utf-8")

    assert "std::vector<std::optional<Outcome>> completed_;" in executor
    assert "slot->ordinal == next_take_ordinal_" in executor
    assert "store_outcome_locked" in executor


def test_v88_all_56_pairs_inherit_slot_authoritative_coordination() -> None:
    """Every supported source/sink pair crosses the common arena contract."""
    pairs = concurrency_pair_guarantees()

    assert sum(len(outputs) for outputs in pairs.values()) == 56
    for input_name, outputs in pairs.items():
        assert len(outputs) == 7, input_name
        for guarantee in outputs.values():
            assert (
                "slot_terminal_state_pre_wait_coordination" in guarantee["shared_parallel_stages"]
            )
            assert guarantee["source_to_sink_parallel_path"] is True
            assert guarantee["eligible_multi_benefit"] is True


def test_v88_native_completion_preserves_order_for_two_and_four_workers() -> None:
    """Both measured worker counts retain exact native completion invariants."""
    require_native()
    for workers in (2, 4):
        elapsed, completed, checksum, started, peak, queued, submitted = (
            native_core.ordered_executor_arena_completion_probe(workers, 20_000, 0)
        )
        assert elapsed > 0
        assert completed == 20_000
        assert checksum >= 0
        assert started == workers
        assert 1 <= peak <= workers
        assert queued == 0
        assert submitted == 20_000
