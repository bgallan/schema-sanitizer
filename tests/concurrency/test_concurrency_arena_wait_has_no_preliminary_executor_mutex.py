"""Regression coverage for concurrency arena wait has no preliminary executor mutex."""

from __future__ import annotations

from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
EXECUTOR = ROOT / "cpp/src/internal/runtime/ordered_executor.hh"
COMPLETION = ROOT / "cpp/src/internal/runtime/ordered_executor_arena_completion.cc.inc"


def _take_next_arena_source() -> str:
    """Return the arena-backed TakeNext implementation for structural checks."""
    source = COMPLETION.read_text(encoding="utf-8")
    start = source.index("sanitize::Result<Outcome> take_next_arena()")
    end = source.index("void close_empty_arena_slot", start)
    return source[start:end]


def test_arena_wait_has_no_preliminary_executor_mutex() -> None:
    """The slot wait occurs before the one authoritative executor lock."""
    take = _take_next_arena_source()
    wait = take.index("WaitOnAtomic(slot.state")
    first_executor_lock = take.index("std::lock_guard lock(mutex_)")

    assert "const auto expected_ordinal = next_take_ordinal_;" in take[:wait]
    assert "std::lock_guard lock(mutex_)" not in take[:wait]
    assert first_executor_lock > wait
    assert take.count("std::lock_guard lock(mutex_)") == 1


def test_local_pool_keeps_ring_outcome_validation() -> None:
    """Only arena completion changes; local/inline fallback keeps ordinal checks."""
    executor = EXECUTOR.read_text(encoding="utf-8")

    assert "std::vector<std::optional<Outcome>> completed_;" in executor
    assert "slot->ordinal == next_take_ordinal_" in executor
    assert "store_outcome_locked" in executor
