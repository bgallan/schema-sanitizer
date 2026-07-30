"""Regression coverage for v87 modulo-free ordered completion rings."""

from __future__ import annotations

from pathlib import Path

from conftest import require_native

from schema_sanitizer.core_impl.concurrency_coverage import (
    concurrency_pair_guarantees,
)
from schema_sanitizer.core_impl.native_runtime import native_core

ROOT = Path(__file__).resolve().parents[1]
RING = ROOT / "cpp/src/internal/runtime/ordered_executor_completion_ring.hh"
EXECUTOR = ROOT / "cpp/src/internal/runtime/ordered_executor.hh"
COMPLETION = ROOT / "cpp/src/internal/runtime/ordered_executor_arena_completion.cc.inc"
SUBMISSION = ROOT / "cpp/src/internal/runtime/ordered_executor_submission.cc.inc"


def test_v87_completion_ring_uses_branch_wrap_without_runtime_modulo() -> None:
    """Submission and take cursors wrap without integer division."""
    ring = RING.read_text(encoding="utf-8")

    assert "class CompletionRingCursor final" in ring
    assert "ReserveSubmit" in ring
    assert "RollbackSubmit" in ring
    assert "NextTake" in ring
    assert "AdvanceTake" in ring
    assert "%" not in ring
    assert "if (++next_submit_ == capacity_)" in ring
    assert "if (++next_take_ == capacity_)" in ring


def test_v87_reserved_slot_crosses_all_executor_submission_paths() -> None:
    """Inline, local-pool, normal arena, and high-core arena paths reserve once."""
    executor = EXECUTOR.read_text(encoding="utf-8")
    submission = SUBMISSION.read_text(encoding="utf-8")

    assert "ScheduledOrdinalPacket<Packet>" in executor
    assert executor.count("completion_ring_.ReserveSubmit()") == 3
    assert "completion_ring_.ReserveSubmit()" in submission
    assert "completion_ring_.RollbackSubmit()" in executor
    assert "completion_ring_.RollbackSubmit()" in submission
    assert "std::deque<ScheduledPacket> tasks_;" in executor


def test_v87_publication_and_take_reuse_reserved_slot() -> None:
    """Workers and the coordinator no longer derive the normal slot by ordinal."""
    executor = EXECUTOR.read_text(encoding="utf-8")
    completion = COMPLETION.read_text(encoding="utf-8")

    assert "arena_completed_[completion_slot]" in completion
    assert "arena_completed_[completion_ring_.NextTake()]" in completion
    assert "completed_[completion_ring_.NextTake()]" in executor
    assert "completed_[completion_slot]" in executor
    assert "completion_ring_.AdvanceTake()" in completion
    assert "completion_ring_.AdvanceTake()" in executor
    # Modulo remains only for the one-shot empty-slot close at finish.
    assert completion.count("slot_index(") == 1


def test_v87_packet_and_outcome_aggregate_layouts_remain_unchanged() -> None:
    """The slot metadata is internal and cannot break existing initializers."""
    executor = EXECUTOR.read_text(encoding="utf-8")
    packet = executor[executor.index("template <class Payload> struct OrdinalPacket") :]
    packet = packet[: packet.index("template <class Value>")]
    outcome = executor[executor.index("template <class Value> struct OrdinalOutcome") :]
    outcome = outcome[: outcome.index("template <class Input")]

    assert "completion_slot" not in packet
    assert "completion_slot" not in outcome
    assert "Payload payload;" in packet
    assert "sanitize::Result<Value> result;" in outcome


def test_v87_all_56_pairs_inherit_modulo_free_ordered_commit() -> None:
    """Every supported source and sink crosses the common completion ring."""
    pairs = concurrency_pair_guarantees()

    assert sum(len(outputs) for outputs in pairs.values()) == 56
    for input_name, outputs in pairs.items():
        assert len(outputs) == 7, input_name
        for guarantee in outputs.values():
            assert "modulo_free_ordered_completion_ring" in guarantee["shared_parallel_stages"]
            assert guarantee["source_to_sink_parallel_path"] is True
            assert guarantee["eligible_multi_benefit"] is True


def test_v87_native_completion_ring_preserves_exact_order_and_counts() -> None:
    """The cursor implementation preserves the bounded native completion contract."""
    require_native()
    elapsed, completed, checksum, started, peak, queued, submitted = (
        native_core.ordered_executor_arena_completion_probe(4, 20_000, 0)
    )

    assert elapsed > 0
    assert completed == 20_000
    assert checksum >= 0
    assert started == 4
    assert 1 <= peak <= 4
    assert queued == 0
    assert submitted == 20_000
