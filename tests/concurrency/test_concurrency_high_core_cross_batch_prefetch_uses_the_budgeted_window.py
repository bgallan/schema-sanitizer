"""Regression coverage for concurrency high core cross batch prefetch uses the budgeted window."""

from __future__ import annotations

from pathlib import Path

from conftest import require_native

from schema_sanitizer.core_impl.native_runtime import native_core


def test_high_core_cross_batch_prefetch_uses_the_budgeted_window() -> None:
    """High-core ingestion must not leave already-reserved reorder slots idle."""
    root = Path(__file__).resolve().parents[2]
    dispatch = (
        root / "cpp/src/internal/materialization/ingest_stream/parallel_source_dispatch.cc"
    ).read_text(encoding="utf-8")

    assert "policy_.available_cpus >= 16" in dispatch
    assert "policy_.effective_workers > 8" in dispatch
    assert "const auto submission_window = executor_->dispatch_window();" in dispatch
    assert "policy_.effective_workers + 2" not in dispatch
    assert "executor_->in_flight() >= submission_window" in dispatch
    assert "outstanding_packets_ > 0 && !cross_batch_prefetch" in dispatch


def test_dispatch_source_has_one_canonical_owner() -> None:
    """The dispatch implementation cannot acquire a second unbuilt source head."""
    root = Path(__file__).resolve().parents[2]
    matches = sorted(root.glob("cpp/src/internal/**/parallel_source_dispatch.cc"))

    assert matches == [
        root / "cpp/src/internal/materialization/ingest_stream/parallel_source_dispatch.cc"
    ]


def test_low_and_high_core_ordered_results_remain_identical() -> None:
    """Using more already-budgeted slots cannot change the ordinal oracle."""
    require_native()
    low = native_core.ordered_executor_arena_completion_probe(8, 20_000, 32)
    high = native_core.ordered_executor_arena_completion_probe(16, 20_000, 32)

    assert low[1] == high[1] == 20_000
    assert low[2] == high[2]
    assert low[5] == high[5] == 0
    assert low[6] == high[6] == 20_000
