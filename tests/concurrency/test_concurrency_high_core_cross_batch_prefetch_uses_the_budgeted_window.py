"""Regression coverage for concurrency high core cross batch prefetch uses the budgeted window."""

from __future__ import annotations

from pathlib import Path


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
