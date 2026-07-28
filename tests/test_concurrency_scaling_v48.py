"""Regression coverage for v48 high-core direct submit reservation."""

from __future__ import annotations

from pathlib import Path

from conftest import require_native

from schema_sanitizer.core_impl.native_runtime import native_core


def test_v48_high_core_completion_remains_exact_and_bounded() -> None:
    """The 16-worker path preserves ordinals, checksum, and worker ceilings."""
    require_native()
    elapsed_us, completed, checksum, started, peak, queued, submitted = (
        native_core.ordered_executor_arena_completion_probe(16, 50_000, 4)
    )

    assert elapsed_us > 0
    assert completed == 50_000
    assert checksum != 0
    assert 1 <= started <= 16
    assert 1 <= peak <= 16
    assert queued == 0
    assert submitted == 50_000


def test_v48_high_core_and_legacy_paths_are_value_deterministic() -> None:
    """Crossing the eight-worker gate cannot change ordered results."""
    require_native()
    legacy = native_core.ordered_executor_arena_completion_probe(8, 20_000, 16)
    high_core = native_core.ordered_executor_arena_completion_probe(16, 20_000, 16)

    assert legacy[1] == high_core[1] == 20_000
    assert legacy[2] == high_core[2]
    assert legacy[5] == high_core[5] == 0
    assert legacy[6] == high_core[6] == 20_000


def test_v48_source_keeps_v47_submit_path_through_eight_workers() -> None:
    """Only high-core arena executors bypass the duplicate precheck lock."""
    root = Path(__file__).resolve().parents[1]
    source = (root / "cpp/src/internal/runtime/ordered_executor.hh").read_text(encoding="utf-8")

    assert "worker_count_ > 8 && uses_arena_completion_slots()" in source
    assert "submit_high_core_arena(std::move(packet))" in source
    assert "One-through-eight workers keep the v47 path" in source
    helper = (root / "cpp/src/internal/runtime/ordered_executor_submission.cc.inc").read_text(
        encoding="utf-8"
    )
    assert helper.count("std::lock_guard lock(mutex_)") == 2
    assert "dispatch window is full" in helper
    assert "++scheduled_external_tasks_[completion_shard];" in helper
    assert "arena_->Submit" in helper
