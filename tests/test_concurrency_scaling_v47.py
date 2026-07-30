"""Regression coverage for v47 distributed arena completion publication."""

from __future__ import annotations

from pathlib import Path

from conftest import require_native

from schema_sanitizer.core_impl.native_runtime import native_core


def test_v47_reuses_bounded_completion_slots_in_exact_ordinal_order() -> None:
    """Many circular slot generations drain without loss or reordering."""
    require_native()
    elapsed_us, completed, checksum, started, peak, queued, submitted = (
        native_core.ordered_executor_arena_completion_probe(16, 40_000, 16)
    )

    assert elapsed_us > 0
    assert completed == 40_000
    assert checksum != 0
    assert 1 <= started <= 16
    assert 1 <= peak <= 16
    assert queued == 0
    assert submitted == 40_000


def test_v47_completion_result_is_worker_count_deterministic() -> None:
    """Changing arena width cannot change ordered values or ownership."""
    require_native()
    narrow = native_core.ordered_executor_arena_completion_probe(4, 20_000, 32)
    wide = native_core.ordered_executor_arena_completion_probe(16, 20_000, 32)

    assert narrow[1] == wide[1] == 20_000
    assert narrow[2] == wide[2]
    assert narrow[5] == wide[5] == 0
    assert narrow[6] == wide[6] == 20_000


def test_v47_strict_single_worker_path_remains_inline() -> None:
    """One worker still creates no arena helper and uses the legacy slot path."""
    require_native()
    _, completed, checksum, started, peak, queued, submitted = (
        native_core.ordered_executor_arena_completion_probe(1, 10_000, 8)
    )

    assert completed == 10_000
    assert checksum != 0
    assert started == 0
    assert peak == 0
    assert queued == 0
    assert submitted == 0


def test_v47_worker_completion_has_no_executor_wide_result_lock() -> None:
    """Arena workers publish to ordinal-owned slots without one shared mutex."""
    root = Path(__file__).resolve().parents[1]
    executor = (root / "cpp/src/internal/runtime/ordered_executor.hh").read_text(encoding="utf-8")
    arena_completion = (
        root / "cpp/src/internal/runtime/ordered_executor_arena_completion.cc.inc"
    ).read_text(encoding="utf-8")
    publisher = arena_completion.split("void publish_external_outcome", 1)[1].split(
        "sanitize::Result<Outcome> take_next_arena", 1
    )[0]

    assert "compare_exchange_strong" in publisher
    assert "ArenaSlotState::kPublishing" in publisher
    assert "slot.state.notify_one()" in publisher
    assert "lock(mutex_)" not in publisher
    assert "shared completion counter" in publisher
    assert "arena_completed_(uses_arena_completion_slots()" in executor


def test_v47_preserves_prompt_arena_stage_cancellation() -> None:
    """Per-slot publication does not strand cancelled in-flight workers."""
    require_native()
    elapsed_us, active, observed_stop, queued = (
        native_core.operation_task_arena_cancellation_probe()
    )

    assert elapsed_us < 100_000
    assert active == 0
    assert observed_stop == 4
    assert queued == 0
