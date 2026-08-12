"""Regression coverage for concurrency high core completion remains exact and bounded."""

from __future__ import annotations

from conftest import require_native

from schema_sanitizer.core_impl.native_runtime import native_core


def test_high_core_completion_remains_exact_and_bounded() -> None:
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


def test_high_core_and_legacy_paths_are_value_deterministic() -> None:
    """Crossing the eight-worker gate cannot change ordered results."""
    require_native()
    legacy = native_core.ordered_executor_arena_completion_probe(8, 20_000, 16)
    high_core = native_core.ordered_executor_arena_completion_probe(16, 20_000, 16)

    assert legacy[1] == high_core[1] == 20_000
    assert legacy[2] == high_core[2]
    assert legacy[5] == high_core[5] == 0
    assert legacy[6] == high_core[6] == 20_000
