"""Regression coverage for concurrency reuses bounded completion slots in exact ordinal order."""

from __future__ import annotations

from conftest import require_native

from schema_sanitizer.core_impl.native_runtime import native_core


def test_preserves_prompt_arena_stage_cancellation() -> None:
    """Per-slot publication does not strand cancelled in-flight workers."""
    require_native()
    drained, active, observed_stop, queued = native_core.operation_task_arena_cancellation_probe()

    assert drained is True
    assert active == 0
    assert 1 <= observed_stop <= 4
    assert queued == 0
