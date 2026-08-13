"""Shared invariants and load bounds for the native completion probe."""

from __future__ import annotations

from collections.abc import Sequence

FUNCTIONAL_WORKERS = (1, 2, 4, 5, 8, 16)
FUNCTIONAL_TASKS = 2_048
FUNCTIONAL_WORK_ITERATIONS = 1
DETERMINISM_WORK_ITERATIONS = 32
STRESS_WORKERS = 16
STRESS_TASKS = 50_000
STRESS_WORK_ITERATIONS = 4
UINT64_MASK = (1 << 64) - 1
PROBE_SEED = 0x9E3779B97F4A7C15
PROBE_MULTIPLIER = 0xBF58476D1CE4E5B9


def completion_ring_capacity(workers: int) -> int:
    """Return the capacity selected by the native probe."""
    return max(32, workers * 4)


def assert_ring_generations(workers: int, tasks: int, minimum: int) -> None:
    """Prove that a case crosses enough complete completion-ring generations."""
    assert tasks // completion_ring_capacity(workers) >= minimum


def completion_checksum(tasks: int, work_iterations: int) -> int:
    """Reproduce the probe's independent ordered uint64 value oracle."""
    checksum = 0
    for ordinal in range(tasks):
        value = ordinal ^ PROBE_SEED
        for _ in range(work_iterations):
            value ^= (value << 7) & UINT64_MASK
            value ^= value >> 9
            value = (value * PROBE_MULTIPLIER) & UINT64_MASK
        checksum ^= (value + ordinal) & UINT64_MASK
    return checksum


def assert_exact_completion(
    result: Sequence[int],
    *,
    workers: int,
    tasks: int,
    work_iterations: int,
) -> None:
    """Assert the complete native result contract for inline and arena paths."""
    _elapsed_us, completed, checksum, started, peak, queued, submitted = result

    assert completed == tasks
    assert checksum == completion_checksum(tasks, work_iterations)
    assert queued == 0
    if workers == 1:
        assert started == 0
        assert peak == 0
        assert submitted == 0
    else:
        if workers in (2, 4):
            # These widths fit every standard platform runner and historically
            # prove that all requested arena workers become live.
            assert started == workers
        else:
            assert 1 <= started <= workers
        assert 1 <= peak <= started
        assert submitted == tasks
