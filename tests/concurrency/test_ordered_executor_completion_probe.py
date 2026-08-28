"""Canonical cross-platform coverage for native ordered completion."""

from __future__ import annotations

import ast
from pathlib import Path

import pytest
from _ordered_executor_probe import (
    DETERMINISM_WORK_ITERATIONS,
    FUNCTIONAL_TASKS,
    FUNCTIONAL_WORK_ITERATIONS,
    FUNCTIONAL_WORKERS,
    STRESS_TASKS,
    STRESS_WORK_ITERATIONS,
    STRESS_WORKERS,
    assert_exact_completion,
    assert_ring_generations,
)
from conftest import require_native

from schema_sanitizer.core_impl.native_runtime import native_core

CONCURRENCY_TESTS = Path(__file__).resolve().parent


@pytest.mark.parametrize("workers", FUNCTIONAL_WORKERS)
def test_completion_probe_matrix_is_exact_and_bounded(workers: int) -> None:
    """Every scheduler boundary gets the same functional load on every OS."""
    require_native()
    assert_ring_generations(workers, FUNCTIONAL_TASKS, minimum=32)

    result = native_core.ordered_executor_arena_completion_probe(
        workers,
        FUNCTIONAL_TASKS,
        FUNCTIONAL_WORK_ITERATIONS,
    )

    assert_exact_completion(
        result,
        workers=workers,
        tasks=FUNCTIONAL_TASKS,
        work_iterations=FUNCTIONAL_WORK_ITERATIONS,
    )


def test_completion_probe_is_worker_count_deterministic() -> None:
    """Crossing the eight-worker gate cannot change the ordered value oracle."""
    require_native()
    low = native_core.ordered_executor_arena_completion_probe(
        8,
        FUNCTIONAL_TASKS,
        DETERMINISM_WORK_ITERATIONS,
    )
    high = native_core.ordered_executor_arena_completion_probe(
        16,
        FUNCTIONAL_TASKS,
        DETERMINISM_WORK_ITERATIONS,
    )

    assert_exact_completion(
        low,
        workers=8,
        tasks=FUNCTIONAL_TASKS,
        work_iterations=DETERMINISM_WORK_ITERATIONS,
    )
    assert_exact_completion(
        high,
        workers=16,
        tasks=FUNCTIONAL_TASKS,
        work_iterations=DETERMINISM_WORK_ITERATIONS,
    )
    assert low[1] == high[1] == FUNCTIONAL_TASKS
    assert low[2] == high[2]


@pytest.mark.native_stress
def test_completion_probe_sustains_many_ring_generations() -> None:
    """One explicit heavy case exercises the maximum 16-worker load."""
    require_native()
    assert_ring_generations(STRESS_WORKERS, STRESS_TASKS, minimum=781)

    result = native_core.ordered_executor_arena_completion_probe(
        STRESS_WORKERS,
        STRESS_TASKS,
        STRESS_WORK_ITERATIONS,
    )

    assert_exact_completion(
        result,
        workers=STRESS_WORKERS,
        tasks=STRESS_TASKS,
        work_iterations=STRESS_WORK_ITERATIONS,
    )


def test_completion_probe_has_one_dynamic_test_owner() -> None:
    """Contract modules cannot multiply the high-volume native workload again."""
    owners: dict[str, int] = {}
    for path in CONCURRENCY_TESTS.glob("test_*.py"):
        tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
        calls = sum(
            1
            for node in ast.walk(tree)
            if isinstance(node, ast.Call)
            and isinstance(node.func, ast.Attribute)
            and node.func.attr == "ordered_executor_arena_completion_probe"
        )
        if calls:
            owners[path.name] = calls

    assert owners == {Path(__file__).name: 4}
