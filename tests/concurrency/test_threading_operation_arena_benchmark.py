"""Validate inputs and baseline selection for the operation-arena scaling benchmark.

Worker counts must be positive, sorted, and unique, and reported speedups must use the
multi-threading run at one worker as their consistent scaling baseline.
"""

from __future__ import annotations

from benchmarks.concurrency.threading import operation_arena_scaling


def test_worker_counts_are_positive_sorted_and_unique() -> None:
    """The scaling matrix canonicalizes affinity values deterministically."""
    assert operation_arena_scaling._parse_worker_counts("8, 2,4,2,1") == (1, 2, 4, 8)


def test_summary_uses_multi_one_worker_as_scaling_baseline() -> None:
    """Speedup curves compare identical multi pipelines across affinities."""
    results = {}
    for workers, seconds in ((1, 8.0), (2, 5.0), (4, 2.0)):
        cases = {
            name: {"multi_seconds": seconds, "equivalent": True}
            for name in operation_arena_scaling.PIPELINE_CASES
        }
        results[workers] = {"effective_workers": workers, "cases": cases}

    summary = operation_arena_scaling._summarize(results, operation_arena_scaling.PIPELINE_CASES)

    for pipeline in operation_arena_scaling.PIPELINE_CASES:
        curve = summary[pipeline]["curve"]
        assert [point["speedup_vs_one_worker"] for point in curve] == [1.0, 1.6, 4.0]
        assert [point["marginal_speedup_vs_previous"] for point in curve] == [
            None,
            1.6,
            2.5,
        ]
        assert [point["added_effective_workers"] for point in curve] == [None, 1, 2]
        assert summary[pipeline]["best_speedup"] == 4.0
        assert summary[pipeline]["monotonic_non_regression"] is True
        assert summary[pipeline]["minimum_marginal_speedup"] == 1.6
