"""Validate inputs and baseline selection for the operation-arena scaling benchmark.

Worker counts must be positive, sorted, and unique, and reported speedups must use the
multi-threading run at one worker as their consistent scaling baseline.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from benchmarks.concurrency.threading import operation_arena_scaling
from benchmarks.concurrency.threading.dimensions import (
    benchmark_dimensions,
    expected_benchmark_case_names,
)


def test_worker_counts_are_positive_sorted_and_unique() -> None:
    """The scaling matrix canonicalizes affinity values deterministically."""
    assert operation_arena_scaling._parse_worker_counts("8, 2,4,2,1") == (1, 2, 4, 8)


def test_summary_uses_multi_one_worker_as_scaling_baseline() -> None:
    """Speedup curves compare identical multi pipelines across affinities."""
    pipeline_cases = expected_benchmark_case_names("pipeline")
    results = {}
    for workers, seconds in ((1, 8.0), (2, 5.0), (4, 2.0)):
        cases = {name: {"multi_seconds": seconds, "equivalent": True} for name in pipeline_cases}
        results[workers] = {"effective_workers": workers, "cases": cases}

    summary = operation_arena_scaling._summarize(results, pipeline_cases)

    for pipeline in pipeline_cases:
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


def test_worker_case_requires_strict_true_equivalence(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Truthy non-booleans cannot certify a worker scaling child report."""
    selected = expected_benchmark_case_names(
        "pipeline", pipeline_shape="scalar", pipeline_format="csv"
    )

    def fake_run(command: list[str], **_kwargs: object) -> None:
        """Write a truthy but invalid equivalence result for the child command."""
        output = Path(command[command.index("--output") + 1])
        dimensions = benchmark_dimensions(
            rows=8,
            memory_mib=64,
            wide_columns=16,
            nested_depth=2,
            source_count=1,
            parquet_compression="snappy",
            cpu_quota=1,
            warmups=0,
            repeats=1,
            selection="pipeline",
            pipeline_shape="scalar",
            pipeline_format="csv",
        )
        output.write_text(
            json.dumps(
                {
                    "dimensions": dimensions,
                    "cases": {selected[0]: {"equivalent": 1}},
                }
            ),
            encoding="utf-8",
        )

    monkeypatch.setattr(operation_arena_scaling, "run_command", fake_run)

    with pytest.raises(RuntimeError, match="equivalence must be a boolean"):
        operation_arena_scaling._run_worker_case(
            1,
            rows=8,
            sources=1,
            memory_mib=64,
            warmups=0,
            repeats=1,
            pipeline_shape="scalar",
            pipeline_format="csv",
            nested_depth=2,
            parquet_compression="snappy",
            directory=tmp_path,
        )
