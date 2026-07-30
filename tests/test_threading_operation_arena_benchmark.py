"""Contracts for the operation-wide native arena scaling benchmark."""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SCRIPT = ROOT / "benchmarks" / "bench_operation_arena_scaling.py"


def _module():
    """Load the scaling benchmark as a testable module."""
    spec = importlib.util.spec_from_file_location("bench_operation_arena_scaling", SCRIPT)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def test_worker_counts_are_positive_sorted_and_unique() -> None:
    """The scaling matrix canonicalizes affinity values deterministically."""
    module = _module()

    assert module._parse_worker_counts("8, 2,4,2,1") == (1, 2, 4, 8)


def test_summary_uses_multi_one_worker_as_scaling_baseline() -> None:
    """Speedup curves compare identical multi pipelines across affinities."""
    module = _module()
    results = {}
    for workers, seconds in ((1, 8.0), (2, 5.0), (4, 2.0)):
        cases = {
            name: {"multi_seconds": seconds, "equivalent": True} for name in module.PIPELINE_CASES
        }
        results[workers] = {"effective_workers": workers, "cases": cases}

    summary = module._summarize(results, module.PIPELINE_CASES)

    for pipeline in module.PIPELINE_CASES:
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
