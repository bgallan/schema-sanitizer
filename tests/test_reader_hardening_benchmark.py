"""Smoke coverage for the isolated reader hardening A/B benchmark."""

from __future__ import annotations

import importlib.util
import json
from pathlib import Path

from conftest import require_native

ROOT = Path(__file__).resolve().parents[1]
SCRIPT = ROOT / "benchmarks" / "bench_reader_hardening_ab.py"


def test_reader_hardening_ab_benchmark_runs_isolated_trees() -> None:
    """A tiny same-tree comparison validates all four valid-reader probes."""
    require_native()
    spec = importlib.util.spec_from_file_location("bench_reader_hardening_ab", SCRIPT)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)

    report = module.compare(ROOT, ROOT, rows=16, width=2, repeats=1)
    names = {item["name"] for item in report["comparisons"]}
    assert names == {
        "jsonl_single",
        "jsonl_multi",
        "csv_single",
        "csv_multi",
        "xml_single",
        "xml_multi",
        "parquet_preflight",
    }
    assert all(item["candidate_to_baseline_ratio"] > 0 for item in report["comparisons"])


def test_recorded_reader_hardening_release_benchmark_stays_within_reviewed_budget() -> None:
    """The matched Release A/B evidence must remain inside the reviewed envelope."""
    report = json.loads(
        (ROOT / "benchmarks" / "reader_hardening_pass4_ab.json").read_text(encoding="utf-8")
    )
    policy = json.loads(
        (ROOT / "benchmarks" / "reader_hardening_performance_budget.json").read_text(
            encoding="utf-8"
        )
    )["maximum_candidate_to_baseline_ratio"]
    observed = {
        item["name"]: float(item["candidate_to_baseline_ratio"]) for item in report["comparisons"]
    }
    assert observed.keys() == policy.keys()
    assert {
        name: {"observed": observed[name], "maximum": maximum}
        for name, maximum in policy.items()
        if observed[name] > float(maximum)
    } == {}
