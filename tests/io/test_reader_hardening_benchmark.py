"""Smoke coverage for the isolated reader hardening A/B benchmark."""

from __future__ import annotations

import json
from pathlib import Path

from benchmarks.readers import hardening_ab

ROOT = Path(__file__).resolve().parents[2]


def test_reader_hardening_ab_benchmark_runs_isolated_trees(require_native: None) -> None:
    """A tiny same-tree comparison validates all four valid-reader probes."""
    report = hardening_ab.compare(ROOT, ROOT, rows=16, width=2, repeats=1)
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
        (ROOT / "benchmarks" / "evidence" / "readers" / "hardening-ab.json").read_text(
            encoding="utf-8"
        )
    )
    policy = json.loads(
        (ROOT / "benchmarks" / "evidence" / "readers" / "performance-budget.json").read_text(
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
