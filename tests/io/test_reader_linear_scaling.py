"""Regression coverage for the reader linear-complexity contract."""

from __future__ import annotations

import json
from pathlib import Path

from conftest import require_native

from benchmarks.readers import linear_scaling

ROOT = Path(__file__).resolve().parents[2]
REPORT = ROOT / "benchmarks" / "evidence" / "readers" / "linear-scaling.json"


def test_recorded_reader_linear_scaling_evidence_stays_within_budget() -> None:
    """The retained matched-build run must remain inside the reviewed gate."""
    report = json.loads(REPORT.read_text(encoding="utf-8"))
    comparisons = {item["name"]: item for item in report["comparisons"]}

    assert report["within_budget"] is True
    assert report["failures"] == {}
    assert set(comparisons) == {
        "csv_single",
        "csv_multi",
        "jsonl_single",
        "jsonl_multi",
        "xml_single",
        "xml_multi",
    }
    assert all(
        float(item["max_time_growth_per_input_growth"])
        <= float(report["maximum_normalized_growth"])
        for item in comparisons.values()
    )


def test_reader_linear_scaling_harness_runs_all_text_frontends() -> None:
    """A tiny smoke run protects the executable benchmark contract."""
    require_native()
    report = linear_scaling.run(ROOT, sizes=[8, 16], repeats=1)
    names = {item["name"] for item in report["comparisons"]}
    assert names == {
        "csv_single",
        "csv_multi",
        "jsonl_single",
        "jsonl_multi",
        "xml_single",
        "xml_multi",
    }
    assert all(len(item["growth"]) == 1 for item in report["comparisons"])


def test_reader_complexity_contract_documents_every_native_reader() -> None:
    """The public contract must cover text readers and Parquet explicitly."""
    contract = (ROOT / "docs" / "operations" / "reader-complexity.md").read_text(encoding="utf-8")

    assert "O(input bytes + decoded output bytes)" in contract
    for reader in ("CSV", "JSON", "XML", "Parquet"):
        assert f"- {reader}" in contract
    assert "benchmarks.readers.linear_scaling" in contract
