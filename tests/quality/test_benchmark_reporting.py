"""Regression tests for the benchmark reporting harness."""

from __future__ import annotations

import json
import re
from pathlib import Path

from benchmarks.ingestion.reporting import write_report
from benchmarks.ingestion.timing import records, reset_records, set_default_warmups, time_call

ROOT = Path(__file__).resolve().parents[2]
HISTORICAL_FILENAME = re.compile(
    r"(?:^|[-_])(?:v\d{2,}|pass\d+|phase\d+|version\d+)(?:[-_.]|$)",
    re.IGNORECASE,
)


def test_benchmark_python_modules_are_grouped_by_domain() -> None:
    """Keep executable implementations out of the benchmark package root."""
    root = ROOT / "benchmarks"

    assert {path.name for path in root.glob("*.py")} == {"__init__.py"}
    expected_modules = {
        "concurrency/__init__.py",
        "concurrency/telemetry/__init__.py",
        "concurrency/telemetry/analysis.py",
        "concurrency/telemetry/cli.py",
        "concurrency/telemetry/high_core_evidence.py",
        "concurrency/telemetry/high_core_suite.py",
        "concurrency/telemetry/runner.py",
        "concurrency/telemetry/support.py",
        "concurrency/threading/__init__.py",
        "concurrency/threading/dimensions.py",
        "concurrency/threading/matrix.py",
        "concurrency/threading/modes.py",
        "concurrency/threading/operation_arena_scaling.py",
        "ingestion/__init__.py",
        "ingestion/cases.py",
        "ingestion/cli.py",
        "ingestion/compare.py",
        "ingestion/fixtures.py",
        "ingestion/read_cases.py",
        "ingestion/reporting.py",
        "ingestion/route_details.py",
        "ingestion/timing.py",
        "ingestion/write_cases.py",
        "pipeline/__init__.py",
        "pipeline/partition_lookahead.py",
        "readers/__init__.py",
        "readers/hardening_ab.py",
        "readers/linear_scaling.py",
        "readers/review_limits.py",
        "remote/__init__.py",
        "remote/providers.py",
    }
    actual_modules = {
        path.relative_to(root).as_posix()
        for path in root.rglob("*.py")
        if "__pycache__" not in path.parts
    }

    assert actual_modules == {"__init__.py", *expected_modules}


def test_benchmark_filenames_describe_stable_contracts() -> None:
    """Keep implementation milestones in provenance, not maintained paths."""
    root = ROOT / "benchmarks"
    offenders = {
        path.relative_to(root).as_posix()
        for path in root.rglob("*")
        if path.is_file()
        and "__pycache__" not in path.parts
        and any(HISTORICAL_FILENAME.search(part) for part in path.relative_to(root).parts)
    }

    assert offenders == set()


def test_concurrency_manifest_has_exact_evidence_and_probe_coverage() -> None:
    """Index every retained concurrency report and native probe exactly once."""
    manifest_path = ROOT / "benchmarks" / "evidence" / "concurrency" / "manifest.json"
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    records = manifest["records"]
    domains = set(manifest["domains"])

    assert domains == {"layout", "lifecycle", "safety", "scheduler", "telemetry"}
    assert len({record["id"] for record in records}) == len(records)

    evidence = [record["evidence"] for record in records if record["evidence"] is not None]
    probes = [probe for record in records for probe in record["probes"]]
    assert len(evidence) == len(set(evidence))
    assert len(probes) == len(set(probes))

    evidence_root = ROOT / "benchmarks" / "evidence" / "concurrency"
    actual_evidence = {
        path.relative_to(ROOT).as_posix()
        for path in evidence_root.rglob("*.json")
        if path != manifest_path
    }
    probe_root = ROOT / "benchmarks" / "probes" / "concurrency"
    actual_probes = {path.relative_to(ROOT).as_posix() for path in probe_root.rglob("*.cc")}
    assert set(evidence) == actual_evidence
    assert set(probes) == actual_probes

    for record in records:
        assert record["domain"] in domains
        if record["evidence"] is not None:
            assert f"/concurrency/{record['domain']}/" in record["evidence"]
        assert all(f"/concurrency/{record['domain']}/" in probe for probe in record["probes"])
        assert set(record) == {"id", "domain", "evidence", "probes"}


def test_retained_benchmark_evidence_is_valid_json() -> None:
    """Keep committed evidence machine-readable after moves and consolidation."""
    evidence_root = ROOT / "benchmarks" / "evidence"
    for path in evidence_root.rglob("*.json"):
        json.loads(path.read_text(encoding="utf-8"))


def test_time_call_records_median_p95_sizes_and_warmups(tmp_path: Path) -> None:
    """Record robust timings, sizes, repeats, and warmup counts."""
    calls = 0
    output = tmp_path / "out.bin"

    def work() -> object:
        """Write a deterministic output for each measured invocation."""
        nonlocal calls
        calls += 1
        output.write_bytes(b"result")
        return object()

    reset_records()
    set_default_warmups(2)
    record = time_call(
        "case",
        work,
        rows=10,
        repeats=3,
        input_bytes=100,
        output_bytes=output,
    )

    assert calls == 5
    assert record.warmups == 2
    assert record.repeats == 3
    assert record.input_bytes == 100
    assert record.output_bytes == len(b"result")
    assert record.median_seconds >= 0
    assert record.p95_seconds >= record.median_seconds
    assert records() == [record]


def test_write_report_contains_platform_fixture_and_records(tmp_path: Path) -> None:
    """Persist benchmark records together with fixture and platform metadata."""
    reset_records()
    set_default_warmups(0)
    record = time_call("noop", lambda: None, rows=1, repeats=1)
    output = tmp_path / "benchmark.json"

    write_report(output, [record], fixture_metadata={"rows": 1, "case": "noop"})

    payload = json.loads(output.read_text(encoding="utf-8"))
    assert payload["schema_version"] == 1
    assert payload["fixture"] == {"rows": 1, "case": "noop"}
    assert payload["platform"]["python"]
    assert payload["benchmarks"][0]["label"] == "noop"
