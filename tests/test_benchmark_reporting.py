"""Regression tests for the benchmark reporting harness."""

from __future__ import annotations

import json
import sys
from pathlib import Path

BENCHMARKS = Path(__file__).resolve().parents[1] / "benchmarks"
if str(BENCHMARKS) not in sys.path:
    sys.path.insert(0, str(BENCHMARKS))

from bench_timer import records, reset_records, set_default_warmups, time_call  # noqa: E402
from benchmark_report import write_report  # noqa: E402


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
