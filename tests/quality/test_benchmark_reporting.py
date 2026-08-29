"""Regression tests for the benchmark reporting harness.

It validates shell-free command isolation, package ownership, timing records,
machine-readable reports, route details, and privacy-safe limit reviews.
"""

from __future__ import annotations

import importlib.util
import json
import sys
from pathlib import Path

import pyarrow.parquet as pq
import pytest

from benchmarks.concurrency import assets as concurrency_assets
from benchmarks.concurrency.assets import load_catalog, stage_probes
from benchmarks.concurrency.threading import modes
from benchmarks.ingestion.reporting import write_report
from benchmarks.ingestion.timing import records, reset_records, set_default_warmups, time_call
from benchmarks.support.command import CAPTURE, DISCARD, MERGE_WITH_STDOUT, run_command

ROOT = Path(__file__).resolve().parents[2]


def test_run_command_uses_argv_and_captures_text() -> None:
    """A validated interpreter runs without a shell and returns captured streams."""
    completed = run_command(
        [sys.executable, "-c", "import sys; print('out'); print('err', file=sys.stderr)"],
        check=True,
        stdout=CAPTURE,
        stderr=CAPTURE,
        text=True,
        timeout=10,
    )

    assert completed.stdout.splitlines() == ["out"]
    assert completed.stderr.splitlines() == ["err"]


def test_run_command_supports_discard_and_stderr_merge() -> None:
    """Only the explicitly supported stream combinations are accepted."""
    completed = run_command(
        [sys.executable, "-c", "import sys; print('out'); print('err', file=sys.stderr)"],
        check=True,
        stdout=DISCARD,
        stderr=MERGE_WITH_STDOUT,
        text=True,
        timeout=10,
    )

    assert completed.stdout is None
    assert completed.stderr is None


def test_run_command_preserves_python_environment_prefix() -> None:
    """Executable validation must not resolve a virtualenv launcher out of its env."""
    completed = run_command(
        [sys.executable, "-c", "import sys; print(sys.prefix)"],
        check=True,
        stdout=CAPTURE,
        text=True,
    )

    assert isinstance(completed.stdout, str)
    assert Path(completed.stdout.strip()).resolve() == Path(sys.prefix).resolve()


@pytest.mark.parametrize("command", [[], "echo value", [""], ["python\0evil"]])
def test_run_command_rejects_non_argv_commands(command: object) -> None:
    """Shell strings, empty commands, and NUL-bearing arguments fail before execution."""
    with pytest.raises((TypeError, ValueError)):
        run_command(command)  # type: ignore[arg-type]


def test_run_command_rejects_stdout_merge() -> None:
    """The stderr-only merge sentinel cannot be misapplied to stdout."""
    with pytest.raises(ValueError, match="valid only for stderr"):
        run_command([sys.executable, "-c", "pass"], stdout=MERGE_WITH_STDOUT)


def test_threading_parquet_digest_closes_reader_before_cleanup(tmp_path: Path, monkeypatch) -> None:
    """The Windows benchmark must release its Parquet handle before unlinking."""
    output = tmp_path / "output.parquet"
    output.write_bytes(b"placeholder")
    lifecycle: list[str] = []

    class FakeTable:
        def to_pylist(self) -> list[dict[str, int]]:
            """Return one deterministic row after recording materialization."""
            lifecycle.append("materialize")
            return [{"value": 1}]

    class FakeParquetFile:
        def __init__(self, path: Path) -> None:
            """Record construction for the expected output path."""
            assert path == output
            lifecycle.append("construct")

        def __enter__(self) -> "FakeParquetFile":
            """Open the fake reader and return its context value."""
            lifecycle.append("open")
            return self

        def read(self) -> FakeTable:
            """Record the read and return a materializable table."""
            lifecycle.append("read")
            return FakeTable()

        def __exit__(self, *_exc: object) -> None:
            """Record that the reader closed before fixture cleanup."""
            lifecycle.append("close")

    monkeypatch.setattr(pq, "ParquetFile", FakeParquetFile)

    assert len(modes._logical_digest(output)) == 64
    assert lifecycle == ["construct", "open", "read", "materialize", "close"]
    output.unlink()


def test_benchmark_python_modules_are_grouped_by_domain() -> None:
    """Keep executable implementations out of the benchmark package root."""
    root = ROOT / "benchmarks"

    assert not [path for path in root.glob("*.py") if path.name != "__init__.py"]
    modules = [
        path
        for path in root.rglob("*.py")
        if path != root / "__init__.py" and "__pycache__" not in path.parts
    ]
    assert modules
    assert all(len(path.relative_to(root).parts) >= 2 for path in modules)
    assert all((path.parent / "__init__.py").is_file() for path in modules)


def test_concurrency_catalog_stages_every_probe(tmp_path: Path) -> None:
    """The current catalog is complete and every indexed probe remains compilable source."""
    catalog = load_catalog()
    expected = {probe for record in catalog["records"] for probe in record["probes"]}
    staged = stage_probes(tmp_path)

    assert {path.relative_to(tmp_path).as_posix() for path in staged} == expected
    assert all("main(" in path.read_text(encoding="utf-8") for path in staged)


def test_concurrency_probe_archive_repacking_is_safe_and_deterministic(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A staged tree round-trips exactly and rejects path traversal lookups."""
    staged_root = tmp_path / "staged"
    stage_probes(staged_root)
    archive = tmp_path / "concurrency.zip"
    monkeypatch.setattr(concurrency_assets, "PROBE_ARCHIVE", archive)

    concurrency_assets.pack_staged_probes(staged_root)
    first = archive.read_bytes()
    concurrency_assets.pack_staged_probes(staged_root)

    assert archive.read_bytes() == first
    assert "main(" in concurrency_assets.load_probe("layout/compact-queued-task-tsan.cc")
    with pytest.raises(ValueError, match="unsafe"):
        concurrency_assets.load_probe("../escape.cc")


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


def _review_module():
    """Load the reader-limit review utility directly from its repository path."""
    path = ROOT / "benchmarks/readers/review_limits.py"
    spec = importlib.util.spec_from_file_location("review_reader_limits", path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_review_waits_for_production_telemetry_without_hiding_clean_fuzzing() -> None:
    """Verify review waits for production telemetry without hiding clean fuzzing."""
    review = _review_module().build_review(
        {"status": "passed", "sanitizer_findings": 0, "mutation_runs": 40000}, []
    )
    assert review["review_status"] == "awaiting_production_telemetry"
    assert review["production_telemetry_present"] is False
    assert review["automatic_limit_change"] is False


def test_review_aggregates_only_privacy_safe_resource_counters() -> None:
    """Verify review aggregates only privacy safe resource counters."""
    review = _review_module().build_review(
        {"status": "passed", "sanitizer_findings": 0},
        [
            {
                "peak_charged_memory_bytes": 75,
                "operation_memory_limit_bytes": 100,
                "parser_max_depth": 12,
                "decompression_ratio": 4.0,
                "cancellation_reason": "consumer_close",
                "secret": "must-not-propagate",  # pragma: allowlist secret
            },
            {
                "peak_charged_memory_bytes": 20,
                "operation_memory_limit_bytes": 100,
                "parser_max_depth": 7,
                "cancellation_reason": "consumer_close",
            },
        ],
    )
    assert review["review_status"] == "complete"
    assert review["telemetry"]["max_peak_to_limit_ratio"] == 0.75
    assert review["telemetry"]["maxima"]["parser_max_depth"] == 12
    assert review["telemetry"]["cancellation_reasons"] == {"consumer_close": 2}
    assert "secret" not in str(review)
