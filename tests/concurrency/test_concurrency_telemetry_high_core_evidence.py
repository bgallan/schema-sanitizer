"""Regression coverage for concurrency telemetry high core evidence."""

from __future__ import annotations

import json
from pathlib import Path

from conftest import require_native

from benchmarks.concurrency.telemetry.runner import run_operation

_COLUMNS = tuple(f"column_{index:04d}" for index in range(128))
_MEMORY_LIMIT = 128 * 1024 * 1024


def _write_fixture(path: Path, rows: int) -> None:
    """Write a deterministic very-wide JSONL fixture."""
    with path.open("w", encoding="utf-8") as handle:
        for row_index in range(rows):
            row = {name: row_index + ordinal for ordinal, name in enumerate(_COLUMNS)}
            handle.write(json.dumps(row, separators=(",", ":")) + "\n")


def test_arrow_stream_workload_consumes_and_releases_without_pyarrow(
    tmp_path: Path,
) -> None:
    """The Arrow-only baseline owns and releases every C Stream object directly."""
    require_native()
    source = tmp_path / "wide.jsonl"
    _write_fixture(source, 512)

    _elapsed, report, output = run_operation(
        source,
        tmp_path / "unused.jsonl",
        workload="arrow_stream",
        expected_rows=512,
        memory_limit_bytes=_MEMORY_LIMIT,
    )

    assert output["rows"] == 512
    assert output["batches"] > 0
    assert report["finished"] is True
    assert report["counters"]["source_rows"] == 512
    assert report["counters"]["output_batches"] == output["batches"]


def test_full_output_workload_retains_identical_row_count(tmp_path: Path) -> None:
    """The paired JSONL workload exercises the complete output encoder."""
    require_native()
    source = tmp_path / "wide.jsonl"
    output_path = tmp_path / "wide-output.jsonl"
    _write_fixture(source, 128)

    _, report, output = run_operation(
        source,
        output_path,
        workload="jsonl_to_jsonl",
        expected_rows=128,
        memory_limit_bytes=_MEMORY_LIMIT,
    )

    assert output["rows"] == 128
    assert sum(1 for _ in output_path.open(encoding="utf-8")) == 128
    assert report["finished"] is True
    assert report["phases"]["output"]["calls"] > 0
