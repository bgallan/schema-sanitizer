"""Runtime guarantees for the global per-operation memory limit."""

from __future__ import annotations

import json
import subprocess
import sys
import textwrap
from pathlib import Path
from typing import Any

import pytest
from conftest import require_native

import schema_sanitizer as ss
from schema_sanitizer.api_impl.execution_context import default_pool

_MIB = 1024 * 1024
_MULTI_THREAD_LIMIT_BYTES = 32 * _MIB
_REJECTION_LIMIT_BYTES = 1 * _MIB


@pytest.fixture(scope="module")
def jsonl_source_larger_than_budget(tmp_path_factory: pytest.TempPathFactory) -> Path:
    """Create a valid input whose file size is larger than the tested budget."""
    source = tmp_path_factory.mktemp("memory-limit") / "larger-than-budget.jsonl"
    whitespace_chunk = b" " * _MIB
    with source.open("wb") as handle:
        for _ in range(40):
            handle.write(whitespace_chunk)
        for index in range(4_096):
            row = {
                "id": index,
                "payload": f"row-{index:05d}",
                "values": [index, index + 1, index + 2],
            }
            handle.write(json.dumps(row, separators=(",", ":")).encode("utf-8"))
            handle.write(b"\n")
    assert source.stat().st_size > _MULTI_THREAD_LIMIT_BYTES
    return source


def _writer(output_format: str) -> Any:
    """Return a public file writer by output format."""
    return {
        "jsonl": ss.to_jsonl,
        "csv": ss.to_csv,
        "parquet": ss.to_parquet,
    }[output_format]


@pytest.mark.parametrize("output_format", ["jsonl", "csv", "parquet"])
def test_multithread_file_outputs_keep_native_peak_within_global_limit(
    output_format: str,
    jsonl_source_larger_than_budget: Path,
    tmp_path: Path,
) -> None:
    """All file sinks share one bounded pool, even when the source is larger."""
    require_native()
    output = tmp_path / f"bounded.{output_format}"

    result = _writer(output_format)(
        jsonl_source_larger_than_budget,
        output,
        input_format="jsonl",
        memory_limit_bytes=_MULTI_THREAD_LIMIT_BYTES,
        multi_threading=True,
    )
    telemetry = default_pool().get().performance_stats()
    memory = telemetry["memory"]

    assert output.is_file()
    assert result.clean_data is None
    assert result.execution_policy is not None
    assert telemetry["effective_workers"] == result.execution_policy["effective_workers"] >= 1
    assert result.execution_policy["effective_workers"] <= result.execution_policy["available_cpus"]
    assert telemetry["threading_mode"] == "multi"
    assert telemetry["finished"] is True
    assert memory["limit_bytes"] == _MULTI_THREAD_LIMIT_BYTES
    assert 0 < memory["peak_bytes"] <= memory["limit_bytes"]
    assert memory["current_bytes"] == 0
    assert 0.0 < memory["peak_to_limit_ratio"] <= 1.0


@pytest.mark.parametrize("output_format", ["jsonl", "csv", "parquet"])
def test_oversized_row_is_rejected_without_publishing_partial_output(
    output_format: str,
    tmp_path: Path,
) -> None:
    """A single allocation that cannot fit fails before the final file exists."""
    require_native()
    source = tmp_path / "oversized-row.jsonl"
    output = tmp_path / f"must-not-exist.{output_format}"
    source.write_text(
        '{"payload":"' + ("x" * (2 * _MIB)) + '"}\n',
        encoding="utf-8",
    )

    with pytest.raises(
        ss.SchemaSanitizerOutOfMemoryError,
        match="out of memory|memory_limit_bytes",
    ):
        _writer(output_format)(
            source,
            output,
            input_format="jsonl",
            memory_limit_bytes=_REJECTION_LIMIT_BYTES,
        )

    memory = default_pool().get().performance_stats()["memory"]
    assert not output.exists()
    assert {path.name for path in tmp_path.iterdir()} == {source.name}
    assert memory["limit_bytes"] == _REJECTION_LIMIT_BYTES
    assert 0 <= memory["peak_bytes"] <= memory["limit_bytes"]
    assert memory["current_bytes"] == 0


@pytest.mark.skipif(
    sys.platform != "linux",
    reason="ru_maxrss byte conversion and allocator baseline are Linux-specific",
)
def test_large_file_does_not_cause_file_sized_resident_memory_growth(
    tmp_path: Path,
) -> None:
    """Catch regressions that read an input file wholesale outside the native pool."""
    require_native()
    limit_bytes = 8 * _MIB
    source = tmp_path / "mostly-whitespace.jsonl"
    output = tmp_path / "bounded.jsonl"
    whitespace_chunk = b" " * _MIB
    with source.open("wb") as handle:
        for _ in range(64):
            handle.write(whitespace_chunk)
        for index in range(20):
            handle.write(f'{{"value":{index}}}\n'.encode())

    child = textwrap.dedent(
        """
        import json
        import resource
        import sys
        import tempfile
        from pathlib import Path

        import schema_sanitizer as ss
        from schema_sanitizer.api_impl.execution_context import default_pool

        source = Path(sys.argv[1])
        output = Path(sys.argv[2])
        limit_bytes = int(sys.argv[3])
        with tempfile.TemporaryDirectory() as directory:
            warm_source = Path(directory) / "warm.jsonl"
            warm_output = Path(directory) / "warm-output.jsonl"
            warm_source.write_text('{"value":0}\\n', encoding="utf-8")
            ss.to_jsonl(
                warm_source,
                warm_output,
                input_format="jsonl",
                memory_limit_bytes=limit_bytes,
            )
            peak_before = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss * 1024
            ss.to_jsonl(
                source,
                output,
                input_format="jsonl",
                memory_limit_bytes=limit_bytes,
            )
            peak_after = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss * 1024
            telemetry = default_pool().get().performance_stats()
            print(
                json.dumps(
                    {
                        "rss_growth_bytes": max(0, peak_after - peak_before),
                        "memory": telemetry["memory"],
                    }
                )
            )
        """
    )
    source_root = str(Path(__file__).resolve().parents[2] / "src")
    completed = subprocess.run(
        [sys.executable, "-c", child, str(source), str(output), str(limit_bytes)],
        check=True,
        capture_output=True,
        text=True,
        timeout=60,
        env={"PYTHONPATH": source_root},
    )
    payload = json.loads(completed.stdout)
    memory = payload["memory"]

    assert source.stat().st_size > 8 * limit_bytes
    assert len(output.read_text(encoding="utf-8").splitlines()) == 20
    assert memory["limit_bytes"] == limit_bytes
    assert 0 < memory["peak_bytes"] <= limit_bytes
    assert memory["current_bytes"] == 0
    assert payload["rss_growth_bytes"] <= 3 * limit_bytes
