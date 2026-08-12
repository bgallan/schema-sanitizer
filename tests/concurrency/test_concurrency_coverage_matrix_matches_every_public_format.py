"""Regression coverage for concurrency coverage matrix matches every public format."""

from __future__ import annotations

import inspect
from pathlib import Path

import pytest
from _support.diagnostics import assert_diagnostics_semantically_equal

import schema_sanitizer as ss
from schema_sanitizer.core_impl.concurrency_coverage import (
    INPUT_CONCURRENCY_COVERAGE,
    OUTPUT_CONCURRENCY_COVERAGE,
    concurrency_coverage,
)
from schema_sanitizer.core_impl.execution import ExecutionContext
from schema_sanitizer.input_impl.selection import _FILE_FORMATS
from schema_sanitizer.options_impl.call_options import normalize_call_options

ROOT = Path(__file__).resolve().parents[2]
ROW_STREAM = ROOT / "cpp/src/sanitize/core/row_stream.hh"
PREPARE = ROOT / "cpp/src/ingest/prepare/prepare.cc"
CSV_FRONTEND = ROOT / "cpp/src/frontends/csv/frontend.cc"
XML_FRONTEND = ROOT / "cpp/src/frontends/xml/frontend.cc"
JSON_GROUP = ROOT / "cpp/src/frontends/json/path_group_frontend.cc"
PARQUET_INPUT = ROOT / "cpp/src/internal/parquet/footer_reader"
CSV_OUTPUT = ROOT / "cpp/src/internal/csv/csv_stream_writer.cc"
JSONL_OUTPUT = ROOT / "cpp/src/internal/json_output/jsonl_stream_writer.cc"
PARQUET_OUTPUT = ROOT / "cpp/src/internal/parquet/stream_writer"
ORDERED_TEXT_OUTPUT = ROOT / "cpp/src/internal/output/ordered_text_output.hh"


def _options(*, threading_mode: str, xml_row_tag: str | None = None):
    """Build the native options used by format-concurrency probes."""
    kwargs = {
        "multi_threading": threading_mode == "multi",
        "memory_limit_bytes": 128 * 1024 * 1024,
        "field_name_policy": "preserve",
    }
    if xml_row_tag is not None:
        kwargs["xml_row_tag"] = xml_row_tag
    return normalize_call_options(**kwargs).raw


def _probe_path(path: Path, *, input_format: str, threading_mode: str, **kwargs):
    """Probe one path and return both logical output and task telemetry."""
    context = ExecutionContext()
    result = context.schema_probe_paths(
        input_format,
        [str(path)],
        _options(threading_mode=threading_mode, **kwargs),
    )
    return result, context.performance_stats()


def _assert_probe_equal(single, multi) -> None:
    """Require exact schema, field order, and diagnostics parity."""
    assert multi.schema_payload == single.schema_payload
    assert multi.field_names == single.field_names
    assert_diagnostics_semantically_equal(multi.diagnostics, single.diagnostics)


def _input_tasks(stats: dict) -> int:
    """Return submitted input tasks from one native telemetry report."""
    return int(stats.get("tasks", {}).get("input", {}).get("submitted", 0))


def test_coverage_matrix_matches_every_public_format() -> None:
    """Every supported input and output has a declared concurrent stage."""
    assert set(INPUT_CONCURRENCY_COVERAGE) == {
        "csv",
        "json",
        "json_array",
        "jsonl",
        "ndjson",
        "xml",
        "parquet",
        "python",
    }
    assert set(OUTPUT_CONCURRENCY_COVERAGE) == {
        "csv",
        "jsonl",
        "parquet",
        "pyarrow",
        "pandas",
        "polars",
        "duckdb",
    }
    assert set(INPUT_CONCURRENCY_COVERAGE) - {"ndjson", "python"} == set(_FILE_FORMATS)
    assert all(stages for stages in INPUT_CONCURRENCY_COVERAGE.values())
    assert all(stages for stages in OUTPUT_CONCURRENCY_COVERAGE.values())
    for output_name in OUTPUT_CONCURRENCY_COVERAGE:
        public_converter = getattr(ss, f"to_{output_name}")
        parameters = inspect.signature(public_converter).parameters
        assert parameters["multi_threading"].default is False
        assert "threading_mode" not in parameters
    assert concurrency_coverage() == {
        "inputs": dict(INPUT_CONCURRENCY_COVERAGE),
        "outputs": dict(OUTPUT_CONCURRENCY_COVERAGE),
    }


def test_frontends_receive_the_shared_operation_arena() -> None:
    """All native text frontends can participate in the common task arena."""
    row_stream = ROW_STREAM.read_text(encoding="utf-8")
    prepare = PREPARE.read_text(encoding="utf-8")
    csv = CSV_FRONTEND.read_text(encoding="utf-8")
    xml = XML_FRONTEND.read_text(encoding="utf-8")
    json_group = JSON_GROUP.read_text(encoding="utf-8")

    assert "void (*set_task_arena)" in row_stream
    assert "frontend.set_task_arena(task_arena)" in prepare
    assert ".set_task_arena = &csv_set_task_arena" in csv
    assert ".set_task_arena = &xml_set_task_arena" in xml
    assert ".set_task_arena = &group_set_task_arena" in json_group


def test_every_native_input_and_output_has_a_parallel_route() -> None:
    """Static contracts guard the concurrent route of every native format."""
    csv = CSV_FRONTEND.read_text(encoding="utf-8")
    xml = XML_FRONTEND.read_text(encoding="utf-8")
    parquet_input = "\n".join(
        path.read_text(encoding="utf-8")
        for path in PARQUET_INPUT.rglob("*")
        if path.is_file() and path.suffix in {".cc", ".inc"}
    )
    csv_output = CSV_OUTPUT.read_text(encoding="utf-8")
    jsonl_output = JSONL_OUTPUT.read_text(encoding="utf-8")
    ordered_text_output = ORDERED_TEXT_OUTPUT.read_text(encoding="utf-8")
    parquet_output = "\n".join(
        path.read_text(encoding="utf-8")
        for path in PARQUET_OUTPUT.rglob("*")
        if path.is_file() and path.suffix in {".cc", ".inc"}
    )

    assert "OrderedExecutor" in csv
    assert "TaskTelemetryKind::kInput" in csv
    assert "OrderedExecutor" in xml
    assert "TaskTelemetryKind::kInput" in xml
    assert "worker_count * 2U" in xml
    assert "TaskTelemetryKind::kInput" in parquet_input
    assert "ordered_text_output::write_stream" in csv_output
    assert "ordered_text_output::write_stream" in jsonl_output
    assert "TaskTelemetryKind::kOutput" in ordered_text_output
    assert "TaskTelemetryKind::kOutput" in parquet_output

    combined = csv + xml
    assert "getenv" not in combined
    assert "std::thread" not in combined


def test_csv_rows_decode_in_parallel_with_exact_parity(tmp_path: Path) -> None:
    """Eligible CSV inference publishes input work and preserves exact output."""
    path = tmp_path / "wide.csv"
    columns = [f"field_{index:02d}" for index in range(24)]
    lines = [",".join(columns)]
    for row in range(4_096):
        values = [
            f'"row {row}, column {column}, quoted ""value"""' for column in range(len(columns))
        ]
        lines.append(",".join(values))
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")

    single, single_stats = _probe_path(path, input_format="csv", threading_mode="single")
    multi, multi_stats = _probe_path(path, input_format="csv", threading_mode="multi")

    _assert_probe_equal(single, multi)
    assert _input_tasks(single_stats) == 0
    assert _input_tasks(multi_stats) >= 2
    assert multi_stats["counters"]["peak_active_tasks"] >= 2


def test_xml_rows_decode_in_parallel_with_exact_parity(tmp_path: Path) -> None:
    """Eligible row-tag XML publishes ordered input work with exact parity."""
    path = tmp_path / "rows.xml"
    padding = "parallel-xml-payload-" * 32
    rows = []
    for row in range(1_024):
        fields = "".join(
            f"<field_{column:02d}>{padding}{row}-{column}</field_{column:02d}>"
            for column in range(12)
        )
        rows.append(f"<record><ordinal>{row}</ordinal>{fields}</record>")
    path.write_text("<root>" + "".join(rows) + "</root>", encoding="utf-8")

    single, single_stats = _probe_path(
        path,
        input_format="xml",
        threading_mode="single",
        xml_row_tag="record",
    )
    multi, multi_stats = _probe_path(
        path,
        input_format="xml",
        threading_mode="multi",
        xml_row_tag="record",
    )

    _assert_probe_equal(single, multi)
    assert _input_tasks(single_stats) == 0
    assert _input_tasks(multi_stats) >= 2
    assert multi_stats["counters"]["peak_active_tasks"] >= 2


def test_small_xml_keeps_the_overhead_avoiding_fallback(tmp_path: Path) -> None:
    """Tiny XML rows avoid artificial tasks while downstream remains concurrent."""
    path = tmp_path / "small.xml"
    path.write_text(
        "<root>"
        + "".join(
            f"<record><ordinal>{row}</ordinal><value>{row}</value></record>" for row in range(32)
        )
        + "</root>",
        encoding="utf-8",
    )
    single, single_stats = _probe_path(
        path,
        input_format="xml",
        threading_mode="single",
        xml_row_tag="record",
    )
    multi, multi_stats = _probe_path(
        path,
        input_format="xml",
        threading_mode="multi",
        xml_row_tag="record",
    )

    _assert_probe_equal(single, multi)
    assert _input_tasks(single_stats) == 0
    assert _input_tasks(multi_stats) == 0


@pytest.mark.parametrize(
    "output_name, expected_stage",
    [
        ("csv", "native_parallel_sink"),
        ("jsonl", "native_parallel_sink"),
        ("parquet", "native_parallel_sink"),
        ("pyarrow", "parallel_native_stream"),
        ("pandas", "parallel_native_stream"),
        ("polars", "parallel_native_stream"),
        ("duckdb", "parallel_native_stream"),
    ],
)
def test_output_coverage_is_explicit(output_name: str, expected_stage: str) -> None:
    """Each public output documents the concurrent stage it receives."""
    assert expected_stage in OUTPUT_CONCURRENCY_COVERAGE[output_name]
