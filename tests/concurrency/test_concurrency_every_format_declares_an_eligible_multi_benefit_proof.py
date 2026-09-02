"""Require every format to declare executable evidence for useful multi-worker execution.

Parquet cases complement the coverage proof by routing small row groups through the common output
arena, committing page indexes only after absolute output commit, and preserving logical results.
"""

from __future__ import annotations

import csv
import json
from pathlib import Path

import pytest
from _support.resource_fakes import CapsuleStream

import schema_sanitizer as ss
from schema_sanitizer.api_impl.execution_context import default_pool
from schema_sanitizer.core_impl.concurrency_coverage import (
    INPUT_CONCURRENCY_COVERAGE,
    OUTPUT_CONCURRENCY_COVERAGE,
    concurrency_guarantees,
)
from schema_sanitizer.core_impl.native_runtime import native_core

ROOT = Path(__file__).resolve().parents[2]
WRITER_API = ROOT / "cpp/src/internal/parquet/stream_writer/stream_writer_api.cc.inc"
ROW_GROUP_PARALLEL = (
    ROOT / "cpp/src/internal/parquet/stream_writer/stream_writer_row_group_parallel.cc.inc"
)

_GENERATED_COLUMNS = {
    "schema_registry",
    "schema_drifts",
    "source_file",
    "ingestion_timestamp",
}


def _write_wide_jsonl(path: Path, *, rows: int = 4_096, columns: int = 24) -> None:
    """Write deterministic wide input that creates several short Arrow batches."""
    with path.open("w", encoding="utf-8") as handle:
        for row in range(rows):
            payload = {
                f"field_{column:02d}": (
                    f"row-{row}-column-{column}" if column % 2 == 0 else row * (column + 3)
                )
                for column in range(columns)
            }
            handle.write(json.dumps(payload, separators=(",", ":")))
            handle.write("\n")


def _user_csv_rows(path: Path) -> list[dict[str, str]]:
    """Return ordered user data while excluding invocation metadata columns."""
    with path.open("r", encoding="utf-8", newline="") as handle:
        return [
            {key: value for key, value in row.items() if key not in _GENERATED_COLUMNS}
            for row in csv.DictReader(handle)
        ]


def test_every_format_declares_an_eligible_multi_benefit_proof() -> None:
    """Every supported input/output has an explicit executable benefit contract."""
    guarantees = concurrency_guarantees()
    assert set(guarantees["inputs"]) == set(INPUT_CONCURRENCY_COVERAGE)
    assert set(guarantees["outputs"]) == set(OUTPUT_CONCURRENCY_COVERAGE)
    for family in guarantees.values():
        for contract in family.values():
            assert contract["eligible_multi_benefit"] is True
            assert contract["benefit_proof"]
            assert contract["parallel_stages"]
            assert contract["serial_boundaries"]
    assert "ordered_row_group_overlap" in OUTPUT_CONCURRENCY_COVERAGE["parquet"]


def test_small_parquet_row_groups_use_the_common_output_arena() -> None:
    """Four-worker operations overlap row groups without creating another pool."""
    source = WRITER_API.read_text(encoding="utf-8")
    assert "arena_workers >= 4U" in source
    assert "std::max<std::size_t>(2U, arena_workers / 8U)" in source
    assert "fraction continues growing on hosts wider than 32 workers" in source
    assert "RowGroupExecutor::Make" in source
    assert "TaskArenaLane::kOutput" in source
    assert "TaskTelemetryKind::kOutput" in source
    assert "task_arena" in source
    assert "std::thread" not in source
    assert "getenv" not in source


def test_page_indexes_are_encoded_only_after_absolute_commit() -> None:
    """Asynchronous row groups publish page indexes with absolute file offsets."""
    source = ROW_GROUP_PARALLEL.read_text(encoding="utf-8")
    prepare_start = source.index("prepare_serial_row_group(")
    commit_start = source.index("commit_prepared_row_group(")
    prepare = source[prepare_start:commit_start]
    commit = source[commit_start:]

    assert "write_row_group_page_indexes" not in prepare
    assert commit.index("add_row_group_base_offset") < commit.index("out.Write")
    assert commit.index("out.Write") < commit.index("write_row_group_page_indexes")
    assert "const std::vector<LeafColumn> &columns" in commit


def test_parquet_output_parallelizes_and_remains_logically_exact(
    tmp_path: Path,
    require_native: None,
) -> None:
    """Eligible Parquet output uses output workers and remains natively readable."""
    source = tmp_path / "wide.jsonl"
    _write_wide_jsonl(source)

    outputs: dict[str, Path] = {}
    stats: dict[str, dict[str, object]] = {}
    footers: dict[str, dict[str, object]] = {}
    for mode in ("single", "multi"):
        output = tmp_path / f"{mode}.parquet"
        outputs[mode] = output
        ss.to_parquet(
            source,
            output,
            input_format="jsonl",
            multi_threading=mode == "multi",
            memory_limit_bytes=128 << 20,
            parse_integers=True,
            on_error="stop",
            parquet_compression="uncompressed",
        )
        stats[mode] = default_pool().get().performance_stats()
        footers[mode] = json.loads(native_core.parquet_footer_info_json(str(output)))

    if int(stats["multi"].get("effective_workers", 1)) < 4:
        pytest.skip(
            "every-format-declares-an-eligible-multi-benefit row-group overlap requires at least four effective workers"
        )

    single_tasks = stats["single"]["tasks"]["output"]
    multi_tasks = stats["multi"]["tasks"]["output"]
    assert (
        int(single_tasks["submitted"])
        == int(single_tasks["started"])
        == int(single_tasks["finished"])
        == 0
    )
    assert (
        int(multi_tasks["submitted"])
        == int(multi_tasks["started"])
        == int(multi_tasks["finished"])
        >= 2
    )
    for mode in ("single", "multi"):
        assert int(stats[mode]["memory"]["peak_bytes"]) <= 128 << 20
        assert footers[mode]["native_reader_ready"] == 1
        assert footers[mode]["native_reader_blockers"] == []
        assert footers[mode]["num_rows"] == 4_096
    assert footers["single"]["schema_elements"] == footers["multi"]["schema_elements"]
    assert len(footers["multi"]["row_groups"]) >= 2

    from schema_sanitizer.core_impl.execution import ExecutionContext
    from schema_sanitizer.core_impl.native_symbols import CSV_STREAM_WRITE
    from schema_sanitizer.options_impl.call_options import normalize_call_options

    normalized: dict[str, list[dict[str, str]]] = {}
    for mode in ("single", "multi"):
        csv_output = tmp_path / f"{mode}.csv"
        projected = [
            str(element["name"])
            for element in footers[mode]["schema_elements"]
            if "physical_type" in element and str(element["name"]) not in _GENERATED_COLUMNS
        ]
        capsule = native_core.parquet_stream_read(str(outputs[mode]), projected, 128 << 20)
        context = ExecutionContext()
        options = normalize_call_options(
            multi_threading=False,
            memory_limit_bytes=128 << 20,
            on_error="stop",
        ).raw
        stream = context.to_sink_arrow_stream("stream", "arrow", CapsuleStream(capsule), options)
        CSV_STREAM_WRITE(stream, str(csv_output), 128 << 20, 0)
        stream.close_main_stream()
        normalized[mode] = _user_csv_rows(csv_output)
    assert normalized["single"] == normalized["multi"]
