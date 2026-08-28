"""Regression coverage for concurrency groups only sustained fixed cost columns."""

from __future__ import annotations

import hashlib
from pathlib import Path

from _support.resource_fakes import CapsuleStream

ROOT = Path(__file__).resolve().parents[2]
ROW_GROUP_DIR = (
    ROOT / "cpp/src/internal/parquet/footer_reader/native_stream/materialization/row_group"
)
ROW_GROUP = ROW_GROUP_DIR / "native_stream_row_group.cc.inc"
PARALLEL = ROW_GROUP_DIR / "native_stream_parallel_columns.cc.inc"
FOOTER = ROOT / "cpp/src/internal/parquet/footer_reader/footer_reader.cc"


def test_groups_only_sustained_fixed_cost_columns() -> None:
    """Wide fixed columns group; variable and repeated columns stay stream-owned."""
    source = PARALLEL.read_text(encoding="utf-8")

    assert "native_parquet_grouped_column_decode_eligible" in source
    assert "row_group.columns.size() < worker_count * 4" in source
    assert "std::ranges::all_of(row_group.columns" in source
    assert "column.repeated_level_layouts.empty()" in source
    for kind in (
        "plain_byte_array",
        "dictionary_byte_array",
        "delta_length_byte_array",
    ):
        case = f"case NativeValueBufferKind::{kind}:"
        assert case in source
    assert "materialize_native_row_group_columns_individual" in source
    assert "materialize_native_row_group_columns_grouped" in source


def test_grouped_ranges_preserve_column_order_and_one_budget() -> None:
    """Ranges are contiguous, ordered, bounded, and committed by exact ordinal."""
    source = PARALLEL.read_text(encoding="utf-8")
    row_group = ROW_GROUP.read_text(encoding="utf-8")
    footer = FOOTER.read_text(encoding="utf-8")

    assert "NativeParquetColumnRange" in source
    assert "native_parquet_balanced_column_ranges" in source
    assert "column.index != expected.first + offset" in source
    assert "commit_native_parquet_decoded_column" in source
    assert "enforce_native_array_retained_budget" in source
    assert "Executor::Make(worker_count, worker_count, worker_count" in source
    assert "TaskArenaLane::kUpstream" in source
    assert "TaskTelemetryKind::kInput" in source
    assert "memory_limit_bytes" not in source
    assert "getenv" not in source + row_group
    assert (
        footer.count(
            '#include "native_stream/materialization/row_group/'
            'native_stream_parallel_columns.cc.inc"'
        )
        == 1
    )


def test_fixed_wide_parquet_single_and_multi_are_byte_identical(
    tmp_path: Path,
    require_native: None,
) -> None:
    """Grouped decode preserves exact Arrow ownership and deterministic output."""
    from schema_sanitizer.core_impl.execution import ExecutionContext
    from schema_sanitizer.core_impl.native_runtime import native_core
    from schema_sanitizer.core_impl.native_symbols import PARQUET_STREAM_WRITE
    from schema_sanitizer.options_impl.call_options import normalize_call_options

    source = tmp_path / "source.parquet"
    rows = (
        {f"value_{column:02d}": row * (column + 3) for column in range(32)} for row in range(65_536)
    )
    source_options = normalize_call_options(
        memory_limit_bytes=512 << 20,
        on_error="stop",
    ).raw
    source_stream = ExecutionContext().to_sink_python("stream", rows, source_options)
    PARQUET_STREAM_WRITE(source_stream, str(source), "uncompressed", -1, 128 << 20)

    outputs: dict[str, bytes] = {}
    for mode in ("single", "multi"):
        context = ExecutionContext()
        options = normalize_call_options(
            multi_threading=mode == "multi",
            memory_limit_bytes=64 << 20,
            on_error="stop",
        ).raw
        capsule = native_core.parquet_stream_read(str(source), [], 64 << 20)
        sink = context.to_sink_arrow_stream("stream", "arrow", CapsuleStream(capsule), options)
        output = tmp_path / f"{mode}.parquet"
        PARQUET_STREAM_WRITE(sink, str(output), "uncompressed", -1, 128 << 20)
        sink.close_main_stream()
        outputs[mode] = output.read_bytes()

    assert outputs["single"] == outputs["multi"]
    assert hashlib.sha256(outputs["single"]).digest() == hashlib.sha256(outputs["multi"]).digest()
