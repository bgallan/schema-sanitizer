"""Regression coverage for concurrency native parquet decode reuses the operation arena."""

from __future__ import annotations

import hashlib
from pathlib import Path

from _support.resource_fakes import CapsuleStream

ROOT = Path(__file__).resolve().parents[2]
FOOTER = ROOT / "cpp/src/internal/parquet/footer_reader/footer_reader.cc"
ROW_GROUP = (
    ROOT
    / "cpp/src/internal/parquet/footer_reader/native_stream/materialization/row_group"
    / "native_stream_row_group.cc.inc"
)
PARALLEL = (
    ROOT
    / "cpp/src/internal/parquet/footer_reader/native_stream/materialization/row_group"
    / "native_stream_parallel_columns.cc.inc"
)
RETAINED = (
    ROOT
    / "cpp/src/internal/parquet/footer_reader/native_stream/materialization/row_group"
    / "native_stream_retained_budget.cc.inc"
)


def test_native_parquet_decode_reuses_the_operation_arena() -> None:
    """The reader uses ordered upstream tasks and retains a serial fallback."""
    footer = FOOTER.read_text(encoding="utf-8")
    row_group = ROW_GROUP.read_text(encoding="utf-8")
    parallel = PARALLEL.read_text(encoding="utf-8")

    assert '"internal/arrow_c/cdata_stream_runtime.hh"' in footer
    assert '"internal/runtime/ordered_executor.hh"' in footer
    assert "task_arena_for_stream(owner_stream)" in row_group
    assert "TaskArenaLane::kUpstream" in parallel
    assert "TaskTelemetryKind::kInput" in parallel
    assert "materialize_native_row_group_columns_parallel" in row_group
    assert "materialize_native_row_group_columns_parallel" in parallel
    assert "if (column_workers > 1)" in row_group
    assert "for (std::size_t i = 0; i < row_group.columns.size(); ++i)" in row_group


def test_parquet_parallel_scratch_is_derived_and_saturating() -> None:
    """Parallel scratch remains a conservative fraction of the one budget."""
    row_group = ROW_GROUP.read_text(encoding="utf-8")
    parallel = PARALLEL.read_text(encoding="utf-8")
    retained = RETAINED.read_text(encoding="utf-8")
    budget_sources = parallel + retained

    assert "stream.max_buffer_bytes / 4" in parallel
    assert "native_parquet_max_column_scratch_bytes" in budget_sources
    assert "std::numeric_limits<std::int64_t>::max() - value" in retained
    assert "estimate = std::numeric_limits<std::int64_t>::max()" in retained
    assert "enforce_native_array_retained_budget" in budget_sources
    assert "getenv" not in row_group + budget_sources


def test_parquet_single_and_multi_are_byte_identical(tmp_path: Path, require_native: None) -> None:
    """Parallel column decode preserves exact deterministic Parquet output."""
    from schema_sanitizer.core_impl.execution import ExecutionContext
    from schema_sanitizer.core_impl.native_runtime import native_core
    from schema_sanitizer.core_impl.native_symbols import PARQUET_STREAM_WRITE
    from schema_sanitizer.options_impl.call_options import normalize_call_options

    source = tmp_path / "source.parquet"
    rows = (
        {
            **{f"i_{column:02d}": row * (column + 3) for column in range(16)},
            **{
                f"s_{column:02d}": f"group-{(row + column) % 97:02d}-{row % 1000:03d}"
                for column in range(8)
            },
        }
        for row in range(4_096)
    )
    source_stream = ExecutionContext().to_sink_python("stream", rows, None)
    PARQUET_STREAM_WRITE(source_stream, str(source), "uncompressed", -1, 128 << 20)

    outputs: dict[str, bytes] = {}
    for mode in ("single", "multi"):
        context = ExecutionContext()
        options = normalize_call_options(
            multi_threading=mode == "multi",
            memory_limit_bytes=128 << 20,
            on_error="stop",
        ).raw
        capsule = native_core.parquet_stream_read(str(source), [], 128 << 20)
        sink = context.to_sink_arrow_stream("stream", "arrow", CapsuleStream(capsule), options)
        output = tmp_path / f"{mode}.parquet"
        PARQUET_STREAM_WRITE(sink, str(output), "uncompressed", -1, 128 << 20)
        sink.close_main_stream()
        outputs[mode] = output.read_bytes()

    assert hashlib.sha256(outputs["single"]).digest() == hashlib.sha256(outputs["multi"]).digest()
    assert outputs["single"] == outputs["multi"]
