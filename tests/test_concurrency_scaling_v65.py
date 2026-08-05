"""Regression coverage for v65 persistent native Parquet worker state."""

from __future__ import annotations

import hashlib
from pathlib import Path
from typing import Any

from conftest import require_native

ROOT = Path(__file__).resolve().parents[1]
STATE = (
    ROOT
    / "cpp/src/internal/parquet/footer_reader/native_stream/schema"
    / "native_stream_arrow_state.cc.inc"
)
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


class _CapsuleStream:
    """Expose one owned Arrow C Stream capsule to a native consumer."""

    def __init__(self, capsule: Any):
        """Retain the owned Arrow C Stream capsule."""
        self._capsule = capsule

    def __arrow_c_stream__(self, requested_schema: Any = None) -> Any:
        """Return the capsule through the Arrow PyCapsule protocol."""
        del requested_schema
        return self._capsule


def test_v65_parquet_worker_resources_live_on_the_stream() -> None:
    """Worker handles and scratch belong to the native stream state."""
    state = STATE.read_text(encoding="utf-8")
    row_group = ROW_GROUP.read_text(encoding="utf-8")
    parallel = PARALLEL.read_text(encoding="utf-8")

    assert "std::vector<NativeParquetColumnWorkerState> parallel_column_workers" in state
    assert "stream->parallel_column_workers.resize(worker_count)" in parallel
    assert "auto *worker_states = &stream->parallel_column_workers" in parallel
    assert "std::make_shared<std::vector<NativeParquetColumnWorkerState>>" not in parallel
    assert "worker_state->file.open(path, std::ios::binary)" in parallel
    assert "materialize_native_row_group_columns_parallel" in row_group


def test_v65_parallel_scratch_is_released_at_row_group_boundary() -> None:
    """Persistent worker scratch is cleared between row groups."""
    row_group = ROW_GROUP.read_text(encoding="utf-8")
    parallel = PARALLEL.read_text(encoding="utf-8")

    boundary = row_group.index("if (stream->row_group_row_offset >= row_group.num_rows)")
    tail = row_group[boundary : boundary + 1_200]
    assert "for (auto &worker_state : stream->parallel_column_workers)" in tail
    assert "worker_state.page_scratch = NativeParquetPageScratch{}" in tail
    assert "stream.max_buffer_bytes / 4" in parallel
    assert "getenv" not in row_group + parallel


def test_v65_parquet_single_and_multi_remain_byte_identical(tmp_path: Path) -> None:
    """Persistent resources preserve deterministic single/multi output."""
    require_native()
    from schema_sanitizer.core_impl.execution import ExecutionContext
    from schema_sanitizer.core_impl.native_runtime import native_core
    from schema_sanitizer.core_impl.native_symbols import PARQUET_STREAM_WRITE
    from schema_sanitizer.options_impl.call_options import normalize_call_options

    source = tmp_path / "source.parquet"
    rows = (
        {f"value_{column:02d}": row * (column + 1) for column in range(24)} for row in range(16_384)
    )
    source_options = normalize_call_options(
        memory_limit_bytes=256 << 20,
        on_error="stop",
    ).raw
    source_stream = ExecutionContext().to_sink_python("stream", rows, source_options)
    PARQUET_STREAM_WRITE(source_stream, str(source), "uncompressed", -1, 64 << 20)

    outputs: dict[str, bytes] = {}
    for mode in ("single", "multi"):
        context = ExecutionContext()
        options = normalize_call_options(
            multi_threading=mode == "multi", memory_limit_bytes=32 << 20, on_error="stop"
        ).raw
        capsule = native_core.parquet_stream_read(str(source), [], 32 << 20)
        sink = context.to_sink_arrow_stream("stream", "arrow", _CapsuleStream(capsule), options)
        output = tmp_path / f"{mode}.parquet"
        PARQUET_STREAM_WRITE(sink, str(output), "uncompressed", -1, 64 << 20)
        sink.close_main_stream()
        outputs[mode] = output.read_bytes()

    assert outputs["single"] == outputs["multi"]
    assert hashlib.sha256(outputs["single"]).digest() == hashlib.sha256(outputs["multi"]).digest()
