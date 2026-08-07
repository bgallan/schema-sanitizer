"""Regression coverage for coalescing and bounded temporary input memory."""

from __future__ import annotations

import io
import json
from pathlib import Path
from typing import Any

import pytest
from conftest import require_native

ROOT = Path(__file__).resolve().parents[2]


class _CapsuleStream:
    """Expose an owned Arrow C Stream capsule to another native entry point."""

    def __init__(self, capsule: Any):
        """Implement the test-double protocol method."""
        self._capsule = capsule

    def __arrow_c_stream__(self, requested_schema: Any = None) -> Any:
        """Implement the test-double protocol method."""
        del requested_schema
        return self._capsule


def test_coalescer_allocates_validity_only_after_first_null() -> None:
    """Verify the defensive regression contract."""
    append = (ROOT / "cpp/src/api/python_abi3/streaming/coalesce_append.cc").read_text()
    estimate = (ROOT / "cpp/src/api/python_abi3/streaming/coalesce_export.cc").read_text()
    validity = append.split("sanitize::Status append_validity", 1)[1].split(
        "sanitize::Status ensure_child_count", 1
    )[0]
    assert "bool has_null = false" in validity
    assert validity.index("if (!out->validity.empty() || has_null)") < validity.index(
        "out->validity.resize"
    )
    assert "const bool needs_validity" in estimate


def test_coalescer_uses_the_single_memory_budget() -> None:
    """Verify the defensive regression contract."""
    stream = (ROOT / "cpp/src/api/python_abi3/streaming/coalesce_stream.cc").read_text()
    state = (ROOT / "cpp/src/api/python_abi3/streaming/coalesce_stream_internal.hh").read_text()
    assert "memory_budget_from_limit(memory_limit_bytes)" in stream
    assert "single row exceeds hard batch byte limit" in stream
    assert "retained bytes exceed hard batch limit" in stream
    assert "std::size_t max_batch_bytes" in state
    assert "getenv" not in stream


def test_folder_reader_checks_hostile_chunk_before_retaining_it() -> None:
    """Verify the defensive regression contract."""
    from schema_sanitizer.errors import SchemaSanitizerResourceError
    from schema_sanitizer.input_impl.directory_inputs import FolderFile, read_folder_file_bytes

    requested: list[int] = []

    class OversizedReader:
        """Provide a lightweight test double."""

        def read(self, size: int = -1, /) -> bytes:
            """Provide a test helper implementation."""
            requested.append(size)
            return b"x" * 4096

        def close(self) -> None:
            """Provide a test helper implementation."""
            return None

    file = FolderFile("hostile.bin", "hostile.bin", None, OversizedReader)
    with pytest.raises(SchemaSanitizerResourceError, match="hostile.bin"):
        read_folder_file_bytes(file, memory_limit_bytes=32, stage="bounded read")
    assert requested == [33]


def test_folder_reader_wipes_temporary_accumulator(monkeypatch: pytest.MonkeyPatch) -> None:
    """Verify the defensive regression contract."""
    from schema_sanitizer.input_impl import directory_inputs

    observed: list[bytes] = []
    original_zero = directory_inputs._zero_bytearray_range

    def capture(buffer: bytearray, start: int, end: int) -> None:
        """Provide a test helper implementation."""
        original_zero(buffer, start, end)
        observed.append(bytes(buffer))

    monkeypatch.setattr(directory_inputs, "_zero_bytearray_range", capture)
    file = directory_inputs.FolderFile(
        display_name="secret.bin",
        name="secret.bin",
        size=None,
        open_binary=lambda: io.BytesIO(b"secret"),
    )
    assert (
        directory_inputs.read_folder_file_bytes(file, memory_limit_bytes=64, stage="bounded read")
        == b"secret"
    )
    assert observed == [b"\x00" * 6]


def test_native_coalescer_preserves_late_nulls_without_eager_validity(tmp_path: Path) -> None:
    """Verify the defensive regression contract."""
    require_native()
    from schema_sanitizer.core_impl.execution import ExecutionContext
    from schema_sanitizer.core_impl.native_symbols import COALESCING_STREAM_WRAP, JSONL_STREAM_WRITE

    rows = [
        {"id": 1, "payload": "first", "nested": {"value": 10}},
        {"id": 2, "payload": None, "nested": None},
        {"id": 3, "payload": "last", "nested": {"value": None}},
    ]
    source = ExecutionContext().to_sink_python("stream", rows, None)
    capsule = COALESCING_STREAM_WRAP(source, 1 << 20)
    output = tmp_path / "late-nulls.jsonl"
    JSONL_STREAM_WRITE(_CapsuleStream(capsule), str(output), 1 << 20)
    assert [json.loads(line) for line in output.read_text().splitlines()] == rows


def test_native_coalescer_rejects_one_row_over_budget(tmp_path: Path) -> None:
    """Verify the defensive regression contract."""
    require_native()
    from schema_sanitizer.core_impl.execution import ExecutionContext
    from schema_sanitizer.core_impl.native_symbols import (
        COALESCING_STREAM_WRAP,
        PARQUET_STREAM_WRITE,
    )

    source = ExecutionContext().to_sink_python("stream", [{"payload": "x" * 4096}], None)
    capsule = COALESCING_STREAM_WRAP(source, 512)
    with pytest.raises(
        RuntimeError, match="(single row exceeds hard batch byte limit|logical byte limit)"
    ):
        PARQUET_STREAM_WRITE(
            _CapsuleStream(capsule), str(tmp_path / "bounded.parquet"), "uncompressed", -1, 512
        )
