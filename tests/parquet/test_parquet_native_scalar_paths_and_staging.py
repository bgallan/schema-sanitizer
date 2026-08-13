"""Native scalar Parquet path and staging runtime tests."""

from __future__ import annotations

import datetime as dt
import logging
from pathlib import Path

import pytest
from _support.parquet_runtime import pa, pq, sample_table
from _support.parquet_runtime import requires_pyarrow as _requires_pyarrow
from conftest import read_test_parquet, require_native

import schema_sanitizer as ss


@_requires_pyarrow
def test_parquet_path_auto(tmp_path: Path) -> None:
    """Verify parquet path auto."""
    require_native()
    path = tmp_path / "data.parquet"
    pq.write_table(sample_table(pa), path)

    result = read_test_parquet(path)
    assert result.clean_data.num_rows == 3
    assert result.clean_data.schema.names == ["a", "b"]


@_requires_pyarrow
def test_parquet_path_with_temporal_values(tmp_path: Path) -> None:
    """Verify parquet path with temporal values."""
    from schema_sanitizer.adapters.parquet.status import native_parquet_footer_info

    require_native()
    path = tmp_path / "data.parquet"
    pq.write_table(
        pa.table(
            {
                "d": pa.array([dt.date(2024, 1, 2)], type=pa.date32()),
                "ts": pa.array([dt.datetime(2024, 1, 2, 3, 4, 5)], type=pa.timestamp("us")),
            }
        ),
        path,
    )

    result = read_test_parquet(path)

    assert result.clean_data.num_rows == 1
    assert result.clean_data.schema.names == ["d", "ts"]
    info = native_parquet_footer_info(path)
    assert info is not None
    assert info["schema_elements"][1]["logical_type"] == "date"
    assert info["schema_elements"][2]["logical_type"] == "timestamp"
    assert info["schema_elements"][2]["logical_type_time_unit"] == "micros"
    assert info["schema_elements"][2]["logical_type_is_adjusted_to_utc"] == 0
    assert info["row_groups"][0]["columns"][0]["path_in_schema"] == ["d"]
    assert info["row_groups"][0]["columns"][1]["path_in_schema"] == ["ts"]
    assert info["row_groups"][0]["columns"][0]["native_arrow_format"] == "tdD"
    assert info["row_groups"][0]["columns"][1]["native_arrow_format"] == "tsu:"


@_requires_pyarrow
def test_native_parquet_footer_info_maps_utc_timestamp_timezone(tmp_path: Path) -> None:
    """Verify adjusted UTC Parquet timestamps expose an Arrow timezone."""
    from schema_sanitizer.adapters.parquet.status import native_parquet_footer_info

    require_native()
    path = tmp_path / "data.parquet"
    pq.write_table(
        pa.table(
            {
                "ts": pa.array(
                    [dt.datetime(2024, 1, 2, 3, 4, 5, tzinfo=dt.UTC)],
                    type=pa.timestamp("us", tz="UTC"),
                ),
            }
        ),
        path,
    )

    info = native_parquet_footer_info(path)

    assert info is not None
    assert info["schema_elements"][1]["logical_type"] == "timestamp"
    assert info["schema_elements"][1]["logical_type_time_unit"] == "micros"
    assert info["schema_elements"][1]["logical_type_is_adjusted_to_utc"] == 1
    assert info["row_groups"][0]["columns"][0]["native_arrow_format"] == "tsu:UTC"


@_requires_pyarrow
def test_read_parquet_path_materializes_table(tmp_path: Path) -> None:
    """Verify read parquet path materializes table."""
    from schema_sanitizer.adapters.parquet.telemetry import (
        last_parquet_native_reader_diagnostics,
        last_parquet_stream_factory_route,
    )

    require_native()

    path = tmp_path / "data.parquet"
    pq.write_table(sample_table(pa), path)

    result = read_test_parquet(path)

    assert result.clean_data.to_pylist() == sample_table(pa).to_pylist()
    assert last_parquet_stream_factory_route() == "pyarrow_dataset_scanner"
    diagnostics = last_parquet_native_reader_diagnostics()
    assert diagnostics["attempted"] is True
    assert diagnostics["ready"] is False
    assert diagnostics["reason"] == "not_ready"
    assert any("file was not written" in blocker for blocker in diagnostics["blockers"])


@_requires_pyarrow
def test_native_snappy_parquet_roundtrip_uses_native_reader(tmp_path: Path) -> None:
    """Verify native Snappy pages are written and read by the native route."""
    from schema_sanitizer.adapters.parquet.status import native_parquet_footer_info
    from schema_sanitizer.adapters.parquet.telemetry import (
        last_parquet_native_reader_diagnostics,
        last_parquet_stream_factory_route,
    )

    require_native()
    source = tmp_path / "native-snappy.jsonl"
    source.write_text(
        '{"id":1,"name":"alpha"}\n{"id":2,"name":"alpha"}\n{"id":3,"name":"beta"}\n',
        encoding="utf-8",
    )
    path = tmp_path / "native-snappy.parquet"
    ss.to_parquet(
        source,
        path,
        input_format="jsonl",
        parquet_compression="snappy",
    )

    info = native_parquet_footer_info(path)

    assert info is not None
    assert info["native_reader_ready"] == 1
    assert info["native_reader_blockers"] == []
    assert {column["codec"] for column in info["row_groups"][0]["columns"]} == {1}
    for column in info["row_groups"][0]["columns"]:
        for page in column["pages"]:
            assert page["payload_verified"] == 1
            assert page["values_decoded"] == 1

    result = read_test_parquet(path)

    assert result.clean_data.select(["id", "name"]).to_pylist() == [
        {"id": 1, "name": "alpha"},
        {"id": 2, "name": "alpha"},
        {"id": 3, "name": "beta"},
    ]
    assert last_parquet_stream_factory_route() == "native_parquet_stream"
    diagnostics = last_parquet_native_reader_diagnostics()
    assert diagnostics["attempted"] is True
    assert diagnostics["ready"] is True
    assert diagnostics["reason"] == "native_stream"


@_requires_pyarrow
def test_native_parquet_reader_logs_not_ready_fallback(
    tmp_path: Path,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Verify unsupported native Parquet attempts leave useful fallback diagnostics."""
    from schema_sanitizer.adapters.parquet.record_batch_factory import (
        open_parquet_record_batch_stream_factory,
    )
    from schema_sanitizer.adapters.parquet.telemetry import (
        last_parquet_native_reader_diagnostics,
        last_parquet_stream_factory_route,
    )

    require_native()
    path = tmp_path / "pyarrow.parquet"
    pq.write_table(sample_table(pa), path)
    caplog.set_level(logging.DEBUG, logger="schema_sanitizer.adapters.parquet.record_batch_factory")

    factory = open_parquet_record_batch_stream_factory(path, source="path", feature="test")
    reader = pa.RecordBatchReader.from_stream(factory)

    assert reader.read_all().to_pylist() == sample_table(pa).to_pylist()
    assert last_parquet_stream_factory_route() == "pyarrow_dataset_scanner"
    diagnostics = last_parquet_native_reader_diagnostics()
    assert diagnostics["attempted"] is True
    assert diagnostics["ready"] is False
    assert diagnostics["reason"] == "not_ready"
    assert any("file was not written" in blocker for blocker in diagnostics["blockers"])
    assert "Native Parquet reader skipped; retrying input with PyArrow" in caplog.text


@_requires_pyarrow
def test_parquet_file_like_records_non_native_source_diagnostics() -> None:
    """Verify file-like Parquet inputs explain why native reader was bypassed."""
    from io import BytesIO

    from schema_sanitizer.adapters.parquet.record_batch_factory import (
        open_parquet_record_batch_stream_factory,
    )
    from schema_sanitizer.adapters.parquet.telemetry import (
        last_parquet_native_reader_diagnostics,
        last_parquet_stream_factory_route,
    )

    require_native()
    data = BytesIO()
    pq.write_table(sample_table(pa), data)
    data.seek(0)

    factory = open_parquet_record_batch_stream_factory(data, source="stream", feature="test")
    reader = pa.RecordBatchReader.from_stream(factory)

    assert reader.read_all().to_pylist() == sample_table(pa).to_pylist()
    assert last_parquet_stream_factory_route() == "pyarrow_parquetfile_iter_batches"
    diagnostics = last_parquet_native_reader_diagnostics()
    assert diagnostics["attempted"] is False
    assert diagnostics["ready"] is False
    assert diagnostics["reason"] == "source_not_path"
    assert diagnostics["blockers"] == []


@_requires_pyarrow
def test_parquet_buffer_stages_then_falls_back_for_external_writer() -> None:
    """Verify buffer-backed external Parquet gets native diagnostics before fallback."""
    from schema_sanitizer.adapters.parquet.record_batch_factory import (
        open_parquet_record_batch_stream_factory,
    )
    from schema_sanitizer.adapters.parquet.telemetry import (
        last_parquet_native_reader_diagnostics,
        last_parquet_stream_factory_route,
    )

    require_native()
    sink = pa.BufferOutputStream()
    pq.write_table(sample_table(pa), sink)
    data = sink.getvalue().to_pybytes()

    factory = open_parquet_record_batch_stream_factory(data, source="text", feature="test")
    reader = pa.RecordBatchReader.from_stream(factory)

    assert reader.read_all().to_pylist() == sample_table(pa).to_pylist()
    assert last_parquet_stream_factory_route() == "pyarrow_dataset_scanner"
    diagnostics = last_parquet_native_reader_diagnostics()
    assert diagnostics["attempted"] is True
    assert diagnostics["ready"] is False
    assert diagnostics["reason"] == "not_ready"
    assert diagnostics["native_source_kind"] == "staged_text"
    assert diagnostics["blockers"]


@_requires_pyarrow
def test_parquet_buffer_projection_materializes_requested_columns() -> None:
    """Verify buffer-backed Parquet projections expose the projected schema."""
    from schema_sanitizer.adapters.parquet.record_batch_factory import (
        open_parquet_record_batch_stream_factory,
    )
    from schema_sanitizer.adapters.parquet.telemetry import (
        last_parquet_stream_factory_route,
    )

    require_native()
    table = pa.table({"a": [1, 2], "b": ["x", "y"], "c": [True, False]})
    sink = pa.BufferOutputStream()
    pq.write_table(table, sink)

    factory = open_parquet_record_batch_stream_factory(
        sink.getvalue().to_pybytes(),
        source="text",
        feature="test",
        columns=["b"],
    )
    reader = pa.RecordBatchReader.from_stream(factory)

    assert reader.read_all().to_pylist() == [{"b": "x"}, {"b": "y"}]
    assert last_parquet_stream_factory_route() == "pyarrow_dataset_scanner"


@_requires_pyarrow
def test_native_parquet_stream_materializes_plain_fixed_width_rows(
    tmp_path: Path,
) -> None:
    """Verify the native Parquet stream materializes supported fixed-width pages."""
    from schema_sanitizer.adapters.parquet.record_batch_factory import (
        open_parquet_record_batch_stream_factory,
    )
    from schema_sanitizer.adapters.parquet.status import native_parquet_footer_info
    from schema_sanitizer.adapters.parquet.telemetry import (
        last_parquet_native_reader_diagnostics,
        last_parquet_stream_factory_route,
    )
    from schema_sanitizer.api_impl.file_conversion.writers import (
        write_parquet_native_first_stream,
    )

    require_native()
    path = tmp_path / "native.parquet"
    table = pa.table({"a": [10, 20, None, 40], "b": [1000, -5, 7, None]})
    write_parquet_native_first_stream(
        pa.RecordBatchReader.from_batches(table.schema, table.to_batches()),
        path,
        feature="test",
        parquet_compression="uncompressed",
    )
    info = native_parquet_footer_info(path)

    assert info is not None
    assert info["native_reader_ready"] == 1
    assert {
        tuple(column["path_in_schema"]): column["native_read_value_buffer_kind"]
        for column in info["row_groups"][0]["columns"]
    } == {("a",): "fixed_width", ("b",): "fixed_width"}

    factory = open_parquet_record_batch_stream_factory(path, source="path", feature="test")
    reader = pa.RecordBatchReader.from_stream(factory)

    assert reader.read_all().to_pylist() == table.to_pylist()
    assert last_parquet_stream_factory_route() == "native_parquet_stream"
    diagnostics = last_parquet_native_reader_diagnostics()
    assert diagnostics["attempted"] is True
    assert diagnostics["ready"] is True
    assert diagnostics["reason"] == "native_stream"
    assert diagnostics["blockers"] == []
    assert diagnostics["row_group_count"] == 1
    assert diagnostics["num_rows"] == 4


@_requires_pyarrow
def test_native_parquet_stream_materializes_staged_buffer(
    tmp_path: Path,
) -> None:
    """Verify native-writer Parquet bytes can use the native reader via staging."""
    from schema_sanitizer.adapters.parquet.record_batch_factory import (
        open_parquet_record_batch_stream_factory,
    )
    from schema_sanitizer.adapters.parquet.telemetry import (
        last_parquet_native_reader_diagnostics,
        last_parquet_stream_factory_route,
    )
    from schema_sanitizer.api_impl.file_conversion.writers import (
        write_parquet_native_first_stream,
    )

    require_native()
    path = tmp_path / "native-buffer.parquet"
    table = pa.table({"a": [10, 20, None], "b": ["x", "y", None]})
    write_parquet_native_first_stream(
        pa.RecordBatchReader.from_batches(table.schema, table.to_batches()),
        path,
        feature="test",
        parquet_compression="uncompressed",
    )

    factory = open_parquet_record_batch_stream_factory(
        path.read_bytes(),
        source="text",
        feature="test",
    )
    reader = pa.RecordBatchReader.from_stream(factory)

    assert reader.read_all().to_pylist() == table.to_pylist()
    assert last_parquet_stream_factory_route() == "native_parquet_stream"
    diagnostics = last_parquet_native_reader_diagnostics()
    assert diagnostics["attempted"] is True
    assert diagnostics["ready"] is True
    assert diagnostics["reason"] == "native_stream"
    assert diagnostics["native_source_kind"] == "staged_text"


@_requires_pyarrow
def test_native_parquet_stream_materializes_file_backed_stream(
    tmp_path: Path,
) -> None:
    """Verify file-like objects backed by local files can use the native reader."""
    from schema_sanitizer.adapters.parquet.record_batch_factory import (
        open_parquet_record_batch_stream_factory,
    )
    from schema_sanitizer.adapters.parquet.telemetry import (
        last_parquet_native_reader_diagnostics,
        last_parquet_stream_factory_route,
    )
    from schema_sanitizer.api_impl.file_conversion.writers import (
        write_parquet_native_first_stream,
    )

    require_native()
    path = tmp_path / "native-stream.parquet"
    table = pa.table({"a": [1, 2, 3]})
    write_parquet_native_first_stream(
        pa.RecordBatchReader.from_batches(table.schema, table.to_batches()),
        path,
        feature="test",
        parquet_compression="uncompressed",
    )

    with path.open("rb") as handle:
        factory = open_parquet_record_batch_stream_factory(
            handle,
            source="stream",
            feature="test",
        )
        reader = pa.RecordBatchReader.from_stream(factory)
        assert reader.read_all().to_pylist() == table.to_pylist()

    assert last_parquet_stream_factory_route() == "native_parquet_stream"
    diagnostics = last_parquet_native_reader_diagnostics()
    assert diagnostics["attempted"] is True
    assert diagnostics["ready"] is True
    assert diagnostics["reason"] == "native_stream"
    assert diagnostics["native_source_kind"] == "stream_path"
