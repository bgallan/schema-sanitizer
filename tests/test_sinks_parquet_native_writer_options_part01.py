"""Tests read adapters and file-to-file converters."""

from __future__ import annotations

import logging
from decimal import Decimal
from pathlib import Path

import pytest
from conftest import require_native

# Split from test_sinks_parquet_native_writer_options.py: test_parquet_native_file_output_uses_native_writer_when_available, test_parquet_native_file_output_falls_back_when_gzip_lacks_zlib, test_parquet_native_file_output_retries_pyarrow_after_native_failure, ...


def test_parquet_native_file_output_uses_native_writer_when_available(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Verify Parquet output prefers the native writer when one is exported."""
    require_native()
    pa = pytest.importorskip("pyarrow")
    from schema_sanitizer.adapters.pyarrow.file_metadata import last_metadata_route
    from schema_sanitizer.api_impl.file_conversion import direct_writers as native_parquet_output
    from schema_sanitizer.api_impl.file_conversion import writers as native_file_output

    def fake_native_write(
        stream: object,
        output_path: str,
        compression: str,
        gzip_level: int,
        memory_limit_bytes: int,
    ) -> None:
        """Write a marker file through the fake native Parquet writer."""
        assert hasattr(stream, "__arrow_c_stream__")
        assert (compression, gzip_level, memory_limit_bytes) == ("gzip", -1, -1)
        Path(output_path).write_bytes(b"native-parquet")

    def fail_pyarrow_sink(*_args: object, **_kwargs: object) -> None:
        """Fail when the PyArrow Parquet sink fallback is called."""
        raise AssertionError("PyArrow sink fallback should not be used")

    monkeypatch.setattr(native_parquet_output, "PARQUET_STREAM_WRITE", fake_native_write)
    monkeypatch.setattr(native_file_output, "_write_parquet_stream", fail_pyarrow_sink)
    batch = pa.record_batch({"a": pa.array(["1", "2"])})
    stream = pa.RecordBatchReader.from_batches(batch.schema, [batch])
    out = tmp_path / "direct.parquet"

    native_file_output.write_parquet_native_first_stream(
        stream,
        out,
        feature="test_parquet_native_file_output_uses_native_writer_when_available",
    )

    assert out.read_bytes() == b"native-parquet"
    assert native_file_output.last_parquet_stream_route() == "native"
    assert last_metadata_route() == "none"


def test_parquet_native_file_output_falls_back_when_gzip_lacks_zlib(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Verify no-zlib native Parquet builds fall back to PyArrow output."""
    require_native()
    pa = pytest.importorskip("pyarrow")
    from schema_sanitizer.api_impl.file_conversion import direct_writers as native_parquet_output
    from schema_sanitizer.api_impl.file_conversion import writers as native_file_output

    fallback_calls: list[Path] = []

    def fake_native_write(
        _stream: object,
        _output_path: str,
        _compression: str,
        _gzip_level: int,
        _memory_limit_bytes: int,
    ) -> None:
        """Simulate a native build without zlib."""
        raise RuntimeError(
            "native Parquet writer: gzip compression requested but zlib is not available"
        )

    def fake_pyarrow_sink(_stream: object, output_path: Path, **_kwargs: object) -> None:
        """Record PyArrow fallback and write a marker file."""
        fallback_calls.append(output_path)
        output_path.write_bytes(b"pyarrow-parquet")

    monkeypatch.setattr(native_parquet_output, "PARQUET_STREAM_WRITE", fake_native_write)
    monkeypatch.setattr(native_file_output, "_write_parquet_stream", fake_pyarrow_sink)
    batch = pa.record_batch({"a": pa.array(["1"])})
    stream = pa.RecordBatchReader.from_batches(batch.schema, [batch])
    out = tmp_path / "fallback.parquet"

    native_file_output.write_parquet_native_first_stream(
        stream,
        out,
        feature="test_parquet_native_file_output_falls_back_when_gzip_lacks_zlib",
    )

    assert fallback_calls == [out]
    assert out.read_bytes() == b"pyarrow-parquet"
    assert native_file_output.last_parquet_stream_route() == "pyarrow"


def test_parquet_native_file_output_retries_pyarrow_after_native_failure(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    """Verify a native Parquet crash falls back with a fresh PyArrow stream."""
    require_native()
    pa = pytest.importorskip("pyarrow")
    pq = pytest.importorskip("pyarrow.parquet")
    from schema_sanitizer.api_impl.file_conversion import direct_writers as native_parquet_output
    from schema_sanitizer.api_impl.file_conversion import writers as native_file_output

    def failing_native_write(
        stream: object,
        _output_path: str,
        _compression: str,
        _gzip_level: int,
        _memory_limit_bytes: int,
    ) -> None:
        """Consume part of the stream before simulating a native writer bug."""
        assert hasattr(stream, "read_next_batch")
        stream.read_next_batch()
        raise RuntimeError("native Parquet writer: simulated fatal bug")

    monkeypatch.setattr(native_parquet_output, "PARQUET_STREAM_WRITE", failing_native_write)
    caplog.set_level(logging.ERROR, logger="schema_sanitizer.api_impl.file_conversion.writers")
    batches = [
        pa.record_batch({"a": pa.array(["1", "2"])}),
        pa.record_batch({"a": pa.array(["3"])}),
    ]
    stream = pa.RecordBatchReader.from_batches(batches[0].schema, batches)
    out = tmp_path / "native-failure-fallback.parquet"

    native_file_output.write_parquet_native_first_stream(
        stream,
        out,
        feature="test_parquet_native_file_output_retries_pyarrow_after_native_failure",
        parquet_compression="uncompressed",
    )

    assert pq.read_table(out).column("a").to_pylist() == ["1", "2", "3"]
    assert native_file_output.last_parquet_stream_route() == "pyarrow"
    assert "retrying Parquet output with PyArrow" in caplog.text


def test_raw_parquet_file_output_retries_pyarrow_after_native_failure(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Verify raw native Parquet output can retry PyArrow after partial consumption."""
    require_native()
    pa = pytest.importorskip("pyarrow")
    pq = pytest.importorskip("pyarrow.parquet")
    from schema_sanitizer.api_impl.file_conversion import direct_writers as native_parquet_output
    from schema_sanitizer.api_impl.file_conversion import writers as native_file_output
    from schema_sanitizer.api_impl.stream_output import write_raw_stream_to_file

    def failing_native_write(
        stream: object,
        _output_path: str,
        _compression: str,
        _gzip_level: int,
        _memory_limit_bytes: int,
    ) -> None:
        """Fail after reading one batch from each native attempt."""
        assert hasattr(stream, "read_next_batch")
        stream.read_next_batch()
        raise RuntimeError("native Parquet writer: simulated raw fatal bug")

    monkeypatch.setattr(native_parquet_output, "PARQUET_STREAM_WRITE", failing_native_write)
    batches = [
        pa.record_batch({"a": pa.array(["1"])}),
        pa.record_batch({"a": pa.array(["2", "3"])}),
    ]
    raw = pa.RecordBatchReader.from_batches(batches[0].schema, batches)
    out = tmp_path / "raw-native-failure-fallback.parquet"

    write_raw_stream_to_file(
        raw,
        out,
        writer=native_file_output.write_parquet_native_first_stream,
        feature="test_raw_parquet_file_output_retries_pyarrow_after_native_failure",
        first_row_columns=None,
        parquet_compression="uncompressed",
    )

    assert pq.read_table(out).column("a").to_pylist() == ["1", "2", "3"]
    assert native_file_output.last_parquet_stream_route() == "pyarrow"


def test_parquet_native_file_output_writes_metadata_without_pyarrow_sink(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Verify native Parquet output can receive native metadata arguments."""
    require_native()
    pa = pytest.importorskip("pyarrow")
    from schema_sanitizer.adapters.pyarrow.file_metadata import last_metadata_route
    from schema_sanitizer.api_impl.file_conversion import direct_writers as native_parquet_output
    from schema_sanitizer.api_impl.file_conversion import writers as native_file_output

    captured: dict[str, object] = {}

    def fake_native_write(
        stream: object,
        output_path: str,
        first_row_columns: dict[str, object],
        all_row_columns: dict[str, object],
        row_span_columns: dict[str, list[tuple[int, str | None]]],
        timestamp_columns: tuple[str, ...],
        compression: str,
        gzip_level: int,
        memory_limit_bytes: int,
    ) -> None:
        """Write a marker file through the fake native metadata Parquet writer."""
        assert hasattr(stream, "__arrow_c_stream__")
        captured["first_row_columns"] = first_row_columns
        captured["all_row_columns"] = all_row_columns
        captured["row_span_columns"] = row_span_columns
        captured["timestamp_columns"] = timestamp_columns
        assert (compression, gzip_level, memory_limit_bytes) == ("gzip", -1, -1)
        Path(output_path).write_bytes(b"native-parquet-metadata")

    def fail_pyarrow_sink(*_args: object, **_kwargs: object) -> None:
        """Fail when the PyArrow Parquet sink fallback is called."""
        raise AssertionError("PyArrow sink fallback should not be used")

    monkeypatch.setattr(
        native_parquet_output, "PARQUET_STREAM_WRITE_WITH_METADATA", fake_native_write
    )
    monkeypatch.setattr(native_file_output, "_write_parquet_stream", fail_pyarrow_sink)
    batch = pa.record_batch({"a": pa.array(["1", "2"])})
    stream = pa.RecordBatchReader.from_batches(batch.schema, [batch])
    out = tmp_path / "direct-metadata.parquet"

    native_file_output.write_parquet_native_first_stream(
        stream,
        out,
        feature="test_parquet_native_file_output_writes_metadata_without_pyarrow_sink",
        first_row_columns={"schema_registry": "{}"},
        all_row_columns={"source_file": "/tmp/source.parquet"},
        timestamp_columns=("ingestion_timestamp",),
    )

    assert out.read_bytes() == b"native-parquet-metadata"
    assert captured == {
        "first_row_columns": {"schema_registry": "{}"},
        "all_row_columns": {"source_file": "/tmp/source.parquet"},
        "row_span_columns": {},
        "timestamp_columns": ("ingestion_timestamp",),
    }
    assert native_file_output.last_parquet_stream_route() == "native"
    assert last_metadata_route() == "native"


def test_parquet_native_file_output_writes_supported_flat_stream(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Verify the real native Parquet writer produces readable flat output."""
    require_native()
    pa = pytest.importorskip("pyarrow")
    pq = pytest.importorskip("pyarrow.parquet")
    from schema_sanitizer.adapters.pyarrow.file_metadata import last_metadata_route
    from schema_sanitizer.api_impl.file_conversion import writers as native_file_output

    def fail_pyarrow_sink(*_args: object, **_kwargs: object) -> None:
        """Fail when the PyArrow Parquet sink fallback is called."""
        raise AssertionError("PyArrow sink fallback should not be used")

    monkeypatch.setattr(native_file_output, "_write_parquet_stream", fail_pyarrow_sink)
    batch = pa.record_batch(
        {
            "id": pa.array([1, None, 3], type=pa.int64()),
            "name": pa.array(["one", None, "three"], type=pa.string()),
            "payload": pa.array([b"bb", None, b"a"], type=pa.binary()),
            "score": pa.array([1.5, None, 3.25], type=pa.float64()),
            "ok": pa.array([True, None, False], type=pa.bool_()),
            "amount": pa.array(
                [Decimal("123.45"), None, Decimal("-0.10")],
                type=pa.decimal128(10, 2),
            ),
            "big_amount": pa.array(
                [
                    Decimal("123456789012345678901234567890.1234"),
                    None,
                    Decimal("-1.0000"),
                ],
                type=pa.decimal256(40, 4),
            ),
        }
    )
    stream = pa.RecordBatchReader.from_batches(batch.schema, [batch])
    out = tmp_path / "native-flat.parquet"

    native_file_output.write_parquet_native_first_stream(
        stream,
        out,
        feature="test_parquet_native_file_output_writes_supported_flat_stream",
    )

    assert pq.read_table(out).to_pylist() == [
        {
            "id": 1,
            "name": "one",
            "payload": b"bb",
            "score": 1.5,
            "ok": True,
            "amount": Decimal("123.45"),
            "big_amount": Decimal("123456789012345678901234567890.1234"),
        },
        {
            "id": None,
            "name": None,
            "payload": None,
            "score": None,
            "ok": None,
            "amount": None,
            "big_amount": None,
        },
        {
            "id": 3,
            "name": "three",
            "payload": b"a",
            "score": 3.25,
            "ok": False,
            "amount": Decimal("-0.10"),
            "big_amount": Decimal("-1.0000"),
        },
    ]
    parquet_file = pq.ParquetFile(out)
    id_stats = parquet_file.metadata.row_group(0).column(0).statistics
    assert id_stats.null_count == 1
    assert id_stats.min == 1
    assert id_stats.max == 3
    name_stats = parquet_file.metadata.row_group(0).column(1).statistics
    assert name_stats.null_count == 1
    assert name_stats.min == "one"
    assert name_stats.max == "three"
    payload_stats = parquet_file.metadata.row_group(0).column(2).statistics
    assert payload_stats.null_count == 1
    assert payload_stats.min == b"a"
    assert payload_stats.max == b"bb"
    score_stats = parquet_file.metadata.row_group(0).column(3).statistics
    assert score_stats.null_count == 1
    assert score_stats.min == 1.5
    assert score_stats.max == 3.25
    score_meta = parquet_file.metadata.row_group(0).column(3)
    assert any(encoding in score_meta.encodings for encoding in ("BYTE_STREAM_SPLIT", "PLAIN"))
    ok_stats = parquet_file.metadata.row_group(0).column(4).statistics
    assert ok_stats.null_count == 1
    assert ok_stats.min is False
    assert ok_stats.max is True
    amount_stats = parquet_file.metadata.row_group(0).column(5).statistics
    assert amount_stats.null_count == 1
    assert amount_stats.min == Decimal("-0.10")
    assert amount_stats.max == Decimal("123.45")
    big_amount_stats = parquet_file.metadata.row_group(0).column(6).statistics
    assert big_amount_stats.null_count == 1
    assert big_amount_stats.min == Decimal("-1.0000")
    assert big_amount_stats.max == Decimal("123456789012345678901234567890.1234")
    assert native_file_output.last_parquet_stream_route() == "native"
    assert last_metadata_route() == "none"
