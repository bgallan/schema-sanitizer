"""Tests read adapters and file-to-file converters."""

from __future__ import annotations

import math
from pathlib import Path

import pytest
from conftest import require_native

import schema_sanitizer as ss

_GENERATED_METADATA_COLUMNS = {
    "schema_registry",
    "schema_drifts",
    "source_file",
    "ingestion_timestamp",
}


def _write_csv(path: Path, text: str = "a,b\n1,2\n3,4\n") -> Path:
    """Write csv."""
    path.write_text(text, encoding="utf-8")
    return path


def _without_generated_metadata(row: dict[str, object]) -> dict[str, object]:
    """Return row data excluding generated file-converter metadata columns."""
    return {k: v for k, v in row.items() if k not in _GENERATED_METADATA_COLUMNS}


def _without_generated_metadata_rows(rows: list[dict[str, object]]) -> list[dict[str, object]]:
    """Return rows excluding generated file-converter metadata columns."""
    return [_without_generated_metadata(row) for row in rows]


def _native_parquet_zlib_available(pa: object, tmp_path: Path) -> bool:
    """Return whether the compiled native Parquet writer can emit gzip pages."""
    from schema_sanitizer.api_impl.file_conversion import direct_writers as native_parquet_output

    write = native_parquet_output.PARQUET_STREAM_WRITE
    if write is None:
        return False
    batch = pa.record_batch({"text": pa.array(["probe"], type=pa.string())})
    stream = pa.RecordBatchReader.from_batches(batch.schema, [batch])
    try:
        write(stream, str(tmp_path / "native-zlib-probe.parquet"))
    except RuntimeError as exc:
        if "zlib is not available" in str(exc):
            return False
        raise
    return True


# Split from test_sinks_parquet_native_writer_options.py: test_parquet_native_file_output_writes_float_statistics_without_nan_bounds, test_parquet_native_file_output_skips_column_index_without_page_bounds, test_parquet_native_file_output_splits_large_batches_into_row_groups, ...


def test_parquet_native_file_output_writes_float_statistics_without_nan_bounds(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Verify native Parquet float stats skip NaN values."""
    require_native()
    pa = pytest.importorskip("pyarrow")
    pq = pytest.importorskip("pyarrow.parquet")
    from schema_sanitizer.api_impl.file_conversion import writers as native_file_output

    def fail_pyarrow_sink(*_args: object, **_kwargs: object) -> None:
        """Fail when the PyArrow Parquet sink fallback is called."""
        raise AssertionError("PyArrow sink fallback should not be used")

    monkeypatch.setenv("SCHEMA_SANITIZER_NATIVE_PARQUET_COMPRESSION", "uncompressed")
    monkeypatch.setattr(native_file_output, "_write_parquet_stream", fail_pyarrow_sink)
    batch = pa.record_batch(
        {
            "reading": pa.array([float("nan"), 1.5, None, -2.0], type=pa.float64()),
            "empty_reading": pa.array(
                [float("nan"), None, float("nan"), None],
                type=pa.float32(),
            ),
            "zero": pa.array([-0.0, 0.0, None, None], type=pa.float64()),
        }
    )
    stream = pa.RecordBatchReader.from_batches(batch.schema, [batch])
    out = tmp_path / "float-stats.parquet"

    native_file_output.write_parquet_native_first_stream(
        stream,
        out,
        feature="test_parquet_native_file_output_writes_float_statistics_without_nan_bounds",
    )

    parquet_file = pq.ParquetFile(out)
    reading_stats = parquet_file.metadata.row_group(0).column(0).statistics
    assert reading_stats.null_count == 1
    assert reading_stats.min == -2.0
    assert reading_stats.max == 1.5
    empty_stats = parquet_file.metadata.row_group(0).column(1).statistics
    assert empty_stats.null_count == 2
    assert not empty_stats.has_min_max
    zero_stats = parquet_file.metadata.row_group(0).column(2).statistics
    assert zero_stats.null_count == 2
    assert zero_stats.min == -0.0
    assert zero_stats.max == 0.0
    assert math.copysign(1.0, zero_stats.min) == -1.0
    assert math.copysign(1.0, zero_stats.max) == 1.0
    assert native_file_output.last_parquet_stream_route() == "native"


def test_parquet_native_file_output_skips_column_index_without_page_bounds(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Verify pages without min/max do not expose misleading column indexes."""
    require_native()
    pa = pytest.importorskip("pyarrow")
    pq = pytest.importorskip("pyarrow.parquet")
    from schema_sanitizer.api_impl.file_conversion import writers as native_file_output

    def fail_pyarrow_sink(*_args: object, **_kwargs: object) -> None:
        """Fail when the PyArrow Parquet sink fallback is called."""
        raise AssertionError("PyArrow sink fallback should not be used")

    monkeypatch.setenv("SCHEMA_SANITIZER_NATIVE_PARQUET_COMPRESSION", "uncompressed")
    monkeypatch.setattr(native_file_output, "_write_parquet_stream", fail_pyarrow_sink)
    batch = pa.record_batch({"value": pa.array([float("nan"), float("nan")], type=pa.float64())})
    stream = pa.RecordBatchReader.from_batches(batch.schema, [batch])
    out = tmp_path / "nan-only-no-column-index.parquet"

    native_file_output.write_parquet_native_first_stream(
        stream,
        out,
        feature="test_parquet_native_file_output_skips_column_index_without_page_bounds",
    )

    metadata = pq.ParquetFile(out).metadata.row_group(0).column(0)
    assert not metadata.has_column_index
    assert metadata.has_offset_index
    stats = metadata.statistics
    assert stats is not None
    assert not stats.has_min_max
    rows = pq.read_table(out).to_pylist()
    assert len(rows) == 2
    assert all(math.isnan(row["value"]) for row in rows)
    assert native_file_output.last_parquet_stream_route() == "native"


def test_parquet_native_file_output_splits_large_batches_into_row_groups(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Verify native Parquet output caps staged row-group size."""
    require_native()
    pa = pytest.importorskip("pyarrow")
    pq = pytest.importorskip("pyarrow.parquet")
    from schema_sanitizer.api_impl.file_conversion import writers as native_file_output

    def fail_pyarrow_sink(*_args: object, **_kwargs: object) -> None:
        """Fail when the PyArrow Parquet sink fallback is called."""
        raise AssertionError("PyArrow sink fallback should not be used")

    monkeypatch.setenv("SCHEMA_SANITIZER_NATIVE_PARQUET_ROW_GROUP_ROWS", "2")
    monkeypatch.setenv("SCHEMA_SANITIZER_NATIVE_PARQUET_COMPRESSION", "uncompressed")
    monkeypatch.setattr(native_file_output, "_write_parquet_stream", fail_pyarrow_sink)
    batch = pa.record_batch(
        {
            "id": pa.array([1, 2, 3, 4, 5], type=pa.int64()),
            "name": pa.array([f"name-{index}" for index in range(5)], type=pa.string()),
        }
    )
    stream = pa.RecordBatchReader.from_batches(batch.schema, [batch])
    out = tmp_path / "split-row-groups.parquet"

    native_file_output.write_parquet_native_first_stream(
        stream,
        out,
        feature="test_parquet_native_file_output_splits_large_batches_into_row_groups",
    )

    parquet_file = pq.ParquetFile(out)
    assert parquet_file.metadata.num_rows == 5
    assert parquet_file.metadata.num_row_groups == 3
    assert [parquet_file.metadata.row_group(i).num_rows for i in range(3)] == [2, 2, 1]
    assert pq.read_table(out).to_pylist() == [
        {"id": index + 1, "name": f"name-{index}"} for index in range(5)
    ]
    assert native_file_output.last_parquet_stream_route() == "native"


def test_parquet_native_file_output_respects_uncompressed_override(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Verify native Parquet compression can be disabled explicitly."""
    require_native()
    pa = pytest.importorskip("pyarrow")
    pq = pytest.importorskip("pyarrow.parquet")
    from schema_sanitizer.api_impl.file_conversion import writers as native_file_output

    def fail_pyarrow_sink(*_args: object, **_kwargs: object) -> None:
        """Fail when the PyArrow Parquet sink fallback is called."""
        raise AssertionError("PyArrow sink fallback should not be used")

    monkeypatch.setenv("SCHEMA_SANITIZER_NATIVE_PARQUET_COMPRESSION", "uncompressed")
    monkeypatch.setattr(native_file_output, "_write_parquet_stream", fail_pyarrow_sink)
    batch = pa.record_batch({"text": pa.array(["same"] * 32, type=pa.string())})
    stream = pa.RecordBatchReader.from_batches(batch.schema, [batch])
    out = tmp_path / "uncompressed.parquet"

    native_file_output.write_parquet_native_first_stream(
        stream,
        out,
        feature="test_parquet_native_file_output_respects_uncompressed_override",
    )

    parquet_file = pq.ParquetFile(out)
    assert parquet_file.metadata.row_group(0).column(0).compression == "UNCOMPRESSED"
    assert pq.read_table(out).to_pylist() == [{"text": "same"}] * 32
    assert native_file_output.last_parquet_stream_route() == "native"


def test_parquet_native_file_output_defaults_to_gzip_when_available(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Verify native Parquet defaults to GZIP page compression when built in."""
    require_native()
    pa = pytest.importorskip("pyarrow")
    pq = pytest.importorskip("pyarrow.parquet")
    from schema_sanitizer.api_impl.file_conversion import writers as native_file_output

    def fail_pyarrow_sink(*_args: object, **_kwargs: object) -> None:
        """Fail when the PyArrow Parquet sink fallback is called."""
        raise AssertionError("PyArrow sink fallback should not be used")

    monkeypatch.delenv("SCHEMA_SANITIZER_NATIVE_PARQUET_COMPRESSION", raising=False)
    monkeypatch.delenv("SCHEMA_SANITIZER_NATIVE_PARQUET_GZIP_LEVEL", raising=False)
    if not _native_parquet_zlib_available(pa, tmp_path):
        pytest.skip("native Parquet writer was built without zlib")
    monkeypatch.setattr(native_file_output, "_write_parquet_stream", fail_pyarrow_sink)
    batch = pa.record_batch({"text": pa.array(["compressible"] * 128, type=pa.string())})
    stream = pa.RecordBatchReader.from_batches(batch.schema, [batch])
    out = tmp_path / "gzip.parquet"

    native_file_output.write_parquet_native_first_stream(
        stream,
        out,
        feature="test_parquet_native_file_output_defaults_to_gzip_when_available",
    )

    parquet_file = pq.ParquetFile(out)
    compression = parquet_file.metadata.row_group(0).column(0).compression
    if compression == "UNCOMPRESSED":
        pytest.skip("native Parquet writer was built without zlib")
    assert compression == "GZIP"
    assert pq.read_table(out).to_pylist() == [{"text": "compressible"}] * 128
    assert native_file_output.last_parquet_stream_route() == "native"


def test_parquet_native_file_output_accepts_gzip_level(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Verify native Parquet exposes zlib gzip level while staying GZIP."""
    require_native()
    pa = pytest.importorskip("pyarrow")
    pq = pytest.importorskip("pyarrow.parquet")
    from schema_sanitizer.api_impl.file_conversion import writers as native_file_output

    monkeypatch.setenv("SCHEMA_SANITIZER_NATIVE_PARQUET_COMPRESSION", "gzip")
    monkeypatch.setenv("SCHEMA_SANITIZER_NATIVE_PARQUET_GZIP_LEVEL", "9")
    if not _native_parquet_zlib_available(pa, tmp_path):
        pytest.skip("native Parquet writer was built without zlib")
    batch = pa.record_batch({"text": pa.array(["compressible"] * 128, type=pa.string())})
    stream = pa.RecordBatchReader.from_batches(batch.schema, [batch])
    out = tmp_path / "gzip-level.parquet"

    native_file_output.write_parquet_native_first_stream(
        stream,
        out,
        feature="test_parquet_native_file_output_accepts_gzip_level",
    )

    parquet_file = pq.ParquetFile(out)
    assert parquet_file.metadata.row_group(0).column(0).compression == "GZIP"
    assert pq.read_table(out).to_pylist() == [{"text": "compressible"}] * 128


def test_parquet_native_file_output_rejects_invalid_gzip_level(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Verify invalid gzip levels fail before writing ambiguous output."""
    require_native()
    pa = pytest.importorskip("pyarrow")
    from schema_sanitizer.api_impl.file_conversion import writers as native_file_output

    monkeypatch.setenv("SCHEMA_SANITIZER_NATIVE_PARQUET_COMPRESSION", "gzip")
    monkeypatch.delenv("SCHEMA_SANITIZER_NATIVE_PARQUET_GZIP_LEVEL", raising=False)
    if not _native_parquet_zlib_available(pa, tmp_path):
        pytest.skip("native Parquet writer was built without zlib")
    monkeypatch.setenv("SCHEMA_SANITIZER_NATIVE_PARQUET_GZIP_LEVEL", "fast")
    batch = pa.record_batch({"text": pa.array(["x"], type=pa.string())})
    stream = pa.RecordBatchReader.from_batches(batch.schema, [batch])

    with pytest.raises(Exception, match="gzip level"):
        native_file_output.write_parquet_native_first_stream(
            stream,
            tmp_path / "bad-gzip-level.parquet",
            feature="test_parquet_native_file_output_rejects_invalid_gzip_level",
        )


@pytest.mark.parametrize("compression", ["brotli", "none"])
def test_parquet_native_file_output_rejects_unknown_compression(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, compression: str
) -> None:
    """Verify unknown or removed compression names do not fall back to PyArrow."""
    require_native()
    pa = pytest.importorskip("pyarrow")
    from schema_sanitizer.api_impl.file_conversion import writers as native_file_output

    monkeypatch.setenv("SCHEMA_SANITIZER_NATIVE_PARQUET_COMPRESSION", compression)
    batch = pa.record_batch({"text": pa.array(["x"], type=pa.string())})
    stream = pa.RecordBatchReader.from_batches(batch.schema, [batch])

    with pytest.raises(Exception, match="unsupported compression"):
        native_file_output.write_parquet_native_first_stream(
            stream,
            tmp_path / "bad-compression.parquet",
            feature="test_parquet_native_file_output_rejects_unknown_compression",
        )
    assert native_file_output.last_parquet_stream_route() == "none"


def test_to_parquet_public_compression_option_writes_uncompressed(tmp_path: Path) -> None:
    """Verify public to_parquet can disable compression without environment variables."""
    require_native()
    pq = pytest.importorskip("pyarrow.parquet")

    source = tmp_path / "rows.jsonl"
    source.write_text('{"text":"same"}\n{"text":"same"}\n', encoding="utf-8")
    out = tmp_path / "public-uncompressed.parquet"

    ss.to_parquet(source, out, input_format="jsonl", parquet_compression="uncompressed")

    parquet_file = pq.ParquetFile(out)
    assert parquet_file.metadata.row_group(0).column(0).compression == "UNCOMPRESSED"


def test_to_parquet_public_gzip_level_option_writes_gzip(tmp_path: Path) -> None:
    """Verify public to_parquet exposes gzip compression level."""
    require_native()
    pq = pytest.importorskip("pyarrow.parquet")

    source = tmp_path / "rows.jsonl"
    source.write_text('{"text":"same"}\n{"text":"same"}\n', encoding="utf-8")
    out = tmp_path / "public-gzip.parquet"

    ss.to_parquet(
        source,
        out,
        input_format="jsonl",
        parquet_compression="gzip",
        parquet_gzip_level=9,
    )

    parquet_file = pq.ParquetFile(out)
    assert parquet_file.metadata.row_group(0).column(0).compression == "GZIP"


def test_to_parquet_public_compression_option_rejects_unknown(tmp_path: Path) -> None:
    """Verify public compression validation fails before conversion."""
    source = tmp_path / "rows.jsonl"
    source.write_text('{"text":"same"}\n', encoding="utf-8")

    with pytest.raises(ValueError, match="parquet_compression"):
        ss.to_parquet(
            source, tmp_path / "out.parquet", input_format="jsonl", parquet_compression="brotli"
        )


def test_to_parquet_public_gzip_level_option_rejects_out_of_range(tmp_path: Path) -> None:
    """Verify public gzip level validation fails before conversion."""
    source = tmp_path / "rows.jsonl"
    source.write_text('{"text":"same"}\n', encoding="utf-8")

    with pytest.raises(ValueError, match="parquet_gzip_level"):
        ss.to_parquet(
            source,
            tmp_path / "out.parquet",
            input_format="jsonl",
            parquet_gzip_level=10,
        )


def test_to_parquet_public_compression_option_reaches_pyarrow_fallback(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Verify the PyArrow fallback uses the same public compression option."""
    require_native()
    pq = pytest.importorskip("pyarrow.parquet")
    from schema_sanitizer.api_impl import stream_output
    from schema_sanitizer.api_impl.file_conversion import direct_writers as native_parquet_output

    monkeypatch.setattr(
        stream_output,
        "try_write_raw_native_file_output",
        lambda *_args, **_kwargs: False,
    )
    monkeypatch.setattr(
        native_parquet_output,
        "try_write_parquet_direct_native",
        lambda *_args, **_kwargs: False,
    )
    source = tmp_path / "rows.jsonl"
    source.write_text('{"text":"same"}\n{"text":"same"}\n', encoding="utf-8")
    out = tmp_path / "pyarrow-uncompressed.parquet"

    ss.to_parquet(source, out, input_format="jsonl", parquet_compression="uncompressed")

    parquet_file = pq.ParquetFile(out)
    assert parquet_file.metadata.row_group(0).column(0).compression == "UNCOMPRESSED"
