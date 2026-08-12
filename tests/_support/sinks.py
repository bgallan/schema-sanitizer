"""Shared fixtures and normalization helpers for sink tests."""

from __future__ import annotations

from pathlib import Path

GENERATED_METADATA_COLUMNS = {
    "schema_registry",
    "schema_drifts",
    "source_file",
    "ingestion_timestamp",
}


def write_csv(path: Path, text: str = "a,b\n1,2\n3,4\n") -> Path:
    """Write one UTF-8 CSV test input and return its path."""
    path.write_text(text, encoding="utf-8")
    return path


def without_generated_metadata(row: dict[str, object]) -> dict[str, object]:
    """Return row data excluding generated file-converter metadata columns."""
    return {key: value for key, value in row.items() if key not in GENERATED_METADATA_COLUMNS}


def without_generated_metadata_rows(
    rows: list[dict[str, object]],
) -> list[dict[str, object]]:
    """Return rows excluding generated file-converter metadata columns."""
    return [without_generated_metadata(row) for row in rows]


def native_parquet_zlib_available(pa: object, tmp_path: Path) -> bool:
    """Return whether the compiled native Parquet writer can emit gzip pages."""
    from schema_sanitizer.api_impl.file_conversion import direct_writers as native_parquet_output

    write = native_parquet_output.PARQUET_STREAM_WRITE
    if write is None:
        return False
    batch = pa.record_batch({"text": pa.array(["probe"], type=pa.string())})
    stream = pa.RecordBatchReader.from_batches(batch.schema, [batch])
    try:
        write(stream, str(tmp_path / "native-zlib-probe.parquet"), "gzip", -1, -1)
    except RuntimeError as exc:
        if "zlib is not available" in str(exc):
            return False
        raise
    return True


def fail_pyarrow_sink(*_args: object, **_kwargs: object) -> None:
    """Fail when a native-writer test unexpectedly uses the PyArrow fallback."""
    raise AssertionError("PyArrow sink fallback should not be used")
