"""Tests read adapters and file-to-file converters."""

from __future__ import annotations

import csv
import datetime as dt
import json
from pathlib import Path

import pytest
from conftest import require_native

import schema_sanitizer as ss
from schema_sanitizer.core_impl.schema_registry import merge_schema_registry

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
        write(stream, str(tmp_path / "native-zlib-probe.parquet"), "gzip", -1, -1)
    except RuntimeError as exc:
        if "zlib is not available" in str(exc):
            return False
        raise
    return True


# Split from test_sinks_parquet_public_and_metadata.py: test_to_parquet_alphabetically_orders_incremental_registry_struct_fields, test_to_parquet_writes_timestamp_micros_by_default, test_to_parquet_can_write_timestamp_nanos, ...


def test_to_parquet_alphabetically_orders_incremental_registry_struct_fields(
    tmp_path: Path,
) -> None:
    """Verify physical Parquet schemas sort additive nested registry fields."""
    require_native()
    pq = pytest.importorskip("pyarrow.parquet")

    first_source = tmp_path / "first.jsonl"
    first_source.write_text(
        json.dumps({"variables": {"email": "a@example.com", "phone": "1"}}) + "\n",
        encoding="utf-8",
    )
    first_out = tmp_path / "first.parquet"
    first = ss.to_parquet(
        first_source,
        first_out,
        input_format="jsonl",
        field_name_policy="lower_snake",
        column_order="alphabetically",
    )

    second_source = tmp_path / "second.jsonl"
    second_source.write_text(
        json.dumps(
            {
                "variables": {
                    "birthday": "2026-01-01",
                    "company": "acme",
                    "country": "ES",
                    "email": "b@example.com",
                }
            }
        )
        + "\n",
        encoding="utf-8",
    )
    second_out = tmp_path / "second.parquet"
    second = ss.to_parquet(
        second_source,
        second_out,
        input_format="jsonl",
        field_name_policy="lower_snake",
        column_order="alphabetically",
        schema_registry=first.schema_registry,
    )

    physical_schema = pq.read_schema(second_out)
    variable_names = [field.name for field in physical_schema.field("variables").type]
    assert variable_names == ["birthday", "company", "country", "email", "phone"]
    assert pq.read_table(second_out, columns=["variables"])["variables"].to_pylist() == [
        {
            "birthday": "2026-01-01",
            "company": "acme",
            "country": "ES",
            "email": "b@example.com",
            "phone": None,
        }
    ]

    registry_fields = second.schema_registry["canonical_schema"]["fields"]
    variables = next(field for field in registry_fields if field["name"] == "variables")
    assert [field["name"] for field in variables["type"]["fields"]] == variable_names


def test_to_parquet_writes_timestamp_micros_by_default(tmp_path: Path) -> None:
    """Verify parquet timestamps default to BigQuery-compatible microseconds."""
    require_native()
    pa = pytest.importorskip("pyarrow")
    pq = pytest.importorskip("pyarrow.parquet")

    source = tmp_path / "rows.jsonl"
    source.write_text('{"ts":"2026-01-01T03:01:26.123456789Z"}\n', encoding="utf-8")
    out = tmp_path / "out.parquet"

    ss.to_parquet(
        source,
        out,
        input_format="jsonl",
        parse_iso_timestamps=True,
    )

    assert pq.read_schema(out).field("ts").type == pa.timestamp("us")
    assert "microseconds" in str(pq.ParquetFile(out).schema.column(0).logical_type)


def test_to_parquet_can_write_timestamp_nanos(tmp_path: Path) -> None:
    """Verify parquet timestamp nanos can still be requested explicitly."""
    require_native()
    pa = pytest.importorskip("pyarrow")
    pq = pytest.importorskip("pyarrow.parquet")

    source = tmp_path / "rows.jsonl"
    source.write_text('{"ts":"2026-01-01T03:01:26.123456789Z"}\n', encoding="utf-8")
    out = tmp_path / "out.parquet"

    ss.to_parquet(
        source,
        out,
        input_format="jsonl",
        timestamp_precision="TIMESTAMP_NANOS",
        parse_iso_timestamps=True,
    )

    assert pq.read_schema(out).field("ts").type == pa.timestamp("ns")
    assert "nanoseconds" in str(pq.ParquetFile(out).schema.column(0).logical_type)


def test_to_parquet_covers_schema_sanitizer_emitted_time(
    tmp_path: Path,
) -> None:
    """Verify emitted time32[s] schemas stay on native Parquet output."""
    require_native()
    pa = pytest.importorskip("pyarrow")
    pq = pytest.importorskip("pyarrow.parquet")

    source = tmp_path / "rows.jsonl"
    source.write_text(
        '{"clock":"01:02:03"}\n{"clock":null}\n',
        encoding="utf-8",
    )
    out = tmp_path / "out.parquet"

    ss.to_parquet(
        source,
        out,
        input_format="jsonl",
        parse_iso_times=True,
    )

    table = pq.read_table(out)
    row_data = _without_generated_metadata_rows(table.to_pylist())
    assert table.schema.field("clock").type == pa.time32("ms")
    assert row_data == [
        {"clock": dt.time(1, 2, 3)},
        {"clock": None},
    ]
    parquet_schema = str(pq.ParquetFile(out).schema)
    assert "Time(isAdjustedToUTC=false, timeUnit=milliseconds)" in parquet_schema


def test_metadata_native_stream_handles_all_row_and_timestamp_columns() -> None:
    """Verify native metadata injection covers single-file generated columns."""
    require_native()
    pa = pytest.importorskip("pyarrow")
    from schema_sanitizer.adapters.pyarrow.file_metadata import (
        last_metadata_route,
        prepare_file_output_metadata_stream,
    )

    first = pa.record_batch({"a": pa.array(["1", "2"])})
    second = pa.record_batch({"a": pa.array(["3"])})
    stream = pa.RecordBatchReader.from_batches(first.schema, [first, second])
    metadata = prepare_file_output_metadata_stream(
        stream,
        {"schema_registry": "{}"},
        {"source_file": "/tmp/source.jsonl"},
        timestamp_columns=("ingestion_timestamp",),
        pa=pa,
    )

    try:
        assert last_metadata_route() == "native"
        assert metadata.schema.field("source_file").type == pa.string()
        assert metadata.schema.field("ingestion_timestamp").type == pa.timestamp("us")
        rows = metadata.reader.read_all().to_pylist()
    finally:
        metadata.close()

    assert [row["source_file"] for row in rows] == ["/tmp/source.jsonl"] * 3
    assert [row["schema_registry"] for row in rows] == ["{}", None, None]
    assert all(isinstance(row["ingestion_timestamp"], dt.datetime) for row in rows)


def test_metadata_native_stream_handles_row_span_columns_across_batches() -> None:
    """Verify native metadata injection can track directory source-file spans."""
    require_native()
    pa = pytest.importorskip("pyarrow")
    from schema_sanitizer.adapters.pyarrow.file_metadata import (
        last_metadata_route,
        prepare_file_output_metadata_stream,
    )

    first = pa.record_batch({"a": pa.array(["1", "2"])})
    second = pa.record_batch({"a": pa.array(["3", "4"])})
    stream = pa.RecordBatchReader.from_batches(first.schema, [first, second])
    metadata = prepare_file_output_metadata_stream(
        stream,
        {"schema_registry": "{}"},
        row_span_columns={"source_file": [(1, "/tmp/first.jsonl"), (2, "/tmp/second.jsonl")]},
        timestamp_columns=("ingestion_timestamp",),
        pa=pa,
    )

    try:
        assert last_metadata_route() == "native"
        assert metadata.schema.field("source_file").type == pa.string()
        rows = metadata.reader.read_all().to_pylist()
    finally:
        metadata.close()

    assert [row["source_file"] for row in rows] == [
        "/tmp/first.jsonl",
        "/tmp/second.jsonl",
        "/tmp/second.jsonl",
        None,
    ]
    assert [row["schema_registry"] for row in rows] == ["{}", None, None, None]
    assert all(isinstance(row["ingestion_timestamp"], dt.datetime) for row in rows)


@pytest.mark.parametrize("suffix", [".csv", ".jsonl", ".parquet"])
def test_to_file_embeds_native_schema_registry(tmp_path: Path, suffix: str) -> None:
    """Verify all file sinks can embed native schema registry metadata."""
    require_native()
    pa = pytest.importorskip("pyarrow")
    pq = pytest.importorskip("pyarrow.parquet")

    sentence_struct = pa.struct([pa.field("text", pa.string())])
    previous_schema = pa.schema([pa.field("sentences", sentence_struct)])
    previous_registry = merge_schema_registry(
        inferred_schema=previous_schema,
        schema_registry={"schema_generation": 1},
        field_name_policy="lower_snake",
    ).schema_registry
    source = tmp_path / "rows.jsonl"
    source.write_text(
        '{"sentences":[{"text":"two"}]}\n{"sentences":[{"text":"three"}]}\n',
        encoding="utf-8",
    )
    out = tmp_path / f"out{suffix}"
    converter = {".csv": ss.to_csv, ".jsonl": ss.to_jsonl, ".parquet": ss.to_parquet}[suffix]

    result = converter(
        source,
        out,
        input_format="jsonl",
        schema_mode="strict",
        field_name_policy="lower_snake",
        schema_registry=previous_registry,
    )

    if suffix == ".csv":
        with out.open("r", encoding="utf-8", newline="") as f:
            rows = list(csv.DictReader(f))
    elif suffix == ".jsonl":
        rows = [json.loads(line) for line in out.read_text(encoding="utf-8").splitlines()]
    else:
        rows = pq.read_table(out).to_pylist()

    row = rows[0]
    second_row = rows[1]
    registry = json.loads(row["schema_registry"])
    drifts = json.loads(row["schema_drifts"])
    assert row["source_file"] == str(source)
    assert second_row["source_file"] == str(source)
    if suffix == ".parquet":
        assert pq.read_schema(out).field("ingestion_timestamp").type == pa.timestamp("us")
        assert isinstance(row["ingestion_timestamp"], dt.datetime)
        assert isinstance(second_row["ingestion_timestamp"], dt.datetime)
    else:
        assert isinstance(row["ingestion_timestamp"], str)
        assert isinstance(second_row["ingestion_timestamp"], str)
        assert row["ingestion_timestamp"]
        assert second_row["ingestion_timestamp"]
    assert second_row["schema_registry"] in (None, "")
    assert second_row["schema_drifts"] in (None, "")
    assert result.schema_registry == registry
    assert result.schema_drifts == drifts
    assert result.schema_registry_json == row["schema_registry"]
    assert result.schema_drifts_json == row["schema_drifts"]
    assert registry["schema_generation"] == 3
    assert drifts[0]["output_name"] == "sentences_v2_struct_array"
    assert drifts[0]["drift_type"] == "new_version_generated"
    assert isinstance(drifts[0]["detected_at"], str)
    assert drifts[0]["detected_at"].endswith("Z")


def test_embedded_registry_wraps_singleton_into_existing_list(tmp_path: Path) -> None:
    """Verify registry-backed sinks avoid variants when existing lists can wrap values."""
    require_native()
    pa = pytest.importorskip("pyarrow")

    sentence_struct = pa.struct([pa.field("text", pa.string())])
    previous_registry = merge_schema_registry(
        inferred_schema=pa.schema([pa.field("sentences", pa.list_(sentence_struct))]),
        schema_registry=None,
        field_name_policy="lower_snake",
    ).schema_registry

    source = tmp_path / "rows.jsonl"
    source.write_text('{"sentences":{"text":"one"}}\n', encoding="utf-8")
    out = tmp_path / "out.jsonl"

    result = ss.to_jsonl(
        source,
        out,
        input_format="jsonl",
        schema_mode="strict",
        field_name_policy="lower_snake",
        schema_registry=previous_registry,
    )

    row = json.loads(out.read_text(encoding="utf-8").strip())
    registry = json.loads(row["schema_registry"])
    assert row["sentences"] == [{"text": "one"}]
    assert "sentences_v2_struct_array" not in row
    assert result.schema_drifts == []
    assert json.loads(row["schema_drifts"]) == []
    assert registry["variants"]["sentences"]["versions"][0]["output_name"] == "sentences"


def test_analytical_ingestion_timestamp_is_timestamp_micros(tmp_path: Path) -> None:
    """Verify analytical outputs expose ingestion timestamp as TIMESTAMP_MICROS."""
    require_native()
    pa = pytest.importorskip("pyarrow")

    sentence_struct = pa.struct([pa.field("text", pa.string())])
    previous_registry = merge_schema_registry(
        inferred_schema=pa.schema([pa.field("sentences", sentence_struct)]),
        schema_registry=None,
        field_name_policy="lower_snake",
    ).schema_registry
    source = tmp_path / "rows.jsonl"
    source.write_text('{"sentences":[{"text":"two"}]}\n', encoding="utf-8")

    result = ss.to_pyarrow(
        source,
        input_format="jsonl",
        schema_mode="strict",
        field_name_policy="lower_snake",
        schema_registry=previous_registry,
    )

    row = result.clean_data.to_pylist()[0]
    drifts = json.loads(row["schema_drifts"])
    assert result.clean_data.schema.field("ingestion_timestamp").type == pa.timestamp("us")
    assert isinstance(row["ingestion_timestamp"], dt.datetime)
    assert isinstance(drifts[0]["detected_at"], str)
    assert drifts[0]["detected_at"].endswith("Z")


def test_embedded_registry_routes_nested_scalar_versions_without_parent_growth(
    tmp_path: Path,
) -> None:
    """Verify nested scalar variants materialize independently under one parent version."""
    require_native()
    pa = pytest.importorskip("pyarrow")

    numeric_sentiment = pa.struct([pa.field("magnitude", pa.float64())])
    nullable = merge_schema_registry(
        inferred_schema=pa.schema([pa.field("sentiment_analysis", numeric_sentiment)]),
        schema_registry=None,
        field_name_policy="lower_snake",
    )
    repeated = merge_schema_registry(
        inferred_schema=pa.schema([pa.field("sentiment_analysis", pa.list_(numeric_sentiment))]),
        schema_registry=nullable.schema_registry,
        field_name_policy="lower_snake",
    )

    string_source = tmp_path / "string.jsonl"
    string_source.write_text(
        '{"sentiment_analysis":[{"magnitude":"positive"}]}\n',
        encoding="utf-8",
    )
    string_out = tmp_path / "string-out.jsonl"
    string_result = ss.to_jsonl(
        string_source,
        string_out,
        input_format="jsonl",
        schema_mode="strict",
        field_name_policy="lower_snake",
        schema_registry=repeated.schema_registry,
    )

    string_row = json.loads(string_out.read_text(encoding="utf-8").strip())
    assert string_row["sentiment_analysis"] is None
    assert string_row["sentiment_analysis_v2_struct_array"] == [
        {"magnitude": None, "magnitude_v2_string": "positive"}
    ]
    assert "sentiment_analysis_v3_struct_array" not in string_row
    assert [drift["output_name"] for drift in string_result.schema_drifts] == [
        "magnitude_v2_string"
    ]

    numeric_source = tmp_path / "numeric.jsonl"
    numeric_source.write_text(
        '{"sentiment_analysis":[{"magnitude":1.5}]}\n',
        encoding="utf-8",
    )
    numeric_out = tmp_path / "numeric-out.jsonl"
    numeric_result = ss.to_jsonl(
        numeric_source,
        numeric_out,
        input_format="jsonl",
        schema_mode="strict",
        field_name_policy="lower_snake",
        schema_registry=string_result.schema_registry,
    )

    numeric_row = json.loads(numeric_out.read_text(encoding="utf-8").strip())
    assert numeric_row["sentiment_analysis_v2_struct_array"] == [
        {"magnitude": 1.5, "magnitude_v2_string": None}
    ]
    assert numeric_result.schema_drifts == []
