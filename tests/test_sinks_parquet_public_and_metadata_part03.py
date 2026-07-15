"""Tests read adapters and file-to-file converters."""

from __future__ import annotations

import csv
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
_GENERATED_METADATA_COLUMN_ORDER = (
    "schema_registry",
    "schema_drifts",
    "source_file",
    "ingestion_timestamp",
)


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


# Split from test_sinks_parquet_public_and_metadata.py: test_embedded_metadata_uses_fixed_native_source_path_column, test_embedded_metadata_rejects_source_column_collisions, test_embedded_metadata_rejects_direct_parquet_source_collision, ...


def test_embedded_metadata_uses_fixed_native_source_path_column(tmp_path: Path) -> None:
    """Verify generated source path metadata comes from the native helper."""
    require_native()

    pa = pytest.importorskip("pyarrow")
    previous_registry = merge_schema_registry(
        inferred_schema=pa.schema([pa.field("a", pa.string())]),
        schema_registry=None,
        field_name_policy="lower_snake",
    ).schema_registry

    source = tmp_path / "nested" / "rows.jsonl"
    source.parent.mkdir()
    source.write_text('{"a":"1"}\n', encoding="utf-8")
    out = tmp_path / "out.jsonl"

    ss.to_jsonl(
        source,
        out,
        input_format="jsonl",
        schema_mode="strict",
        schema_registry=previous_registry,
    )

    row = json.loads(out.read_text(encoding="utf-8").strip())
    assert row["source_file"] == str(source)
    assert isinstance(row["ingestion_timestamp"], str)
    assert row["ingestion_timestamp"]


@pytest.mark.parametrize(
    ("column_order", "data_columns"),
    [("alphabetically", ["a", "z"]), ("schema_contract_first", ["z", "a"])],
)
def test_generated_etl_columns_are_materialized_last_for_grouped_sources(
    tmp_path: Path,
    column_order: str,
    data_columns: list[str],
) -> None:
    """Verify every output keeps generated ETL columns at the schema tail."""
    require_native()
    pq = pytest.importorskip("pyarrow.parquet")
    source = tmp_path / "parts"
    source.mkdir()
    (source / "one.jsonl").write_text('{"z":1,"a":2}\n', encoding="utf-8")
    (source / "two.jsonl").write_text('{"z":3,"a":4}\n', encoding="utf-8")
    expected = [*data_columns, *_GENERATED_METADATA_COLUMN_ORDER]

    table = ss.to_pyarrow(
        source, input_format="jsonl", input_mode="directory", column_order=column_order
    ).clean_data
    assert table.schema.names == expected

    parquet_out = tmp_path / "out.parquet"
    ss.to_parquet(
        source,
        parquet_out,
        input_format="jsonl",
        input_mode="directory",
        column_order=column_order,
    )
    assert pq.read_schema(parquet_out).names == expected

    csv_out = tmp_path / "out.csv"
    ss.to_csv(
        source,
        csv_out,
        input_format="jsonl",
        input_mode="directory",
        column_order=column_order,
    )
    with csv_out.open(newline="", encoding="utf-8") as csv_file:
        assert next(csv.reader(csv_file)) == expected

    jsonl_out = tmp_path / "out.jsonl"
    ss.to_jsonl(
        source,
        jsonl_out,
        input_format="jsonl",
        input_mode="directory",
        column_order=column_order,
    )
    first_row = json.loads(jsonl_out.read_text(encoding="utf-8").splitlines()[0])
    assert list(first_row) == expected


@pytest.mark.parametrize(
    "reserved_name",
    ("schema_registry", "schema_drifts", "source_file", "ingestion_timestamp"),
)
def test_embedded_metadata_rejects_source_column_collisions(
    tmp_path: Path,
    reserved_name: str,
) -> None:
    """Verify generated embedded metadata column names cannot collide."""
    require_native()

    source = tmp_path / "rows.jsonl"
    source.write_text(json.dumps({reserved_name: "source"}) + "\n", encoding="utf-8")
    out = tmp_path / "out.jsonl"

    with pytest.raises(ValueError, match=rf"generated metadata column '{reserved_name}'"):
        ss.to_jsonl(source, out, input_format="jsonl")
    assert not out.exists()


def test_embedded_metadata_rejects_direct_parquet_source_collision(tmp_path: Path) -> None:
    """Verify direct Parquet ingestion enforces the fixed ETL column contract."""
    require_native()
    pa = pytest.importorskip("pyarrow")
    pq = pytest.importorskip("pyarrow.parquet")

    source = tmp_path / "rows.parquet"
    pq.write_table(pa.table({"schema_registry": ["source"]}), source)
    out = tmp_path / "out.jsonl"

    with pytest.raises(ValueError, match="generated metadata column 'schema_registry'"):
        ss.to_jsonl(source, out, input_format="parquet")
    assert not out.exists()


def test_embedded_metadata_allows_nested_reserved_names(tmp_path: Path) -> None:
    """Verify only top-level ETL column names are reserved."""
    require_native()

    source = tmp_path / "rows.jsonl"
    source.write_text('{"payload":{"source_file":"nested"}}\n', encoding="utf-8")
    out = tmp_path / "out.jsonl"

    ss.to_jsonl(source, out, input_format="jsonl")

    row = json.loads(out.read_text(encoding="utf-8").strip())
    assert row["payload"]["sourcefile"] == "nested"
    assert row["source_file"] == str(source)


def test_embedded_registry_strict_requires_previous_canonical_schema(tmp_path: Path) -> None:
    """Verify strict registry-backed writes cannot bootstrap a new registry."""
    require_native()

    source = tmp_path / "rows.jsonl"
    source.write_text('{"a":1}\n', encoding="utf-8")
    out = tmp_path / "out.jsonl"

    with pytest.raises(ValueError, match="canonical_schema"):
        ss.to_jsonl(
            source,
            out,
            input_format="jsonl",
            schema_mode="strict",
            schema_registry={"schema_generation": 1},
        )


def test_file_uri_input_metadata_preserves_original_uri(tmp_path: Path) -> None:
    """Verify file URI inputs still emit the original URI as source metadata."""
    require_native()
    pytest.importorskip("pyarrow")

    source = _write_csv(tmp_path / "rows.csv", "a,b\n1,2\n")
    out = tmp_path / "out.jsonl"
    source_uri = source.as_uri()

    ss.to_jsonl(source_uri, out, input_format="csv")

    row = json.loads(out.read_text(encoding="utf-8").splitlines()[0])
    assert row["source_file"] == source_uri
    assert _without_generated_metadata(row) == {"a": "1", "b": "2"}


def test_to_parquet_writes_file_uri(tmp_path: Path) -> None:
    """Verify to parquet writes through file URI outputs."""
    require_native()
    pq = pytest.importorskip("pyarrow.parquet")

    out = tmp_path / "out-uri.parquet"
    ss.to_parquet(
        _write_csv(tmp_path / "rows.csv", "a,b\n1,2\n"),
        out.as_uri(),
        input_format="csv",
    )

    assert _without_generated_metadata_rows(pq.read_table(out).to_pylist()) == [
        {"a": "1", "b": "2"}
    ]


@pytest.mark.parametrize("suffix", [".csv", ".jsonl", ".parquet"])
def test_to_file_idempotent_repeated_runs(tmp_path: Path, suffix: str) -> None:
    """Verify to file idempotent repeated runs."""
    require_native()
    pq = pytest.importorskip("pyarrow.parquet")

    path = _write_csv(tmp_path / "rows.csv")
    baseline_rows = None
    converter = {".csv": ss.to_csv, ".jsonl": ss.to_jsonl, ".parquet": ss.to_parquet}[suffix]
    for run_idx in range(3):
        out = tmp_path / f"out_{run_idx}{suffix}"
        converter(path, out, input_format="csv")
        if suffix == ".csv":
            with out.open("r", encoding="utf-8", newline="") as f:
                rows = list(csv.DictReader(f))
        elif suffix == ".jsonl":
            rows = [json.loads(line) for line in out.read_text(encoding="utf-8").splitlines()]
        else:
            rows = pq.read_table(out).to_pylist()
        rows = _without_generated_metadata_rows(rows)
        if run_idx == 0:
            baseline_rows = rows
            continue
        assert rows == baseline_rows
