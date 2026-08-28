"""Tests read adapters and file-to-file converters."""

from __future__ import annotations

import builtins
import csv
import io
import json
from pathlib import Path

import pytest
from _support.sinks import (
    write_csv as _write_csv,
)
from conftest import read_test_csv, read_test_json

import schema_sanitizer as ss

pytestmark = pytest.mark.usefixtures("require_native")


def test_read_pandas_adapter(tmp_path: Path) -> None:
    pd = pytest.importorskip("pandas")
    pytest.importorskip("pyarrow")

    result = read_test_csv(_write_csv(tmp_path / "rows.csv"), output_format="pandas")
    df = result.clean_data
    assert isinstance(df, pd.DataFrame)
    assert list(df.columns) == ["a", "b"]
    assert df.iloc[0].tolist() == ["1", "2"]


def test_read_polars_adapter(tmp_path: Path) -> None:
    pl = pytest.importorskip("polars")
    pytest.importorskip("pyarrow")

    result = read_test_csv(_write_csv(tmp_path / "rows.csv"), output_format="polars")
    df = result.clean_data
    assert isinstance(df, pl.DataFrame)
    assert df.columns == ["a", "b"]
    assert df.row(0) == ("1", "2")


def test_read_duckdb_relation(tmp_path: Path) -> None:
    pytest.importorskip("duckdb")
    pytest.importorskip("pyarrow")

    result = read_test_csv(_write_csv(tmp_path / "rows.csv"), output_format="duckdb")
    rel = result.clean_data
    assert rel.fetchall() == [("1", "2"), ("3", "4")]


def test_optional_adapters_equivalent_to_arrow(tmp_path: Path) -> None:
    pd = pytest.importorskip("pandas")
    pl = pytest.importorskip("polars")
    pytest.importorskip("pyarrow")

    path = _write_csv(tmp_path / "rows.csv")
    baseline = read_test_csv(path).clean_data.to_pylist()

    pandas_df = read_test_csv(path, output_format="pandas").clean_data
    polars_df = read_test_csv(path, output_format="polars").clean_data
    assert isinstance(pandas_df, pd.DataFrame)
    assert isinstance(polars_df, pl.DataFrame)
    assert pandas_df.to_dict(orient="records") == baseline
    assert polars_df.to_dicts() == baseline


def test_filelike_input_is_rejected() -> None:
    with pytest.raises(TypeError):
        read_test_json(io.BytesIO(b'[{"a": 1}, {"a": 2}]'))


@pytest.mark.parametrize("converter,suffix", [(ss.to_csv, ".csv"), (ss.to_jsonl, ".jsonl")])
def test_native_file_output_bypasses_stream_wrapper(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    converter: object,
    suffix: str,
) -> None:
    pytest.importorskip("pyarrow")
    from schema_sanitizer.api_impl import stream_output

    class FailingStream:
        """Fail if the stream-wrapper fallback is used."""

        def __init__(self, *_args: object, **_kwargs: object) -> None:
            raise AssertionError("native file output should bypass Stream(raw)")

    monkeypatch.setattr(stream_output, "Stream", FailingStream)
    out = tmp_path / f"out{suffix}"
    result = converter(
        _write_csv(tmp_path / "rows.csv", "a,b\n1,2\n"),
        out,
        input_format="csv",
    )
    assert isinstance(result, ss.Result)
    assert out.exists()


def test_native_csv_file_output_diagnostics_do_not_post_count(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    pytest.importorskip("pyarrow")

    def fail_reader(*_args: object, **_kwargs: object) -> object:
        """Reject the CSV diagnostics recount fallback."""
        raise AssertionError("native CSV diagnostics should not use csv.reader")

    monkeypatch.setattr(csv, "reader", fail_reader)
    out = tmp_path / "out.csv"
    result = ss.to_csv(
        _write_csv(tmp_path / "rows.csv", "a,b\n1,2\n3,4\n"),
        out,
        input_format="csv",
    )

    assert out.exists()
    assert result.stats["materialized_rows"] == 2
    assert result.stats["batches"] >= 1


def test_native_jsonl_file_output_diagnostics_do_not_post_count(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    pytest.importorskip("pyarrow")

    source = _write_csv(tmp_path / "rows.csv", "a,b\n1,2\n3,4\n")
    out = tmp_path / "out.jsonl"
    real_open = builtins.open

    def guarded_open(file: object, mode: str = "r", *args: object, **kwargs: object) -> object:
        """Reject the JSONL diagnostics recount fallback."""
        try:
            is_output = Path(file) == out
        except TypeError:
            is_output = False
        if is_output and "b" in mode:
            raise AssertionError("native JSONL diagnostics should not reopen output")
        return real_open(file, mode, *args, **kwargs)

    monkeypatch.setattr(builtins, "open", guarded_open)
    result = ss.to_jsonl(source, out, input_format="csv")

    assert out.exists()
    assert result.stats["materialized_rows"] == 2
    assert result.stats["batches"] >= 1


def test_single_source_registry_metadata_is_injected_before_file_output(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    pytest.importorskip("pyarrow")
    from schema_sanitizer.api_impl import stream_output

    real_write = stream_output.try_write_raw_native_file_output
    seen: list[dict[str, object]] = []

    def tracking_write(*args: object, **kwargs: object) -> bool:
        """Record raw file-output metadata arguments."""
        seen.append(dict(kwargs))
        return real_write(*args, **kwargs)

    monkeypatch.setattr(stream_output, "try_write_raw_native_file_output", tracking_write)
    source = tmp_path / "rows.jsonl"
    source.write_text('{"a": 1}\n', encoding="utf-8")
    out = tmp_path / "out.jsonl"

    ss.to_jsonl(source, out, input_format="jsonl")

    rows = [json.loads(line) for line in out.read_text(encoding="utf-8").splitlines()]
    assert rows[0]["a"] == 1
    assert rows[0]["schema_registry"]
    assert rows[0]["schema_drifts"]
    assert rows[0]["source_file"] == str(source)
    assert rows[0]["ingestion_timestamp"]
    assert seen
    assert seen[-1]["first_row_columns"] is None
    assert seen[-1]["all_row_columns"] is None
    assert seen[-1]["timestamp_columns"] == ()


def test_single_source_to_pyarrow_uses_native_registry_metadata_stream(
    tmp_path: Path,
) -> None:
    pytest.importorskip("pyarrow")

    source = tmp_path / "rows.jsonl"
    source.write_text('{"a": 1}\n{"a": 2}\n', encoding="utf-8")

    result = ss.to_pyarrow(source, input_format="jsonl")

    rows = result.clean_data.to_pylist()
    assert [row["a"] for row in rows] == [1, 2]
    assert rows[0]["schema_registry"]
    assert rows[0]["schema_drifts"]
    assert rows[0]["source_file"] == str(source)
    assert rows[1]["source_file"] == str(source)
    assert rows[0]["ingestion_timestamp"] is not None
    assert rows[1]["ingestion_timestamp"] is not None


def test_registry_source_sink_accepts_native_row_span_metadata() -> None:
    pa = pytest.importorskip("pyarrow")
    from schema_sanitizer.api_impl.execution_context import ExecutionContext
    from schema_sanitizer.api_impl.streams import Stream
    from schema_sanitizer.core_impl.resource_lifecycle import _close_suppressing_errors

    raw = ExecutionContext()._raw.to_registry_sink_from_source(
        "stream",
        "json",
        "text",
        '{"a":1}\n{"a":2}\n{"a":3}\n',
        None,
        registry_json="{}",
        field_name_policy="lower_alpha",
        schema_mode="additive",
        first_row_columns={},
        all_row_columns={},
        row_span_columns={"source_file": [(1, "/tmp/first.jsonl"), (2, "/tmp/second.jsonl")]},
        timestamp_columns=("ingestion_timestamp",),
    )
    stream = Stream(raw)
    try:
        table = pa.Table.from_batches(stream, schema=stream.schema)
    finally:
        _close_suppressing_errors(stream)
        _close_suppressing_errors(raw)

    rows = table.to_pylist()
    assert [row["a"] for row in rows] == [1, 2, 3]
    assert [row["source_file"] for row in rows] == [
        "/tmp/first.jsonl",
        "/tmp/second.jsonl",
        "/tmp/second.jsonl",
    ]
    assert rows[0]["schema_registry"]
    assert rows[0]["schema_drifts"]
    assert rows[1]["schema_registry"] is None
    assert rows[0]["ingestion_timestamp"] is not None
    assert rows[1]["ingestion_timestamp"] is not None
