"""Tests read adapters and file-to-file converters."""

from __future__ import annotations

import builtins
import csv
import datetime as dt
import io
import json
import logging
import math
from decimal import Decimal
from pathlib import Path

import pytest
from conftest import read_test_csv, read_test_json, require_native

import schema_sanitizer as ss
from schema_sanitizer.adapters.pyarrow_jsonl_sink import _schema_supports_native_jsonl
from schema_sanitizer.api_impl.schema_registry import merge_schema_registry

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
    from schema_sanitizer.api_impl import direct_native_file_output

    write = direct_native_file_output.PARQUET_STREAM_WRITE.get()
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


def test_read_pandas_adapter(tmp_path: Path) -> None:
    """Verify read pandas adapter."""
    require_native()
    pd = pytest.importorskip("pandas")
    pytest.importorskip("pyarrow")

    result = read_test_csv(_write_csv(tmp_path / "rows.csv"), output_format="pandas")
    df = result.clean_data
    assert isinstance(df, pd.DataFrame)
    assert list(df.columns) == ["a", "b"]
    assert df.iloc[0].tolist() == ["1", "2"]


def test_read_polars_adapter(tmp_path: Path) -> None:
    """Verify read polars adapter."""
    require_native()
    pl = pytest.importorskip("polars")
    pytest.importorskip("pyarrow")

    result = read_test_csv(_write_csv(tmp_path / "rows.csv"), output_format="polars")
    df = result.clean_data
    assert isinstance(df, pl.DataFrame)
    assert df.columns == ["a", "b"]
    assert df.row(0) == ("1", "2")


def test_read_duckdb_relation(tmp_path: Path) -> None:
    """Verify read duckdb relation."""
    require_native()
    pytest.importorskip("duckdb")
    pytest.importorskip("pyarrow")

    result = read_test_csv(_write_csv(tmp_path / "rows.csv"), output_format="duckdb")
    rel = result.clean_data
    assert rel.fetchall() == [("1", "2"), ("3", "4")]


def test_optional_adapters_equivalent_to_arrow(tmp_path: Path) -> None:
    """Verify optional adapters equivalent to arrow."""
    require_native()
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
    """Verify filelike input is rejected."""
    require_native()
    with pytest.raises(TypeError):
        read_test_json(io.BytesIO(b'[{"a": 1}, {"a": 2}]'))


def test_to_csv_writes_file(tmp_path: Path) -> None:
    """Verify to csv writes file."""
    require_native()
    pytest.importorskip("pyarrow")
    from schema_sanitizer.adapters.pyarrow_csv_sink import last_csv_stream_route

    out = tmp_path / "out.csv"
    result = ss.to_csv(
        _write_csv(tmp_path / "rows.csv", "a,b\n1,2\n"),
        out,
        input_format="csv",
    )
    assert isinstance(result, ss.Result)
    assert result.clean_data is None
    assert out.exists()
    with out.open("r", encoding="utf-8", newline="") as f:
        rows = list(csv.DictReader(f))
    assert _without_generated_metadata_rows(rows) == [{"a": "1", "b": "2"}]
    assert last_csv_stream_route() == "native"


def test_to_csv_json_stringifies_nested_fields(tmp_path: Path) -> None:
    """Verify to csv json stringifies nested fields."""
    require_native()
    pytest.importorskip("pyarrow")
    from schema_sanitizer.adapters.pyarrow_csv_sink import last_csv_stream_route

    data = [
        {"id": 1, "payload": {"a": 1, "b": "x"}, "items": [1, 2, 3]},
        {"id": 2, "payload": {"a": 2, "b": "y"}, "items": [4]},
    ]
    source = tmp_path / "nested-source.jsonl"
    source.write_text(
        "".join(json.dumps(row) + "\n" for row in data),
        encoding="utf-8",
    )
    out = tmp_path / "nested.csv"
    ss.to_csv(source, out, input_format="jsonl")

    with out.open("r", encoding="utf-8", newline="") as f:
        rows = list(csv.DictReader(f))

    rows = [_without_generated_metadata(row) for row in rows]
    assert len(rows) == 2
    assert rows[0]["id"] == "1"
    assert json.loads(rows[0]["payload"]) == {"a": 1, "b": "x"}
    assert json.loads(rows[0]["items"]) == [1, 2, 3]
    assert last_csv_stream_route() == "native"


@pytest.mark.parametrize("converter,suffix", [(ss.to_csv, ".csv"), (ss.to_jsonl, ".jsonl")])
def test_native_file_output_bypasses_stream_wrapper(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    converter: object,
    suffix: str,
) -> None:
    """Verify native JSONL/CSV file output does not construct a PyArrow stream wrapper."""
    require_native()
    pytest.importorskip("pyarrow")
    from schema_sanitizer.api_impl import stream_writer_core

    class FailingStream:
        """Fail if the legacy stream-wrapper path is used."""

        def __init__(self, *_args: object, **_kwargs: object) -> None:
            """Reject construction."""
            raise AssertionError("native file output should bypass Stream(raw)")

    monkeypatch.setattr(stream_writer_core, "Stream", FailingStream)
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
    """Verify native CSV writer stats avoid Python output-file recounting."""
    require_native()
    pytest.importorskip("pyarrow")

    def fail_reader(*_args: object, **_kwargs: object) -> object:
        """Reject the legacy CSV diagnostics recount path."""
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
    """Verify native JSONL writer stats avoid Python output-file recounting."""
    require_native()
    pytest.importorskip("pyarrow")

    source = _write_csv(tmp_path / "rows.csv", "a,b\n1,2\n3,4\n")
    out = tmp_path / "out.jsonl"
    real_open = builtins.open

    def guarded_open(file: object, mode: str = "r", *args: object, **kwargs: object) -> object:
        """Reject the legacy JSONL diagnostics recount path."""
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
    """Verify single-file generated metadata is present before file output runs."""
    require_native()
    pytest.importorskip("pyarrow")
    from schema_sanitizer.api_impl import stream_writer_core

    real_write = stream_writer_core.try_write_raw_native_file_output
    seen: list[dict[str, object]] = []

    def tracking_write(*args: object, **kwargs: object) -> bool:
        """Record raw file-output metadata arguments."""
        seen.append(dict(kwargs))
        return real_write(*args, **kwargs)

    monkeypatch.setattr(stream_writer_core, "try_write_raw_native_file_output", tracking_write)
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
    """Verify analytical single-file conversion does not use Python metadata wrapping."""
    require_native()
    pytest.importorskip("pyarrow")
    from schema_sanitizer.api_impl import analytical_core

    assert not hasattr(analytical_core, "prepare_metadata_stream")
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


def test_file_sinks_use_shared_output_metadata_planner() -> None:
    """Verify format sinks do not own metadata-stream preparation."""
    from schema_sanitizer import adapters
    from schema_sanitizer.adapters import (
        pyarrow_csv_sink,
        pyarrow_jsonl_sink,
        pyarrow_parquet_sink,
    )

    assert not hasattr(pyarrow_csv_sink, "prepare_metadata_stream")
    assert not hasattr(pyarrow_jsonl_sink, "prepare_metadata_stream")
    assert not hasattr(pyarrow_parquet_sink, "prepare_metadata_stream")
    assert not Path(adapters.__file__).with_name("pyarrow_metadata_streams.py").exists()


def test_pyarrow_adapter_facade_is_removed() -> None:
    """Verify PyArrow helpers are imported from focused modules."""
    from schema_sanitizer import adapters
    from schema_sanitizer.adapters import pyarrow_streams
    from schema_sanitizer.api_impl import direct_native_file_output, native_file_output

    assert not Path(adapters.__file__).with_name("pyarrow.py").exists()
    assert not Path(adapters.__file__).with_name("pyarrow_parquet.py").exists()
    assert not hasattr(pyarrow_streams, "write_csv_stream")
    assert not hasattr(pyarrow_streams, "write_jsonl_stream")
    assert not hasattr(pyarrow_streams, "write_parquet_stream")
    assert hasattr(direct_native_file_output, "try_write_parquet_direct_native")
    assert not hasattr(native_file_output, "PARQUET_STREAM_WRITE")
    assert not hasattr(native_file_output, "try_write_parquet_direct_native")


def test_binary_input_routing_has_no_adapter_fallback_name() -> None:
    """Verify binary input routing exposes direct-native semantics."""
    from schema_sanitizer.api_impl import ingest_runtime_binary

    assert hasattr(ingest_runtime_binary, "reject_unsupported_binary_direct_input")
    assert not hasattr(ingest_runtime_binary, "_maybe_route_binary_formats_via_adapter")


def test_registry_source_sink_accepts_native_row_span_metadata() -> None:
    """Verify source-selected registry sinks can append native row-span metadata."""
    require_native()
    pa = pytest.importorskip("pyarrow")
    from schema_sanitizer.api_impl.context import ExecutionContext
    from schema_sanitizer.api_impl.ingest_lifecycle import _close_suppressing_errors
    from schema_sanitizer.api_impl.ingest_runtime_types import Stream

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


def test_jsonl_native_file_output_writes_metadata_without_pyarrow_sink(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Verify native JSONL output composes metadata injection without PyArrow sink fallback."""
    require_native()
    pa = pytest.importorskip("pyarrow")
    from schema_sanitizer.adapters.pyarrow_jsonl_sink import last_jsonl_stream_route
    from schema_sanitizer.api_impl import native_file_output
    from schema_sanitizer.api_impl.file_output_metadata import last_metadata_route

    def fail_pyarrow_sink(*_args: object, **_kwargs: object) -> None:
        """Fail when the JSONL PyArrow sink fallback is called."""
        raise AssertionError("PyArrow sink fallback should not be used")

    monkeypatch.setattr(native_file_output, "_write_jsonl_stream", fail_pyarrow_sink)
    batch = pa.record_batch({"a": pa.array(["1", "2"])})
    stream = pa.RecordBatchReader.from_batches(batch.schema, [batch])
    out = tmp_path / "direct.jsonl"

    native_file_output.write_jsonl_native_first_stream(
        stream,
        out,
        feature="test_jsonl_native_file_output_writes_metadata_without_pyarrow_sink",
        first_row_columns={"schema_registry": "{}"},
        all_row_columns={"source_file": "/tmp/source.jsonl"},
    )

    rows = [json.loads(line) for line in out.read_text(encoding="utf-8").splitlines()]
    assert rows == [
        {"a": "1", "schema_registry": "{}", "source_file": "/tmp/source.jsonl"},
        {"a": "2", "schema_registry": None, "source_file": "/tmp/source.jsonl"},
    ]
    assert last_jsonl_stream_route() == "native"
    assert last_metadata_route() == "native"


def test_csv_native_file_output_writes_metadata_without_pyarrow_sink(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Verify native CSV output composes metadata injection without PyArrow sink fallback."""
    require_native()
    pa = pytest.importorskip("pyarrow")
    from schema_sanitizer.adapters.pyarrow_csv_sink import last_csv_stream_route
    from schema_sanitizer.api_impl import native_file_output
    from schema_sanitizer.api_impl.file_output_metadata import last_metadata_route

    def fail_pyarrow_sink(*_args: object, **_kwargs: object) -> None:
        """Fail when the CSV PyArrow sink fallback is called."""
        raise AssertionError("PyArrow sink fallback should not be used")

    monkeypatch.setattr(native_file_output, "_write_csv_stream", fail_pyarrow_sink)
    batch = pa.record_batch({"a": pa.array(["1", "2"])})
    stream = pa.RecordBatchReader.from_batches(batch.schema, [batch])
    out = tmp_path / "direct.csv"

    native_file_output.write_csv_native_first_stream(
        stream,
        out,
        feature="test_csv_native_file_output_writes_metadata_without_pyarrow_sink",
        first_row_columns={"schema_registry": "{}"},
        all_row_columns={"source_file": "/tmp/source.csv"},
    )

    with out.open("r", encoding="utf-8", newline="") as f:
        rows = list(csv.DictReader(f))
    assert rows == [
        {"a": "1", "schema_registry": "{}", "source_file": "/tmp/source.csv"},
        {"a": "2", "schema_registry": "", "source_file": "/tmp/source.csv"},
    ]
    assert last_csv_stream_route() == "native"
    assert last_metadata_route() == "native"


def test_parquet_native_file_output_uses_native_writer_when_available(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Verify Parquet output prefers the native writer when one is exported."""
    require_native()
    pa = pytest.importorskip("pyarrow")
    from schema_sanitizer.api_impl import direct_native_file_output, native_file_output
    from schema_sanitizer.api_impl.file_output_metadata import last_metadata_route

    def fake_native_write(stream: object, output_path: str) -> None:
        """Write a marker file through the fake native Parquet writer."""
        assert hasattr(stream, "__arrow_c_stream__")
        Path(output_path).write_bytes(b"native-parquet")

    def fail_pyarrow_sink(*_args: object, **_kwargs: object) -> None:
        """Fail when the PyArrow Parquet sink fallback is called."""
        raise AssertionError("PyArrow sink fallback should not be used")

    monkeypatch.setattr(
        direct_native_file_output.PARQUET_STREAM_WRITE,
        "get",
        lambda: fake_native_write,
    )
    monkeypatch.setenv("SCHEMA_SANITIZER_NATIVE_PARQUET_COMPRESSION", "uncompressed")
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
    from schema_sanitizer.api_impl import direct_native_file_output, native_file_output

    fallback_calls: list[Path] = []

    def fake_native_write(_stream: object, _output_path: str) -> None:
        """Simulate a native build without zlib."""
        raise RuntimeError(
            "native Parquet writer: gzip compression requested but zlib is not available"
        )

    def fake_pyarrow_sink(_stream: object, output_path: Path, **_kwargs: object) -> None:
        """Record PyArrow fallback and write a marker file."""
        fallback_calls.append(output_path)
        output_path.write_bytes(b"pyarrow-parquet")

    monkeypatch.setattr(
        direct_native_file_output.PARQUET_STREAM_WRITE,
        "get",
        lambda: fake_native_write,
    )
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
    from schema_sanitizer.api_impl import direct_native_file_output, native_file_output

    def failing_native_write(stream: object, _output_path: str) -> None:
        """Consume part of the stream before simulating a native writer bug."""
        assert hasattr(stream, "read_next_batch")
        stream.read_next_batch()
        raise RuntimeError("native Parquet writer: simulated fatal bug")

    monkeypatch.setattr(
        direct_native_file_output.PARQUET_STREAM_WRITE,
        "get",
        lambda: failing_native_write,
    )
    caplog.set_level(logging.ERROR, logger="schema_sanitizer.api_impl.native_file_output")
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
    from schema_sanitizer.api_impl import direct_native_file_output, native_file_output
    from schema_sanitizer.api_impl.stream_writer_core import write_raw_stream_to_file

    def failing_native_write(stream: object, _output_path: str) -> None:
        """Fail after reading one batch from each native attempt."""
        assert hasattr(stream, "read_next_batch")
        stream.read_next_batch()
        raise RuntimeError("native Parquet writer: simulated raw fatal bug")

    monkeypatch.setattr(
        direct_native_file_output.PARQUET_STREAM_WRITE,
        "get",
        lambda: failing_native_write,
    )
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
    from schema_sanitizer.api_impl import direct_native_file_output, native_file_output
    from schema_sanitizer.api_impl.file_output_metadata import last_metadata_route

    captured: dict[str, object] = {}

    def fake_native_write(
        stream: object,
        output_path: str,
        first_row_columns: dict[str, object],
        all_row_columns: dict[str, object],
        row_span_columns: dict[str, list[tuple[int, str | None]]],
        timestamp_columns: tuple[str, ...],
    ) -> None:
        """Write a marker file through the fake native metadata Parquet writer."""
        assert hasattr(stream, "__arrow_c_stream__")
        captured["first_row_columns"] = first_row_columns
        captured["all_row_columns"] = all_row_columns
        captured["row_span_columns"] = row_span_columns
        captured["timestamp_columns"] = timestamp_columns
        Path(output_path).write_bytes(b"native-parquet-metadata")

    def fail_pyarrow_sink(*_args: object, **_kwargs: object) -> None:
        """Fail when the PyArrow Parquet sink fallback is called."""
        raise AssertionError("PyArrow sink fallback should not be used")

    monkeypatch.setattr(
        direct_native_file_output.PARQUET_STREAM_WRITE_WITH_METADATA,
        "get",
        lambda: fake_native_write,
    )
    monkeypatch.setenv("SCHEMA_SANITIZER_NATIVE_PARQUET_COMPRESSION", "uncompressed")
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
    from schema_sanitizer.api_impl import native_file_output
    from schema_sanitizer.api_impl.file_output_metadata import last_metadata_route

    def fail_pyarrow_sink(*_args: object, **_kwargs: object) -> None:
        """Fail when the PyArrow Parquet sink fallback is called."""
        raise AssertionError("PyArrow sink fallback should not be used")

    monkeypatch.setenv("SCHEMA_SANITIZER_NATIVE_PARQUET_COMPRESSION", "uncompressed")
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


def test_parquet_native_file_output_writes_float_statistics_without_nan_bounds(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Verify native Parquet float stats skip NaN values."""
    require_native()
    pa = pytest.importorskip("pyarrow")
    pq = pytest.importorskip("pyarrow.parquet")
    from schema_sanitizer.api_impl import native_file_output

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
    from schema_sanitizer.api_impl import native_file_output

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
    from schema_sanitizer.api_impl import native_file_output

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
    from schema_sanitizer.api_impl import native_file_output

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
    from schema_sanitizer.api_impl import native_file_output

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
    from schema_sanitizer.api_impl import native_file_output

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
    from schema_sanitizer.api_impl import native_file_output

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


def test_parquet_native_file_output_rejects_unknown_compression(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Verify unknown native compression settings do not fall back to PyArrow."""
    require_native()
    pa = pytest.importorskip("pyarrow")
    from schema_sanitizer.api_impl import native_file_output

    monkeypatch.setenv("SCHEMA_SANITIZER_NATIVE_PARQUET_COMPRESSION", "snappy")
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
            source, tmp_path / "out.parquet", input_format="jsonl", parquet_compression="snappy"
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
    from schema_sanitizer.api_impl import direct_native_file_output, stream_writer_core

    monkeypatch.setattr(
        stream_writer_core,
        "try_write_raw_native_file_output",
        lambda *_args, **_kwargs: False,
    )
    monkeypatch.setattr(
        direct_native_file_output,
        "try_write_parquet_direct_native",
        lambda *_args, **_kwargs: False,
    )
    source = tmp_path / "rows.jsonl"
    source.write_text('{"text":"same"}\n{"text":"same"}\n', encoding="utf-8")
    out = tmp_path / "pyarrow-uncompressed.parquet"

    ss.to_parquet(source, out, input_format="jsonl", parquet_compression="uncompressed")

    parquet_file = pq.ParquetFile(out)
    assert parquet_file.metadata.row_group(0).column(0).compression == "UNCOMPRESSED"


def test_parquet_native_file_output_dictionary_encodes_repeated_byte_arrays(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Verify repeated string/binary values use Parquet dictionary encoding."""
    require_native()
    pa = pytest.importorskip("pyarrow")
    pq = pytest.importorskip("pyarrow.parquet")
    from schema_sanitizer.api_impl import native_file_output

    def fail_pyarrow_sink(*_args: object, **_kwargs: object) -> None:
        """Fail when the PyArrow Parquet sink fallback is called."""
        raise AssertionError("PyArrow sink fallback should not be used")

    monkeypatch.setenv("SCHEMA_SANITIZER_NATIVE_PARQUET_COMPRESSION", "uncompressed")
    monkeypatch.setattr(native_file_output, "_write_parquet_stream", fail_pyarrow_sink)
    batch = pa.record_batch(
        {
            "name": pa.array(["alpha", "beta", "alpha", None, "alpha"], type=pa.string()),
            "payload": pa.array([b"x", b"x", None, b"y", b"x"], type=pa.binary()),
            "unique": pa.array(["a", "b", "c", "d", "e"], type=pa.string()),
        }
    )
    stream = pa.RecordBatchReader.from_batches(batch.schema, [batch])
    out = tmp_path / "dictionary-encoded.parquet"

    native_file_output.write_parquet_native_first_stream(
        stream,
        out,
        feature="test_parquet_native_file_output_dictionary_encodes_repeated_byte_arrays",
    )

    table = pq.read_table(out)
    assert table.to_pylist() == [
        {"name": "alpha", "payload": b"x", "unique": "a"},
        {"name": "beta", "payload": b"x", "unique": "b"},
        {"name": "alpha", "payload": None, "unique": "c"},
        {"name": None, "payload": b"y", "unique": "d"},
        {"name": "alpha", "payload": b"x", "unique": "e"},
    ]
    parquet_file = pq.ParquetFile(out)
    name_meta = parquet_file.metadata.row_group(0).column(0)
    payload_meta = parquet_file.metadata.row_group(0).column(1)
    unique_meta = parquet_file.metadata.row_group(0).column(2)
    assert "RLE_DICTIONARY" in name_meta.encodings
    assert name_meta.dictionary_page_offset is not None
    assert "DELTA_LENGTH_BYTE_ARRAY" in payload_meta.encodings
    assert payload_meta.dictionary_page_offset is None
    assert "RLE_DICTIONARY" not in unique_meta.encodings
    assert "DELTA_LENGTH_BYTE_ARRAY" in unique_meta.encodings
    assert unique_meta.dictionary_page_offset is None
    assert native_file_output.last_parquet_stream_route() == "native"


def test_parquet_native_file_output_preserves_null_dictionary_values(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Verify dictionary values that are null materialize as Parquet nulls."""
    require_native()
    pa = pytest.importorskip("pyarrow")
    pq = pytest.importorskip("pyarrow.parquet")
    from schema_sanitizer.api_impl import native_file_output

    def fail_pyarrow_sink(*_args: object, **_kwargs: object) -> None:
        """Fail when the PyArrow Parquet sink fallback is called."""
        raise AssertionError("PyArrow sink fallback should not be used")

    monkeypatch.setenv("SCHEMA_SANITIZER_NATIVE_PARQUET_COMPRESSION", "uncompressed")
    monkeypatch.setattr(native_file_output, "_write_parquet_stream", fail_pyarrow_sink)
    batch = pa.record_batch(
        {
            "name": pa.DictionaryArray.from_arrays(
                pa.array([0, 1, None, 0], type=pa.int8()),
                pa.array([None, "x"], type=pa.string()),
            ),
            "score": pa.DictionaryArray.from_arrays(
                pa.array([0, 1, None, 0], type=pa.int8()),
                pa.array([None, 7], type=pa.int64()),
            ),
        }
    )
    stream = pa.RecordBatchReader.from_batches(batch.schema, [batch])
    out = tmp_path / "dictionary-null-values.parquet"

    native_file_output.write_parquet_native_first_stream(
        stream,
        out,
        feature="test_parquet_native_file_output_preserves_null_dictionary_values",
    )

    assert native_file_output.last_parquet_stream_route() == "native"
    assert pq.read_table(out).to_pylist() == [
        {"name": None, "score": None},
        {"name": "x", "score": 7},
        {"name": None, "score": None},
        {"name": None, "score": None},
    ]
    parquet_file = pq.ParquetFile(out)
    assert parquet_file.metadata.row_group(0).column(0).statistics.null_count == 3
    assert parquet_file.metadata.row_group(0).column(1).statistics.null_count == 3


def test_parquet_native_file_output_accepts_coalesced_empty_byte_arrays(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Verify coalesced empty string/binary buffers write through native Parquet."""
    require_native()
    pa = pytest.importorskip("pyarrow")
    pq = pytest.importorskip("pyarrow.parquet")
    from schema_sanitizer.adapters.pyarrow_metadata_native import CapsuleArrowStream
    from schema_sanitizer.api_impl import native_file_output
    from schema_sanitizer.core_impl.native_functions import COALESCING_STREAM_WRAP

    wrap = COALESCING_STREAM_WRAP.get()
    if wrap is None:
        pytest.skip("native coalescing stream wrapper is unavailable")

    monkeypatch.setenv("SCHEMA_SANITIZER_NATIVE_PARQUET_COMPRESSION", "uncompressed")
    schema = pa.schema(
        [
            pa.field("text", pa.string()),
            pa.field("payload", pa.binary()),
        ]
    )
    batches = [
        pa.record_batch(
            {
                "text": pa.array(["", ""], type=pa.string()),
                "payload": pa.array([b"", b""], type=pa.binary()),
            },
            schema=schema,
        ),
        pa.record_batch(
            {
                "text": pa.array([""], type=pa.string()),
                "payload": pa.array([b""], type=pa.binary()),
            },
            schema=schema,
        ),
    ]
    reader = pa.RecordBatchReader.from_batches(schema, batches)
    stream = CapsuleArrowStream(wrap(reader, 1024))
    out = tmp_path / "coalesced-empty-byte-arrays.parquet"

    native_file_output.write_parquet_native_first_stream(
        stream,
        out,
        feature="test_parquet_native_file_output_accepts_coalesced_empty_byte_arrays",
    )

    assert native_file_output.last_parquet_stream_route() == "native"
    assert pq.read_table(out).to_pylist() == [
        {"text": "", "payload": b""},
        {"text": "", "payload": b""},
        {"text": "", "payload": b""},
    ]


def test_parquet_native_file_output_skips_dictionary_when_payload_is_larger(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Verify dictionary encoding is used only when it reduces value payload size."""
    require_native()
    pa = pytest.importorskip("pyarrow")
    pq = pytest.importorskip("pyarrow.parquet")
    from schema_sanitizer.api_impl import native_file_output

    def fail_pyarrow_sink(*_args: object, **_kwargs: object) -> None:
        """Fail when the PyArrow Parquet sink fallback is called."""
        raise AssertionError("PyArrow sink fallback should not be used")

    monkeypatch.setenv("SCHEMA_SANITIZER_NATIVE_PARQUET_COMPRESSION", "uncompressed")
    monkeypatch.setattr(native_file_output, "_write_parquet_stream", fail_pyarrow_sink)
    batch = pa.record_batch(
        {
            "short_text": pa.array(["a", "a", "b", "c", "d"], type=pa.string()),
            "small_int": pa.array([1, 1, 2, 3, 4], type=pa.int32()),
            "long_text": pa.array(["alphabet" * 8, "alphabet" * 8, "x", "y", "z"]),
        }
    )
    stream = pa.RecordBatchReader.from_batches(batch.schema, [batch])
    out = tmp_path / "dictionary-profit.parquet"

    native_file_output.write_parquet_native_first_stream(
        stream,
        out,
        feature="test_parquet_native_file_output_skips_dictionary_when_payload_is_larger",
    )

    assert pq.read_table(out).to_pylist() == batch.to_pylist()
    parquet_file = pq.ParquetFile(out)
    short_text_meta = parquet_file.metadata.row_group(0).column(0)
    small_int_meta = parquet_file.metadata.row_group(0).column(1)
    long_text_meta = parquet_file.metadata.row_group(0).column(2)
    assert "RLE_DICTIONARY" not in short_text_meta.encodings
    assert short_text_meta.dictionary_page_offset is None
    assert "RLE_DICTIONARY" not in small_int_meta.encodings
    assert small_int_meta.dictionary_page_offset is None
    assert "RLE_DICTIONARY" in long_text_meta.encodings
    assert long_text_meta.dictionary_page_offset is not None
    assert native_file_output.last_parquet_stream_route() == "native"


def test_parquet_native_file_output_splits_large_pages_without_dictionary(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Verify oversized column payloads are split into readable non-dictionary pages."""
    require_native()
    pa = pytest.importorskip("pyarrow")
    pq = pytest.importorskip("pyarrow.parquet")
    from schema_sanitizer.api_impl import native_file_output

    def fail_pyarrow_sink(*_args: object, **_kwargs: object) -> None:
        """Fail when the PyArrow Parquet sink fallback is called."""
        raise AssertionError("PyArrow sink fallback should not be used")

    monkeypatch.setenv("SCHEMA_SANITIZER_NATIVE_PARQUET_PAGE_BYTES", "128")
    monkeypatch.setenv("SCHEMA_SANITIZER_NATIVE_PARQUET_ROW_GROUP_BYTES", "1048576")
    monkeypatch.setenv("SCHEMA_SANITIZER_NATIVE_PARQUET_COMPRESSION", "uncompressed")
    monkeypatch.setattr(native_file_output, "_write_parquet_stream", fail_pyarrow_sink)
    value = "source-file-path/" * 16
    batch = pa.record_batch({"source_file": pa.array([value] * 20, type=pa.string())})
    stream = pa.RecordBatchReader.from_batches(batch.schema, [batch])
    out = tmp_path / "page-split.parquet"

    native_file_output.write_parquet_native_first_stream(
        stream,
        out,
        feature="test_parquet_native_file_output_splits_large_pages_without_dictionary",
    )

    assert pq.read_table(out).to_pylist() == batch.to_pylist()
    parquet_file = pq.ParquetFile(out)
    assert parquet_file.metadata.num_row_groups == 1
    metadata = parquet_file.metadata.row_group(0).column(0)
    assert "RLE_DICTIONARY" not in metadata.encodings
    assert metadata.dictionary_page_offset is None
    assert metadata.has_column_index
    assert metadata.has_offset_index
    assert native_file_output.last_parquet_stream_route() == "native"


def test_parquet_native_file_output_splits_row_groups_by_byte_budget(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Verify row groups are bounded by estimated uncompressed column bytes."""
    require_native()
    pa = pytest.importorskip("pyarrow")
    pq = pytest.importorskip("pyarrow.parquet")
    from schema_sanitizer.api_impl import native_file_output

    def fail_pyarrow_sink(*_args: object, **_kwargs: object) -> None:
        """Fail when the PyArrow Parquet sink fallback is called."""
        raise AssertionError("PyArrow sink fallback should not be used")

    monkeypatch.setenv("SCHEMA_SANITIZER_NATIVE_PARQUET_COMPRESSION", "uncompressed")
    monkeypatch.setenv("SCHEMA_SANITIZER_NATIVE_PARQUET_ROW_GROUP_ROWS", "100")
    monkeypatch.setenv("SCHEMA_SANITIZER_NATIVE_PARQUET_ROW_GROUP_BYTES", "512")
    monkeypatch.setattr(native_file_output, "_write_parquet_stream", fail_pyarrow_sink)
    rows = [f"{index:03d}-" + ("payload" * 20) for index in range(30)]
    batch = pa.record_batch({"message": pa.array(rows, type=pa.string())})
    stream = pa.RecordBatchReader.from_batches(batch.schema, [batch])
    out = tmp_path / "row-group-byte-budget.parquet"

    native_file_output.write_parquet_native_first_stream(
        stream,
        out,
        feature="test_parquet_native_file_output_splits_row_groups_by_byte_budget",
    )

    assert pq.read_table(out).to_pylist() == batch.to_pylist()
    assert pq.ParquetFile(out).metadata.num_row_groups > 1
    assert native_file_output.last_parquet_stream_route() == "native"


def test_parquet_native_file_output_skips_delta_encoding_on_int64_overflow(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Verify extreme int64 deltas fall back to a non-overflowing encoding."""
    require_native()
    pa = pytest.importorskip("pyarrow")
    pq = pytest.importorskip("pyarrow.parquet")
    from schema_sanitizer.api_impl import native_file_output

    def fail_pyarrow_sink(*_args: object, **_kwargs: object) -> None:
        """Fail when the PyArrow Parquet sink fallback is called."""
        raise AssertionError("PyArrow sink fallback should not be used")

    monkeypatch.setenv("SCHEMA_SANITIZER_NATIVE_PARQUET_COMPRESSION", "uncompressed")
    monkeypatch.setattr(native_file_output, "_write_parquet_stream", fail_pyarrow_sink)
    values = [-(2**63), 2**63 - 1, 0, None]
    batch = pa.record_batch({"value": pa.array(values, type=pa.int64())})
    stream = pa.RecordBatchReader.from_batches(batch.schema, [batch])
    out = tmp_path / "int64-delta-overflow.parquet"

    native_file_output.write_parquet_native_first_stream(
        stream,
        out,
        feature="test_parquet_native_file_output_skips_delta_encoding_on_int64_overflow",
    )

    metadata = pq.ParquetFile(out).metadata.row_group(0).column(0)
    assert "DELTA_BINARY_PACKED" not in metadata.encodings
    assert native_file_output.last_parquet_stream_route() == "native"
    assert pq.read_table(out).to_pylist() == [
        {"value": values[0]},
        {"value": values[1]},
        {"value": values[2]},
        {"value": None},
    ]


def test_parquet_native_file_output_dictionary_encodes_repeated_fixed_values(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Verify repeated fixed-width values use Parquet dictionary encoding."""
    require_native()
    pa = pytest.importorskip("pyarrow")
    pq = pytest.importorskip("pyarrow.parquet")
    from schema_sanitizer.api_impl import native_file_output

    def fail_pyarrow_sink(*_args: object, **_kwargs: object) -> None:
        """Fail when the PyArrow Parquet sink fallback is called."""
        raise AssertionError("PyArrow sink fallback should not be used")

    monkeypatch.setenv("SCHEMA_SANITIZER_NATIVE_PARQUET_COMPRESSION", "uncompressed")
    monkeypatch.setattr(native_file_output, "_write_parquet_stream", fail_pyarrow_sink)
    batch = pa.record_batch(
        {
            "id": pa.array([7, 8, 7, None, 7], type=pa.int64()),
            "score": pa.array([1.5, 2.5, 1.5, None, 1.5], type=pa.float64()),
            "amount": pa.array(
                [Decimal("1.00"), Decimal("2.00"), Decimal("1.00"), None, None],
                type=pa.decimal128(10, 2),
            ),
            "flag": pa.array([True, False, True, None, None], type=pa.bool_()),
            "unique": pa.array([1, 2, 3, 4, None], type=pa.int32()),
        }
    )
    stream = pa.RecordBatchReader.from_batches(batch.schema, [batch])
    out = tmp_path / "fixed-dictionary-encoded.parquet"

    native_file_output.write_parquet_native_first_stream(
        stream,
        out,
        feature="test_parquet_native_file_output_dictionary_encodes_repeated_fixed_values",
    )

    assert pq.read_table(out).to_pylist() == [
        {"id": 7, "score": 1.5, "amount": Decimal("1.00"), "flag": True, "unique": 1},
        {"id": 8, "score": 2.5, "amount": Decimal("2.00"), "flag": False, "unique": 2},
        {"id": 7, "score": 1.5, "amount": Decimal("1.00"), "flag": True, "unique": 3},
        {"id": None, "score": None, "amount": None, "flag": None, "unique": 4},
        {"id": 7, "score": 1.5, "amount": None, "flag": None, "unique": None},
    ]
    parquet_file = pq.ParquetFile(out)
    id_meta = parquet_file.metadata.row_group(0).column(0)
    score_meta = parquet_file.metadata.row_group(0).column(1)
    amount_meta = parquet_file.metadata.row_group(0).column(2)
    assert "DELTA_BINARY_PACKED" in id_meta.encodings
    assert id_meta.dictionary_page_offset is None
    assert "RLE_DICTIONARY" in score_meta.encodings
    assert score_meta.dictionary_page_offset is not None
    assert "RLE_DICTIONARY" in amount_meta.encodings
    assert amount_meta.dictionary_page_offset is not None
    for column_index in (3, 4):
        metadata = parquet_file.metadata.row_group(0).column(column_index)
        assert "RLE_DICTIONARY" not in metadata.encodings
        assert metadata.dictionary_page_offset is None
    assert native_file_output.last_parquet_stream_route() == "native"


def test_parquet_native_file_output_writes_integer_width_logical_types(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Verify native Parquet preserves small and unsigned integer schemas."""
    require_native()
    pa = pytest.importorskip("pyarrow")
    pq = pytest.importorskip("pyarrow.parquet")
    from schema_sanitizer.api_impl import native_file_output

    def fail_pyarrow_sink(*_args: object, **_kwargs: object) -> None:
        """Fail when the PyArrow Parquet sink fallback is called."""
        raise AssertionError("PyArrow sink fallback should not be used")

    monkeypatch.setenv("SCHEMA_SANITIZER_NATIVE_PARQUET_COMPRESSION", "uncompressed")
    monkeypatch.setattr(native_file_output, "_write_parquet_stream", fail_pyarrow_sink)
    batch = pa.record_batch(
        {
            "i8": pa.array([-5, 7, None, -5], type=pa.int8()),
            "u8": pa.array([250, 1, None, 250], type=pa.uint8()),
            "i16": pa.array([-300, 12, None, -300], type=pa.int16()),
            "u16": pa.array([65000, 2, None, 65000], type=pa.uint16()),
            "u32": pa.array([4_000_000_000, 7, None, 4_000_000_000], type=pa.uint32()),
            "u64": pa.array(
                [2**63 + 5, 9, None, 2**63 + 5],
                type=pa.uint64(),
            ),
        }
    )
    stream = pa.RecordBatchReader.from_batches(batch.schema, [batch])
    out = tmp_path / "integer-logical-types.parquet"

    native_file_output.write_parquet_native_first_stream(
        stream,
        out,
        feature="test_parquet_native_file_output_writes_integer_width_logical_types",
    )

    table = pq.read_table(out)
    assert table.schema == batch.schema
    assert table.to_pylist() == [
        {
            "i8": -5,
            "u8": 250,
            "i16": -300,
            "u16": 65000,
            "u32": 4_000_000_000,
            "u64": 2**63 + 5,
        },
        {"i8": 7, "u8": 1, "i16": 12, "u16": 2, "u32": 7, "u64": 9},
        {"i8": None, "u8": None, "i16": None, "u16": None, "u32": None, "u64": None},
        {
            "i8": -5,
            "u8": 250,
            "i16": -300,
            "u16": 65000,
            "u32": 4_000_000_000,
            "u64": 2**63 + 5,
        },
    ]
    assert native_file_output.last_parquet_stream_route() == "native"


def test_parquet_native_file_output_writes_timestamp_nanos(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Verify native Parquet writes Arrow timestamp[ns] as Parquet NANOS."""
    require_native()
    pa = pytest.importorskip("pyarrow")
    pq = pytest.importorskip("pyarrow.parquet")
    from schema_sanitizer.api_impl import native_file_output

    def fail_pyarrow_sink(*_args: object, **_kwargs: object) -> None:
        """Fail when the PyArrow Parquet sink fallback is called."""
        raise AssertionError("PyArrow sink fallback should not be used")

    monkeypatch.setenv("SCHEMA_SANITIZER_NATIVE_PARQUET_COMPRESSION", "uncompressed")
    monkeypatch.setattr(native_file_output, "_write_parquet_stream", fail_pyarrow_sink)
    batch = pa.record_batch(
        {
            "event_at": pa.array(
                [1_640_995_200_000_000_123, None, 1_640_995_200_000_000_456],
                type=pa.timestamp("ns"),
            )
        }
    )
    stream = pa.RecordBatchReader.from_batches(batch.schema, [batch])
    out = tmp_path / "timestamp-nanos.parquet"

    native_file_output.write_parquet_native_first_stream(
        stream,
        out,
        feature="test_parquet_native_file_output_writes_timestamp_nanos",
    )

    table = pq.read_table(out)
    assert table.schema == batch.schema
    assert table.to_pylist() == batch.to_pylist()
    parquet_file = pq.ParquetFile(out)
    assert "Timestamp" in str(parquet_file.schema)
    assert native_file_output.last_parquet_stream_route() == "native"


def test_parquet_native_file_output_writes_schema_sanitizer_logical_surface(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Verify native Parquet covers every schema-sanitizer logical kind."""
    require_native()
    pa = pytest.importorskip("pyarrow")
    pq = pytest.importorskip("pyarrow.parquet")
    from schema_sanitizer.api_impl import native_file_output

    def fail_pyarrow_sink(*_args: object, **_kwargs: object) -> None:
        """Fail when the PyArrow Parquet sink fallback is called."""
        raise AssertionError("PyArrow sink fallback should not be used")

    monkeypatch.setenv("SCHEMA_SANITIZER_NATIVE_PARQUET_COMPRESSION", "uncompressed")
    monkeypatch.setattr(native_file_output, "_write_parquet_stream", fail_pyarrow_sink)
    struct_type = pa.struct([pa.field("x", pa.int64()), pa.field("s", pa.string())])
    batch = pa.record_batch(
        {
            "empty": pa.array([None, None], type=pa.null()),
            "ok": pa.array([True, None], type=pa.bool_()),
            "id": pa.array([7, None], type=pa.int64()),
            "score": pa.array([1.5, None], type=pa.float64()),
            "text": pa.array(["value", None], type=pa.string()),
            "event_at": pa.array(
                [1_640_995_200_000_000, None],
                type=pa.timestamp("us"),
            ),
            "event_date": pa.array([19_723, None], type=pa.date32()),
            "clock": pa.array([3723, None], type=pa.time32("s")),
            "payload": pa.array(
                [{"x": 10, "s": "nested"}, None],
                type=struct_type,
            ),
            "items": pa.array([[1, None, 3], None], type=pa.list_(pa.int64())),
        }
    )
    stream = pa.RecordBatchReader.from_batches(batch.schema, [batch])
    out = tmp_path / "schema-sanitizer-logical-surface.parquet"

    native_file_output.write_parquet_native_first_stream(
        stream,
        out,
        feature="test_parquet_native_file_output_writes_schema_sanitizer_logical_surface",
    )

    table = pq.read_table(out)
    assert table.schema.field("empty").type == pa.null()
    assert table.schema.field("ok").type == pa.bool_()
    assert table.schema.field("id").type == pa.int64()
    assert table.schema.field("score").type == pa.float64()
    assert table.schema.field("text").type == pa.string()
    assert table.schema.field("event_at").type == pa.timestamp("us")
    assert table.schema.field("event_date").type == pa.date32()
    assert table.schema.field("clock").type == pa.time32("ms")
    assert table.schema.field("payload").type == struct_type
    assert table.schema.field("items").type == pa.list_(pa.int64())
    assert table.to_pylist() == [
        {
            "empty": None,
            "ok": True,
            "id": 7,
            "score": 1.5,
            "text": "value",
            "event_at": dt.datetime(2022, 1, 1),
            "event_date": dt.date(2024, 1, 1),
            "clock": dt.time(1, 2, 3),
            "payload": {"x": 10, "s": "nested"},
            "items": [1, None, 3],
        },
        {
            "empty": None,
            "ok": None,
            "id": None,
            "score": None,
            "text": None,
            "event_at": None,
            "event_date": None,
            "clock": None,
            "payload": None,
            "items": None,
        },
    ]
    parquet_schema = str(pq.ParquetFile(out).schema)
    assert "Null" in parquet_schema
    assert "Time(isAdjustedToUTC=false, timeUnit=milliseconds)" in parquet_schema
    assert native_file_output.last_parquet_stream_route() == "native"


def test_parquet_native_file_output_preserves_sliced_batch_offsets(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Verify sliced Arrow batches preserve nested values."""
    require_native()
    pa = pytest.importorskip("pyarrow")
    pq = pytest.importorskip("pyarrow.parquet")
    from schema_sanitizer.api_impl import native_file_output

    def fail_pyarrow_sink(*_args: object, **_kwargs: object) -> None:
        """Fail when the PyArrow Parquet sink fallback is called."""
        raise AssertionError("PyArrow sink fallback should not be used")

    monkeypatch.setenv("SCHEMA_SANITIZER_NATIVE_PARQUET_COMPRESSION", "uncompressed")
    monkeypatch.setattr(native_file_output, "_write_parquet_stream", fail_pyarrow_sink)
    schema = pa.schema(
        [
            pa.field("id", pa.int64()),
            pa.field("name", pa.string()),
            pa.field("scores", pa.list_(pa.int64())),
            pa.field("fixed", pa.list_(pa.int64(), 2)),
            pa.field(
                "payload",
                pa.struct([pa.field("x", pa.int64()), pa.field("s", pa.string())]),
            ),
        ]
    )
    rows = [
        {
            "id": 0,
            "name": "zero",
            "scores": [0],
            "fixed": [0, 10],
            "payload": {"x": 0, "s": "zero"},
        },
        {
            "id": 1,
            "name": "one",
            "scores": [1, 2],
            "fixed": [1, 11],
            "payload": {"x": 1, "s": "one"},
        },
        {
            "id": 2,
            "name": None,
            "scores": [],
            "fixed": [None, 12],
            "payload": None,
        },
        {
            "id": 3,
            "name": "three",
            "scores": None,
            "fixed": [3, None],
            "payload": {"x": None, "s": "three"},
        },
    ]
    table = pa.Table.from_pylist(rows, schema=schema)
    batch = table.slice(1, 2).to_batches()[0]
    stream = pa.RecordBatchReader.from_batches(batch.schema, [batch])
    out = tmp_path / "sliced.parquet"

    native_file_output.write_parquet_native_first_stream(
        stream,
        out,
        feature="test_parquet_native_file_output_preserves_sliced_batch_offsets",
    )

    assert pq.read_table(out).to_pylist() == rows[1:3]
    assert native_file_output.last_parquet_stream_route() == "native"


def test_parquet_native_file_output_is_duckdb_readable_across_row_groups(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Verify DuckDB can scan native Parquet output with nested row groups."""
    require_native()
    duckdb = pytest.importorskip("duckdb")
    pa = pytest.importorskip("pyarrow")
    from schema_sanitizer.api_impl import native_file_output

    def fail_pyarrow_sink(*_args: object, **_kwargs: object) -> None:
        """Fail when the PyArrow Parquet sink fallback is called."""
        raise AssertionError("PyArrow sink fallback should not be used")

    monkeypatch.setenv("SCHEMA_SANITIZER_NATIVE_PARQUET_ROW_GROUP_ROWS", "2")
    monkeypatch.setenv("SCHEMA_SANITIZER_NATIVE_PARQUET_COMPRESSION", "uncompressed")
    monkeypatch.setattr(native_file_output, "_write_parquet_stream", fail_pyarrow_sink)
    schema = pa.schema(
        [
            pa.field("id", pa.int64()),
            pa.field("name", pa.string()),
            pa.field(
                "payload",
                pa.struct([pa.field("x", pa.int64()), pa.field("s", pa.string())]),
            ),
            pa.field("scores", pa.list_(pa.int64())),
        ]
    )
    rows = [
        {
            "id": index,
            "name": f"name-{index}",
            "payload": {"x": index * 10, "s": f"payload-{index}"},
            "scores": [index, index + 1],
        }
        for index in range(5)
    ]
    batch = pa.Table.from_pylist(rows, schema=schema).to_batches()[0]
    stream = pa.RecordBatchReader.from_batches(batch.schema, [batch])
    out = tmp_path / "duckdb-readable.parquet"

    native_file_output.write_parquet_native_first_stream(
        stream,
        out,
        feature="test_parquet_native_file_output_is_duckdb_readable_across_row_groups",
    )

    with duckdb.connect() as connection:
        assert connection.execute(
            """
            SELECT count(*), sum(id), max(payload.x), sum(list_sum(scores))
            FROM read_parquet(?)
            """,
            [str(out)],
        ).fetchone() == (5, 10, 40, 25)
        assert connection.execute(
            """
            SELECT payload.s
            FROM read_parquet(?)
            WHERE id = 3
            """,
            [str(out)],
        ).fetchone() == ("payload-3",)
    assert native_file_output.last_parquet_stream_route() == "native"


def test_parquet_native_file_output_writes_nested_stream(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Verify the real native Parquet writer produces readable nested output."""
    require_native()
    pa = pytest.importorskip("pyarrow")
    pq = pytest.importorskip("pyarrow.parquet")
    from schema_sanitizer.api_impl import native_file_output

    def fail_pyarrow_sink(*_args: object, **_kwargs: object) -> None:
        """Fail when the PyArrow Parquet sink fallback is called."""
        raise AssertionError("PyArrow sink fallback should not be used")

    monkeypatch.setenv("SCHEMA_SANITIZER_NATIVE_PARQUET_COMPRESSION", "uncompressed")
    monkeypatch.setattr(native_file_output, "_write_parquet_stream", fail_pyarrow_sink)
    schema = pa.schema(
        [
            pa.field(
                "payload",
                pa.struct([pa.field("id", pa.int64()), pa.field("name", pa.string())]),
            ),
            pa.field("scores", pa.list_(pa.int64())),
            pa.field("large_scores", pa.large_list(pa.int64())),
            pa.field(
                "items",
                pa.list_(
                    pa.struct(
                        [
                            pa.field("score", pa.int64()),
                            pa.field("amount", pa.decimal128(10, 2)),
                            pa.field("label", pa.string()),
                            pa.field("flags", pa.list_(pa.bool_())),
                        ]
                    )
                ),
            ),
            pa.field("matrix", pa.list_(pa.list_(pa.int64()))),
        ]
    )
    rows = [
        {
            "payload": {"id": 1, "name": "one"},
            "scores": [1, 2],
            "large_scores": [100, 200],
            "items": [
                {
                    "score": 10,
                    "amount": Decimal("12.34"),
                    "label": "a",
                    "flags": [True, False],
                }
            ],
            "matrix": [[1, 2], [], None, [3]],
        },
        {"payload": None, "scores": [], "large_scores": [], "items": [], "matrix": []},
        {
            "payload": {"id": None, "name": None},
            "scores": None,
            "large_scores": None,
            "items": None,
            "matrix": None,
        },
        {
            "payload": {"id": 4, "name": None},
            "scores": [None, 5],
            "large_scores": [None, 500],
            "items": [
                None,
                {"score": None, "amount": None, "label": "b", "flags": []},
                {
                    "score": 7,
                    "amount": Decimal("-0.01"),
                    "label": None,
                    "flags": None,
                },
            ],
            "matrix": [None, [None, 5], []],
        },
    ]
    batch = pa.Table.from_pylist(rows, schema=schema).to_batches()[0]
    stream = pa.RecordBatchReader.from_batches(batch.schema, [batch])
    out = tmp_path / "nested.parquet"

    native_file_output.write_parquet_native_first_stream(
        stream,
        out,
        feature="test_parquet_native_file_output_writes_nested_stream",
    )

    assert pq.read_table(out).to_pylist() == rows
    assert native_file_output.last_parquet_stream_route() == "native"


def test_parquet_native_file_output_writes_dictionary_stream(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Verify the real native Parquet writer decodes dictionary columns."""
    require_native()
    pa = pytest.importorskip("pyarrow")
    pq = pytest.importorskip("pyarrow.parquet")
    from schema_sanitizer.api_impl import native_file_output

    def fail_pyarrow_sink(*_args: object, **_kwargs: object) -> None:
        """Fail when the PyArrow Parquet sink fallback is called."""
        raise AssertionError("PyArrow sink fallback should not be used")

    monkeypatch.setenv("SCHEMA_SANITIZER_NATIVE_PARQUET_COMPRESSION", "uncompressed")
    monkeypatch.setattr(native_file_output, "_write_parquet_stream", fail_pyarrow_sink)
    batch = pa.record_batch(
        {
            "name": pa.array(
                ["one", "two", "one", None],
                type=pa.dictionary(pa.int8(), pa.string()),
            ),
            "flag": pa.array(
                [True, False, True, None],
                type=pa.dictionary(pa.int8(), pa.bool_()),
            ),
        }
    )
    stream = pa.RecordBatchReader.from_batches(batch.schema, [batch])
    out = tmp_path / "dictionary.parquet"

    native_file_output.write_parquet_native_first_stream(
        stream,
        out,
        feature="test_parquet_native_file_output_writes_dictionary_stream",
    )

    assert pq.read_table(out).to_pylist() == [
        {"name": "one", "flag": True},
        {"name": "two", "flag": False},
        {"name": "one", "flag": True},
        {"name": None, "flag": None},
    ]
    assert native_file_output.last_parquet_stream_route() == "native"


def test_parquet_native_file_output_writes_map_stream(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Verify the real native Parquet writer produces readable map output."""
    require_native()
    pa = pytest.importorskip("pyarrow")
    pq = pytest.importorskip("pyarrow.parquet")
    from schema_sanitizer.api_impl import native_file_output

    def fail_pyarrow_sink(*_args: object, **_kwargs: object) -> None:
        """Fail when the PyArrow Parquet sink fallback is called."""
        raise AssertionError("PyArrow sink fallback should not be used")

    monkeypatch.setenv("SCHEMA_SANITIZER_NATIVE_PARQUET_COMPRESSION", "uncompressed")
    monkeypatch.setattr(native_file_output, "_write_parquet_stream", fail_pyarrow_sink)
    schema = pa.schema(
        [
            pa.field("attrs", pa.map_(pa.string(), pa.int64())),
            pa.field("series", pa.map_(pa.string(), pa.list_(pa.int64()))),
        ]
    )
    rows = [
        {
            "attrs": [("one", 1), ("two", None)],
            "series": [("a", [1, 2]), ("b", []), ("c", None)],
        },
        {"attrs": [], "series": []},
        {"attrs": None, "series": None},
        {"attrs": [("negative", -3)], "series": [("x", [None, 5])]},
    ]
    batch = pa.record_batch(
        [
            pa.array([row["attrs"] for row in rows], type=schema.field("attrs").type),
            pa.array([row["series"] for row in rows], type=schema.field("series").type),
        ],
        schema=schema,
    )
    stream = pa.RecordBatchReader.from_batches(batch.schema, [batch])
    out = tmp_path / "map.parquet"

    native_file_output.write_parquet_native_first_stream(
        stream,
        out,
        feature="test_parquet_native_file_output_writes_map_stream",
    )

    assert pq.read_table(out).to_pylist() == rows
    assert native_file_output.last_parquet_stream_route() == "native"


def test_parquet_native_file_output_writes_fixed_size_list_stream(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Verify the real native Parquet writer handles fixed-size lists."""
    require_native()
    pa = pytest.importorskip("pyarrow")
    pq = pytest.importorskip("pyarrow.parquet")
    from schema_sanitizer.api_impl import native_file_output

    def fail_pyarrow_sink(*_args: object, **_kwargs: object) -> None:
        """Fail when the PyArrow Parquet sink fallback is called."""
        raise AssertionError("PyArrow sink fallback should not be used")

    monkeypatch.setenv("SCHEMA_SANITIZER_NATIVE_PARQUET_COMPRESSION", "uncompressed")
    monkeypatch.setattr(native_file_output, "_write_parquet_stream", fail_pyarrow_sink)
    schema = pa.schema(
        [
            pa.field("vec", pa.list_(pa.int64(), 2)),
            pa.field(
                "items",
                pa.list_(pa.struct([pa.field("x", pa.int64())]), 2),
            ),
            pa.field("empty", pa.list_(pa.int64(), 0)),
        ]
    )
    rows = [
        {"vec": [1, 2], "items": [{"x": 1}, {"x": None}], "empty": []},
        {"vec": [None, 4], "items": [None, {"x": 2}], "empty": []},
        {"vec": None, "items": None, "empty": None},
    ]
    batch = pa.Table.from_pylist(rows, schema=schema).to_batches()[0]
    stream = pa.RecordBatchReader.from_batches(batch.schema, [batch])
    out = tmp_path / "fixed-size-list.parquet"

    native_file_output.write_parquet_native_first_stream(
        stream,
        out,
        feature="test_parquet_native_file_output_writes_fixed_size_list_stream",
    )

    assert pq.read_table(out).to_pylist() == rows
    assert native_file_output.last_parquet_stream_route() == "native"


def test_parquet_native_file_output_writes_generated_metadata(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Verify the real native Parquet writer handles metadata-wrapped streams."""
    require_native()
    pa = pytest.importorskip("pyarrow")
    pq = pytest.importorskip("pyarrow.parquet")
    from schema_sanitizer.api_impl import native_file_output
    from schema_sanitizer.api_impl.file_output_metadata import last_metadata_route

    def fail_pyarrow_sink(*_args: object, **_kwargs: object) -> None:
        """Fail when the PyArrow Parquet sink fallback is called."""
        raise AssertionError("PyArrow sink fallback should not be used")

    monkeypatch.setenv("SCHEMA_SANITIZER_NATIVE_PARQUET_COMPRESSION", "uncompressed")
    monkeypatch.setattr(native_file_output, "_write_parquet_stream", fail_pyarrow_sink)
    batch = pa.record_batch({"id": pa.array([1, 2], type=pa.int64())})
    stream = pa.RecordBatchReader.from_batches(batch.schema, [batch])
    out = tmp_path / "native-metadata.parquet"

    native_file_output.write_parquet_native_first_stream(
        stream,
        out,
        feature="test_parquet_native_file_output_writes_generated_metadata",
        first_row_columns={"schema_registry": "{}"},
        all_row_columns={"source_file": "/tmp/source.jsonl"},
        timestamp_columns=("ingestion_timestamp",),
    )

    table = pq.read_table(out)
    timestamp_type = table.schema.field("ingestion_timestamp").type
    assert pa.types.is_timestamp(timestamp_type)
    assert timestamp_type.unit == "us"
    rows = table.to_pylist()
    assert rows[0]["id"] == 1
    assert rows[0]["schema_registry"] == "{}"
    assert rows[0]["source_file"] == "/tmp/source.jsonl"
    assert rows[0]["ingestion_timestamp"] is not None
    assert rows[1]["id"] == 2
    assert rows[1]["schema_registry"] is None
    assert rows[1]["source_file"] == "/tmp/source.jsonl"
    assert rows[1]["ingestion_timestamp"] is not None
    assert native_file_output.last_parquet_stream_route() == "native"
    assert last_metadata_route() == "native"


def test_native_jsonl_schema_support_check_uses_native_parser() -> None:
    """Verify JSONL native schema support follows the C++ schema parser."""
    require_native()
    pa = pytest.importorskip("pyarrow")

    supported = pa.schema(
        [
            pa.field("id", pa.int64()),
            pa.field("payload", pa.struct([pa.field("name", pa.string())])),
            pa.field("items", pa.list_(pa.int32())),
            pa.field("amount", pa.decimal128(10, 2)),
            pa.field("half", pa.float16()),
            pa.field("elapsed", pa.duration("us")),
            pa.field("fixed", pa.list_(pa.field("item", pa.int32()), 2)),
            pa.field("coded", pa.dictionary(pa.int8(), pa.string())),
        ]
    )
    unsupported = pa.schema([pa.field("union_value", pa.dense_union([pa.field("a", pa.int8())]))])

    assert _schema_supports_native_jsonl(supported, pa=pa) is True
    assert _schema_supports_native_jsonl(unsupported, pa=pa) is False


def test_to_jsonl_native_writes_float16(tmp_path: Path) -> None:
    """Verify native JSONL writer supports Arrow float16 batches."""
    require_native()
    pa = pytest.importorskip("pyarrow")
    from schema_sanitizer.adapters.pyarrow_jsonl_sink import write_jsonl_stream

    batch = pa.record_batch({"half": pa.array([1.5], type=pa.float16())})
    reader = pa.RecordBatchReader.from_batches(batch.schema, [batch])
    out = tmp_path / "out.jsonl"

    write_jsonl_stream(reader, out, feature="to_jsonl")

    row = json.loads(out.read_text(encoding="utf-8").strip())
    assert row == {"half": 1.5}


def test_jsonl_stream_requires_native_writer(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Verify JSONL stream output does not fall back to Python row serialization."""
    pa = pytest.importorskip("pyarrow")
    from schema_sanitizer.adapters import pyarrow_jsonl_sink

    batch = pa.record_batch({"a": pa.array([1])})
    reader = pa.RecordBatchReader.from_batches(batch.schema, [batch])
    out = tmp_path / "out.jsonl"

    monkeypatch.setattr(pyarrow_jsonl_sink, "_native_jsonl_write_func", lambda: None)
    with pytest.raises(RuntimeError, match=r"native C\+\+ JSONL stream writer"):
        pyarrow_jsonl_sink.write_jsonl_stream(reader, out, feature="to_jsonl")

    assert not out.exists()


def test_csv_stream_requires_native_nested_renderer(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Verify nested CSV output does not fall back to Python value rendering."""
    pa = pytest.importorskip("pyarrow")
    from schema_sanitizer.adapters import pyarrow_csv_sink

    batch = pa.record_batch({"id": pa.array([1]), "items": pa.array([[1, 2]])})
    reader = pa.RecordBatchReader.from_batches(batch.schema, [batch])
    out = tmp_path / "out.csv"

    monkeypatch.setattr(pyarrow_csv_sink, "native_csv_nested_reader", lambda _stream, *, pa: None)
    with pytest.raises(RuntimeError, match=r"native C\+\+ CSV nested renderer"):
        pyarrow_csv_sink.write_csv_stream(reader, out, feature="to_csv")

    assert not out.exists()


def test_to_jsonl_writes_file(tmp_path: Path) -> None:
    """Verify to jsonl writes file."""
    require_native()
    pytest.importorskip("pyarrow")

    out = tmp_path / "out.jsonl"
    result = ss.to_jsonl(
        _write_csv(tmp_path / "rows.csv", "a,b\n1,2\n"),
        out,
        input_format="csv",
    )
    assert isinstance(result, ss.Result)
    assert result.clean_data is None
    row = json.loads(out.read_text(encoding="utf-8").strip())
    assert _without_generated_metadata(row) == {"a": "1", "b": "2"}


def test_to_jsonl_preserves_nested_fields(tmp_path: Path) -> None:
    """Verify to jsonl preserves nested fields."""
    require_native()
    pytest.importorskip("pyarrow")

    data = [
        {"id": 1, "payload": {"a": 1, "b": "x"}, "items": [1, 2, 3]},
        {"id": 2, "payload": {"a": 2, "b": "y"}, "items": [4]},
    ]
    source = tmp_path / "nested-source.jsonl"
    source.write_text(
        "".join(json.dumps(row) + "\n" for row in data),
        encoding="utf-8",
    )
    out = tmp_path / "nested.jsonl"
    ss.to_jsonl(source, out, input_format="jsonl")

    rows = [json.loads(line) for line in out.read_text(encoding="utf-8").splitlines() if line]
    rows = [_without_generated_metadata(row) for row in rows]
    assert rows[0]["payload"] == {"a": 1, "b": "x"}
    assert rows[0]["items"] == [1, 2, 3]


def test_to_jsonl_native_temporal_rendering(tmp_path: Path) -> None:
    """Verify native JSONL renders temporal values as ISO strings."""
    require_native()
    pytest.importorskip("pyarrow")

    source = tmp_path / "rows.jsonl"
    source.write_text('{"ts":"2026-01-01T03:01:26Z"}\n', encoding="utf-8")
    out = tmp_path / "out.jsonl"

    ss.to_jsonl(
        source,
        out,
        input_format="jsonl",
        parse_iso_timestamps=True,
    )

    row = json.loads(out.read_text(encoding="utf-8").splitlines()[0])
    assert _without_generated_metadata(row) == {"ts": "2026-01-01T03:01:26"}


def test_to_jsonl_native_float_rendering(tmp_path: Path) -> None:
    """Verify native JSONL preserves useful float text precision."""
    require_native()
    pytest.importorskip("pyarrow")

    source = tmp_path / "rows.jsonl"
    source.write_text(
        '{"value":1.0}\n{"value":1.23456789012345}\n',
        encoding="utf-8",
    )
    out = tmp_path / "out.jsonl"

    ss.to_jsonl(source, out, input_format="jsonl", parse_floats=True)

    rows = [json.loads(line) for line in out.read_text(encoding="utf-8").splitlines()]
    assert [_without_generated_metadata(row) for row in rows] == [
        {"value": 1.0},
        {"value": 1.23456789012345},
    ]


def test_jsonl_writer_supports_binary_and_map_types(tmp_path: Path) -> None:
    """Verify native JSONL supports binary and map Arrow values."""
    require_native()
    pa = pytest.importorskip("pyarrow")
    from schema_sanitizer.adapters.pyarrow_jsonl_sink import write_jsonl_stream

    map_type = pa.map_(pa.string(), pa.int64())
    batch = pa.record_batch(
        [
            pa.array([b"abc"], type=pa.binary()),
            pa.array([b"wxyz"], type=pa.binary(4)),
            pa.array([[("a", 1), ("b", 2)]], type=map_type),
        ],
        names=["payload", "fixed_payload", "attrs"],
    )
    reader = pa.RecordBatchReader.from_batches(batch.schema, [batch])
    out = tmp_path / "out.jsonl"

    write_jsonl_stream(reader, out, feature="to_jsonl")

    row = json.loads(out.read_text(encoding="utf-8").strip())
    assert row == {
        "payload": "YWJj",
        "fixed_payload": "d3h5eg==",
        "attrs": [{"key": "a", "value": 1}, {"key": "b", "value": 2}],
    }


def test_jsonl_writer_supports_decimal_dictionary_duration_and_fixed_list(
    tmp_path: Path,
) -> None:
    """Verify native JSONL supports Arrow types that avoid Python fallback."""
    require_native()
    pa = pytest.importorskip("pyarrow")
    from schema_sanitizer.adapters.pyarrow_jsonl_sink import write_jsonl_stream

    batch = pa.record_batch(
        [
            pa.array([Decimal("12.34")], type=pa.decimal128(10, 2)),
            pa.array([123], type=pa.duration("us")),
            pa.array([[1, 2]], type=pa.list_(pa.field("item", pa.int32()), 2)),
            pa.array(["blue"], type=pa.dictionary(pa.int8(), pa.string())),
            pa.array([3723001], type=pa.time32("ms")),
        ],
        names=["amount", "elapsed", "fixed", "coded", "clock"],
    )
    reader = pa.RecordBatchReader.from_batches(batch.schema, [batch])
    out = tmp_path / "out.jsonl"

    write_jsonl_stream(reader, out, feature="to_jsonl")

    row = json.loads(out.read_text(encoding="utf-8").strip())
    assert row == {
        "amount": "12.34",
        "elapsed": "123us",
        "fixed": [1, 2],
        "coded": "blue",
        "clock": "01:02:03.001",
    }


def test_to_parquet_writes_file(tmp_path: Path) -> None:
    """Verify to parquet writes file."""
    require_native()
    pq = pytest.importorskip("pyarrow.parquet")

    out = tmp_path / "out.parquet"
    result = ss.to_parquet(
        _write_csv(tmp_path / "rows.csv", "a,b\n1,2\n"),
        out,
        input_format="csv",
    )
    assert isinstance(result, ss.Result)
    assert result.clean_data is None
    rows = pq.read_table(out).to_pylist()
    assert _without_generated_metadata_rows(rows) == [{"a": "1", "b": "2"}]


def test_parquet_sink_native_coalesces_flat_arrow_batches(tmp_path: Path) -> None:
    """Verify the Parquet sink uses native coalescing for supported flat streams."""
    require_native()
    pa = pytest.importorskip("pyarrow")
    pq = pytest.importorskip("pyarrow.parquet")
    from schema_sanitizer.adapters.pyarrow_parquet_sink import (
        _write_coalesced_batches,
        last_parquet_coalesce_route,
    )

    batches = [
        pa.record_batch(
            {
                "id": pa.array([index], type=pa.int64()),
                "name": pa.array([f"name-{index}"], type=pa.string()),
            }
        )
        for index in range(8)
    ]
    reader = pa.RecordBatchReader.from_batches(batches[0].schema, batches)
    out = tmp_path / "flat.parquet"
    writer = pq.ParquetWriter(out, batches[0].schema)
    try:
        _write_coalesced_batches(
            writer,
            reader,
            schema=batches[0].schema,
            pa=pa,
            row_group_rows=1024,
        )
    finally:
        writer.close()

    parquet_file = pq.ParquetFile(out)
    assert last_parquet_coalesce_route() == "native"
    assert parquet_file.metadata.num_rows == 8
    assert parquet_file.metadata.num_row_groups == 1
    assert pq.read_table(out).to_pylist() == [
        {"id": index, "name": f"name-{index}"} for index in range(8)
    ]


def test_parquet_sink_native_coalesces_nested_arrow_batches(tmp_path: Path) -> None:
    """Verify nested streams coalesce through the native path."""
    require_native()
    pa = pytest.importorskip("pyarrow")
    pq = pytest.importorskip("pyarrow.parquet")
    from schema_sanitizer.adapters.pyarrow_parquet_sink import (
        _write_coalesced_batches,
        last_parquet_coalesce_route,
    )

    payload_type = pa.struct(
        [
            pa.field("id", pa.int64()),
            pa.field("name", pa.string()),
            pa.field("flags", pa.list_(pa.bool_())),
        ]
    )
    item_type = pa.struct([pa.field("score", pa.float64()), pa.field("label", pa.string())])
    schema = pa.schema(
        [
            pa.field("payload", payload_type),
            pa.field("items", pa.list_(item_type)),
        ]
    )
    rows = [
        {
            "payload": (
                None
                if index == 5
                else {
                    "id": index,
                    "name": None if index == 2 else f"name-{index}",
                    "flags": None if index == 1 else [True, False] if index % 2 == 0 else [],
                }
            ),
            "items": (
                None
                if index == 4
                else (
                    []
                    if index % 3 == 0
                    else [
                        {"score": index + 0.5, "label": f"a-{index}"},
                        {"score": index + 1.5, "label": None if index == 7 else f"b-{index}"},
                    ]
                )
            ),
        }
        for index in range(8)
    ]
    table = pa.Table.from_pylist(rows, schema=schema)
    batches = [table.slice(index, 1).to_batches()[0] for index in range(8)]
    reader = pa.RecordBatchReader.from_batches(schema, batches)
    out = tmp_path / "nested.parquet"
    writer = pq.ParquetWriter(out, schema)
    try:
        _write_coalesced_batches(
            writer,
            reader,
            schema=schema,
            pa=pa,
            row_group_rows=1024,
        )
    finally:
        writer.close()

    parquet_file = pq.ParquetFile(out)
    assert last_parquet_coalesce_route() == "native"
    assert parquet_file.metadata.num_rows == 8
    assert parquet_file.metadata.num_row_groups == 1
    assert pq.read_table(out).to_pylist() == rows


def test_parquet_sink_native_coalesces_dictionary_arrow_batches(
    tmp_path: Path,
) -> None:
    """Verify dictionary streams coalesce through the native path."""
    require_native()
    pa = pytest.importorskip("pyarrow")
    pq = pytest.importorskip("pyarrow.parquet")
    from schema_sanitizer.adapters.pyarrow_parquet_sink import (
        _write_coalesced_batches,
        last_parquet_coalesce_route,
    )

    schema = pa.schema([pa.field("coded", pa.dictionary(pa.int8(), pa.string()))])
    dictionary = pa.array(["value-0", "value-1"], type=pa.string())
    batches = [
        pa.record_batch(
            [pa.DictionaryArray.from_arrays(pa.array([index % 2], type=pa.int8()), dictionary)],
            schema=schema,
        )
        for index in range(8)
    ]
    reader = pa.RecordBatchReader.from_batches(schema, batches)
    out = tmp_path / "dictionary.parquet"
    writer = pq.ParquetWriter(out, schema)
    try:
        _write_coalesced_batches(
            writer,
            reader,
            schema=schema,
            pa=pa,
            row_group_rows=1024,
        )
    finally:
        writer.close()

    parquet_file = pq.ParquetFile(out)
    assert last_parquet_coalesce_route() == "native"
    assert parquet_file.metadata.num_rows == 8
    assert parquet_file.metadata.num_row_groups == 1
    assert pq.read_table(out).to_pylist() == [{"coded": f"value-{index % 2}"} for index in range(8)]


def test_parquet_sink_rejects_changed_dictionary_during_native_coalescing() -> None:
    """Verify native dictionary coalescing fails before remapping unsafe indices."""
    require_native()
    pa = pytest.importorskip("pyarrow")
    from schema_sanitizer.adapters.pyarrow_parquet_sink import _write_coalesced_batches

    schema = pa.schema([pa.field("coded", pa.dictionary(pa.int8(), pa.string()))])
    batches = [
        pa.record_batch(
            [
                pa.DictionaryArray.from_arrays(
                    pa.array([0], type=pa.int8()),
                    pa.array(["a", "b"], type=pa.string()),
                )
            ],
            schema=schema,
        ),
        pa.record_batch(
            [
                pa.DictionaryArray.from_arrays(
                    pa.array([0], type=pa.int8()),
                    pa.array(["b", "a"], type=pa.string()),
                )
            ],
            schema=schema,
        ),
    ]
    reader = pa.RecordBatchReader.from_batches(schema, batches)

    class Writer:
        """Track whether unsafe dictionary coalescing writes any batch."""

        wrote = False

        def write_batch(self, _batch: object) -> None:
            """Mark unexpected writes."""
            self.wrote = True

    writer = Writer()
    with pytest.raises(Exception, match="dictionary values changed"):
        _write_coalesced_batches(writer, reader, schema=schema, pa=pa, row_group_rows=1024)

    assert writer.wrote is False


def test_to_parquet_omits_empty_container_only_fields(tmp_path: Path) -> None:
    """Verify empty objects and lists do not create inferred source columns."""
    require_native()
    pq = pytest.importorskip("pyarrow.parquet")

    source = tmp_path / "rows.jsonl"
    source.write_text(
        '{"writer":{},"items":[],"wrapper":{"child":{}},"nested_items":[{}]}\n',
        encoding="utf-8",
    )
    out = tmp_path / "out.parquet"

    ss.to_parquet(source, out, input_format="jsonl")

    table = pq.read_table(out)
    assert "writer" not in table.schema.names
    assert "items" not in table.schema.names
    assert "wrapper" not in table.schema.names
    assert "nesteditems" not in table.schema.names
    assert _without_generated_metadata_rows(table.to_pylist()) == [{}]


def test_to_parquet_writes_mixed_empty_and_populated_objects(tmp_path: Path) -> None:
    """Verify established fields materialize empty containers as null."""
    require_native()
    pa = pytest.importorskip("pyarrow")
    pq = pytest.importorskip("pyarrow.parquet")

    source = tmp_path / "rows.jsonl"
    source.write_text(
        '{"writer":{},"items":[]}\n{"writer":{"name":"Alex"},"items":[1,2]}\n',
        encoding="utf-8",
    )
    out = tmp_path / "out.parquet"

    ss.to_parquet(source, out, input_format="jsonl")

    table = pq.read_table(out)
    assert pa.types.is_struct(table.schema.field("writer").type)
    assert pa.types.is_list(table.schema.field("items").type)
    assert _without_generated_metadata_rows(table.to_pylist()) == [
        {"items": None, "writer": None},
        {"items": [1, 2], "writer": {"name": "Alex"}},
    ]


def test_registry_keeps_existing_fields_for_empty_containers(tmp_path: Path) -> None:
    """Verify empty values neither remove registered fields nor create drift."""
    require_native()
    pa = pytest.importorskip("pyarrow")
    pq = pytest.importorskip("pyarrow.parquet")

    previous = merge_schema_registry(
        inferred_schema=pa.schema(
            [
                pa.field("items", pa.list_(pa.int64())),
                pa.field("writer", pa.struct([pa.field("id", pa.int64())])),
            ]
        ),
        schema_registry=None,
        field_name_policy="lower_snake",
    )
    source = tmp_path / "rows.jsonl"
    source.write_text('{"items":[],"writer":{}}\n', encoding="utf-8")
    out = tmp_path / "out.parquet"

    result = ss.to_parquet(
        source,
        out,
        input_format="jsonl",
        schema_mode="strict",
        field_name_policy="lower_snake",
        schema_registry=previous.schema_registry,
    )

    row = _without_generated_metadata_rows(pq.read_table(out).to_pylist())[0]
    assert row == {"items": None, "writer": None}
    assert result.schema_drifts == []
    assert (
        result.schema_registry["schema_generation"] == previous.schema_registry["schema_generation"]
    )


def test_empty_first_partition_does_not_destabilize_registry(tmp_path: Path) -> None:
    """Verify later evidence gets the original name and replays stay stable."""
    require_native()
    pq = pytest.importorskip("pyarrow.parquet")

    empty_source = tmp_path / "empty.jsonl"
    empty_source.write_text('{"items":[],"writer":{}}\n', encoding="utf-8")
    empty_out = tmp_path / "empty.parquet"
    empty_result = ss.to_parquet(
        empty_source,
        empty_out,
        input_format="jsonl",
        field_name_policy="lower_snake",
    )

    populated_source = tmp_path / "populated.jsonl"
    populated_source.write_text(
        '{"items":[1],"writer":{"id":2}}\n',
        encoding="utf-8",
    )
    populated_out = tmp_path / "populated.parquet"
    populated_result = ss.to_parquet(
        populated_source,
        populated_out,
        input_format="jsonl",
        field_name_policy="lower_snake",
        schema_registry=empty_result.schema_registry,
    )

    populated_names = pq.read_table(populated_out).schema.names
    assert {"items", "writer"}.issubset(populated_names)
    assert not any(name.startswith(("items_v", "writer_v")) for name in populated_names)
    assert [drift["output_name"] for drift in populated_result.schema_drifts] == [
        "items",
        "writer",
    ]

    replay_out = tmp_path / "replay.parquet"
    replay_result = ss.to_parquet(
        empty_source,
        replay_out,
        input_format="jsonl",
        schema_mode="strict",
        field_name_policy="lower_snake",
        schema_registry=populated_result.schema_registry,
    )

    replay_row = _without_generated_metadata_rows(pq.read_table(replay_out).to_pylist())[0]
    assert replay_row == {"items": None, "writer": None}
    assert replay_result.schema_drifts == []
    assert (
        replay_result.schema_registry["schema_generation"]
        == populated_result.schema_registry["schema_generation"]
    )


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
    from schema_sanitizer.api_impl.file_output_metadata import (
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
    from schema_sanitizer.api_impl.file_output_metadata import (
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


def test_jsonl_writer_rejects_existing_schema_metadata_collision(tmp_path: Path) -> None:
    """Verify JSONL writer preflights metadata collisions before writing."""
    pa = pytest.importorskip("pyarrow")
    from schema_sanitizer.adapters.pyarrow_jsonl_sink import write_jsonl_stream

    batch = pa.record_batch([pa.array(["data"])], names=["schema_registry"])
    reader = pa.RecordBatchReader.from_batches(batch.schema, [batch])
    out = tmp_path / "out.jsonl"

    with pytest.raises(ValueError, match="already exists"):
        write_jsonl_stream(
            reader,
            out,
            feature="to_jsonl",
            first_row_columns={"schema_registry": "{}"},
        )

    assert not out.exists()


def test_to_csv_writes_file_uri(tmp_path: Path) -> None:
    """Verify to csv writes through file URI outputs."""
    require_native()
    pytest.importorskip("pyarrow")

    out = tmp_path / "out-uri.csv"
    ss.to_csv(
        _write_csv(tmp_path / "rows.csv", "a,b\n1,2\n"),
        out.as_uri(),
        input_format="csv",
    )

    with out.open("r", encoding="utf-8", newline="") as f:
        rows = list(csv.DictReader(f))
    assert _without_generated_metadata_rows(rows) == [{"a": "1", "b": "2"}]


def test_to_jsonl_writes_file_uri(tmp_path: Path) -> None:
    """Verify to jsonl writes through file URI outputs."""
    require_native()
    pytest.importorskip("pyarrow")

    out = tmp_path / "out-uri.jsonl"
    ss.to_jsonl(
        _write_csv(tmp_path / "rows.csv", "a,b\n1,2\n"),
        out.as_uri(),
        input_format="csv",
    )

    rows = [json.loads(line) for line in out.read_text(encoding="utf-8").splitlines()]
    assert _without_generated_metadata_rows(rows) == [{"a": "1", "b": "2"}]


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
