"""Tests read adapters and file-to-file converters."""

from __future__ import annotations

import csv
import json
from decimal import Decimal
from pathlib import Path

import pytest
from conftest import require_native
from sinks_shared import (
    without_generated_metadata as _without_generated_metadata,
)
from sinks_shared import (
    without_generated_metadata_rows as _without_generated_metadata_rows,
)
from sinks_shared import (
    write_csv as _write_csv,
)

import schema_sanitizer as ss
from schema_sanitizer.adapters.pyarrow.jsonl_sink import _schema_supports_native_jsonl


def test_to_csv_writes_file(tmp_path: Path) -> None:
    """Verify to csv writes file."""
    require_native()
    pytest.importorskip("pyarrow")
    from schema_sanitizer.adapters.pyarrow.csv_sink import last_csv_stream_route

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
    from schema_sanitizer.adapters.pyarrow.csv_sink import last_csv_stream_route

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


def test_jsonl_native_file_output_writes_metadata_without_pyarrow_sink(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Verify native JSONL output composes metadata injection without PyArrow sink fallback."""
    require_native()
    pa = pytest.importorskip("pyarrow")
    from schema_sanitizer.adapters.pyarrow.file_metadata import last_metadata_route
    from schema_sanitizer.adapters.pyarrow.jsonl_sink import last_jsonl_stream_route
    from schema_sanitizer.api_impl.file_conversion import writers as native_file_output

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
    from schema_sanitizer.adapters.pyarrow.csv_sink import last_csv_stream_route
    from schema_sanitizer.adapters.pyarrow.file_metadata import last_metadata_route
    from schema_sanitizer.api_impl.file_conversion import writers as native_file_output

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
    from schema_sanitizer.adapters.pyarrow.jsonl_sink import write_jsonl_stream

    batch = pa.record_batch({"half": pa.array([1.5], type=pa.float16())})
    reader = pa.RecordBatchReader.from_batches(batch.schema, [batch])
    out = tmp_path / "out.jsonl"

    write_jsonl_stream(reader, out, feature="to_jsonl")

    row = json.loads(out.read_text(encoding="utf-8").strip())
    assert row == {"half": 1.5}


def test_jsonl_stream_does_not_fall_back_after_native_failure(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Verify native JSONL failures are propagated without a Python fallback."""
    pa = pytest.importorskip("pyarrow")
    from schema_sanitizer.adapters.pyarrow import jsonl_sink as pyarrow_jsonl_sink

    batch = pa.record_batch({"a": pa.array([1])})
    reader = pa.RecordBatchReader.from_batches(batch.schema, [batch])
    out = tmp_path / "out.jsonl"

    def fail_native_writer(*_args: object) -> object:
        """Simulate a required native JSONL writer failure."""
        raise RuntimeError("native JSONL write failed")

    monkeypatch.setattr(pyarrow_jsonl_sink, "JSONL_STREAM_WRITE", fail_native_writer)
    with pytest.raises(RuntimeError, match="native JSONL write failed"):
        pyarrow_jsonl_sink.write_jsonl_stream(reader, out, feature="to_jsonl")

    assert not out.exists()


def test_csv_stream_requires_native_nested_renderer(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Verify nested CSV output does not fall back to Python value rendering."""
    pa = pytest.importorskip("pyarrow")
    from schema_sanitizer.adapters.pyarrow import csv_sink as pyarrow_csv_sink

    batch = pa.record_batch({"id": pa.array([1]), "items": pa.array([[1, 2]])})
    reader = pa.RecordBatchReader.from_batches(batch.schema, [batch])
    out = tmp_path / "out.csv"

    monkeypatch.setattr(
        pyarrow_csv_sink,
        "native_csv_nested_reader",
        lambda _stream, *, pa, memory_limit_bytes=None: None,
    )
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
    from schema_sanitizer.adapters.pyarrow.jsonl_sink import write_jsonl_stream

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
    from schema_sanitizer.adapters.pyarrow.jsonl_sink import write_jsonl_stream

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


def test_jsonl_writer_rejects_existing_schema_metadata_collision(tmp_path: Path) -> None:
    """Verify JSONL writer preflights metadata collisions before writing."""
    pa = pytest.importorskip("pyarrow")
    from schema_sanitizer.adapters.pyarrow.jsonl_sink import write_jsonl_stream

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
