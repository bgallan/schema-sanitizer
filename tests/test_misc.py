"""Miscellaneous behavior tests for the supported public API."""

from __future__ import annotations

import io
import json
from pathlib import Path

import pytest
from conftest import (
    read_test_csv,
    read_test_json,
    read_test_jsonl,
    read_test_path,
    read_test_python,
    require_native,
)

pa = pytest.importorskip("pyarrow")

import schema_sanitizer
from schema_sanitizer.api_impl.execution_context import ExecutionContext
from schema_sanitizer.core_impl.uris import (
    local_path_from_file_uri,
    local_path_or_reject_remote,
)
from schema_sanitizer.input_impl.selection import resolve_source_and_format
from schema_sanitizer.options_impl.call_options import normalize_call_options


def _read_with_contract(
    data, *, schema_contract: pa.Schema, format: str, source: str = "auto", **options
):
    """Read data through the internal registry-derived schema contract path."""
    return ExecutionContext().to_table(
        data,
        options=normalize_call_options(schema_contract=schema_contract, **options),
        format=format,
        source=source,
    )


def _csv_ref_table(text: str, schema: pa.Schema) -> pa.Table:
    """Return csv ref table for the test."""
    from pyarrow import csv as pacsv

    convert = pacsv.ConvertOptions(
        column_types={field.name: field.type for field in schema},
        strings_can_be_null=True,
    )
    read = pacsv.ReadOptions(autogenerate_column_names=False)
    return pacsv.read_csv(
        pa.py_buffer(text.encode("utf8")), read_options=read, convert_options=convert
    )


def test_local_path_uri_helpers_do_not_reject_windows_drive_paths() -> None:
    """Verify Windows drive paths are not mistaken for remote URI schemes."""
    windows_path = r"C:\temp\out.jsonl"
    file_uri_path = Path(local_path_from_file_uri("file:///tmp/out.jsonl"))

    assert local_path_or_reject_remote(windows_path, remote_error="remote") == windows_path
    assert file_uri_path.parts[-2:] == ("tmp", "out.jsonl")
    with pytest.raises(ValueError, match="remote"):
        local_path_or_reject_remote("s3://bucket/out.jsonl", remote_error="remote")


def test_local_path_from_file_uri_normalizes_windows_drive_uri(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Verify Windows file URI drive paths do not keep a leading slash."""
    from schema_sanitizer.core_impl import uris

    monkeypatch.setattr(uris.os, "name", "nt")

    assert (
        uris.local_path_from_file_uri("file:///C:/Users/runner/AppData/out%20file.parquet")
        == r"C:\Users\runner\AppData\out file.parquet"
    )


def test_file_uri_auto_format_uses_platform_path_normalization() -> None:
    """Verify file URI format detection uses native path normalization."""
    data, source, format_name = resolve_source_and_format(
        "file:///tmp/events.jsonl",
        format="auto",
        source="uri",
    )

    assert data == "file:///tmp/events.jsonl"
    assert source == "uri"
    assert format_name == "json"


def test_csv_contract_matches_pyarrow(tmp_path: Path) -> None:
    """Verify csv contract matches pyarrow."""
    csv_text = "a,b\n1,hello\n2,world\n3,\n"
    path = tmp_path / "rows.csv"
    path.write_text(csv_text, encoding="utf-8")
    schema = pa.schema([("a", pa.int64()), ("b", pa.string())])

    result = _read_with_contract(
        path, schema_contract=schema, format="csv", schema_mode="strict", parse_integers=True
    )
    assert result.clean_data.schema == schema
    assert result.clean_data.equals(_csv_ref_table(csv_text, schema))


def test_json_contract_matches_pyarrow_json_reader(tmp_path: Path) -> None:
    """Verify json contract matches pyarrow json reader."""
    jsonl_text = '{"a": 1, "b": "x"}\n{"a": 2, "b": "y"}\n'
    path = tmp_path / "rows.jsonl"
    path.write_text(jsonl_text, encoding="utf-8")
    schema = pa.schema([("a", pa.int64()), ("b", pa.string())])

    result = _read_with_contract(path, schema_contract=schema, format="json", schema_mode="strict")

    try:
        from pyarrow import json as pajson
    except Exception as exc:
        pytest.skip(f"pyarrow.json not available: {exc}")

    ref = pajson.read_json(pa.py_buffer(jsonl_text.encode("utf8")))
    try:
        ref = ref.cast(schema)
    except Exception:
        ref = pa.Table.from_arrays(
            [
                ref.column(name).cast(type_)
                for name, type_ in zip(schema.names, schema.types, strict=True)
            ],
            schema=schema,
        )

    assert result.clean_data.schema == schema
    assert result.clean_data.equals(ref)


def test_strict_schema_rejects_extra_fields() -> None:
    """Verify strict schema rejects extra fields."""
    schema = pa.schema([("a", pa.int64())])

    with pytest.raises(schema_sanitizer.SchemaSanitizerInvalidArgumentError, match="extra field"):
        _read_with_contract(
            [{"a": 1, "b": 2}],
            format="python",
            source="python",
            schema_contract=schema,
            schema_mode="strict",
        )


def test_strict_schema_ignores_empty_container_extra_fields() -> None:
    """Verify empty unknown containers behave like absent fields."""
    schema = pa.schema(
        [
            ("a", pa.int64()),
            ("nested", pa.struct([pa.field("id", pa.int64())])),
        ]
    )

    result = _read_with_contract(
        [
            {
                "a": 1,
                "ignored_object": {},
                "ignored_list": [],
                "nested": {"id": 2, "ignored": {}},
            }
        ],
        format="python",
        source="python",
        schema_contract=schema,
        schema_mode="strict",
    )

    assert result.clean_data.to_pylist() == [{"a": 1, "nested": {"id": 2}}]


def test_strict_csv_schema_rejects_extra_header_field(tmp_path: Path) -> None:
    """Verify strict csv schema rejects extra header field."""
    schema = pa.schema([("a", pa.int64())])
    path = tmp_path / "rows.csv"
    path.write_text("a,b\n1,2\n", encoding="utf-8")

    with pytest.raises(schema_sanitizer.SchemaSanitizerInvalidArgumentError, match="extra field"):
        _read_with_contract(path, schema_contract=schema, format="csv", schema_mode="strict")


def test_strict_xml_schema_rejects_extra_field(tmp_path: Path) -> None:
    """Verify strict xml schema rejects extra fields."""
    schema = pa.schema([("a", pa.int64())])
    path = tmp_path / "rows.xml"
    path.write_text("<rows><row><a>1</a><b>2</b></row></rows>", encoding="utf-8")

    with pytest.raises(schema_sanitizer.SchemaSanitizerInvalidArgumentError, match="extra field"):
        _read_with_contract(
            path,
            format="xml",
            schema_contract=schema,
            schema_mode="strict",
            xml_row_tag="row",
        )


@pytest.mark.parametrize("on_error", ("stop", "skip_row"))
def test_strict_schema_fast_path_uses_contract_without_inference(on_error: str) -> None:
    """Verify strict schema fast path uses contract without inference."""
    schema = pa.schema([("a", pa.int64())])

    result = _read_with_contract(
        [{"a": 1}, {"a": 2}],
        format="python",
        source="python",
        schema_contract=schema,
        schema_mode="strict",
        on_error=on_error,
    )

    assert result.clean_data.to_pylist() == [{"a": 1}, {"a": 2}]
    assert result.stats["inferred_rows"] == 0
    assert result.stats["materialized_rows"] == 2


def test_mixed_scalar_and_struct_list_elements_are_wrapped() -> None:
    """Verify mixed scalar and struct list elements are wrapped."""
    result = read_test_python(
        [{"a": {"b": [1, 2, {"c": "d"}]}}, {"a": {"b": [3]}}],
        output_format="pyarrow",
    )

    assert result.clean_data.to_pylist() == [
        {
            "a": {
                "b": [
                    {"default_key": 1, "c": None},
                    {"default_key": 2, "c": None},
                    {"default_key": None, "c": "d"},
                ]
            }
        },
        {"a": {"b": [{"default_key": 3, "c": None}]}},
    ]
    assert result.stats["scalar_wrappings"] == 3


def test_default_scalar_object_key_is_already_sanitized_in_preserve_mode() -> None:
    """Verify the built-in scalar wrapper key does not contain an underscore."""
    result = read_test_python(
        [{"a": {"b": [1, {"c": "d"}]}}],
        output_format="pyarrow",
        field_name_policy="preserve",
    )

    assert result.clean_data.to_pylist() == [
        {"a": {"b": [{"c": None, "default_key": 1}, {"c": "d", "default_key": None}]}}
    ]


def test_stats_reflect_materialized_batches_and_skipped_rows() -> None:
    """Verify stats reflect materialized batches and skipped rows."""
    schema = pa.schema([("a", pa.int64())])

    result = _read_with_contract(
        [{"a": "bad"}, {"a": 2}],
        format="python",
        source="python",
        schema_contract=schema,
        schema_mode="strict",
        on_error="skip_row",
        parse_integers=False,
    )

    assert result.clean_data.to_pylist() == [{"a": 2}]
    assert result.stats["materialized_rows"] == 1
    assert result.stats["batches"] == 1
    assert result.stats["skipped_rows"] == 1


def test_inference_scans_all_source_rows() -> None:
    """Verify schema inference scans every source row."""
    rows = [{"a": i} for i in range(37)]
    rows.append({"a": 37, "latefield": {"nested": "observed"}})

    result = read_test_python(rows, output_format="pyarrow")

    assert result.stats["inferred_rows"] == len(rows)
    assert result.stats["materialized_rows"] == len(rows)
    assert result.clean_data.schema.get_field_index("latefield") >= 0


def test_schema_contract_rejects_logical_schema_payload_bytes(tmp_path: Path) -> None:
    """Verify schema contract rejects logical schema payload bytes."""
    require_native()
    csv_text = "a,b\n1,hello\n2,world\n"
    path = tmp_path / "rows.csv"
    path.write_text(csv_text, encoding="utf-8")
    schema = pa.schema([("a", pa.int64()), ("b", pa.string())])
    from schema_sanitizer.core_impl import logical_schema as _logical_schema

    schema_payload = _logical_schema.encode_arrow_schema_payload(schema)
    with pytest.raises(TypeError, match="schema_contract"):
        _read_with_contract(
            path, schema_contract=schema_payload, format="csv", schema_mode="strict"
        )


def test_schema_contract_rejects_logical_schema_dict_spec(tmp_path: Path) -> None:
    """Verify schema contract rejects logical schema dict spec."""
    csv_text = "a,b\n1,hello\n2,world\n"
    path = tmp_path / "rows.csv"
    path.write_text(csv_text, encoding="utf-8")
    spec = {
        "fields": [
            {"name": "a", "type": "int64", "nullable": True},
            {"name": "b", "type": "utf8", "nullable": True},
        ]
    }

    with pytest.raises(TypeError, match="schema_contract"):
        _read_with_contract(path, schema_contract=spec, format="csv", schema_mode="strict")


class NonSeekable:
    """A minimal non-seekable file-like: .read() only."""

    def __init__(self, data: bytes):
        """Initialize the test helper."""
        self._data = data
        self._pos = 0

    def read(self, n: int = -1):
        """Read from the test helper."""
        if n is None or n < 0:
            n = len(self._data) - self._pos
        if self._pos >= len(self._data):
            return b""
        out = self._data[self._pos : self._pos + n]
        self._pos += len(out)
        return out


def test_non_seekable_filelike_is_rejected():
    """Verify non seekable filelike is rejected."""
    src = NonSeekable(b'[{"a": 1}, {"a": 2}]')
    with pytest.raises(TypeError):
        read_test_json(src)


def test_seekable_filelike_is_rejected():
    """Verify seekable filelike is rejected."""
    src = io.BytesIO(b'[{"a": 1}, {"a": 2}]')
    with pytest.raises(TypeError):
        read_test_json(src)


def _run_csv(path: Path, chunk_bytes: int) -> pa.Table:
    """Run csv."""
    schema = pa.schema([("a", pa.int64()), ("b", pa.string())])
    return _read_with_contract(
        path,
        format="csv",
        schema_contract=schema,
        schema_mode="strict",
        read_chunk_bytes=chunk_bytes,
    ).clean_data


def test_chunking_invariance_csv(tmp_path: Path) -> None:
    """Verify chunking invariance csv."""
    csv_text = "a,b\n" + "\n".join(f"{i},x{i}" for i in range(1, 5000)) + "\n"
    path = tmp_path / "rows.csv"
    path.write_text(csv_text, encoding="utf-8")

    t1 = _run_csv(path, chunk_bytes=1024 * 1024)
    t2 = _run_csv(path, chunk_bytes=17)

    assert t1.schema == t2.schema
    assert t1.equals(t2)


def test_inference_invariance_json(tmp_path: Path) -> None:
    """Verify inference invariance json."""
    rows = [
        {"id": i, "payload": {"a": i, "b": f"x{i}"}, "items": [i, i + 1]} for i in range(1, 500)
    ]
    path = tmp_path / "rows.jsonl"
    path.write_text("\n".join(json.dumps(row) for row in rows) + "\n", encoding="utf-8")

    t_a = read_test_jsonl(path, read_chunk_bytes=97).clean_data
    t_b = read_test_jsonl(path, read_chunk_bytes=4096).clean_data

    assert t_a.schema == t_b.schema
    assert t_a.equals(t_b)


def test_full_scan_inference_keeps_latefields() -> None:
    """Verify full scan inference keeps late fields."""
    result = read_test_python([{"a": 1}, {"a": 2, "b": 99}])

    assert result.clean_data.schema.names == ["a", "b"]
    assert result.clean_data.to_pylist() == [
        {"a": 1, "b": None},
        {"a": 2, "b": 99},
    ]


def test_full_scan_inference_rejects_type_mismatch() -> None:
    """Verify full scan inference rejects type mismatch."""
    with pytest.raises(schema_sanitizer.SchemaSanitizerError):
        read_test_python([{"a": 1}, {"a": {"x": 1}}], on_error="stop")


def test_string_outputs_are_plain_utf8() -> None:
    """Verify string outputs are plain utf8."""
    result = read_test_python([{"a": "k"} for _ in range(200)])

    assert pa.types.is_string(result.clean_data.schema.field("a").type) or pa.types.is_large_string(
        result.clean_data.schema.field("a").type
    )


def test_input_text_encoding_decodes_non_utf8_path_csv(tmp_path: Path) -> None:
    """Verify input text encoding decodes non utf8 path csv."""
    path = tmp_path / "names.csv"
    path.write_bytes("name\ncafé\n".encode("latin-1"))
    result = read_test_csv(path, input_text_encoding="iso8859-1")
    assert result.clean_data.to_pylist() == [{"name": "café"}]


def test_input_text_encoding_streams_non_utf8_path_csv(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Verify non UTF-8 path CSV input avoids full-file Python reads."""
    path = tmp_path / "names.csv"
    path.write_bytes("name\ncafé\nmañana\n".encode("latin-1"))

    def fail_read_bytes(self: Path) -> bytes:
        """Reject the old full-file read branch."""
        raise AssertionError(f"unexpected full read of {self}")

    monkeypatch.setattr(Path, "read_bytes", fail_read_bytes)

    result = read_test_csv(path, input_text_encoding="iso8859-1")

    assert result.clean_data.to_pylist() == [{"name": "café"}, {"name": "mañana"}]


def test_input_text_encoding_native_supported_path_stays_path(tmp_path: Path) -> None:
    """Verify native-supported encodings do not route local paths through Python streams."""
    from schema_sanitizer.api_impl.input.preparation import prepare_public_input

    path = tmp_path / "names.csv"
    path.write_bytes("name\ncafé\n".encode("latin-1"))

    prepared = prepare_public_input(
        path,
        input_format="csv",
        input_mode="single_file",
        input_text_encoding="iso8859-1",
        xml_row_tag=None,
        csv_delimiter=",",
        csv_has_header=True,
        memory_limit_bytes=None,
    )

    assert prepared.source == "path"
    assert prepared.data == str(path)


def test_execution_context_streams_non_utf8_path_csv(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Verify direct context path input uses bounded transcoding."""
    path = tmp_path / "names.csv"
    path.write_bytes("name\ncafé\n".encode("latin-1"))

    def fail_read_bytes(self: Path) -> bytes:
        """Reject the old full-file read branch."""
        raise AssertionError(f"unexpected full read of {self}")

    monkeypatch.setattr(Path, "read_bytes", fail_read_bytes)

    result = ExecutionContext().to_table(
        path,
        format="csv",
        source="path",
        options=normalize_call_options(input_text_encoding="iso8859-1"),
    )

    assert result.clean_data.to_pylist() == [{"name": "café"}]


def test_input_text_encoding_streams_non_utf8_path_jsonl(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Verify non UTF-8 JSONL paths are transcoded as bounded streams."""
    path = tmp_path / "rows.jsonl"
    path.write_bytes('{"name":"café"}\n{"name":"mañana"}\n'.encode("latin-1"))

    def fail_read_bytes(self: Path) -> bytes:
        """Reject the old full-file read branch."""
        raise AssertionError(f"unexpected full read of {self}")

    monkeypatch.setattr(Path, "read_bytes", fail_read_bytes)

    result = read_test_jsonl(path, input_text_encoding="iso8859-1")

    assert result.clean_data.to_pylist() == [{"name": "café"}, {"name": "mañana"}]


def test_input_text_encoding_streams_non_utf8_path_json_array(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Verify non UTF-8 JSON-array paths are parsed after streaming transcode."""
    path = tmp_path / "rows.json"
    path.write_bytes('[{"name":"café"},{"name":"mañana"}]\n'.encode("latin-1"))

    def fail_read_bytes(self: Path) -> bytes:
        """Reject the old full-file read branch."""
        raise AssertionError(f"unexpected full read of {self}")

    monkeypatch.setattr(Path, "read_bytes", fail_read_bytes)

    result = read_test_path(path, input_format="json_array", input_text_encoding="iso8859-1")

    assert result.clean_data.to_pylist() == [{"name": "café"}, {"name": "mañana"}]


def test_input_text_encoding_decodes_utf16_path_csv(tmp_path: Path) -> None:
    """Verify native path transcoding handles UTF-16 text input."""
    path = tmp_path / "names.csv"
    path.write_bytes("name\ncafé\n".encode("utf-16"))

    result = read_test_csv(path, input_text_encoding="utf-16")

    assert result.clean_data.to_pylist() == [{"name": "café"}]


def test_input_text_encoding_rejects_unknown_codec(tmp_path: Path) -> None:
    """Verify input text encoding rejects unknown codec."""
    path = tmp_path / "names.csv"
    path.write_text("name\ncafe\n", encoding="utf-8")
    with pytest.raises(ValueError, match="input_text_encoding"):
        read_test_csv(path, input_text_encoding="not-a-real-codec")


def test_memory_limit_bytes_raises_typed_error() -> None:
    """Verify memory limit bytes raises typed error."""
    payload = [{"a": "x" * 200_000}]

    with pytest.raises(schema_sanitizer.SchemaSanitizerResourceError) as excinfo:
        read_test_python(payload, batch_memory_limit_bytes=64 * 1024)

    err = excinfo.value
    assert getattr(err, "code", None) == "E_RESOURCE_LIMIT"
    assert "memory_limit_bytes" in str(err)
