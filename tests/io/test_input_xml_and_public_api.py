"""XML input and public API tests.

It covers document and row-tag modes, attributes and repeated children, streaming memory
bounds, DTD rejection, encodings, and scanner validation.
"""

from __future__ import annotations

import inspect
import io
import json
from pathlib import Path

import pytest
from conftest import (
    read_test_csv,
    read_test_jsonl,
    read_test_python,
    read_test_xml,
)

import schema_sanitizer as ss


def test_read_xml_without_row_tag_emits_document_row(tmp_path, require_native: None) -> None:
    """Verify read XML without row tag emits document row."""
    pytest.importorskip("pyarrow")

    path = tmp_path / "document.xml"
    path.write_text("<rows><row><a>1</a></row><row><a>2</a></row></rows>", encoding="utf-8")

    result = read_test_xml(path)

    assert result.clean_data.to_pylist() == [{"row": [{"a": "1"}, {"a": "2"}]}]


def test_read_xml_row_tag_maps_attributes_text_and_repeated_children(
    tmp_path, require_native: None
) -> None:
    """Verify read XML row tag maps attributes text and repeated children."""
    pytest.importorskip("pyarrow")

    path = tmp_path / "rows.xml"
    path.write_text(
        """<?xml version="1.0"?>
        <rows>
          <row id="1"><name>Ana</name><tag>x</tag><tag>y</tag></row>
          <row id="2"><name>Alex</name><note lang="en">hello</note></row>
        </rows>
        """,
        encoding="utf-8",
    )

    result = read_test_xml(path, xml_row_tag="row")

    assert result.clean_data.to_pylist() == [
        {"id": "1", "name": "Ana", "tag": ["x", "y"], "note": None},
        {"id": "2", "name": "Alex", "tag": None, "note": {"lang": "en", "text": "hello"}},
    ]


def test_xml_input_format_is_supported_by_converters(tmp_path, require_native: None) -> None:
    """Verify XML input format is supported by converters."""
    path = tmp_path / "rows.xml"
    out = tmp_path / "rows.jsonl"
    path.write_text("<rows><row><a>1</a></row><row><a>2</a></row></rows>", encoding="utf-8")

    ss.to_jsonl(path, out, input_format="xml", xml_row_tag="row")

    generated = {"schema_registry", "schema_drifts", "source_file", "ingestion_timestamp"}
    rows = [json.loads(line) for line in out.read_text(encoding="utf-8").splitlines()]
    assert [{k: v for k, v in row.items() if k not in generated} for row in rows] == [
        {"a": "1"},
        {"a": "2"},
    ]


def test_read_xml_memory_limit_rejects_large_dom_input(tmp_path, require_native: None) -> None:
    """Verify read XML memory limit rejects large DOM input."""
    path = tmp_path / "large.xml"
    path.write_text(
        "<rows><row><a>" + ("x" * (2 * 1024 * 1024)) + "</a></row></rows>",
        encoding="utf-8",
    )

    with pytest.raises(ss.SchemaSanitizerResourceError):
        read_test_xml(path, memory_limit_bytes=1024 * 1024)


def test_read_xml_row_tag_streams_file_larger_than_memory_limit(
    tmp_path, require_native: None
) -> None:
    """Verify read XML row tag streams file larger than memory limit."""
    pytest.importorskip("pyarrow")

    path = tmp_path / "stream.xml"
    path.write_text(
        "<rows><row><a>0</a></row>" + (" " * (2 * 1024 * 1024)) + "<row><a>1</a></row></rows>",
        encoding="utf-8",
    )

    result = read_test_xml(path, xml_row_tag="row", memory_limit_bytes=1024 * 1024)

    assert result.clean_data.to_pylist() == [{"a": "0"}, {"a": "1"}]


def test_read_xml_row_tag_streams_rows_split_across_chunks(tmp_path, require_native: None) -> None:
    """Verify read XML row tag streams rows split across chunks."""
    pytest.importorskip("pyarrow")

    lower_payload = "a" * 40_000
    upper_payload = "B" * 40_000
    path = tmp_path / "split-chunks.xml"
    path.write_text(
        "<rows>"
        f"<row><id>1</id><payload>{lower_payload}</payload></row>"
        f"<row><id>2</id><payload>{upper_payload}</payload></row>"
        "</rows>",
        encoding="utf-8",
    )

    result = read_test_xml(
        path,
        xml_row_tag="row",
        memory_limit_bytes=1024 * 1024,
    )

    assert result.clean_data.to_pylist() == [
        {"id": "1", "payload": lower_payload},
        {"id": "2", "payload": upper_payload},
    ]


def test_read_xml_row_tag_rejects_single_row_larger_than_memory_limit(
    tmp_path, require_native: None
) -> None:
    """Verify read XML row tag rejects single row larger than memory limit."""
    path = tmp_path / "huge_row.xml"
    path.write_text(
        "<rows><row><payload>" + ("x" * 300) + "</payload></row></rows>", encoding="utf-8"
    )

    with pytest.raises(ss.SchemaSanitizerOutOfMemoryError):
        read_test_xml(path, xml_row_tag="row", memory_limit_bytes=128)


def test_read_xml_rejects_dtd_declarations(tmp_path, require_native: None) -> None:
    """Verify read XML rejects dtd declarations."""
    path = tmp_path / "dtd.xml"
    path.write_text(
        '<!DOCTYPE rows [<!ENTITY x "boom">]><rows><row><a>&x;</a></row></rows>',
        encoding="utf-8",
    )

    with pytest.raises(ss.SchemaSanitizerInvalidArgumentError, match="DTD"):
        read_test_xml(path, xml_row_tag="row")


def test_read_xml_document_mode_rejects_dtd_declarations(tmp_path, require_native: None) -> None:
    """Verify read XML document mode rejects dtd declarations."""
    path = tmp_path / "document_dtd.xml"
    path.write_text(
        '<!DOCTYPE rows [<!ENTITY x "boom">]><rows><row><a>&x;</a></row></rows>',
        encoding="utf-8",
    )

    with pytest.raises(ss.SchemaSanitizerInvalidArgumentError, match="DTD"):
        read_test_xml(path)


def test_all_public_to_functions_share_input_options() -> None:
    """Verify all public to functions share input options."""
    common = set(inspect.signature(ss.to_pyarrow).parameters) - {"input_path"}
    parquet_output_options = {"parquet_compression", "parquet_gzip_level"}
    assert "schema_drift_date" not in common
    assert "schema_registry_column" not in common
    assert "schema_drifts_column" not in common
    assert "source_file_column" not in common
    assert "constant_columns" not in common
    for function in (ss.to_pandas, ss.to_polars, ss.to_duckdb):
        assert set(inspect.signature(function).parameters) - {"input_path"} == common
    for function in (ss.to_csv, ss.to_jsonl):
        assert (
            set(inspect.signature(function).parameters)
            - {
                "input_path",
                "output_path",
            }
            == common
        )
    assert (
        set(inspect.signature(ss.to_parquet).parameters)
        - {
            "input_path",
            "output_path",
        }
        == common | parquet_output_options
    )


def test_file_input_can_exceed_memory_limit_bytes(tmp_path) -> None:
    """Verify file input can exceed memory limit bytes."""
    pytest.importorskip("pyarrow")
    path = tmp_path / "data.jsonl"
    path.write_text(
        ("\n" * (2 * 1024 * 1024)) + "".join(f'{{"a":{i}}}\n' for i in range(20)),
        encoding="utf-8",
    )

    result = read_test_jsonl(path, memory_limit_bytes=1024 * 1024)

    assert result.clean_data.num_rows == 20


def test_file_like_payload_is_rejected_by_format_reader() -> None:
    """Verify file like payload is rejected by format reader."""
    with pytest.raises(TypeError):
        read_test_csv(io.BytesIO(b"a,b\n1,2\n"))


def test_public_input_mode_must_be_a_string(tmp_path) -> None:
    """Verify public input mode must be a string."""
    path = tmp_path / "data.jsonl"
    path.write_text('{"a": 1}\n', encoding="utf-8")
    out = tmp_path / "out.jsonl"

    with pytest.raises(TypeError, match="input_mode"):
        ss.to_jsonl(path, out, input_format="jsonl", input_mode=None)


def test_reader_returns_result_with_clean_data(tmp_path) -> None:
    """Verify reader returns result with clean data."""
    pytest.importorskip("pyarrow")
    path = tmp_path / "data.jsonl"
    path.write_text('{"a": 1}\n', encoding="utf-8")

    result = read_test_jsonl(path, output_format="pyarrow")
    assert isinstance(result, ss.Result)
    assert result.clean_data.to_pylist() == [{"a": 1}]


def test_analytical_function_selects_clean_data_type(tmp_path) -> None:
    """Verify analytical function selects clean data type."""
    pytest.importorskip("pyarrow")
    path = tmp_path / "data.jsonl"
    path.write_text('{"a": 1}\n', encoding="utf-8")

    assert ss.to_pyarrow(path, input_format="jsonl").clean_data.num_rows == 1


def test_unknown_reader_parameter_is_rejected() -> None:
    """Verify unknown reader parameter is rejected."""
    with pytest.raises(TypeError, match="unknown"):
        read_test_python([{"a": 1}], unknown=True)


@pytest.mark.parametrize("invalid_whitespace", ("\x0b", "\x0c"))
def test_xml_scanners_reject_non_xml_whitespace(
    tmp_path: Path,
    invalid_whitespace: str,
) -> None:
    """Vertical tab and form feed are not XML 1.0 whitespace."""
    from schema_sanitizer.core_impl.native_runtime import native_core

    path = tmp_path / "invalid-prefix.xml"
    path.write_text(f"{invalid_whitespace}<event/>", encoding="utf-8")

    with pytest.raises(ValueError, match="expected root element"):
        native_core.xml_folder_effective_row_tag([path], "", -1)
