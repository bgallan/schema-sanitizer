"""XML input and public API tests."""

# ruff: noqa: F405

from __future__ import annotations

from _support.input_contract import *  # noqa: F403


def test_read_xml_without_row_tag_emits_document_row(tmp_path) -> None:
    """Verify read xml defaults to one row for the whole document."""
    pytest.importorskip("pyarrow")
    require_native()

    path = tmp_path / "document.xml"
    path.write_text("<rows><row><a>1</a></row><row><a>2</a></row></rows>", encoding="utf-8")

    result = read_test_xml(path)

    assert result.clean_data.to_pylist() == [{"row": [{"a": "1"}, {"a": "2"}]}]


def test_read_xml_row_tag_maps_attributes_text_and_repeated_children(tmp_path) -> None:
    """Verify xml_row_tag emits deterministic JSON-like rows."""
    pytest.importorskip("pyarrow")
    require_native()

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


def test_xml_input_format_is_supported_by_converters(tmp_path) -> None:
    """Verify converters accept explicit xml input format."""
    require_native()

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


def test_read_xml_memory_limit_rejects_large_dom_input(tmp_path) -> None:
    """Verify whole-document XML parsing respects the operation budget."""
    require_native()

    path = tmp_path / "large.xml"
    path.write_text(
        "<rows><row><a>" + ("x" * (2 * 1024 * 1024)) + "</a></row></rows>",
        encoding="utf-8",
    )

    with pytest.raises(ss.SchemaSanitizerResourceError):
        read_test_xml(path, memory_limit_bytes=1024 * 1024)


def test_read_xml_row_tag_streams_file_larger_than_memory_limit(tmp_path) -> None:
    """Verify row streaming handles a file larger than the operation budget."""
    pytest.importorskip("pyarrow")
    require_native()

    path = tmp_path / "stream.xml"
    path.write_text(
        "<rows><row><a>0</a></row>" + (" " * (2 * 1024 * 1024)) + "<row><a>1</a></row></rows>",
        encoding="utf-8",
    )

    result = read_test_xml(path, xml_row_tag="row", memory_limit_bytes=1024 * 1024)

    assert result.clean_data.to_pylist() == [{"a": "0"}, {"a": "1"}]


def test_read_xml_row_tag_streams_rows_split_across_chunks(tmp_path) -> None:
    """Verify row spans split across derived input chunks remain intact."""
    pytest.importorskip("pyarrow")
    require_native()

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


def test_read_xml_row_tag_rejects_single_row_larger_than_memory_limit(tmp_path) -> None:
    """Verify xml row streaming rejects one row that exceeds the active buffer limit."""
    require_native()

    path = tmp_path / "huge_row.xml"
    path.write_text(
        "<rows><row><payload>" + ("x" * 300) + "</payload></row></rows>", encoding="utf-8"
    )

    with pytest.raises(ss.SchemaSanitizerOutOfMemoryError):
        read_test_xml(path, xml_row_tag="row", memory_limit_bytes=128)


def test_read_xml_rejects_dtd_declarations(tmp_path) -> None:
    """Verify xml parser rejects DTD/entity declarations."""
    require_native()

    path = tmp_path / "dtd.xml"
    path.write_text(
        '<!DOCTYPE rows [<!ENTITY x "boom">]><rows><row><a>&x;</a></row></rows>',
        encoding="utf-8",
    )

    with pytest.raises(ss.SchemaSanitizerInvalidArgumentError, match="DTD"):
        read_test_xml(path, xml_row_tag="row")


def test_read_xml_document_mode_rejects_dtd_declarations(tmp_path) -> None:
    """Verify xml document mode rejects DTD/entity declarations."""
    require_native()

    path = tmp_path / "document_dtd.xml"
    path.write_text(
        '<!DOCTYPE rows [<!ENTITY x "boom">]><rows><row><a>&x;</a></row></rows>',
        encoding="utf-8",
    )

    with pytest.raises(ss.SchemaSanitizerInvalidArgumentError, match="DTD"):
        read_test_xml(path)


def test_all_public_to_functions_share_input_options() -> None:
    """Verify analytical and file converters expose one input option contract."""
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
    """Verify streamed input size may exceed the operation memory budget."""
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
    """Verify the public input-mode selector must be a string."""
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
    """Verify analytical function names select the in-memory output type."""
    pytest.importorskip("pyarrow")
    path = tmp_path / "data.jsonl"
    path.write_text('{"a": 1}\n', encoding="utf-8")

    assert ss.to_pyarrow(path, input_format="jsonl").clean_data.num_rows == 1


def test_unknown_reader_parameter_is_rejected() -> None:
    """Verify unknown reader parameter is rejected."""
    with pytest.raises(TypeError, match="unknown"):
        read_test_python([{"a": 1}], unknown=True)
