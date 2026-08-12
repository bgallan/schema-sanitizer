"""Ownership and layout contracts for structured data format frontends."""

from __future__ import annotations

from pathlib import Path

import pytest
from _support.source_size import (
    DEFAULT_PRODUCT_SOURCE_LINE_LIMIT,
    INTEGRAL_AUTHORITY_LINE_LIMITS,
)

ROOT = Path(__file__).resolve().parents[2]

CPP = ROOT / "cpp/src"


def test_arrow_direct_values_have_one_owner_and_parsed_storage_kind() -> None:
    """Arrow values reuse schema-time storage classification without format strings."""
    package = ROOT / "cpp/src/api/python_abi3/arrow_direct"
    owner = (package / "_core_abi3_arrow_direct_values.cc").read_text(encoding="utf-8")
    model = (package / "_core_abi3_arrow_direct_model.hh").read_text(encoding="utf-8")
    parser = (package / "schema/type.cc").read_text(encoding="utf-8")
    assert len(owner.splitlines()) <= 500
    assert "enum class ArrowStorageKind" in model
    assert "ArrowStorageKind storage_kind" in model
    assert "std::string format;" not in model
    assert "node->storage_kind" in parser
    assert "ref->node->storage_kind" in owner
    assert "const std::string_view format(ref->node->format)" not in owner
    for suffix in ("dictionary", "nested", "temporal"):
        assert not (package / f"_core_abi3_arrow_direct_values_{suffix}.cc").exists()
        assert not (package / f"_core_abi3_arrow_direct_values_{suffix}.hh").exists()
    manifest = (ROOT / "cmake/SchemaSanitizerSources.cmake").read_text(encoding="utf-8")
    assert manifest.count("_core_abi3_arrow_direct_values.cc") == 1
    assert "_core_abi3_arrow_direct_values_dictionary.cc" not in manifest
    assert "_core_abi3_arrow_direct_values_nested.cc" not in manifest
    assert "_core_abi3_arrow_direct_values_temporal.cc" not in manifest


def test_arrow_source_chunks_do_not_retain_all_descriptors() -> None:
    """Chunk iteration consumes one bounded slice from a source iterator."""
    text = (ROOT / "src/schema_sanitizer/api_impl/parquet/arrow_sources.py").read_text(
        encoding="utf-8"
    )
    assert "self._sources = iter(sources)" in text
    assert "chunk = list(islice(self._sources, self._chunk_size))" in text
    assert "tuple(sources)" not in text


def test_arrow_stream_wrappers_have_one_runtime_owner() -> None:
    """Stream protocols, lifecycle, and diagnostics share one cohesive owner."""
    owner = ROOT / "src/schema_sanitizer/api_impl/streams.py"
    assert owner.is_file()
    assert not (ROOT / "src/schema_sanitizer/stream_impl.py").exists()
    assert not (ROOT / "src/schema_sanitizer/api_impl/streams").exists()
    assert len(owner.read_text(encoding="utf-8").splitlines()) <= 500


def test_csv_frontend_has_bounded_cohesive_owners() -> None:
    """CSV lifecycle and batching stay together while projection remains independent."""
    package = ROOT / "cpp/src/frontends/csv"
    assert {path.name for path in package.iterdir() if path.is_file()} == {
        "column_projection.cc",
        "column_projection.hh",
        "frontend.cc",
        "frontend_internal.hh",
        "source_projection.hh",
    }
    assert not (package / "column_projection").exists()
    for name in ("column_projection.cc", "frontend.cc", "source_projection.hh"):
        assert len((package / name).read_text(encoding="utf-8").splitlines()) <= 500
    source = (package / "frontend.cc").read_text(encoding="utf-8")
    assert "CsvFrontend::next_batch" in source
    assert "CsvFrontend::reset" in source
    assert "kCsvVTable" in source


def test_csv_frontend_has_one_lifecycle_owner_and_no_header_allocations() -> None:
    """CSV batching stays cohesive and no-header rows avoid empty string headers."""
    package = ROOT / "cpp/src/frontends/csv"
    assert {path.name for path in package.iterdir() if path.is_file()} == {
        "column_projection.cc",
        "column_projection.hh",
        "frontend.cc",
        "frontend_internal.hh",
        "source_projection.hh",
    }
    frontend = (package / "frontend.cc").read_text(encoding="utf-8")
    projection = (package / "column_projection.cc").read_text(encoding="utf-8")
    manifest = (ROOT / "cmake/SchemaSanitizerSources.cmake").read_text(encoding="utf-8")
    assert "storage->cells.reserve(projection_.column_count_hint())" in frontend
    assert "cells.reserve(projection_.column_count_hint())" in frontend
    assert "if (has_header_ && cells.size() > headers_.size())" in projection
    assert "frontends/csv/frontend_batch.cc" not in manifest
    assert "frontends/csv/frontend_lifecycle.cc" not in manifest


def test_csv_nested_stream_allocates_state_only_for_nested_columns() -> None:
    """Per-batch nested array state scales with nested, not total, columns."""
    package = ROOT / "cpp/src/api/python_abi3/csv/nested_stream"
    owner = (package / "nested_stream.cc").read_text(encoding="utf-8")
    state = (package / "state.hh").read_text(encoding="utf-8")
    assert {path.name for path in package.iterdir() if path.is_file()} == {
        "nested_stream.cc",
        "state.hh",
    }
    assert "std::optional<std::size_t> nested_slot" in state
    assert "std::size_t nested_column_count = 0" in state
    assert "nested_arrays.resize(stream_state->nested_column_count)" in owner
    assert "nested_fields.reserve(stream_state->nested_column_count)" in owner
    assert "nested_arrays.resize(stream_state->columns.size())" not in owner


def test_csv_nested_stream_has_one_bounded_lifecycle_owner() -> None:
    """CSV nested rewriting keeps its Arrow callbacks and wrapper cohesive."""
    csv = ROOT / "cpp/src/api/python_abi3/csv"
    package = csv / "nested_stream"
    assert {path.name for path in package.iterdir() if path.is_file()} == {
        "nested_stream.cc",
        "state.hh",
    }
    owner = package / "nested_stream.cc"
    source = owner.read_text(encoding="utf-8")
    assert len(source.splitlines()) <= 500
    assert "int get_schema" in source
    assert "int get_next" in source
    assert "PyObject *py_csv_nested_stream_wrap" in source
    assert not list(csv.glob("_core_abi3_csv_nested_stream*"))
    manifest = (ROOT / "cmake/SchemaSanitizerSources.cmake").read_text(encoding="utf-8")
    assert "cpp/src/api/python_abi3/csv/nested_stream/nested_stream.cc" in manifest
    for retired in ("callbacks.cc", "columns.cc", "release.cc", "schema.cc", "wrapper.cc"):
        assert f"cpp/src/api/python_abi3/csv/nested_stream/{retired}" not in manifest


def test_csv_streaming_scanner_has_explicit_subsystem() -> None:
    """Chunk flow and multi-chunk record buffering must not return to flat files."""
    streaming = ROOT / "cpp/src/internal/parsing/streaming"
    package = streaming / "csv"
    assert {path.name for path in package.iterdir() if path.is_file()} == {
        "record_buffer.cc",
        "record_span.cc",
        "record_span_internal.hh",
        "scanner.cc",
        "scanner.hh",
    }
    for retired in (
        "csv_record_span_scanner.cc",
        "csv_streaming_scanner.cc",
        "csv_streaming_scanner.hh",
    ):
        assert not (streaming / retired).exists()


def test_integral_authority_size_exceptions_are_narrow_and_live() -> None:
    """Large integral ledgers get only explicit, tight, currently needed ceilings."""
    assert 0 < len(INTEGRAL_AUTHORITY_LINE_LIMITS) <= 4
    for relative_path, line_limit in INTEGRAL_AUTHORITY_LINE_LIMITS.items():
        owner = ROOT / relative_path
        assert owner.is_file()
        current_lines = len(owner.read_text(encoding="utf-8").splitlines())
        assert current_lines > DEFAULT_PRODUCT_SOURCE_LINE_LIMIT
        assert current_lines <= line_limit
        assert line_limit <= current_lines + max(100, current_lines // 10)


def test_json_frontend_matches_its_translation_unit() -> None:
    """The JSON frontend must remain one bounded visible translation unit."""
    frontend = ROOT / "cpp/src/frontends/json/text_frontend.cc"
    source = frontend.read_text(encoding="utf-8")
    assert len(source.splitlines()) <= 600
    assert "class JsonTextRows" in source
    assert "class JsonTextFrontend" in source
    assert "class JsonArrayGroupFrontend" in source
    assert not (frontend.parent / "text").exists()
    assert not list(frontend.parent.glob("*.cc.inc"))


def test_json_ondemand_iteration_is_split_by_container() -> None:
    """Object, array, and child-value iteration remain separate units."""
    package = ROOT / "cpp/src/internal/parsing/json/ondemand"
    assert not (package / "iteration.cc").exists()
    for name in ("array_iteration.cc", "object_iteration.cc", "value_iteration.cc"):
        assert (package / name).is_file()
    manifest = (ROOT / "cmake/SchemaSanitizerSources.cmake").read_text(encoding="utf-8")
    assert "cpp/src/internal/parsing/json/ondemand/iteration.cc" not in manifest
    for name in ("array_iteration.cc", "object_iteration.cc", "value_iteration.cc"):
        assert f"cpp/src/internal/parsing/json/ondemand/{name}" in manifest


def test_json_stream_scanner_is_split_by_responsibility() -> None:
    """Scanner lifecycle, traversal, and value parsing remain independent units."""
    package = ROOT / "cpp/src/internal/parsing/streaming/json"
    assert not (package / "scanner.cc").exists()
    assert {path.name for path in package.glob("scanner_*.cc")} == {
        "scanner_flow.cc",
        "scanner_line.cc",
        "scanner_state.cc",
        "scanner_value.cc",
    }


def test_jsonl_numeric_writers_are_split_by_number_family() -> None:
    """Integer and floating JSON formatting must remain independent units."""
    output = ROOT / "cpp/src/internal/json_output"
    assert not (output / "jsonl_value_writer_numeric.cc").exists()
    assert (output / "jsonl_value_writer_integer.cc").is_file()
    assert (output / "jsonl_value_writer_floating.cc").is_file()


def test_jsonl_output_adapters_have_one_bounded_lifecycle_owner() -> None:
    """Closely coupled JSONL destinations must not return to micro-units."""
    package = ROOT / "cpp/src/api/python_abi3/json/output_adapters"
    owner = package / "output_adapters.cc"
    assert {path.name for path in package.iterdir()} == {"api.hh", "output_adapters.cc"}
    source = owner.read_text(encoding="utf-8")
    assert "class FileJsonlOutput" in source
    assert "class PythonJsonlOutput" in source
    assert "class StringJsonlOutput" in source
    assert len(source.splitlines()) <= 500


def test_jsonl_output_adapters_have_one_translation_unit() -> None:
    """File, Python, and string JSONL destinations share one lifecycle owner."""
    package = CPP / "api/python_abi3/json/output_adapters"
    owner = package / "output_adapters.cc"
    assert {path.name for path in package.iterdir()} == {"api.hh", "output_adapters.cc"}
    source = owner.read_text(encoding="utf-8")
    assert source.count("class FileJsonlOutput") == 1
    assert source.count("class PythonJsonlOutput") == 1
    assert source.count("class StringJsonlOutput") == 1
    manifest = (ROOT / "cmake/SchemaSanitizerSources.cmake").read_text(encoding="utf-8")
    assert manifest.count("json/output_adapters/output_adapters.cc") == 1
    for retired in ("file.cc", "python.cc", "python_stream.cc", "string.cc"):
        assert f"json/output_adapters/{retired}" not in manifest


def test_metadata_stream_allocates_state_by_column_kind() -> None:
    """Metadata batches must not construct UTF-8 and timestamp state per column."""
    owner = ROOT / "cpp/src/api/python_abi3/metadata/stream/array_builder.cc"
    source = owner.read_text(encoding="utf-8")
    assert "struct MetadataColumnData" not in source
    assert "std::vector<Utf8ColumnData> utf8_columns" in source
    assert "std::vector<TimestampMicrosColumnData> timestamp_columns" in source
    assert "std::ranges::count_if" in source
    layout = (owner.parent / "stream.cc").read_text(encoding="utf-8")
    assert "BorrowedStringLookupSet names" in layout


def test_metadata_stream_has_two_bounded_implementation_owners() -> None:
    """Metadata schema/arrays and stream lifecycle stay visible without micro-units."""
    metadata = ROOT / "cpp/src/api/python_abi3/metadata"
    stream = metadata / "stream"
    assert {path.name for path in stream.iterdir() if path.is_file()} == {
        "array_builder.cc",
        "stream.cc",
        "stream.hh",
    }
    assert not (metadata / "utf8").exists()
    assert all(
        (
            len(path.read_text(encoding="utf-8").splitlines()) <= 500
            for path in stream.iterdir()
            if path.is_file()
        )
    )
    sources = (ROOT / "cmake/SchemaSanitizerSources.cmake").read_text(encoding="utf-8")
    assert "metadata/stream/array_builder.cc" in sources
    assert "metadata/stream/stream.cc" in sources
    assert "metadata/utf8/column.cc" not in sources


def test_metadata_stream_is_grouped_by_lifecycle_and_build_phase() -> None:
    """Metadata wrappers keep array construction separate from stream lifecycle."""
    metadata = ROOT / "cpp/src/api/python_abi3/metadata"
    package = metadata / "stream"
    assert {path.name for path in package.iterdir() if path.is_file()} == {
        "array_builder.cc",
        "stream.cc",
        "stream.hh",
    }
    assert not list(metadata.glob("_core_abi3_metadata_stream*"))
    assert not (metadata / "utf8").exists()
    manifest = (ROOT / "cmake/SchemaSanitizerSources.cmake").read_text(encoding="utf-8")
    assert "metadata/stream/array_builder.cc" in manifest
    assert "metadata/stream/stream.cc" in manifest
    assert "metadata/utf8/column.cc" not in manifest


def test_xml_document_parser_is_split_by_phase() -> None:
    """XML document lifecycle, tokens, and recursive elements stay separate."""
    parsing = ROOT / "cpp/src/internal/parsing"
    package = parsing / "xml"
    for name in ("document.cc", "document.hh", "element.cc", "tokens.cc"):
        assert (package / name).is_file()
    assert not (parsing / "xml_document.cc").exists()
    assert not (parsing / "xml_document.hh").exists()


def test_xml_folder_validation_has_one_native_owner() -> None:
    """Folder scanning and its ABI3 entry point form one translation unit."""
    package = ROOT / "cpp/src/api/python_abi3/xml"
    owner = package / "_core_abi3_xml_folder.cc"
    source = owner.read_text(encoding="utf-8")
    assert len(source.splitlines()) <= 500
    assert "PySequence_Fast" in source
    assert "PyList_GetItem" in source
    assert "PyTuple_GetItem" in source
    assert "sequence_item_borrowed_or_new" not in source
    assert not list(package.glob("*_folder_parts.*"))


def test_xml_frontend_has_one_bounded_lifecycle_owner() -> None:
    """XML loading, batching, and vtable wiring stay cohesive without micro-units."""
    package = ROOT / "cpp/src/frontends/xml"
    assert {path.name for path in package.iterdir() if path.is_file()} == {
        "frontend.cc",
        "frontend_internal.hh",
    }
    source = (package / "frontend.cc").read_text(encoding="utf-8")
    assert "XmlFrontend::parse_once" in source
    assert "XmlFrontend::next_batch" in source
    assert "kXmlVTable" in source
    assert len(source.splitlines()) <= 500


@pytest.mark.parametrize("invalid_ws", ("\x0b", "\x0c"))
def test_xml_scanners_reject_non_xml_whitespace(tmp_path: Path, invalid_ws: str) -> None:
    """Vertical tab and form feed are not XML 1.0 whitespace."""
    from schema_sanitizer.core_impl.native_runtime import native_core

    path = tmp_path / "invalid-prefix.xml"
    path.write_text(f"{invalid_ws}<event/>", encoding="utf-8")
    with pytest.raises(ValueError, match="expected root element"):
        native_core.xml_folder_effective_row_tag([path], "", -1)


def test_xml_streaming_scanner_is_grouped_by_phase() -> None:
    """The incremental XML scanner must remain split under its owned package."""
    streaming = ROOT / "cpp/src/internal/parsing/streaming"
    scanner = streaming / "xml"
    assert {path.name for path in scanner.iterdir() if path.is_file()} == {
        "row_scanner.cc",
        "row_scanner.hh",
        "row_scanner_buffer.cc",
        "row_scanner_markup.cc",
    }
    assert not (streaming / "xml_row_tag_scanner.cc").exists()
    assert not (streaming / "xml_row_tag_scanner.hh").exists()
    assert not (streaming / "xml_row_tag_scanner_buffer.cc").exists()


def test_xml_token_matching_has_one_header_only_xml_owner() -> None:
    """Shared XML matching lives with XML parsing and has no duplicate TU."""
    parsing = ROOT / "cpp/src/internal/parsing"
    owner = parsing / "xml/token_match.hh"
    old_header = parsing / "streaming/xml_token_match.hh"
    old_source = parsing / "streaming/xml_token_match.cc"
    assert owner.is_file()
    assert not old_header.exists()
    assert not old_source.exists()
    source = owner.read_text(encoding="utf-8")
    assert "std::ranges::all_of" in source
    assert "std::ranges::equal" in source
    assert "std::tolower" not in source
    assert "value == ' '" in source
    manifest = (ROOT / "cmake/SchemaSanitizerSources.cmake").read_text(encoding="utf-8")
    assert "xml_token_match.cc" not in manifest
