"""Ownership and layout contracts for Parquet readers and decoding."""

from __future__ import annotations

import importlib.util
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[2]

FOOTER = ROOT / "cpp/src/internal/parquet/footer_reader"

PARQUET_FOOTER_CPP = ROOT / "cpp/src/internal/parquet/footer_reader"

PARQUET_INTERNAL_CPP = ROOT / "cpp/src/internal/parquet"

PARQUET_STREAM_SCHEMA = ROOT / "cpp/src/internal/parquet/footer_reader/native_stream/schema"

SRC = ROOT / "src/schema_sanitizer"


def test_byte_stream_split_uses_cxx23_uninitialized_fill() -> None:
    """BYTE_STREAM_SPLIT fills its output directly through the C++23 string API."""
    text = (
        ROOT / "cpp/src/internal/parquet/stream_writer/stream_writer_value_encodings.cc.inc"
    ).read_text(encoding="utf-8")
    assert "resize_and_overwrite" in text
    assert "std::string out(values.size()," not in text


def test_decompressors_write_into_reusable_outputs() -> None:
    """Snappy and gzip verification decode directly into caller-owned storage."""
    pages = FOOTER / "pages"
    owner = pages / "footer_reader_decompression.cc.inc"
    source = owner.read_text(encoding="utf-8")
    assert "snappy_decompress_payload_into" in source
    assert "sanitize::Result<std::string> snappy_decompress_payload" not in source
    assert "out->clear()" in source
    assert "gzip_decompress_payload_into" in source
    assert "sanitize::Result<std::string> gzip_decompress_payload" not in source
    assert "resize_and_overwrite" in source
    assert not (pages / "footer_reader_compression.cc.inc").exists()
    assert not (pages / "footer_reader_snappy_decode.cc.inc").exists()
    assert len(source.splitlines()) <= 500


def test_delta_binary_packed_decode_has_one_bounded_owner() -> None:
    """Stream decode, preview validation, and page updates stay cohesive."""
    pages = ROOT / "cpp/src/internal/parquet/footer_reader/pages"
    owner = pages / "footer_reader_delta_binary.cc.inc"
    assert owner.is_file()
    assert not (pages / "delta_binary").exists()
    text = owner.read_text(encoding="utf-8")
    for symbol in (
        "decode_delta_binary_packed_stream",
        "decode_delta_binary_packed_values",
        "decode_delta_binary_packed_page",
    ):
        assert symbol in text
    assert len(text.splitlines()) <= 500


def test_delta_binary_reader_has_one_visible_owner() -> None:
    """Decode phases stay together without a three-file include package."""
    pages = ROOT / "cpp/src/internal/parquet/footer_reader/pages"
    owner = pages / "footer_reader_delta_binary.cc.inc"
    assert owner.is_file()
    assert len(owner.read_text(encoding="utf-8").splitlines()) <= 500
    assert not (pages / "delta_binary").exists()


def test_delta_binary_widths_are_a_borrowed_view() -> None:
    """DELTA_BINARY_PACKED must not allocate a bit-width vector per block."""
    stream = (
        ROOT / "cpp/src/internal/parquet/footer_reader/pages/footer_reader_delta_binary.cc.inc"
    ).read_text(encoding="utf-8")
    assert "const auto bit_widths =" in stream
    assert "values.substr(offset" in stream
    assert "std::vector<std::uint8_t> bit_widths" not in stream


def test_direct_parquet_routes_have_one_cohesive_owner() -> None:
    """Direct Parquet state, retry, stream, and sink routing share one owner."""
    assert importlib.util.find_spec("schema_sanitizer.api_impl.parquet.direct") is None
    owner = ROOT / "src/schema_sanitizer/api_impl/parquet/direct_routes.py"
    assert owner.is_file()
    assert not owner.with_suffix("").exists()
    text = owner.read_text(encoding="utf-8")
    for symbol in (
        "def last_parquet_direct_route",
        "def should_retry_native_parquet_reader_failure",
        "def parquet_direct_stream_factory_or_none",
        "def parquet_direct_sink_raw_or_none",
        "def parquet_direct_registry_sink_raw_or_none",
    ):
        assert symbol in text
    assert len(text.splitlines()) <= 500


def test_file_output_path_and_parquet_lifecycle_have_direct_owners() -> None:
    """Output path validation is canonical and the one-use sink facade stays removed."""
    uris = (SRC / "core_impl/uris.py").read_text(encoding="utf-8")
    assert "def local_output_path_or_reject_remote" in uris
    assert not (SRC / "adapters/pyarrow/sink_lifecycle.py").exists()
    sink = (SRC / "adapters/parquet/sink.py").read_text(encoding="utf-8")
    assert 'local_output_path_or_reject_remote(out_path, sink_name="Parquet")' in sink
    assert "writer: Any | None = None" in sink
    assert "if writer is not None:" in sink
    assert "open_pyarrow_output_sink" not in sink
    for relative in (
        "api_impl/file_conversion/direct_writers.py",
        "adapters/pyarrow/csv_sink.py",
        "adapters/pyarrow/jsonl_sink.py",
    ):
        source = (SRC / relative).read_text(encoding="utf-8")
        assert "local_output_path_or_reject_remote" in source
        assert "def _local_output_path" not in source


def test_footer_projection_uses_an_ordered_top_level_index() -> None:
    """Projection must not scan every row-group column for every requested name."""
    owner = ROOT / "cpp/src/internal/parquet/footer_reader/reporting/footer_reader_public.cc.inc"
    text = owner.read_text(encoding="utf-8")
    projection = text.split("sanitize::Status project_footer_row_group_columns(", 1)[1].split(
        "sanitize::Result<ArrowArrayStream *>", 1
    )[0]
    assert "projected_top_level_leaf_indices(" in projection
    assert "std::vector<std::size_t> selected_indices" in projection
    assert "std::ranges::equal_range" not in projection
    assert "std::ranges::sort" not in projection
    assert "for (const auto &column : row_group.columns)" not in projection


def test_footer_reader_contract_has_api_and_model_owners() -> None:
    """Footer declarations and metadata models must not reconverge in one header."""
    parquet = ROOT / "cpp/src/internal/parquet"
    reader = parquet / "footer_reader"
    assert (reader / "api.hh").is_file()
    assert {path.name for path in (reader / "model").iterdir() if path.is_file()} == {
        "column.hh",
        "footer.hh",
        "pages.hh",
        "schema.hh",
    }
    assert not (parquet / "parquet_footer_reader.hh").exists()


def test_footer_reader_schema_has_one_bounded_owner() -> None:
    """Leaf formats, levels, and lookup indexing share one cohesive fragment."""
    reader = ROOT / "cpp/src/internal/parquet/footer_reader"
    owner = reader / "footer_reader_schema.cc.inc"
    assert owner.is_file()
    assert len(owner.read_text(encoding="utf-8").splitlines()) <= 500
    assert not (reader / "schema").exists()
    text = owner.read_text(encoding="utf-8")
    assert "struct SchemaTraversalFrame" in text
    assert "pending.reserve(schema.size())" in text
    assert "row_group.columns[index]" in text
    assert "leaf_path_hash" not in text
    assert "std::ranges::equal_range" not in text


def test_footer_reporting_has_three_cohesive_blocks() -> None:
    """Footer reporting should expose its actual JSON, diagnostics, and public owners."""
    reporting = ROOT / "cpp/src/internal/parquet/footer_reader/reporting"
    assert {path.name for path in reporting.iterdir() if path.is_file()} == {
        "footer_reader_diagnostics_json.cc.inc",
        "footer_reader_json.cc.inc",
        "footer_reader_public.cc.inc",
    }
    assert all(
        (
            len(path.read_text(encoding="utf-8").splitlines()) <= 500
            for path in reporting.iterdir()
            if path.is_file()
        )
    )


def test_footer_root_parser_shares_the_metadata_owner() -> None:
    """The tiny root parser stays with the Thrift row-group/footer reader."""
    reader = ROOT / "cpp/src/internal/parquet/footer_reader"
    owner = reader / "thrift/footer_metadata_row_group_reader.cc.inc"
    source = owner.read_text(encoding="utf-8")
    assert "sanitize::Result<FooterInfo>" in source
    assert "parse_footer(std::string_view footer" in source
    assert "read_row_groups" in source
    assert len(source.splitlines()) <= 500
    assert not (reader / "runtime/footer_reader_footer_parse.cc.inc").exists()


def test_level_decoder_fragments_are_consolidated_but_bounded() -> None:
    """Bitmap, primitive, and stream decoding belong to one cohesive owner."""
    owner = FOOTER / "pages/footer_reader_levels.cc.inc"
    source = owner.read_text(encoding="utf-8")
    assert "initialize_validity_bitmap" in source
    assert "read_varint_from" in source
    assert "decode_level_stream" in source
    assert len(source.splitlines()) <= 500
    for retired in (
        "footer_reader_level_bitmap.cc.inc",
        "footer_reader_level_primitives.cc.inc",
        "footer_reader_level_stream.cc.inc",
    ):
        assert not (FOOTER / "pages" / retired).exists()


def test_native_page_layout_blocks_remain_cohesive() -> None:
    """Value classification shares the repeated-layout phase that consumes it."""
    reader = ROOT / "cpp/src/internal/parquet/footer_reader"
    pages = reader / "pages"
    schema = reader / "native_stream/schema"
    repeated = schema / "native_stream_repeated_level_layouts.cc.inc"
    plans = pages / "footer_reader_native_page_plans.cc.inc"
    assert repeated.is_file()
    assert plans.is_file()
    assert "value_buffer_kind_for_page" in repeated.read_text(encoding="utf-8")
    assert len(repeated.read_text(encoding="utf-8").splitlines()) <= 500
    assert len(plans.read_text(encoding="utf-8").splitlines()) <= 500
    assert not (pages / "footer_reader_value_layout.cc.inc").exists()
    assert not (pages / "footer_reader_page_buffer_layout.cc.inc").exists()
    assert not (pages / "footer_reader_native_page_spans.cc.inc").exists()


def test_native_page_scratch_reuses_decode_buffers() -> None:
    """Per-page dictionary and delta buffers stay owned by stream scratch."""
    footer_reader = ROOT / "cpp/src/internal/parquet/footer_reader"
    state = (footer_reader / "native_stream/schema/native_stream_arrow_state.cc.inc").read_text(
        encoding="utf-8"
    )
    decode = "\n".join(
        (
            path.read_text(encoding="utf-8")
            for path in (footer_reader / "native_stream/decode").glob("*.cc.inc")
        )
    )
    assert "dictionary_indices" in state
    assert "delta_i64_values" in state
    assert "delta_i32_lengths" in state
    assert "scratch->dictionary_indices" in decode
    assert "scratch->delta_i64_values" in decode
    assert "scratch->delta_i32_lengths" in decode
    assert "std::vector<std::uint32_t> indices;" not in decode


def test_native_parquet_contract_gates_have_one_owner() -> None:
    """Nested and writer verdicts share one bounded native-contract module."""
    gates = SRC / "adapters/parquet/contract_gates"
    owner = gates / "native.py"
    source = owner.read_text(encoding="utf-8")
    assert "def _native_nested_contract_status_from_summary" in source
    assert "def _native_parquet_writer_contract_status_from_footer_info" in source
    assert (
        importlib.util.find_spec("schema_sanitizer.adapters.parquet.contract_gates.nested") is None
    )
    assert (
        importlib.util.find_spec("schema_sanitizer.adapters.parquet.contract_gates.writer") is None
    )
    assert len(source.splitlines()) <= 500


def test_native_parquet_schema_state_is_not_monolithic() -> None:
    """Arrow state and the recursive model stay bounded; bit primitives stay generic."""
    package = ROOT / "cpp/src/internal/parquet/footer_reader/native_stream/schema"
    assert not (package / "native_stream_schema_types.cc.inc").exists()
    assert not (package / "native_stream_recursive_types.cc.inc").exists()
    for name in ("native_stream_arrow_state.cc.inc", "native_stream_recursive_model.cc.inc"):
        owner = package / name
        assert owner.is_file()
        assert len(owner.read_text(encoding="utf-8").splitlines()) <= 500
    primitives = (
        ROOT / "cpp/src/internal/parquet/footer_reader/pages/footer_reader_format_primitives.cc.inc"
    )
    assert "bool validity_bit_is_set" in primitives.read_text(encoding="utf-8")
    assert not (package / "native_stream_bitmaps.cc.inc").exists()


def test_native_parquet_value_kind_has_one_enum_representation() -> None:
    """Native-read dispatch must not store and compare heap strings per page."""
    pages = ROOT / "cpp/src/internal/parquet/footer_reader/model/pages.hh"
    column = ROOT / "cpp/src/internal/parquet/footer_reader/model/column.hh"
    model = pages.read_text(encoding="utf-8") + column.read_text(encoding="utf-8")
    assert "enum class NativeValueBufferKind" in model
    assert "std::string value_buffer_kind" not in model
    assert "std::string native_read_value_buffer_kind" not in model
    reader = ROOT / "cpp/src/internal/parquet/footer_reader"
    production = "\n".join(
        (
            path.read_text(encoding="utf-8")
            for path in reader.rglob("*")
            if path.is_file() and path.suffix in {".cc", ".hh", ".inc"}
        )
    )
    assert 'native_read_value_buffer_kind == "' not in production
    assert 'native_read_value_buffer_kind != "' not in production


def test_native_reader_limits_are_owned_by_runtime_and_repeated_schema() -> None:
    """Buffer limits and repeated-path support keep direct bounded owners."""
    footer = ROOT / "cpp/src/internal/parquet/footer_reader"
    assert not (footer / "runtime/footer_reader_native_limits.cc.inc").exists()
    assert (footer / "runtime/native_buffer_limits.cc.inc").is_file()
    repeated = footer / "native_stream/schema/native_stream_repeated_path_support.cc.inc"
    assert repeated.is_file()
    assert not (
        footer / "native_stream/schema/native_stream_generic_repeated_limits.cc.inc"
    ).exists()
    assert not (footer / "native_stream/schema/native_stream_path_support.cc.inc").exists()
    entry = (footer / "footer_reader.cc").read_text(encoding="utf-8")
    assert "footer_reader_native_limits.cc.inc" not in entry
    assert "runtime/native_buffer_limits.cc.inc" in entry
    assert "native_stream/schema/native_stream_repeated_path_support.cc.inc" in entry


def test_page_headers_are_read_with_a_growing_window() -> None:
    """The native reader must not allocate/read the 1 MiB ceiling per page."""
    reader = ROOT / "cpp/src/internal/parquet/footer_reader"
    page_io = (reader / "pages/footer_reader_page_read.cc.inc").read_text(encoding="utf-8")
    footer_reader = (reader / "footer_reader.cc").read_text(encoding="utf-8")
    thrift = reader / "thrift"
    assert "kInitialPageHeaderBytes = 256" in page_io
    assert "while (!parsed.ok() && window_size < maximum_window)" in page_io
    assert "window_size = std::min(maximum_window, window_size * 2)" in page_io
    assert "bytes.resize(window_size)" in page_io
    assert "std::string bytes(maximum_window" not in page_io
    assert not (reader / "pages/footer_reader_page_io.cc.inc").exists()
    assert '#include "thrift/compact_reader.cc.inc"' in footer_reader
    assert (thrift / "compact_reader.cc.inc").is_file()
    assert not (thrift / "compact_reader_values.cc.inc").exists()
    assert not (thrift / "compact_reader_skip.cc.inc").exists()


def test_page_index_validation_uses_filtered_view_without_pointer_vector() -> None:
    """Page-index checks should not allocate a pointer vector for every column."""
    owner = ROOT / "cpp/src/internal/parquet/footer_reader/pages/footer_reader_page_indexes.cc.inc"
    text = owner.read_text(encoding="utf-8")
    assert "std::views::filter" in text
    assert "std::vector<const PageHeaderInfo *>" not in text
    assert not (owner.parent / "footer_reader_page_index_parse.cc.inc").exists()
    assert not (owner.parent / "footer_reader_page_index_validation.cc.inc").exists()


def test_page_read_reuses_header_storage_and_owns_exact_io() -> None:
    """Page scanning has one I/O owner and one reusable header buffer."""
    pages = FOOTER / "pages"
    owner = pages / "footer_reader_page_read.cc.inc"
    scratch_owner = pages / "footer_reader_page_scratch.cc.inc"
    source = owner.read_text(encoding="utf-8")
    scratch = scratch_owner.read_text(encoding="utf-8")
    footer_source = (FOOTER / "footer_reader.cc").read_text(encoding="utf-8")
    assert "std::string &bytes" in source
    assert "std::string page_header_bytes;" in scratch
    assert "PageVerificationScratch" in scratch
    assert "read_page_header_at(file, offset, limit, page_header_bytes)" in source
    assert "resize_and_overwrite" in source
    assert "read_exact_payload_into(file, offset, size, &payload)" in source
    assert not (pages / "footer_reader_page_io.cc.inc").exists()
    assert "footer_reader_page_io.cc.inc" not in footer_source
    assert '#include "pages/footer_reader_page_scratch.cc.inc"' in footer_source
    assert len(source.splitlines()) <= 500
    assert len(scratch.splitlines()) <= 500


def test_page_verification_reuses_buffers_and_moves_decoded_pages() -> None:
    """Footer scanning must not allocate or deep-copy page state per page."""
    owner = FOOTER / "pages/footer_reader_page_read.cc.inc"
    scratch_owner = FOOTER / "pages/footer_reader_page_scratch.cc.inc"
    source = owner.read_text(encoding="utf-8")
    scratch = scratch_owner.read_text(encoding="utf-8")
    assert "struct PageVerificationScratch" in scratch
    assert "PageVerificationScratch scratch;" in source
    assert "&scratch->compressed_payload" in source
    assert "&scratch->decompressed_payload" in source
    assert "column->pages.push_back(std::move(page))" in source
    assert "column->pages.push_back(page)" not in source
    assert "std::string decompressed;" not in source
    assert len(source.splitlines()) <= 500
    assert len(scratch.splitlines()) <= 500


def test_parquet_buffer_reader_receives_original_bytes_like_object() -> None:
    """Buffered Parquet fallback should not materialize a second bytes object."""
    from schema_sanitizer.adapters.parquet.record_batch_factory import open_parquet_source

    payload = memoryview(b"PAR1payloadPAR1")
    received: list[Any] = []

    class FakePa:
        """Minimal PyArrow stand-in recording BufferReader input identity."""

        @staticmethod
        def BufferReader(value: Any) -> Any:
            """Record and return the exact bytes-like input."""
            received.append(value)
            return value

    opened, owned = open_parquet_source(payload, source="text", feature="test", pa=FakePa)
    assert opened is payload
    assert owned is payload
    assert received == [payload]
    non_contiguous = memoryview(bytearray(b"abcdef"))[::2]
    opened, owned = open_parquet_source(non_contiguous, source="text", feature="test", pa=FakePa)
    assert opened == b"ace"
    assert owned == b"ace"
    assert received[-1] == b"ace"


def test_parquet_collection_and_thrift_parsing_have_bounded_phase_owners() -> None:
    """Batch collection and compact-Thrift parsing use bounded phase owners."""
    writer = ROOT / "cpp/src/internal/parquet/stream_writer"
    collection = writer / "stream_writer_collection.cc.inc"
    assert collection.is_file()
    assert len(collection.read_text(encoding="utf-8").splitlines()) <= 500
    assert not (writer / "collection").exists()
    thrift = ROOT / "cpp/src/internal/parquet/footer_reader/thrift"
    assert not (thrift / "thrift_schema_pages.cc.inc").exists()
    assert (thrift / "schema_elements.cc.inc").is_file()
    column_reader = thrift / "footer_metadata_column_reader.cc.inc"
    source = column_reader.read_text(encoding="utf-8")
    assert "read_i32_list" in source
    assert "read_i64_list" in source
    assert not (thrift / "primitive_lists.cc.inc").exists()
    assert len(source.splitlines()) <= 500


def test_parquet_factory_owns_source_and_staging_lifecycle() -> None:
    """Parquet source resolution and temporary staging stay with the factory."""
    package = ROOT / "src/schema_sanitizer/adapters/parquet"
    owner = package / "record_batch_factory.py"
    source = owner.read_text(encoding="utf-8")
    assert owner.is_file()
    assert len(source.splitlines()) <= 1300
    assert "reserve_finalizer_cleanup" in source
    assert "StreamingStorageReservation" in source
    assert "acquire_external_runtime_threads" in source
    for name in (
        "local_parquet_path_or_none",
        "open_parquet_source",
        "local_stream_path",
        "stage_parquet_buffer",
        "remove_staged_parquet",
    ):
        assert f"def {name}(" in source
    assert not (package / "source.py").exists()
    assert not (package / "local_staging.py").exists()
    assert "data.tobytes()" in source
    assert "handle.write(_parquet_buffer(data))" in source


def test_parquet_level_decoding_has_one_bounded_owner() -> None:
    """Closely coupled level bitmap, primitive, and stream logic stay together."""
    pages = ROOT / "cpp/src/internal/parquet/footer_reader/pages"
    owner = pages / "footer_reader_levels.cc.inc"
    assert owner.is_file()
    assert len(owner.read_text(encoding="utf-8").splitlines()) <= 500
    for retired in (
        "footer_reader_level_decoding.cc.inc",
        "footer_reader_level_bitmap.cc.inc",
        "footer_reader_level_primitives.cc.inc",
        "footer_reader_level_stream.cc.inc",
    ):
        assert not (pages / retired).exists()


def test_parquet_memory_policy_owns_native_batch_contract() -> None:
    """Batch sizing and footer row-group limits have one memory-policy owner."""
    parquet = ROOT / "src/schema_sanitizer/adapters/parquet"
    owner = parquet / "memory.py"
    text = owner.read_text(encoding="utf-8")
    assert "def _native_parquet_batch_size_contract_issue" in text
    assert "def _native_parquet_max_row_group_rows" in text
    assert not (parquet / "contract_gates/reader_limits.py").exists()
    assert len(text.splitlines()) <= 500


def test_parquet_multisource_is_one_cohesive_module() -> None:
    """The small Parquet directory domain must not be split into shell packages."""
    owner = ROOT / "src/schema_sanitizer/api_impl/parquet/multisource.py"
    assert owner.is_file()
    assert not owner.with_suffix("").is_dir()
    text = owner.read_text(encoding="utf-8")
    assert "class ParquetDirectorySourceManifest" in text
    assert "def parquet_multisource_registry_sink_raw_or_none" in text
    assert "def infer_parquet_multisource_registry" in text
    assert len(text.splitlines()) <= 500


def test_parquet_multisource_registry_has_one_lazy_provider_route() -> None:
    """Parquet registry output keeps one provider route in its cohesive owner."""
    owner = ROOT / "src/schema_sanitizer/api_impl/parquet/multisource.py"
    text = owner.read_text(encoding="utf-8")
    assert "eager_arrow_sources_sink" not in text
    assert "provider_registry_sink" not in text
    assert "to_registry_sink_arrow_source_chunk_provider_auto_registry" in text
    assert "getattr(_native" not in text


def test_parquet_page_buffer_primitives_have_one_owner() -> None:
    """Arrow buffer sizes live with low-level page and bitmap primitives."""
    pages = ROOT / "cpp/src/internal/parquet/footer_reader/pages"
    owner = pages / "footer_reader_format_primitives.cc.inc"
    text = owner.read_text(encoding="utf-8")
    assert "arrow_i32_offset_buffer_bytes" in text
    assert "bool validity_bit_is_set" in text
    assert not (pages / "footer_reader_arrow_buffer_sizes.cc.inc").exists()
    assert len(text.splitlines()) <= 500


def test_parquet_page_splitter_is_single_pass() -> None:
    """Page boundaries must be computed before materializing each slice once."""
    text = (ROOT / "cpp/src/internal/parquet/stream_writer/stream_writer_pages.cc.inc").read_text(
        encoding="utf-8"
    )
    compact_text = " ".join(text.split()).replace("( ", "(")
    assert "build_page_slice_index" in text
    assert "page_row_incremental_bytes" in text
    assert "ranges.emplace_back" in text
    assert (
        "slice_column_page_data(column, page_data, index, range.begin, range.end, range.value_begin, range.value_end, range.byte_begin, range.byte_end)"
        in compact_text
    )
    assert "ColumnPageData candidate" not in text
    assert "best = std::move(candidate)" not in text


def test_parquet_pipeline_status_is_owned_by_telemetry() -> None:
    """Runtime pipeline verdicts remain next to the diagnostics they inspect."""
    owner = SRC / "adapters/parquet/telemetry.py"
    source = owner.read_text(encoding="utf-8")
    assert "def _parquet_pipeline_contract_status_from_diagnostics" in source
    assert "copy.deepcopy" not in source
    assert not (SRC / "adapters/parquet/contract_gates/pipeline.py").exists()
    assert len(source.splitlines()) <= 500


def test_parquet_python_routes_have_direct_bounded_owners() -> None:
    """Arrow-source and direct-route behavior stay in two cohesive modules."""
    parquet = ROOT / "src/schema_sanitizer/api_impl/parquet"
    for name, symbols in (
        (
            "arrow_sources.py",
            (
                "class ParquetArrowSource",
                "class ParquetArrowSourceChunkProvider",
                "def parquet_arrow_stream_factory_or_none",
            ),
        ),
        (
            "direct_routes.py",
            (
                "def parquet_direct_sink_raw_or_none",
                "def parquet_direct_registry_sink_raw_or_none",
                "def should_retry_native_parquet_reader_failure",
            ),
        ),
    ):
        owner = parquet / name
        text = owner.read_text(encoding="utf-8")
        assert len(text.splitlines()) <= 500
        for symbol in symbols:
            assert symbol in text
    assert not (parquet / "arrow_sources").exists()
    assert not (parquet / "direct_routes").exists()


def test_parquet_reader_python_owners_are_direct_and_cohesive() -> None:
    """Native routing and reusable stream creation must not return to micro-packages."""
    parquet = ROOT / "src/schema_sanitizer/adapters/parquet"
    owners = (parquet / "native_reader.py", parquet / "record_batch_factory.py")
    assert all((owner.is_file() for owner in owners))
    owner_limits = (500, 1300)
    assert all(
        (
            len(owner.read_text(encoding="utf-8").splitlines()) <= limit
            for owner, limit in zip(owners, owner_limits, strict=True)
        )
    )
    assert not (parquet / "native_reader").exists()
    assert not (parquet / "record_batch_factory").exists()
    assert not (parquet / "direct_fallback.py").exists()


def test_parquet_readiness_has_one_bounded_runtime_owner() -> None:
    """Closely related readiness checks stay cohesive without micro-fragments."""
    runtime = ROOT / "cpp/src/internal/parquet/footer_reader/runtime"
    owner = runtime / "native_stream_readiness.cc.inc"
    assert owner.is_file()
    assert len(owner.read_text(encoding="utf-8").splitlines()) <= 500
    assert not (runtime / "readiness").exists()
    assert not (runtime / "footer_reader_readiness_gate.cc.inc").exists()
    assert not (runtime / "footer_reader_readiness_gate.cc.inc").exists()


def test_parquet_record_batch_factory_has_one_direct_owner() -> None:
    """Source preparation, schema, lifecycle, and fallback share one factory owner."""
    owner = ROOT / "src/schema_sanitizer/adapters/parquet/record_batch_factory.py"
    assert owner.is_file()
    assert not owner.with_suffix("").exists()
    assert len(owner.read_text(encoding="utf-8").splitlines()) <= 1300
    assert not (owner.parent / "direct_fallback.py").exists()


def test_parquet_replay_is_lazily_reopened_as_an_ipc_stream() -> None:
    """Fallback replay does not rebuild an in-memory list of all record batches."""
    owner = (ROOT / "src/schema_sanitizer/api_impl/parquet/replay_stream.py").read_text(
        encoding="utf-8"
    )
    assert "ipc.new_stream" in owner
    assert "ipc.open_stream" in owner
    assert "ipc.new_file" not in owner
    assert "ipc.open_file" not in owner
    assert "get_batch(index)" not in owner


def test_parquet_runtime_gates_belong_to_status() -> None:
    """Public status and its private reducers have one bounded owner."""
    owner = SRC / "adapters/parquet/status.py"
    source = owner.read_text(encoding="utf-8")
    assert "def _parquet_contract_runtime_readiness_status_from_capabilities" in source
    assert "def _parquet_preflight_contract_status_from_writer_status" in source
    assert "def _parquet_contract_certification_status_from_parts" in source
    assert "contract_gates.runtime" not in source
    assert (
        importlib.util.find_spec("schema_sanitizer.adapters.parquet.contract_gates.runtime") is None
    )
    assert len(source.splitlines()) <= 500


def test_parquet_schema_leaf_analysis_is_iterative_and_reused() -> None:
    """Deep schemas and projections must avoid recursion and repeated sorting."""
    schema = (FOOTER / "footer_reader_schema.cc.inc").read_text(encoding="utf-8")
    public = (FOOTER / "reporting/footer_reader_public.cc.inc").read_text(encoding="utf-8")
    assert "struct SchemaTraversalFrame" in schema
    assert "pending.reserve(schema.size())" in schema
    assert "collect_leaf_levels(" not in schema
    assert "BorrowedStringLookupMap<std::vector<std::size_t>>" in schema
    assert "projected_top_level_leaf_indices" in schema
    assert "leaf_path_hash" not in schema
    assert "std::ranges::sort" not in schema
    assert "SAN_ASSIGN_OR_RAISE(leaves, assign_column_levels(&info))" in public
    assert "projected_top_level_leaf_indices(" in public
    assert "std::ranges::sort" not in public
    assert public.count("projected_top_level_leaf_indices(") == 1


def test_parquet_statistics_borrow_binary_values_until_final_result() -> None:
    """Min/max collection must allocate only the two persisted statistics values."""
    owner = (
        ROOT / "cpp/src/internal/parquet/stream_writer/stream_writer_statistics.cc.inc"
    ).read_text(encoding="utf-8")
    assert "std::optional<std::string_view> min_value" in owner
    assert "const std::string_view current" in owner
    assert "std::string current" not in owner
    assert "if (has_true && has_false)" in owner


def test_parquet_status_and_telemetry_are_direct_owners() -> None:
    """Status and telemetry must not regress to forwarding subpackages."""
    parquet = ROOT / "src/schema_sanitizer/adapters/parquet"
    assert (parquet / "status.py").is_file()
    assert (parquet / "telemetry.py").is_file()
    assert not (parquet / "status").exists()
    assert not (parquet / "telemetry").exists()
    for module in (
        "schema_sanitizer.adapters.parquet.status",
        "schema_sanitizer.adapters.parquet.telemetry",
    ):
        spec = importlib.util.find_spec(module)
        assert spec is not None
        assert spec.submodule_search_locations is None


def test_recursive_parquet_fields_have_one_direct_owner() -> None:
    """Accumulation, leaf contracts, and fingerprints stay in one bounded module."""
    for module in (
        "schema_sanitizer.adapters.parquet.layout.leaf_contracts",
        "schema_sanitizer.adapters.parquet.layout.reducer_fields",
    ):
        assert importlib.util.find_spec(module) is None
    layout = ROOT / "src/schema_sanitizer/adapters/parquet/layout"
    owner = layout / "fields.py"
    assert owner.is_file()
    assert not (layout / "fields").exists()
    assert not (layout / "leaf_contracts.py").exists()
    assert not (layout / "reducer_fields.py").exists()
    text = owner.read_text(encoding="utf-8")
    for symbol in ("def accumulate_recursive_field", "def leaf_contracts_from_field"):
        assert symbol in text
    assert len(text.splitlines()) <= 500


def test_recursive_parquet_model_uses_bounded_iterative_traversal() -> None:
    """Recursive model analysis must not consume one C++ stack frame per schema node."""
    schema = PARQUET_INTERNAL_CPP / "footer_reader/native_stream/schema"
    model = schema / "native_stream_recursive_model.cc.inc"
    source = model.read_text(encoding="utf-8")
    assert len(source.splitlines()) <= 500
    assert "plan_native_recursive_materialization_tree" in source
    assert "tree->leaf_column_indices.reserve(tree->nodes.size())" in source
    assert "std::vector<bool> entered(tree->nodes.size(), false)" in source
    assert "node.children | std::views::reverse" in source
    assert "std::span<const std::size_t>" in source
    assert "std::vector<std::pair<std::size_t, bool>> pending" in source
    schema_root = (schema / "native_stream_arrow_schema_root.cc.inc").read_text(encoding="utf-8")
    schema_builders = (schema / "native_stream_arrow_schema_builders.cc.inc").read_text(
        encoding="utf-8"
    )
    assert "const auto &subtree_counts = field.recursive_subtree_counts" in schema_root
    assert "count_native_recursive_materialization_subtree_resources(" not in schema_root
    assert "native_recursive_schema_resource_counts(subtree_counts" in schema_builders
    assert "native_recursive_schema_resource_counts(tree" not in schema_builders
    for retired in (
        "native_stream_recursive_types.cc.inc",
        "native_stream_recursive_resource_counts.cc.inc",
        "native_stream_recursive_leaf_columns.cc.inc",
    ):
        assert not (schema / retired).exists()


def test_recursive_parquet_tree_operations_are_iterative_and_bounded() -> None:
    """Path build, annotation, clone, and merge must not recurse by depth."""
    owner = PARQUET_STREAM_SCHEMA / "native_stream_recursive_tree.cc.inc"
    source = owner.read_text(encoding="utf-8")
    assert owner.is_file()
    assert len(source.splitlines()) <= 500
    assert source.count("std::views::reverse") >= 4
    assert "pending.reserve(" in source
    assert "destination->nodes.reserve(" in source
    for retired in (
        "native_stream_schema_recursive_build.cc.inc",
        "native_stream_recursive_tree_merge.cc.inc",
    ):
        assert not (PARQUET_STREAM_SCHEMA / retired).exists()
    for function_name in (
        "assign_native_recursive_repeated_layout_indexes",
        "clone_native_recursive_materialization_subtree",
        "merge_native_recursive_materialization_node",
    ):
        definition = source.index(f"{function_name}(")
        body_start = source.index("{", definition)
        next_definition = source.find("\n}\n\n", body_start)
        body = source[body_start:next_definition]
        assert body.count(f"{function_name}(") == 0
    entry = (PARQUET_STREAM_SCHEMA.parents[1] / "footer_reader.cc").read_text(encoding="utf-8")
    assert "native_stream/schema/native_stream_recursive_tree.cc.inc" in entry
    assert "native_stream_schema_recursive_build.cc.inc" not in entry
    assert "native_stream_recursive_tree_merge.cc.inc" not in entry


def test_recursive_row_group_fingerprints_sort_once() -> None:
    """Every fingerprint family reuses one canonical field order per row group."""
    reducer = ROOT / "src/schema_sanitizer/adapters/parquet/layout/reducer.py"
    text = reducer.read_text(encoding="utf-8")
    assert "canonical_bundles = sorted(named_bundles" in text
    assert text.count("canonical_bundles = sorted(") == 1
    assert "canonical_named_fingerprint" not in text


def test_removed_parquet_staging_modules_are_not_importable() -> None:
    """Retired internal staging modules must not return as compatibility facades."""
    import importlib.util

    assert importlib.util.find_spec("schema_sanitizer.adapters.parquet.source") is None
    assert importlib.util.find_spec("schema_sanitizer.adapters.parquet.local_staging") is None


def test_repeated_layout_validation_uses_cached_page_layout_owner() -> None:
    """Repeated validation reuses cached node plans beside decoded page layouts."""
    owner = PARQUET_FOOTER_CPP / "native_stream/materialization/native_stream_page_layout.cc.inc"
    source = owner.read_text(encoding="utf-8")
    assert "native_recursive_layout_column_for_node" in source
    assert "tree.repeated_node_indices" in source
    assert "native_recursive_node_path" not in source
    assert "column_path_has_prefix" not in source
    assert "generic_list_defined_levels_from_path(candidate)" not in source
    assert len(source.splitlines()) <= 500
    assert not (
        PARQUET_FOOTER_CPP / "native_stream/schema/native_stream_repeated_layout_validation.cc.inc"
    ).exists()
    for retired in (
        PARQUET_FOOTER_CPP
        / "native_stream/materialization/layout/native_stream_recursive_paths.cc.inc",
        PARQUET_FOOTER_CPP
        / "native_stream/materialization/layout/native_stream_repeated_column_selection.cc.inc",
        PARQUET_FOOTER_CPP / "native_stream/schema/native_stream_row_group_validation.cc.inc",
    ):
        assert not retired.exists()


def test_repeated_leaf_read_planning_shares_level_decoder_owner() -> None:
    """Repeated page observations and level layout assembly form one bounded phase."""
    schema = ROOT / "cpp/src/internal/parquet/footer_reader/native_stream/schema"
    owner = schema / "native_stream_repeated_level_layouts.cc.inc"
    source = owner.read_text(encoding="utf-8")
    assert "struct RepeatedLeafReadPlanState" in source
    assert "observe_repeated_leaf_data_page" in source
    assert "assign_repeated_leaf_native_read_plan" in source
    assert "assign_simple_list_level_layout" in source
    assert not (schema / "native_stream_repeated_leaf_read_plan.cc.inc").exists()
    assert len(source.splitlines()) <= 500


def test_repeated_path_support_has_one_iterative_cpp_owner() -> None:
    """Repeated-path planning and limits share one bounded non-recursive unit."""
    schema = ROOT / "cpp/src/internal/parquet/footer_reader/native_stream/schema"
    owner = schema / "native_stream_repeated_path_support.cc.inc"
    assert owner.is_file()
    text = owner.read_text(encoding="utf-8")
    assert len(text.splitlines()) <= 500
    assert "struct NativeRecursiveSupportValidationFrame" in text
    assert "pending.reserve(tree.nodes.size())" in text
    assert "std::views::reverse" in text
    assert "validate_native_recursive_materialization_node_supported" not in text
    assert not (schema / "native_stream_path_support.cc.inc").exists()
    assert not (schema / "native_stream_generic_repeated_limits.cc.inc").exists()


def test_runtime_parquet_gate_snapshots_keep_inputs_defensive() -> None:
    """Contract reports copy the mutable lists they expose to callers."""
    from schema_sanitizer.adapters.parquet.status import (
        _parquet_contract_certification_status_from_parts,
        _parquet_preflight_contract_status_from_writer_status,
    )

    writer = {
        "applicable": False,
        "satisfied": False,
        "issues": ["external writer"],
        "nested_contract_issues": ["not applicable"],
    }
    preflight = _parquet_preflight_contract_status_from_writer_status(
        writer, pyarrow_available=True
    )
    projection = {"stable": False, "mismatches": ["drift"]}
    certificate = _parquet_contract_certification_status_from_parts(
        preflight_status=preflight, writer_status=writer, projection_audit=projection
    )
    certificate["preflight_status"]["issues"].append("caller mutation")
    certificate["native_writer_status"]["issues"].append("caller mutation")
    certificate["projection_audit"]["mismatches"].append("caller mutation")
    assert preflight["issues"] == []
    assert writer["issues"] == ["external writer"]
    assert projection["mismatches"] == ["drift"]


def test_runtime_parquet_gates_share_one_owner() -> None:
    """Preflight and certification compose in one bounded runtime owner."""
    owner = SRC / "adapters/parquet/status.py"
    source = owner.read_text(encoding="utf-8")
    assert "def _parquet_contract_runtime_readiness_status_from_capabilities" in source
    assert "def _parquet_preflight_contract_status_from_writer_status" in source
    assert "def _parquet_contract_certification_status_from_parts" in source
    assert "copy.deepcopy" not in source
    assert not (SRC / "adapters/parquet/contract_gates/readiness.py").exists()
    assert not (SRC / "adapters/parquet/contract_gates/certification.py").exists()
    assert not (SRC / "adapters/parquet/contract_gates/runtime.py").exists()
    assert len(source.splitlines()) <= 500


def test_stream_callbacks_live_with_row_group_runtime() -> None:
    """The tiny Arrow stream callbacks stay with their only runtime consumer."""
    reader = ROOT / "cpp/src/internal/parquet/footer_reader"
    owner = reader / "native_stream/materialization/row_group/native_stream_row_group.cc.inc"
    source = owner.read_text(encoding="utf-8")
    assert "native_parquet_stream_get_schema" in source
    assert "native_parquet_stream_get_next" in source
    assert "native_parquet_stream_release" in source
    assert len(source.splitlines()) <= 500
    assert not (reader / "runtime/footer_reader_stream_callbacks.cc.inc").exists()


def test_thrift_primitive_lists_share_column_metadata_owner() -> None:
    """Tiny primitive-list parsing does not remain a detached fragment."""
    owner = FOOTER / "thrift/footer_metadata_column_reader.cc.inc"
    source = owner.read_text(encoding="utf-8")
    assert "read_i32_list" in source
    assert "read_i64_list" in source
    assert "read_column_metadata" in source
    assert not (FOOTER / "thrift/primitive_lists.cc.inc").exists()
    assert len(source.splitlines()) <= 500
