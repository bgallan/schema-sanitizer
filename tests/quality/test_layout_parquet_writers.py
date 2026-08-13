"""Ownership and layout contracts for Parquet writers and materialization."""

from __future__ import annotations

from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]

FOOTER = ROOT / "cpp/src/internal/parquet/footer_reader"

WRITER = ROOT / "cpp/src/internal/parquet/stream_writer"


def test_delta_binary_writer_uses_fixed_block_storage() -> None:
    """DELTA_BINARY_PACKED encoding must not allocate vectors per block."""
    writer = ROOT / "cpp/src/internal/parquet/stream_writer"
    text = (writer / "stream_writer_value_encodings.cc.inc").read_text(encoding="utf-8")
    assert "std::array<std::int64_t, kBlockSize> block_deltas" in text
    assert "std::array<std::array<std::uint64_t, kMiniBlockSize>" in text
    assert "std::span<const std::uint64_t> values" in text
    assert "std::vector<std::int64_t> deltas" not in text
    assert "std::array<std::vector<std::uint64_t>" not in text


def test_delta_length_encoding_has_no_payload_staging_buffer() -> None:
    """Delta-length byte arrays append source slices after encoding lengths."""
    text = (
        ROOT / "cpp/src/internal/parquet/stream_writer/stream_writer_value_encodings.cc.inc"
    ).read_text(encoding="utf-8")
    assert "std::string bytes" not in text
    assert "out.append(values.substr(offset" in text
    assert "saturating_size_multiply(value_count, sizeof(std::uint32_t))" in text
    assert "std::vector<std::int64_t> lengths" not in text


def test_dictionary_encoding_borrows_page_values() -> None:
    """Dictionary discovery stores views and copies each unique value once."""
    text = (
        ROOT / "cpp/src/internal/parquet/stream_writer/stream_writer_value_encodings.cc.inc"
    ).read_text(encoding="utf-8")
    assert "std::vector<std::string_view> dictionary" in text
    assert "BorrowedStringLookupMap<std::uint32_t> index_by_value" in text
    assert "out.dictionary_values.reserve(dictionary_bytes)" in text
    assert "kInitialUniqueReserve = 4096" in text
    assert "append_item(std::string" not in text
    assert "std::vector<std::string> dictionary" not in text


def test_fixed_width_finish_transfers_column_buffers() -> None:
    """Finishing fixed-width columns must transfer, not duplicate, their vectors."""
    owner = ROOT / "cpp/src/internal/materialization/builders/scalar.cc"
    text = owner.read_text(encoding="utf-8")
    for member in ("f64", "i64", "i32"):
        assert f"payload->{member} = std::move(values_)" in text
    assert ".assign(values_.begin(), values_.end())" not in text


def test_native_parquet_stream_reuses_one_output_layout_plan() -> None:
    """Schema and every row group reuse the output tree and allocation counts."""
    reader = ROOT / "cpp/src/internal/parquet/footer_reader"
    state = (reader / "native_stream/schema/native_stream_arrow_state.cc.inc").read_text(
        encoding="utf-8"
    )
    schema = (reader / "native_stream/schema/native_stream_arrow_schema_root.cc.inc").read_text(
        encoding="utf-8"
    )
    rows = (
        reader / "native_stream/materialization/row_group/native_stream_row_group.cc.inc"
    ).read_text(encoding="utf-8")
    public = (reader / "reporting/footer_reader_public.cc.inc").read_text(encoding="utf-8")
    assert "std::vector<NativeParquetOutputField> output_layout" in state
    assert "bool output_layout_initialized" in state
    assert "recursive_struct_array_count" in state
    assert "recursive_list_array_count" in state
    assert "def initialize_native_stream_output_layout" not in schema
    assert "initialize_native_stream_output_layout(" in schema
    assert "stream->output_layout_initialized = true" in schema
    assert "for (const auto &field : stream->output_layout)" in rows
    assert "build_native_output_layout(row_group.columns" not in rows
    assert "validate_native_recursive_row_group_output_layout(row_group)" not in public


def test_native_projection_moves_unique_column_chunks() -> None:
    """Normal projections avoid deep-copying decoded page and column state."""
    owner = ROOT / "cpp/src/internal/parquet/footer_reader/reporting/footer_reader_public.cc.inc"
    text = owner.read_text(encoding="utf-8")
    assert "duplicate_selection" in text
    assert "selected.push_back(std::move(row_group.columns[index]))" in text
    assert "selected.push_back(row_group.columns[index])" in text


def test_native_snappy_writer_uses_copy_records() -> None:
    """The native Snappy path must perform compression, not only framing."""
    source = (
        ROOT / "cpp/src/internal/parquet/stream_writer/stream_writer_compression.cc.inc"
    ).read_text(encoding="utf-8")
    assert "append_snappy_copy" in source
    assert "snappy_encode_payload" in source
    assert "snappy_encode_literal_payload" not in source
    assert "0x02U" in source
    assert len(source.splitlines()) <= 500


def test_nested_validity_has_one_bounded_materialization_owner() -> None:
    """Container validity remains cohesive without a directory of tiny fragments."""
    materialization = ROOT / "cpp/src/internal/parquet/footer_reader/native_stream/materialization"
    owner = materialization / "native_stream_validity.cc.inc"
    assert owner.is_file()
    assert len(owner.read_text(encoding="utf-8").splitlines()) <= 500
    assert not (materialization / "validity").exists()
    assert not (materialization / "native_stream_nested_validity.cc.inc").exists()
    assert not (materialization / "native_stream_nested_validity.cc.inc").exists()


def test_parquet_dictionary_encoding_borrows_values_without_per_row_indices() -> None:
    """Dictionary indexing retains unique keys and emits row indices directly."""
    writer = (
        ROOT / "cpp/src/internal/parquet/stream_writer/stream_writer_value_encodings.cc.inc"
    ).read_text(encoding="utf-8")
    entry = (ROOT / "cpp/src/internal/parquet/stream_writer/stream_writer.cc").read_text(
        encoding="utf-8"
    )
    assert "BorrowedStringLookupMap<std::uint32_t> index_by_value" in writer
    assert "dictionary.reserve(initial_unique_capacity)" in writer
    assert "std::vector<std::uint32_t> indices" not in writer
    assert "append_rle_run_u32(out.encoded_indices" in writer
    assert '"internal/string_lookup.hh"' in entry


def test_parquet_output_layout_indexes_top_level_fields_once() -> None:
    """Wide nested schemas must not linearly rescan prior output fields per leaf."""
    footer = ROOT / "cpp/src/internal/parquet/footer_reader"
    layout = (footer / "native_stream/schema/native_stream_output_layout.cc.inc").read_text(
        encoding="utf-8"
    )
    validation = layout
    entry = (footer / "footer_reader.cc").read_text(encoding="utf-8")
    assert '"internal/string_lookup.hh"' in entry
    assert "StringLookupMap<std::size_t> *field_index_by_name" in layout
    assert "field_index_by_name->find(top_level_name)" in layout
    assert "field_index_by_name->try_emplace" in layout
    assert "std::find_if(fields->begin(), fields->end()" not in layout
    assert validation.count("field_index_by_name.reserve(") == 2
    assert validation.count("&field_index_by_name") == 2
    assert "std::ranges::sort(recursive_leaf_columns)" not in layout


def test_parquet_stream_plans_layout_once_and_loads_row_groups_lazily() -> None:
    """Stream opening plans metadata once and decodes only the active row group."""
    reader = ROOT / "cpp/src/internal/parquet/footer_reader"
    public = (reader / "reporting/footer_reader_public.cc.inc").read_text(encoding="utf-8")
    row_group = (
        reader / "native_stream/materialization/row_group/native_stream_row_group.cc.inc"
    ).read_text(encoding="utf-8")
    compact_row_group = " ".join(row_group.split())
    schema = (reader / "native_stream/schema/native_stream_arrow_schema_root.cc.inc").read_text(
        encoding="utf-8"
    )
    assert "read_footer_metadata_impl(path, projected_columns)" in public
    assert "initialize_native_stream_output_layout(state.get())" in public
    assert "native_reader_readiness(info, &planned_output_layout)" not in public
    assert "prepare_native_row_group" in row_group
    assert "auto status = read_page_headers(stream->file, &current," in compact_row_group
    assert "release_native_row_group_runtime_state(&row_group)" in row_group
    assert "validate_native_recursive_row_group_output_layout(" in row_group
    assert "finalize_native_stream_output_layout_plan" in schema


def test_parquet_writer_analyzes_schema_once_iteratively() -> None:
    """Leaf ranges, levels, paths, and subtree sizes share one bounded traversal."""
    nodes = (WRITER / "stream_writer_schema_nodes.cc.inc").read_text(encoding="utf-8")
    types = (WRITER / "stream_writer_types.cc.inc").read_text(encoding="utf-8")
    api = (WRITER / "stream_writer_api.cc.inc").read_text(encoding="utf-8")
    assert "std::vector<LeafColumn> analyze_schema(ParquetNode *root)" in nodes
    assert "std::views::reverse" in nodes
    assert "first_leaf_index" in types
    assert "leaf_count" in types
    assert "subtree_schema_element_count" in types
    assert "auto columns = analyze_schema(&root);" in api
    for retired_function in (
        "assign_leaf_indexes(&root)",
        "assign_repetition_levels(&root)",
        "collect_leaf_columns(root)",
    ):
        assert retired_function not in api
    assert not (WRITER / "stream_writer_schema_levels.cc.inc").exists()


def test_parquet_writer_domains_are_consolidated_without_mega_fragments() -> None:
    """Schema nodes, collection, and page handling each have one bounded owner."""
    writer = ROOT / "cpp/src/internal/parquet/stream_writer"
    expected = {
        "stream_writer_schema_nodes.cc.inc",
        "stream_writer_collection.cc.inc",
        "stream_writer_pages.cc.inc",
    }
    for name in expected:
        owner = writer / name
        assert owner.is_file()
        assert len(owner.read_text(encoding="utf-8").splitlines()) <= 500
    for removed in ("schema", "collection", "pages"):
        assert not (writer / removed).exists()


def test_parquet_writer_encoding_phases_have_bounded_owners() -> None:
    """Statistics and adaptive encodings stay consolidated without mega-files."""
    writer = ROOT / "cpp/src/internal/parquet/stream_writer"
    expected = {"stream_writer_statistics.cc.inc": 500, "stream_writer_value_encodings.cc.inc": 500}
    for name, limit in expected.items():
        path = writer / name
        assert path.is_file()
        assert len(path.read_text(encoding="utf-8").splitlines()) <= limit
    for removed in (
        "stream_writer_min_max_statistics.cc.inc",
        "stream_writer_column_statistics.cc.inc",
        "stream_writer_dictionary_encoding.cc.inc",
        "stream_writer_delta_binary_encoding.cc.inc",
        "stream_writer_adaptive_encoding.cc.inc",
    ):
        assert not (writer / removed).exists()


def test_parquet_writer_schema_nodes_share_one_bounded_owner() -> None:
    """Primitive and nested Parquet schema construction stay together without a micro-folder."""
    writer = ROOT / "cpp/src/internal/parquet/stream_writer"
    owner = writer / "stream_writer_schema_nodes.cc.inc"
    assert owner.is_file()
    assert len(owner.read_text(encoding="utf-8").splitlines()) <= 500
    assert not (writer / "schema").exists()


def test_parquet_writer_uses_cxx23_endian_primitives() -> None:
    """Little-endian helpers use the standard C++23 byte-order primitives."""
    writer = ROOT / "cpp/src/internal/parquet/stream_writer"
    entry = (writer / "stream_writer.cc").read_text(encoding="utf-8")
    write_values = (writer / "stream_writer_arrow_values.cc.inc").read_text(encoding="utf-8")
    statistics = (writer / "stream_writer_statistics.cc.inc").read_text(encoding="utf-8")
    assert "#include <bit>" in entry
    assert "std::endian::native" in write_values
    assert "std::byteswap" in write_values
    assert "std::byteswap" in statistics


def test_parquet_writer_value_collection_is_split_by_domain() -> None:
    """Null propagation stays with collection while value encoders remain separate."""
    writer = ROOT / "cpp/src/internal/parquet/stream_writer"
    collection = writer / "stream_writer_collection.cc.inc"
    assert collection.is_file()
    assert "def emit_nulls_for_subtree" not in collection.read_text(encoding="utf-8")
    assert "void emit_nulls_for_subtree" in collection.read_text(encoding="utf-8")
    assert not (writer / "stream_writer_null_collection.cc.inc").exists()
    values = (writer / "stream_writer_arrow_values.cc.inc").read_text(encoding="utf-8")
    assert "append_plain_primitive_value" in values
    assert "append_dictionary_value" in values
    assert len(values.splitlines()) <= 500
    assert not (writer / "stream_writer_plain_values.cc.inc").exists()
    assert not (writer / "stream_writer_dictionary_values.cc.inc").exists()


def test_recursive_layout_fingerprints_are_bundled_once_per_field() -> None:
    """Row-group and final maps share one normalized fingerprint bundle."""
    package = ROOT / "src/schema_sanitizer/adapters/parquet/layout"
    fingerprints = (package / "fingerprints.py").read_text(encoding="utf-8")
    finalization = (package / "finalization.py").read_text(encoding="utf-8")
    assert "class FieldFingerprintBundle" in fingerprints
    assert "def field_fingerprint_bundle(" in fingerprints
    assert "bundle = field_fingerprint_bundle(field)" in finalization
    for duplicate_helper in (
        "leaf_contract_fingerprint_from_field",
        "leaf_contracts_from_field",
        "leaf_level_fingerprint_from_field",
        "leaf_repeated_ancestor_fingerprint_from_field",
        "leaf_repetition_path_fingerprint_from_field",
        "recursive_field_fingerprint_from_field",
        "root_contract_fingerprint_from_field",
        "root_contract_from_field",
    ):
        assert duplicate_helper not in finalization


def test_recursive_layout_helpers_are_direct_bounded_owners() -> None:
    """Field reduction and finalization must not return to micro-packages."""
    layout = ROOT / "src/schema_sanitizer/adapters/parquet/layout"
    for name, symbols in (
        ("fields.py", ("def accumulate_recursive_field", "def leaf_contracts_from_field")),
        (
            "finalization.py",
            (
                "def collect_layout_path_collisions",
                "def build_layout_contract_maps",
                "def finalize_recursive_layout_summary",
            ),
        ),
    ):
        owner = layout / name
        text = owner.read_text(encoding="utf-8")
        assert len(text.splitlines()) <= 500
        for symbol in symbols:
            assert symbol in text
    assert not (layout / "fields").exists()
    assert not (layout / "finalization").exists()


def test_recursive_layout_reducer_has_one_bounded_owner() -> None:
    """Reducer state, validation, and row-group folding stay in one module."""
    package = ROOT / "src/schema_sanitizer/adapters/parquet/layout"
    owner = package / "reducer.py"
    source = owner.read_text(encoding="utf-8")
    assert len(source.splitlines()) <= 500
    for retired in ("reducer_fingerprints.py", "reducer_state.py", "reducer_validation.py"):
        assert not (package / retired).exists()
    assert "field_fingerprint_bundle" in source
    assert "bundles = [field_fingerprint_bundle(field) for field in fields]" in source
    assert "strict=True" in source
    assert "len(set(values))" not in source
    assert "any(value != values[0] for value in values[1:])" in source


def test_recursive_materialization_caches_container_columns() -> None:
    """Container materialization must not rebuild paths or rescan subtree leaves."""
    reader = ROOT / "cpp/src/internal/parquet/footer_reader"
    model = (reader / "native_stream/schema/native_stream_recursive_model.cc.inc").read_text(
        encoding="utf-8"
    )
    output = (reader / "native_stream/schema/native_stream_output_layout.cc.inc").read_text(
        encoding="utf-8"
    )
    children = (
        reader / "native_stream/materialization/native_stream_recursive_children.cc.inc"
    ).read_text(encoding="utf-8")
    containers = (
        reader / "native_stream/materialization/native_stream_recursive_containers.cc.inc"
    ).read_text(encoding="utf-8")
    page_layout = (
        reader / "native_stream/materialization/native_stream_page_layout.cc.inc"
    ).read_text(encoding="utf-8")
    assert "std::optional<std::size_t> layout_column_index" in model
    assert "std::optional<std::int16_t> definition_level" in model
    assert "std::vector<std::size_t> repeated_node_indices" in model
    assert "plan_native_recursive_layout_columns" in output
    assert "select_native_recursive_layout_column_index" in output
    assert "native_recursive_layout_column_for_node" in page_layout
    assert "tree.repeated_node_indices" in page_layout
    combined = "\n".join((children, containers, page_layout))
    for retired in (
        "native_recursive_node_path",
        "definition_level_for_path_prefix",
        "column_path_has_prefix",
    ):
        assert retired not in combined
    assert not (
        reader / "native_stream/schema/native_stream_repeated_layout_validation.cc.inc"
    ).exists()


def test_recursive_struct_materialization_reuses_path_and_scalar_depth() -> None:
    """Struct materialization avoids duplicate tree walks and temporary level vectors."""
    materialization = ROOT / "cpp/src/internal/parquet/footer_reader/native_stream/materialization"
    containers = (materialization / "native_stream_recursive_containers.cc.inc").read_text(
        encoding="utf-8"
    )
    children = (materialization / "native_stream_recursive_children.cc.inc").read_text(
        encoding="utf-8"
    )
    assert "generic_list_defined_levels_from_path(candidate).size()" not in containers
    assert "candidate.max_repetition_level" not in containers
    assert "native_recursive_layout_column_for_node" in containers
    assert "native_recursive_node_path(tree, struct_node_index)" not in children
    assert "struct_node.definition_level" in children
    assert "map_column_indices" not in children


def test_recursive_tree_caches_parent_and_leaf_ranges() -> None:
    """Subtree leaf lookups and node paths reuse one finalized traversal plan."""
    reader = ROOT / "cpp/src/internal/parquet/footer_reader"
    model = (reader / "native_stream/schema/native_stream_recursive_model.cc.inc").read_text(
        encoding="utf-8"
    )
    validation = (
        reader / "native_stream/materialization/native_stream_page_layout.cc.inc"
    ).read_text(encoding="utf-8")
    output = (reader / "native_stream/schema/native_stream_output_layout.cc.inc").read_text(
        encoding="utf-8"
    )
    assert "std::optional<std::size_t> parent_index" in model
    assert "std::size_t subtree_leaf_offset" in model
    assert "std::size_t subtree_leaf_count" in model
    assert "std::vector<std::size_t> leaf_column_indices" in model
    assert "plan_native_recursive_materialization_tree" in model
    assert "std::span<const std::size_t>" in model
    assert "collect_native_recursive_materialization_leaf_columns" not in model
    assert "plan_native_recursive_materialization_tree(" in output
    assert "native_recursive_layout_column_for_node" in validation
    assert "native_recursive_node_path" not in validation
    assert "collect_native_recursive_materialization_leaf_columns" not in validation


def test_recursive_tree_mutation_and_model_analysis_stay_separate() -> None:
    """Tree construction and mutation stay separate from traversal analysis."""
    schema = ROOT / "cpp/src/internal/parquet/footer_reader/native_stream/schema"
    assert not (schema / "native_stream_schema_recursive_merge.cc.inc").exists()
    assert not (schema / "native_stream_recursive_resource_counts.cc.inc").exists()
    tree = schema / "native_stream_recursive_tree.cc.inc"
    model = schema / "native_stream_recursive_model.cc.inc"
    assert tree.is_file()
    assert model.is_file()
    assert "count_native_recursive_materialization" not in tree.read_text(encoding="utf-8")
    assert "merge_native_recursive_materialization" not in model.read_text(encoding="utf-8")
    assert not (schema / "native_stream_recursive_tree_merge.cc.inc").exists()
    assert not (schema / "native_stream_schema_recursive_build.cc.inc").exists()


def test_scalar_materialization_has_one_cohesive_scalar_owner() -> None:
    """Scalar conversion stays visible in one real translation unit."""
    conversion = ROOT / "cpp/src/internal/materialization/conversion"
    owner = conversion / "scalar.cc"
    assert owner.is_file()
    assert not (conversion / "scalar").exists()
    text = owner.read_text(encoding="utf-8")
    assert "convert_bool_scalar" in text
    assert "convert_timestamp_scalar" in text
    assert ".cc.inc" not in text
    assert len(text.splitlines()) <= 500


def test_scalar_materialization_matches_real_translation_units() -> None:
    """Scalar builders and conversion remain cohesive, visible compilation units."""
    materialization = ROOT / "cpp/src/internal/materialization"
    owners = (materialization / "builders/scalar.cc", materialization / "conversion/scalar.cc")
    for owner in owners:
        assert owner.is_file()
        assert not owner.with_suffix("").exists()
        assert len(owner.read_text(encoding="utf-8").splitlines()) <= 500
    assert not list(materialization.rglob("*.cc.inc"))


def test_value_layout_classification_shares_repeated_plan_owner() -> None:
    """Page value classification is not kept in a standalone microfragment."""
    pages = FOOTER / "pages"
    owner = FOOTER / "native_stream/schema/native_stream_repeated_level_layouts.cc.inc"
    source = owner.read_text(encoding="utf-8")
    footer = (FOOTER / "footer_reader.cc").read_text(encoding="utf-8")
    assert "value_buffer_kind_for_page" in source
    assert "arrow_buffer_count_for_value_kind" in source
    assert not (pages / "footer_reader_value_layout.cc.inc").exists()
    assert "footer_reader_value_layout.cc.inc" not in footer
    assert len(source.splitlines()) <= 500
