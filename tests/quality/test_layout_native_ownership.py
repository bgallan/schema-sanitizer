"""Ownership and layout contracts for native runtime implementation units."""

from __future__ import annotations

import importlib.util
from pathlib import Path

from schema_sanitizer.adapters.parquet.projection.audits.summary import duplicate_names

ROOT = Path(__file__).resolve().parents[2]

CPP = ROOT / "cpp/src"

FOOTER = ROOT / "cpp/src/internal/parquet/footer_reader"

PARQUET_FOOTER_CPP = ROOT / "cpp/src/internal/parquet/footer_reader"

WRITER = ROOT / "cpp/src/internal/parquet/stream_writer"

DIAGNOSTICS = FOOTER / "native_stream/diagnostics"

PARQUET_FOOTER_SCHEMA = FOOTER / "native_stream/schema"


def test_abi3_internal_contracts_have_one_bounded_method_catalogue() -> None:
    """The ABI3 bridge keeps base, capsules, and method declarations direct."""
    abi = ROOT / "cpp/src/internal/abi"
    contracts = abi / "python_abi3"
    assert not (abi / "core_abi3_internal.hh").exists()
    assert {path.name for path in contracts.iterdir() if path.is_file()} == {
        "base.hh",
        "capsules.hh",
        "methods.hh",
    }
    assert not (contracts / "methods").exists()
    assert len((contracts / "methods.hh").read_text(encoding="utf-8").splitlines()) <= 500


def test_abi3_method_declarations_have_one_catalogue() -> None:
    """ABI3 declarations stay in one catalogue instead of six include fragments."""
    abi = CPP / "internal/abi/python_abi3"
    owner = abi / "methods.hh"
    assert owner.is_file()
    assert not (abi / "methods").exists()
    source = owner.read_text(encoding="utf-8")
    assert "py_context_new" in source
    assert "py_options_catalog" in source
    assert "py_schema_registry_merge" in source
    assert "py_context_to_registry_sink_arrow_sources" in source
    assert len(source.splitlines()) <= 500
    production = "\n".join(
        (
            path.read_text(encoding="utf-8")
            for path in CPP.rglob("*")
            if path.is_file() and path.suffix in {".cc", ".cpp", ".hh", ".hpp", ".inc"}
        )
    )
    assert "internal/abi/python_abi3/methods/" not in production


def test_abi3_module_definition_has_no_fragmented_method_catalog() -> None:
    """The ABI3 initializer and table remain a single deduced static owner."""
    owner = ROOT / "cpp/src/api/python_abi3/_core_abi3_module.cc"
    text = owner.read_text(encoding="utf-8")
    assert "std::to_array<PyMethodDef>" in text
    assert "kModuleMethodCount" not in text
    assert "PyMODINIT_FUNC PyInit__core_abi3" in text
    assert len(text.splitlines()) <= 750
    for retired in ("_core_abi3.cc", "_core_abi3_module.hh", "module_methods"):
        assert not (owner.parent / retired).exists()


def test_abi3_module_has_one_compile_time_owner() -> None:
    """Initializer, definition, and method table share one bounded static TU."""
    owner = ROOT / "cpp/src/api/python_abi3/_core_abi3_module.cc"
    implementation = owner.read_text(encoding="utf-8")
    method_entries = implementation.count(".ml_name =")
    assert method_entries >= 98
    assert implementation.count(".ml_meth =") == method_entries
    assert implementation.count(".ml_flags =") == method_entries
    assert implementation.count(".ml_doc =") == method_entries
    assert implementation.count(".ml_name = nullptr") == 1
    assert '"options_with_detected_at"' in implementation
    assert '"options_with_operation_context"' in implementation
    ledger_methods = ("create", "reserve", "reserve_snapshot", "release", "snapshot", "diagnostics")
    assert implementation.count('"operation_memory_ledger_') == len(ledger_methods)
    assert all((f'"operation_memory_ledger_{name}"' in implementation for name in ledger_methods))
    assert '"process_resident_memory_stats"' in implementation
    assert "std::to_array<PyMethodDef>" in implementation
    assert "PyMODINIT_FUNC PyInit__core_abi3" in implementation
    assert "PyModuleDef kModule" in implementation
    assert "module_methods()" not in implementation
    assert len(implementation.splitlines()) <= 750
    retired = (
        owner.with_name("_core_abi3.cc"),
        owner.with_name("_core_abi3_module.hh"),
        owner.parent / "module_methods",
    )
    assert not [path for path in retired if path.exists()]
    manifest = (ROOT / "cmake/SchemaSanitizerSources.cmake").read_text(encoding="utf-8")
    assert manifest.count("cpp/src/api/python_abi3/_core_abi3_module.cc") == 1
    assert "module_methods/module_methods.cc" not in manifest


def test_abi_method_table_has_one_direct_static_owner() -> None:
    """The ABI method table and module definition are initialized in one TU."""
    owner = ROOT / "cpp/src/api/python_abi3/_core_abi3_module.cc"
    implementation = owner.read_text(encoding="utf-8")
    assert "std::to_array<PyMethodDef>" in implementation
    assert "std::ranges::copy" not in implementation
    assert "PyModuleDef kModule" in implementation
    assert "create_module()" in implementation
    assert "kModuleMethodCount" not in implementation
    assert len(implementation.splitlines()) <= 750
    assert not (owner.parent / "module_methods").exists()


def test_call_option_filter_uses_copy_and_key_removal() -> None:
    """Wrapper filtering should use C-level dict copying, not scan every item."""
    from schema_sanitizer.options_impl.call_options import call_options_from_locals

    values = {"input_path": "in", "output_path": "out", "schema_mode": "additive"}
    result = call_options_from_locals(values, frozenset({"input_path", "output_path"}))
    assert result == {"schema_mode": "additive"}
    assert values == {"input_path": "in", "output_path": "out", "schema_mode": "additive"}
    source = (ROOT / "src/schema_sanitizer/options_impl/call_options.py").read_text(
        encoding="utf-8"
    )
    assert "options = values.copy()" in source
    assert "options.pop(key, None)" in source
    assert "for key, value in values.items()" not in source


def test_compact_reader_has_one_bounded_implementation_owner() -> None:
    """Primitive reads and bounded skipping share one cohesive implementation."""
    package = ROOT / "cpp/src/internal/parquet/footer_reader/thrift"
    declaration = package / "compact_reader.hh.inc"
    owner = package / "compact_reader.cc.inc"
    assert declaration.is_file() and owner.is_file()
    assert not (package / "compact_reader_values.cc.inc").exists()
    assert not (package / "compact_reader_skip.cc.inc").exists()
    source = owner.read_text(encoding="utf-8")
    assert "CompactReader::read_varint" in source
    assert "CompactReader::skip_struct" in source
    assert len(source.splitlines()) <= 500


def test_compiled_plan_layout_has_an_independent_builder() -> None:
    """Struct lookup construction must not reconverge with plan recursion."""
    planning = ROOT / "cpp/src/planning"
    internal = ROOT / "cpp/src/internal/planning"
    assert (planning / "struct_layout.cpp").is_file()
    assert (internal / "struct_layout.hh").is_file()
    plan = (planning / "plan.cpp").read_text(encoding="utf-8")
    assert "build_dispatch_table" not in plan
    assert "make_struct_layout" in plan
    manifest = (ROOT / "cmake/SchemaSanitizerSources.cmake").read_text(encoding="utf-8")
    assert "cpp/src/planning/struct_layout.cpp" in manifest


def test_cpp23_contains_is_used_in_path_probe() -> None:
    """The path probe uses the direct C++23 membership operation."""
    source = (ROOT / "cpp/src/api/python_abi3/path_sources/path_source_probe.cc").read_text(
        encoding="utf-8"
    )
    assert ".contains(" in source
    assert "!= std::string_view::npos" not in source


def test_cpp23_contains_replaces_manual_npos_checks() -> None:
    """C++ string membership checks use the direct C++23 API."""
    owners = (
        ROOT / "cpp/src/api/python_abi3/path_sources/path_sources.cc",
        ROOT / "cpp/src/api/python_abi3/registry/path_source_provider.cc",
        ROOT / "cpp/src/internal/json_output/jsonl_value_writer_floating.cc",
    )
    source = "\n".join((path.read_text(encoding="utf-8") for path in owners))
    assert source.count(".contains(") >= 5
    assert "!= std::string::npos" not in source
    assert "== std::string_view::npos" not in source


def test_duplicate_names_is_deterministic_and_linear_by_contract() -> None:
    """The shared duplicate detector returns sorted unique duplicate names."""
    assert duplicate_names(["b", "a", "b", "c", "a", "b"]) == ["a", "b"]
    assert duplicate_names(iter(["only", "once"])) == []


def test_execution_probe_methods_have_one_cohesive_owner() -> None:
    """Probe dispatch should not retain pass-through modules around ABI3 calls."""
    owner = ROOT / "src/schema_sanitizer/core_impl/probes.py"
    assert owner.is_file()
    assert not owner.with_suffix("").exists()
    text = owner.read_text(encoding="utf-8")
    for name in (
        "_ExecutionSchemaProbeMethods",
        "_ExecutionRegistryInputProbeMethods",
        "_ExecutionRegistryPathSourceProbeMethods",
    ):
        assert f"class {name}" in text
    assert "probe_dependencies" not in text
    assert len(text.splitlines()) <= 500


def test_heterogeneous_string_lookup_has_one_cpp_owner() -> None:
    """CSV, JSON, and schema registry hot paths share one transparent hash contract."""
    shared = ROOT / "cpp/src/internal/string_lookup.hh"
    assert shared.is_file()
    consumers = (
        ROOT / "cpp/src/frontends/csv/column_projection.hh",
        ROOT / "cpp/src/frontends/json/root_field_filter.hh",
        ROOT / "cpp/src/schema_registry/schema_registry.cc",
        ROOT / "cpp/src/schema_registry/schema_registry_numeric.cc",
    )
    assert all(
        ('"internal/string_lookup.hh"' in path.read_text(encoding="utf-8") for path in consumers)
    )
    text = "\n".join((path.read_text(encoding="utf-8") for path in consumers))
    assert "CsvProjectionStringHash" not in text
    assert "StringViewHash" not in text


def test_layout_finalization_has_one_direct_owner() -> None:
    """Collision detection, contract maps, and summary assembly stay cohesive."""
    assert (
        importlib.util.find_spec("schema_sanitizer.adapters.parquet.layout.reducer_finalize")
        is None
    )
    layout = ROOT / "src/schema_sanitizer/adapters/parquet/layout"
    owner = layout / "finalization.py"
    assert owner.is_file()
    assert not (layout / "finalization").exists()
    text = owner.read_text(encoding="utf-8")
    for symbol in (
        "def collect_layout_path_collisions",
        "def build_layout_contract_maps",
        "def finalize_recursive_layout_summary",
    ):
        assert symbol in text
    assert len(text.splitlines()) <= 500


def test_low_level_execution_is_one_cohesive_module() -> None:
    """Small ABI3 sink routes share one context owner instead of mixin shells."""
    owner = ROOT / "src/schema_sanitizer/core_impl/execution.py"
    assert owner.is_file()
    assert not owner.with_suffix("").is_dir()
    text = owner.read_text(encoding="utf-8")
    assert "class ExecutionContext" in text
    assert "def to_sink_from_source" in text
    assert "def to_sink_path_sources" in text
    assert "def to_sink_arrow_stream" in text
    assert len(text.splitlines()) <= 500


def test_metadata_column_parsing_has_one_bounded_owner() -> None:
    """Closely coupled metadata parsers stay in one ABI3 translation unit."""
    metadata = ROOT / "cpp/src/api/python_abi3/metadata"
    assert not (metadata / "_core_abi3_metadata_columns.cc").exists()
    assert not (metadata / "_core_abi3_metadata_columns.hh").exists()
    columns = metadata / "columns"
    assert {path.name for path in columns.iterdir()} == {"api.hh", "columns.cc"}
    owner = (columns / "columns.cc").read_text(encoding="utf-8")
    assert "append_row_span_columns_from_dict" in owner
    assert "append_timestamp_columns" in owner
    assert "std::in_range<std::int64_t>" in owner
    assert len(owner.splitlines()) <= 500


def test_metadata_parser_has_one_owner_and_reserves_known_sizes() -> None:
    """Metadata parsing stays cohesive and avoids predictable reallocations."""
    columns = CPP / "api/python_abi3/metadata/columns"
    assert {path.name for path in columns.iterdir()} == {"api.hh", "columns.cc"}
    source = (columns / "columns.cc").read_text(encoding="utf-8")
    assert source.count("out->reserve(") >= 3
    assert "column.spans.reserve(" in source
    assert "std::in_range<std::int64_t>" in source
    assert "append_registry_metadata_columns" in source
    registry_text = "\n".join(
        (
            path.read_text(encoding="utf-8")
            for path in (CPP / "api/python_abi3/registry").glob("*.cc")
        )
    )
    assert registry_text.count("append_registry_metadata_columns") == 13
    assert "append_first_row_columns_from_dict" not in registry_text
    assert "append_timestamp_columns" not in registry_text
    manifest = (ROOT / "cmake/SchemaSanitizerSources.cmake").read_text(encoding="utf-8")
    assert manifest.count("metadata/columns/columns.cc") == 1
    assert "metadata/columns/spans.cc" not in manifest
    assert "metadata/columns/values.cc" not in manifest


def test_native_directory_arguments_live_with_their_consumers() -> None:
    """A two-function argument facade must not reappear under public input."""
    package = ROOT / "src/schema_sanitizer/api_impl/input"
    directory = (package / "directory_preparation.py").read_text(encoding="utf-8")
    native_options = (ROOT / "src/schema_sanitizer/core_impl/native_options.py").read_text(
        encoding="utf-8"
    )
    assert "def _all_files_have_native_paths" in directory
    assert "def optional_memory_limit_arg" in native_options
    assert not (package / "native_arguments.py").exists()


def test_native_options_reuse_object_local_compiled_state() -> None:
    """The options owner retains one compiled capsule until a catalog value changes."""
    owner = (ROOT / "src/schema_sanitizer/core_impl/native_options.py").read_text(encoding="utf-8")
    assert 'object.__setattr__(self, "_prepared_capsule", None)' in owner
    assert "capsule = options._prepared_capsule" in owner
    assert 'object.__setattr__(options, "_prepared_capsule", capsule)' in owner
    assert "_string_list_fingerprint" in owner


def test_native_runtime_loader_is_separate_from_cohesive_option_contract() -> None:
    """ABI3 loading stays neutral while the option domain has one owner."""
    assert importlib.util.find_spec("schema_sanitizer.core_impl.native") is None
    core = ROOT / "src/schema_sanitizer/core_impl"
    runtime = core / "native_runtime.py"
    options = core / "native_options.py"
    assert runtime.is_file() and options.is_file()
    assert not (core / "native_runtime").exists()
    assert not (core / "native_options").exists()
    assert "SchemaEvolutionMode" not in runtime.read_text(encoding="utf-8")
    option_text = options.read_text(encoding="utf-8")
    assert "class SchemaEvolutionMode" in option_text
    assert "class OptionSpec" in option_text


def test_native_stream_decoders_are_grouped_by_data_model() -> None:
    """Native decoders stay cohesive without returning to per-codec fragments.

    Binary list leaves have a separate owner because their offset/value-buffer
    lifecycle differs from fixed-width list leaves and scalar binary columns.
    """
    package = ROOT / "cpp/src/internal/parquet/footer_reader/native_stream/decode"
    assert {path.name for path in package.glob("*.cc.inc")} == {
        "native_stream_binary_columns.cc.inc",
        "native_stream_dictionary_binary_columns.cc.inc",
        "native_stream_dictionary_fixed_columns.cc.inc",
        "native_stream_list_binary_columns.cc.inc",
        "native_stream_list_columns.cc.inc",
        "native_stream_scalar_columns.cc.inc",
    }
    assert all(
        (
            path.read_text(encoding="utf-8").count("sanitize::Status") > 1
            for path in package.glob("*.cc.inc")
        )
    )


def test_null_propagation_uses_precomputed_leaf_ranges() -> None:
    """Null collection writes leaves directly instead of recursively walking nodes."""
    source = (WRITER / "stream_writer_collection.cc.inc").read_text(encoding="utf-8")
    begin = source.index("void emit_nulls_for_subtree")
    end = source.index("\n}\n", begin) + 3
    body = source[begin:end]
    assert "node.first_leaf_index" in body
    assert "node.leaf_count" in body
    assert "for (std::size_t" in body
    assert body.count("emit_nulls_for_subtree") == 1


def test_numeric_primitives_are_split_by_number_family() -> None:
    """Integer parsing and locale-aware floating parsing must compile separately."""
    core = ROOT / "cpp/src/core"
    assert not (core / "primitives_numeric.cpp").exists()
    assert (core / "numeric/integer.cpp").is_file()
    assert (core / "numeric/floating.cpp").is_file()
    sources = (ROOT / "cmake/SchemaSanitizerSources.cmake").read_text(encoding="utf-8")
    assert "cpp/src/core/primitives_numeric.cpp" not in sources
    assert "cpp/src/core/numeric/integer.cpp" in sources
    assert "cpp/src/core/numeric/floating.cpp" in sources


def test_option_deserialization_separates_envelope_and_fields() -> None:
    """SZOPT envelope validation and field decoding remain independent."""
    planning = ROOT / "cpp/src/planning"
    internal = ROOT / "cpp/src/internal/planning"
    assert not (planning / "options_io.cc").exists()
    assert (planning / "options_deserialization.cc").is_file()
    assert (planning / "options_field_deserialization.cc").is_file()
    assert (internal / "options_deserialization.hh").is_file()
    manifest = (ROOT / "cmake/SchemaSanitizerSources.cmake").read_text(encoding="utf-8")
    assert "cpp/src/planning/options_deserialization.cc" in manifest
    assert "cpp/src/planning/options_field_deserialization.cc" in manifest
    assert "cpp/src/planning/options_io.cc" not in manifest


def test_prepared_options_resolution_has_one_abi3_owner() -> None:
    """Default preparation and capsule unwrapping must not be reimplemented per method."""
    c_api = (CPP / "api/c/schema_sanitizer_c.cc").read_text(encoding="utf-8")
    capsules = (CPP / "api/python_abi3/context/_core_abi3_capsules.cc").read_text(encoding="utf-8")
    abi3 = "\n".join(
        (path.read_text(encoding="utf-8") for path in (CPP / "api/python_abi3").rglob("*.cc"))
    )
    assert "static const auto prepared" in c_api
    assert "bool resolve_prepared_options(" in capsules
    assert "resolve_chunk_provider_prepared_options" not in abi3
    assert abi3.count("default_prepared_options()") == 1


def test_recursive_diagnostics_use_one_iterative_snapshot() -> None:
    """Recursive Parquet diagnostics traverse each field tree only once."""
    owner = DIAGNOSTICS / "native_stream_recursive_diagnostics.cc.inc"
    source = owner.read_text(encoding="utf-8")
    output = (DIAGNOSTICS / "native_stream_output_layout.cc.inc").read_text(encoding="utf-8")
    assert "native_recursive_materialization_diagnostics(" in source
    assert "std::vector<NativeRecursiveDiagnosticsFrame> pending" in source
    assert "std::vector<bool> visited" in source
    assert "native_recursive_materialization_diagnostics(field.recursive_tree)" in output
    assert output.count("native_recursive_materialization_diagnostics(") == 1
    assert "native_recursive_materialization_shape_signature(" not in output
    assert "native_recursive_leaf_paths(" not in output
    assert not (DIAGNOSTICS / "native_stream_structural_paths.cc.inc").exists()
    assert not (PARQUET_FOOTER_SCHEMA / "native_stream_schema_diagnostics.cc.inc").exists()
    assert len(source.splitlines()) <= 500


def test_recursive_metadata_validation_is_iterative() -> None:
    """Metadata validation does not consume one C++ stack frame per node."""
    owner = PARQUET_FOOTER_CPP / "native_stream/schema/native_stream_output_layout.cc.inc"
    source = owner.read_text(encoding="utf-8")
    assert "struct NativeRecursiveMetadataValidationState" in source
    assert "while (!pending.empty())" in source
    assert "validate_native_recursive_materialization_node_metadata" not in source
    assert len(source.splitlines()) <= 500


def test_row_appender_uses_prehashed_compiled_column_names() -> None:
    """CSV row adaptation should reuse hashes compiled once with the plan."""
    plan = (ROOT / "cpp/src/sanitize/planning/plan.hh").read_text(encoding="utf-8")
    compile_source = (ROOT / "cpp/src/planning/plan.cpp").read_text(encoding="utf-8")
    row_source = (ROOT / "cpp/src/internal/materialization/row_appender.cc").read_text(
        encoding="utf-8"
    )
    assert "uint64_t name_hash = 0;" in plan
    assert "p.name_hash = sanitize::detail::hash_key64(p.name);" in compile_source
    assert ".key_hash = plan.columns[i].name_hash" in row_source


def test_row_appenders_have_one_bounded_owner() -> None:
    """Closely coupled CSV, JSON, and materialized row adapters share one owner."""
    materialization = ROOT / "cpp/src/internal/materialization"
    owner = materialization / "row_appender.cc"
    assert owner.is_file()
    assert not (materialization / "row_appender").exists()
    assert len(owner.read_text(encoding="utf-8").splitlines()) <= 500


def test_statistics_scan_separates_recursive_values_and_rows() -> None:
    """Inference statistics stay cohesive without mixing row and nested scans."""
    inference = ROOT / "cpp/src/internal/inference"
    package = inference / "statistics"
    for retired in (
        "state.hh",
        "statistics_state.cc",
        "statistics_scan_internal.hh",
        "statistics_scan_nested.cc",
        "statistics_scan_row.cc",
        "statistics_scan.cc",
    ):
        assert not (inference / retired).exists()
    assert {path.name for path in package.iterdir()} == {
        "scan_internal.hh",
        "scan_nested.cc",
        "scan_row.cc",
        "state.cc",
        "state.hh",
    }
    manifest = (ROOT / "cmake/SchemaSanitizerSources.cmake").read_text(encoding="utf-8")
    assert "cpp/src/internal/inference/statistics_scan" not in manifest
    assert "cpp/src/internal/inference/statistics/scan_nested.cc" in manifest
    assert "cpp/src/internal/inference/statistics/scan_row.cc" in manifest
    assert "cpp/src/internal/inference/statistics/state.cc" in manifest


def test_value_view_owns_empty_container_detection_without_message_building() -> None:
    """Empty-container checks stay beside ValueView and use allocation-free cancellation."""
    core = CPP / "core"
    assert not (core / "value_view_empty.cc").exists()
    assert not (core / "value_view_empty.hh").exists()
    source = (core / "value_view.cpp").read_text(encoding="utf-8")
    header = (CPP / "sanitize/core/value_view.hh").read_text(encoding="utf-8")
    assert "Status ValueView::container_is_empty" in source
    assert "Status(StatusCode::kCancelled, {})" in source
    assert "Status::Cancelled" not in source
    assert "Status container_is_empty(bool *out) const" in header


def test_variant_siblings_are_grouped_without_all_pairs_scan() -> None:
    """Unique field families should be annotated in linear expected time."""
    source = (ROOT / "cpp/src/planning/plan.cpp").read_text(encoding="utf-8")
    plan = (ROOT / "cpp/src/sanitize/planning/plan.hh").read_text(encoding="utf-8")
    assert "BorrowedStringLookupMap<std::size_t> family_indices" in source
    assert "family_indices.try_emplace" in source
    assert "std::string variant_family_base" not in plan
    assert "for (std::size_t j = 0; j < columns->size(); ++j)" not in source
    assert "column.variant_sibling_indices = family;" in source
