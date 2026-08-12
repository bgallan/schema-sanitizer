"""Ownership and layout contracts for registry implementation units."""

from __future__ import annotations

from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]

CPP = ROOT / "cpp/src"


def test_core_registry_methods_are_direct_owner_modules() -> None:
    """Registry methods must not regress into a forwarding package."""
    core = ROOT / "src/schema_sanitizer/core_impl"
    assert not (core / "registry").exists()
    assert not (core / "registry_sources.py").exists()
    owner = core / "registry_sinks.py"
    source = owner.read_text(encoding="utf-8")
    for owner_type in (
        "_RegistryArrowSinkMethods",
        "_RegistryPathProviderSinkMethods",
        "_RegistryPathSourceSinkMethods",
    ):
        assert f"class {owner_type}" in source
    assert "_registry_sink_output" in source
    assert len(source.splitlines()) <= 500
    assert not (core / "registry_arrow.py").exists()
    assert not (core / "registry_paths.py").exists()
    execution = (core / "execution.py").read_text(encoding="utf-8")
    assert "def _call_native_registry_sink_from_source" in execution
    assert "_registry_sink_output" in execution
    assert len(execution.splitlines()) <= 500


def test_later_registry_chunks_do_not_copy_first_row_values() -> None:
    """Null metadata on later chunks must not first copy large registry JSON strings."""
    source = (ROOT / "cpp/src/api/python_abi3/registry/registry_stream_metadata.cc").read_text(
        encoding="utf-8"
    )
    assert "for (const auto &source : first_row_columns)" in source
    assert "if (first_row_pending) {\n      column.value = source.value;" in source
    assert "for (auto column : first_row_columns)" not in source
    assert "column.value.clear()" not in source


def test_numeric_registry_planning_borrows_names_before_moving_fields() -> None:
    """Numeric normalization avoids an owned string and a second parse per field."""
    source = (CPP / "schema_registry/schema_registry_numeric.cc").read_text(encoding="utf-8")
    assert "BorrowedStringLookupMap<std::size_t> plan_index_by_family" in source
    assert "std::vector<std::size_t> plan_by_field" in source
    assert "StringLookupMap<NumericFamilyPlan>" not in source
    second_pass = source[source.index("std::vector<LogicalField> out") :]
    assert "family_base_name(field)" not in second_pass


def test_registry_has_no_python_fallback_head() -> None:
    """Registry creation and contract extraction must be native-only."""
    registry = (ROOT / "src/schema_sanitizer/core_impl/schema_registry.py").read_text(
        encoding="utf-8"
    )
    contract_body = registry.split("def schema_contract_from_registry_json", 1)[1].split(
        "def native_registry_state_from_json", 1
    )[0]
    symbols = (ROOT / "src/schema_sanitizer/core_impl/native_symbols.py").read_text(
        encoding="utf-8"
    )
    assert 'getattr(_native, "schema_registry_empty"' not in registry
    assert 'getattr(_native, "schema_registry_contract_payload"' not in registry
    assert "ensure_pyarrow" not in contract_body
    assert "_merge_schema_registry_json" not in contract_body
    assert "REGISTRY_STATE_FROM_JSON" not in symbols


def test_registry_json_array_append_has_one_owner() -> None:
    """Arrow and path providers must share the same drift-array appender."""
    registry = ROOT / "cpp/src/api/python_abi3/registry"
    definitions = []
    for path in registry.rglob("*"):
        if path.is_file() and path.suffix in {".cc", ".inc"}:
            text = path.read_text(encoding="utf-8")
            if "void append_json_array_items" in text:
                definitions.append(path)
    assert definitions == [registry / "registry_stream_metadata.cc"]
    text = definitions[0].read_text(encoding="utf-8")
    assert "out->reserve(out->size() + delimiter_size + array_json.size())" in text


def test_registry_method_units_are_cmake_sources() -> None:
    """Every public registry method unit must compile independently."""
    cmake = (ROOT / "cmake/SchemaSanitizerSources.cmake").read_text(encoding="utf-8")
    for owner in (
        "registry/arrow_source_registry_methods.cc",
        "registry/arrow_source_provider_methods.cc",
        "registry/arrow_source_probe_methods.cc",
        "registry/path_source_input_methods.cc",
        "registry/path_source_registry_methods.cc",
        "registry/path_source_auto_methods.cc",
    ):
        assert owner in cmake
    assert "registry/arrow_source_methods.cc" not in cmake
    assert "registry/path_source_methods.cc" not in cmake


def test_registry_numeric_normalization_is_a_direct_bounded_unit() -> None:
    """The final oversized C++ registry owner stays split at a real phase boundary."""
    package = ROOT / "cpp/src/schema_registry"
    recursive = (package / "schema_registry.cc").read_text(encoding="utf-8")
    numeric = (package / "schema_registry_numeric.cc").read_text(encoding="utf-8")
    assert len(recursive.splitlines()) <= 500
    assert len(numeric.splitlines()) <= 500
    assert "void normalize_integer_float_schema" not in recursive
    assert "void normalize_integer_float_schema" in numeric
    assert "BorrowedStringLookupMap<std::size_t> emitted_by_name" in numeric
    assert "std::vector<NumericFamilyPlan> plans" in numeric
    assert "BorrowedStringLookupMap<std::size_t> plan_index_by_family" in numeric
    assert "std::vector<std::size_t> plan_by_field" in numeric
    assert "positions_by_family" not in numeric


def test_registry_output_is_one_direct_owner() -> None:
    """Registry file routing must not return to format-specific pass-through modules."""
    owner = ROOT / "src/schema_sanitizer/api_impl/registry_output.py"
    assert owner.is_file()
    assert not owner.with_suffix("").exists()
    text = owner.read_text(encoding="utf-8")
    assert "def write_registry_raw_stream_to_file" in text
    assert "def write_parquet_registry_file" in text
    assert "def write_jsonl_registry_file" in text
    assert "def write_csv_registry_file" in text
    assert len(text.splitlines()) <= 500


def test_registry_path_sink_methods_have_one_direct_owner() -> None:
    """Path collections and lazy providers must not regain pass-through packages."""
    owner = ROOT / "src/schema_sanitizer/core_impl/registry_sinks.py"
    assert owner.is_file()
    assert not owner.with_suffix("").exists()
    source = owner.read_text(encoding="utf-8")
    assert "class _RegistryPathProviderSinkMethods" in source
    assert "class _RegistryPathSourceSinkMethods" in source
    assert "call_native_registry_sink" not in source
    assert len(source.splitlines()) <= 500


def test_registry_plan_has_one_bounded_native_owner() -> None:
    """Plan construction, capsule ownership, and parsing stay in one cohesive unit."""
    package = ROOT / "cpp/src/api/python_abi3/registry/plan"
    assert {path.name for path in package.iterdir()} == {"plan.cc", "plan.hh"}
    source = (package / "plan.cc").read_text(encoding="utf-8")
    header = (package / "plan.hh").read_text(encoding="utf-8")
    assert len(source.splitlines()) <= 500
    assert "make_native_registry_plan" in source
    assert "wrap_native_registry_state" in source
    assert "py_registry_state_from_json" in source
    assert "struct NativeRegistryPlan" in header
    manifest = (ROOT / "cmake/SchemaSanitizerSources.cmake").read_text(encoding="utf-8")
    assert "registry/plan/plan.cc" in manifest
    for retired in ("model.cc", "capsule.cc", "python_method.cc"):
        assert f"registry/plan/{retired}" not in manifest


def test_registry_plan_stays_consolidated_without_forwarding_headers() -> None:
    """The registry plan has one implementation and one direct contract."""
    package = ROOT / "cpp/src/api/python_abi3/registry/plan"
    assert {path.name for path in package.iterdir()} == {"plan.cc", "plan.hh"}
    source = (package / "plan.cc").read_text(encoding="utf-8")
    assert len(source.splitlines()) <= 500
    assert "std::ranges::find_if" in source
    for retired in ("capsule.hh", "model.hh", "capsule.cc", "model.cc", "python_method.cc"):
        assert not (package / retired).exists()


def test_registry_provider_code_uses_real_compilation_units() -> None:
    """Registry providers must not return to textual or oversized public units."""
    registry = ROOT / "cpp/src/api/python_abi3/registry"
    runtime_owners = ("arrow_source_sinks.cc", "path_source_sinks.cc")
    method_owners = (
        "arrow_source_registry_methods.cc",
        "arrow_source_provider_methods.cc",
        "arrow_source_probe_methods.cc",
        "path_source_input_methods.cc",
        "path_source_registry_methods.cc",
        "path_source_auto_methods.cc",
    )
    for name in runtime_owners:
        source = registry / name
        assert source.is_file()
        assert len(source.read_text(encoding="utf-8").splitlines()) <= 1000
    for name in method_owners:
        source = registry / name
        assert source.is_file()
        assert len(source.read_text(encoding="utf-8").splitlines()) <= 500
    assert not (registry / "arrow_source_methods.cc").exists()
    assert not (registry / "path_source_methods.cc").exists()
    assert not list(registry.rglob("*.cc.inc"))


def test_registry_provider_helpers_live_in_real_compilation_units() -> None:
    """Registry providers and public methods use explicit compilation units."""
    registry = ROOT / "cpp/src/api/python_abi3/registry"
    runtime_owners = (registry / "arrow_source_sinks.cc", registry / "path_source_sinks.cc")
    method_owners = (
        registry / "arrow_source_registry_methods.cc",
        registry / "arrow_source_provider_methods.cc",
        registry / "arrow_source_probe_methods.cc",
        registry / "path_source_input_methods.cc",
        registry / "path_source_registry_methods.cc",
        registry / "path_source_auto_methods.cc",
    )
    assert all((owner.is_file() for owner in (*runtime_owners, *method_owners)))
    assert all(
        (len(owner.read_text(encoding="utf-8").splitlines()) <= 1000 for owner in runtime_owners)
    )
    assert all(
        (len(owner.read_text(encoding="utf-8").splitlines()) <= 500 for owner in method_owners)
    )
    assert not (registry / "arrow_source_methods.cc").exists()
    assert not (registry / "path_source_methods.cc").exists()
    assert not (registry / "arrow_source_sinks").exists()
    assert not (registry / "path_source_sinks").exists()


def test_registry_public_methods_are_small_real_units() -> None:
    """Public ABI methods remain split by operation rather than textual fragments."""
    registry = ROOT / "cpp/src/api/python_abi3/registry"
    owners = {
        "arrow_source_registry_methods.cc",
        "arrow_source_provider_methods.cc",
        "arrow_source_probe_methods.cc",
        "path_source_input_methods.cc",
        "path_source_registry_methods.cc",
        "path_source_auto_methods.cc",
    }
    for name in owners:
        owner = registry / name
        assert owner.is_file()
        assert len(owner.read_text(encoding="utf-8").splitlines()) <= 500
    assert not (registry / "arrow_source_methods.cc").exists()
    assert not (registry / "path_source_methods.cc").exists()


def test_registry_results_use_the_current_explicit_state_contract() -> None:
    """Registry consumers must not accept pre-state tuple or attribute variants."""
    owners = (
        ROOT / "src/schema_sanitizer/core_impl/native_results.py",
        ROOT / "src/schema_sanitizer/pipeline/registry_warmup.py",
        ROOT / "src/schema_sanitizer/pipeline/partition_execution.py",
        ROOT / "src/schema_sanitizer/api_impl/source_plan/probing.py",
        ROOT / "src/schema_sanitizer/api_impl/source_plan/registry.py",
        ROOT / "src/schema_sanitizer/api_impl/registry_output.py",
        ROOT / "src/schema_sanitizer/api_impl/analytical.py",
    )
    owner_text = "\n".join((path.read_text(encoding="utf-8") for path in owners))
    assert 'getattr(raw, "native_registry_state"' not in owner_text
    assert 'getattr(opened, "native_registry_state"' not in owner_text
    assert 'getattr(result, "native_registry_state"' not in owner_text
    assert "*extra = native_result" not in owner_text
    assert "parsed_registry =" not in owner_text


def test_registry_sink_abi_has_one_fixed_tuple_shape() -> None:
    """Every registry sink returns metadata plus an explicit state slot."""
    packing = (ROOT / "cpp/src/api/python_abi3/sinks/_core_abi3_sink_result_packing.cc").read_text(
        encoding="utf-8"
    )
    state_packing = (
        ROOT / "cpp/src/api/python_abi3/registry/registry_stream_metadata.cc"
    ).read_text(encoding="utf-8")
    wrapper = (ROOT / "src/schema_sanitizer/core_impl/native_results.py").read_text(
        encoding="utf-8"
    )
    assert "PyTuple_New(6)" in packing
    assert "native_registry_state ? native_registry_state : Py_None" in packing
    assert "PyTuple_New(6)" not in state_packing
    assert "conversion_timestamp, state" in state_packing
    assert "*extra = native_result" not in wrapper


def test_registry_sink_methods_share_one_bounded_owner() -> None:
    """Arrow and path registry routes must not return to one-consumer modules."""
    core = ROOT / "src/schema_sanitizer/core_impl"
    owner = core / "registry_sinks.py"
    source = owner.read_text(encoding="utf-8")
    for owner_type in (
        "_RegistryArrowSinkMethods",
        "_RegistryPathProviderSinkMethods",
        "_RegistryPathSourceSinkMethods",
    ):
        assert f"class {owner_type}" in source
    execution = (core / "execution.py").read_text(encoding="utf-8")
    assert "from .registry_sinks import (" in execution
    assert not (core / "registry_arrow.py").exists()
    assert not (core / "registry_paths.py").exists()
    assert len(source.splitlines()) <= 500


def test_registry_sink_routes_call_abi_directly() -> None:
    """Registry methods must not regain parallel call-wrapper packages."""
    core = ROOT / "src/schema_sanitizer/core_impl"
    owner = core / "registry_sinks.py"
    source = owner.read_text(encoding="utf-8")
    for owner_type in (
        "_RegistryArrowSinkMethods",
        "_RegistryPathProviderSinkMethods",
        "_RegistryPathSourceSinkMethods",
    ):
        assert f"class {owner_type}" in source
    assert "native_core as _native" in source
    assert len(source.splitlines()) <= 500
    assert not (core / "registry_arrow.py").exists()
    assert not (core / "registry_paths.py").exists()
    execution = (core / "execution.py").read_text(encoding="utf-8")
    assert "def to_registry_sink_from_source" in execution
    assert "context_to_registry_sink_from_source" in execution
    assert not (core / "registry_sources.py").exists()
    assert not (core / "registry").exists()


def test_registry_state_endpoints_are_grouped_in_the_path_methods_unit() -> None:
    """Explicit registry-state path methods share one focused compilation unit."""
    registry = ROOT / "cpp/src/api/python_abi3/registry"
    methods = registry / "path_source_registry_methods.cc"
    source = methods.read_text(encoding="utf-8")
    assert "py_context_to_registry_sink_from_path_sources_registry_state" in source
    assert "py_context_to_registry_sink_from_path_source_chunk_provider_registry_state" in source
    assert not (registry / "path_source_methods.cc").exists()
    assert len(source.splitlines()) <= 500


def test_registry_state_has_one_parse_and_one_native_call() -> None:
    """Registry state must not probe canonical presence before extraction or compilation."""
    python_owner = (ROOT / "src/schema_sanitizer/core_impl/schema_registry.py").read_text(
        encoding="utf-8"
    )
    model = (ROOT / "cpp/src/api/python_abi3/registry/plan/plan.cc").read_text(encoding="utf-8")
    methods = (ROOT / "cpp/src/api/python_abi3/_core_abi3_module.cc").read_text(encoding="utf-8")
    query = (ROOT / "cpp/src/api/python_abi3/registry/schema_registry_methods.cc").read_text(
        encoding="utf-8"
    )
    contract_body = python_owner.split("def schema_contract_from_registry_json", 1)[1].split(
        "def native_registry_state_from_json", 1
    )[0]
    state_body = python_owner.split("def native_registry_state_from_json", 1)[1].split(
        "_NATIVE_REGISTRY_STATE", 1
    )[0]
    assert "field_name_policy" not in contract_body
    assert "field_name_policy" not in state_body
    assert state_body.count("_native.registry_state_from_json") == 1
    assert "_registry_has_canonical_schema" not in python_owner
    assert "schema_registry_has_canonical_schema" not in python_owner
    assert model.count("canonical_schema_from_registry_json") == 1
    assert "schema_registry_has_canonical_schema" not in model
    assert "merge_schema_registry(" not in model
    assert "schema_registry_has_canonical_schema" not in methods
    assert "py_schema_registry_has_canonical_schema" not in query
    assert "Py_RETURN_NONE" in query
    assert "std::ranges::find_if" in query


def test_registry_state_result_is_packed_without_an_intermediate_tuple() -> None:
    """The state-aware packer must delegate directly to the six-slot owner."""
    metadata = (ROOT / "cpp/src/api/python_abi3/registry/registry_stream_metadata.cc").read_text(
        encoding="utf-8"
    )
    sink_packing = (
        ROOT / "cpp/src/api/python_abi3/sinks/_core_abi3_sink_result_packing.cc"
    ).read_text(encoding="utf-8")
    assert "PyObject *base = pack_registry_stream_result" not in metadata
    assert "conversion_timestamp, state" in metadata
    assert "PyTuple_New(6)" in sink_packing
    assert "native_registry_state ? native_registry_state : Py_None" in sink_packing


def test_registry_stream_validation_does_not_allocate_sink_names() -> None:
    """Registry stream-only methods compare borrowed sink names directly."""
    registry = ROOT / "cpp/src/api/python_abi3/registry"
    sources = "\n".join((path.read_text(encoding="utf-8") for path in registry.glob("*.cc")))
    assert 'std::string(sink_name) != "stream"' not in sources
    assert sources.count('std::string_view(sink_name) != "stream"') >= 7


def test_registry_warmup_has_one_direct_owner() -> None:
    """Warm-up input preparation and inference remain one cohesive workflow."""
    pipeline = ROOT / "src/schema_sanitizer/pipeline"
    owner = pipeline / "registry_warmup.py"
    source = owner.read_text(encoding="utf-8")
    assert owner.is_file()
    assert len(source.splitlines()) <= 500
    assert not (pipeline / "registry_warmup").exists()
    assert "def prepare_schema_warm_up_input(" in source
    assert "def infer_warm_up_schema_registry_state(" in source


def test_remote_registry_probing_has_one_native_owner() -> None:
    """Remote registry inference and chunk ownership share one direct module."""
    owner = ROOT / "src/schema_sanitizer/api_impl/source_plan/remote.py"
    assert owner.is_file()
    assert not owner.with_suffix("").exists()
    text = owner.read_text(encoding="utf-8")
    assert "class RemotePathSourceChunkProvider" in text
    assert "def probe_remote_registry" in text
    assert "registry_probe_path_source_chunk_provider" in text
    assert "staged_probe" not in text


def test_remote_registry_stream_has_one_current_native_route() -> None:
    """Current ABI ownership must not retain auto/bounded compatibility strategies."""
    owner_remote = ROOT / "src/schema_sanitizer/api_impl/source_plan/remote.py"
    assert owner_remote.is_file()
    assert not owner_remote.with_suffix("").exists()
    owner = ROOT / "src/schema_sanitizer/api_impl/source_plan/registry.py"
    text = owner.read_text(encoding="utf-8")
    assert "to_registry_sink_path_source_chunk_provider_auto_registry" in text
    assert "getattr(_native" not in text
    assert "supports_auto_registry" not in text
    assert len(text.splitlines()) <= 500


def test_schema_registry_json_writers_are_split_by_document_kind() -> None:
    """Registry documents and drift event arrays must use separate units."""
    package = ROOT / "cpp/src/schema_registry"
    assert not (package / "schema_registry_json_write.cc").exists()
    assert (package / "schema_registry_document_json.cc").is_file()
    assert (package / "schema_registry_drift_json.cc").is_file()
    assert "DriftEvent" not in (package / "schema_registry_document_json.cc").read_text(
        encoding="utf-8"
    )


def test_schema_registry_merge_has_explicit_recursive_and_numeric_owners() -> None:
    """Registry recursion and numeric-family normalization stay in direct source files."""
    package = ROOT / "cpp/src/schema_registry"
    owner = package / "schema_registry.cc"
    numeric = package / "schema_registry_numeric.cc"
    assert owner.is_file()
    assert numeric.is_file()
    assert not list(package.glob("schema_registry_*.cc.inc"))
    source = owner.read_text(encoding="utf-8")
    numeric_source = numeric.read_text(encoding="utf-8")
    assert len(source.splitlines()) <= 500
    assert len(numeric_source.splitlines()) <= 500
    assert "std::ranges::find_if" in source
    assert "out.reserve(maximum_field_count)" in source
    assert "build_field_merge_index(out, maximum_field_count)" in source
    assert "StringLookupMap<VariantFamilyIndex> families" in source
    assert "normalize_integer_float_schema" not in source
    assert "void normalize_integer_float_schema" in numeric_source
    assert "std::vector<NumericFamilyPlan> plans" in numeric_source
    assert "BorrowedStringLookupMap<std::size_t> plan_index_by_family" in numeric_source
    assert "std::vector<std::size_t> plan_by_field" in numeric_source
    assert "positions_by_family" not in numeric_source
    assert "variants_by_base" not in source
    assert "next_variant_version" not in source
    entry = (package / "schema_registry_entry.cc").read_text(encoding="utf-8")
    assert len(entry.splitlines()) <= 500
    assert "drifts.reserve(input.inferred_schema.fields.size())" in entry


def test_schema_registry_methods_share_static_empty_payloads() -> None:
    """Registry query and merge methods must share one owner and no JSON builder."""
    package = ROOT / "cpp/src/api/python_abi3/registry"
    owner = package / "schema_registry_methods.cc"
    source = owner.read_text(encoding="utf-8")
    assert len(source.splitlines()) <= 500
    assert "kEmptyRegistryPayloads" in source
    assert "std::ranges::find_if" in source
    assert "append_string_field" not in source
    assert not (package / "schema_registry").exists()


def test_schema_registry_uses_one_version_name_parser_and_borrowed_indexes() -> None:
    """Registry merge loops borrow stable names and use the planning parser directly."""
    package = ROOT / "cpp/src/schema_registry"
    header = (package / "schema_registry_internal.hh").read_text(encoding="utf-8")
    merge = (package / "schema_registry.cc").read_text(encoding="utf-8")
    numeric = (package / "schema_registry_numeric.cc").read_text(encoding="utf-8")
    types = (package / "schema_registry_types.cc").read_text(encoding="utf-8")
    assert "variant_base_name" not in header
    assert "variant_version" not in header
    assert "parse_versioned_field_name" in merge
    assert "variant_family_base" in merge
    assert merge.count("BorrowedStringLookupMap") >= 3
    assert "BorrowedStringLookupMap<std::size_t> emitted_by_name" in numeric
    assert "std::string_view\nsource_segment_for_output" in types


def test_source_plan_registry_and_probing_are_direct_owners() -> None:
    """Source-plan lifecycle and probing must not regain router packages."""
    package = ROOT / "src/schema_sanitizer/api_impl/source_plan"
    expected = {
        "registry.py": (
            "class OpenedSourcePlanRegistryStream",
            "def open_source_plan_registry_stream",
            "def materialize_opened_registry_stream",
            "def write_source_plan_registry_to_file",
        ),
        "probing.py": (
            "def probe_source_plan_registry",
            "def _probe_sequence_registry",
            "def probe_prepared_source_plan_registry",
        ),
    }
    for filename, symbols in expected.items():
        owner = package / filename
        assert owner.is_file()
        assert not owner.with_suffix("").exists()
        text = owner.read_text(encoding="utf-8")
        for symbol in symbols:
            assert symbol in text
        assert len(text.splitlines()) <= 500


def test_source_plan_registry_has_one_cohesive_owner() -> None:
    """Opening, materialization, and output share one explicit registry owner."""
    owner = ROOT / "src/schema_sanitizer/api_impl/source_plan/registry.py"
    assert owner.is_file()
    assert not owner.with_suffix("").exists()
    text = owner.read_text(encoding="utf-8")
    assert "class OpenedSourcePlanRegistryStream" in text
    assert "def open_source_plan_registry_stream" in text
    assert "def materialize_opened_registry_stream" in text
    assert "def write_source_plan_registry_to_file" in text
    assert len(text.splitlines()) <= 500


def test_source_selected_registry_routes_live_with_execution_context() -> None:
    """A one-consumer registry mixin must not split the execution ABI owner."""
    core = ROOT / "src/schema_sanitizer/core_impl"
    owner = core / "execution.py"
    source = owner.read_text(encoding="utf-8")
    assert "class ExecutionContext" in source
    assert "def _call_native_registry_sink_from_source" in source
    assert "def to_registry_sink_from_source" in source
    assert "context_to_registry_sink_from_source" in source
    assert "_RegistrySourceSinkMethods" not in source
    assert not (core / "registry_sources.py").exists()
    assert len(source.splitlines()) <= 500
