"""Ownership and layout contracts for schema and inference units."""

from __future__ import annotations

from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]

CPP = ROOT / "cpp/src"


def test_arrow_direct_schema_parser_has_explicit_subsystem() -> None:
    """Arrow schema dispatch, nested parsing, and payload encoding stay separate."""
    package = ROOT / "cpp/src/api/python_abi3/arrow_direct/schema"
    assert {path.name for path in package.iterdir() if path.is_file()} == {
        "logical.cc",
        "logical.hh",
        "nested.cc",
        "parser_internal.hh",
        "payload.cc",
        "payload.hh",
        "type.cc",
    }
    parent = package.parent
    for retired in (
        "_core_abi3_arrow_direct_schema.cc",
        "_core_abi3_arrow_direct_schema.hh",
        "_core_abi3_arrow_direct_schema_payload.cc",
    ):
        assert not (parent / retired).exists()


def test_cpp23_enum_serialization_uses_to_underlying() -> None:
    """Parquet enum serialization should use the C++23 enum conversion utility."""
    source = (
        CPP / "internal/parquet/stream_writer/stream_writer_schema_elements.cc.inc"
    ).read_text(encoding="utf-8")
    assert source.count("std::to_underlying") >= 4
    assert "static_cast<std::int32_t>(node.physical_type)" not in source


def test_enum_coercion_has_one_python_validator() -> None:
    """Grouped options and wire encoding must share enum coercion."""
    enums = (ROOT / "src/schema_sanitizer/core_impl/native_options.py").read_text(encoding="utf-8")
    groups = (ROOT / "src/schema_sanitizer/options_impl/options.py").read_text(encoding="utf-8")
    assert "def coerce_enum_member" in enums
    assert "coerce_enum_member(" in groups
    assert "_ENUM_VALUES_BY_OPTION_NAME" not in groups
    assert "def _norm_enum" not in groups


def test_enum_validation_uses_portable_search_and_underlying_values() -> None:
    """Native wire enums avoid repeated comparisons and unavailable range algorithms."""
    owner = (ROOT / "cpp/src/planning/options_field_deserialization.cc").read_text(encoding="utf-8")
    assert "std::find(allowed.cbegin(), allowed.cend(), value)" in owner
    assert "std::ranges::contains" not in owner
    assert "std::to_underlying" in owner


def test_field_name_planning_has_one_native_owner() -> None:
    """Policy, collision, and recursive schema naming share one implementation."""
    internal = ROOT / "cpp/src/internal/planning"
    planning = ROOT / "cpp/src/planning"
    owner = planning / "field_name_sanitizer.cc"
    contract = internal / "field_name_sanitizer.hh"
    source = owner.read_text(encoding="utf-8")
    assert owner.is_file()
    assert contract.is_file()
    assert len(source.splitlines()) <= 500
    assert "std::pmr::unordered_map<std::string_view, std::size_t" in source
    assert "base_counts(resource)" in source
    assert "std::pmr::unordered_set<std::string_view" in source
    assert "used(resource)" in source
    assert "base_counts.size() == dirty_names.size()" in source
    assert "sanitize_logical_schema_field_names" in source
    assert "out.reserve(base.size() + length)" in source
    removed = (
        internal / "field_name_collision.cc",
        internal / "field_name_collision.hh",
        internal / "field_name_policy.cc",
        internal / "field_name_policy.hh",
        planning / "logical_field_name_sanitizer.cc",
    )
    assert not [path for path in removed if path.exists()]
    manifest = (ROOT / "cmake/SchemaSanitizerSources.cmake").read_text(encoding="utf-8")
    assert "logical_field_name_sanitizer.cc" not in manifest
    assert "field_name_collision.cc" not in manifest
    assert "field_name_policy.cc" not in manifest


def test_inference_statistics_are_grouped_by_responsibility() -> None:
    """State and recursive scans stay in their cohesive C++ subsystem."""
    package = ROOT / "cpp/src/internal/inference/statistics"
    assert {path.name for path in package.iterdir()} == {
        "scan_internal.hh",
        "scan_nested.cc",
        "scan_row.cc",
        "state.cc",
        "state.hh",
    }
    inference = package.parent
    assert not [
        path.name
        for path in inference.iterdir()
        if path.name.startswith(("statistics_scan", "statistics_state"))
    ]


def test_inference_statistics_have_one_canonical_child_store() -> None:
    """The wide-field dispatch index must reference, not duplicate, child entries."""
    state = (ROOT / "cpp/src/internal/inference/statistics/state.hh").read_text(encoding="utf-8")
    implementation = (ROOT / "cpp/src/internal/inference/statistics/state.cc").read_text(
        encoding="utf-8"
    )
    assert "std::pmr::vector<ChildEntry> children" in state
    assert "std::pmr::vector<uint32_t> slots" in state
    assert "std::vector<uint64_t> hashes" not in state
    assert "std::vector<StrId> keys" not in state
    assert "std::vector<StatsNode *> values" not in state
    assert "std::construct_at(node, arena)" in implementation
    assert "build_from(entries)" in implementation


def test_json_writer_schema_has_explicit_subsystem() -> None:
    """Schema parsing, format mapping, and array validation stay separate."""
    package = ROOT / "cpp/src/internal/json_output/schema"
    assert {path.name for path in package.iterdir() if path.is_file()} == {
        "array_validation.cc",
        "field.cc",
        "format.cc",
        "model.hh",
    }
    assert not (ROOT / "cpp/src/internal/json_output/jsonl_stream_writer_schema.cc").exists()
    assert not (ROOT / "cpp/src/internal/json_output/jsonl_stream_writer_schema.hh").exists()


def test_logical_field_name_traversal_shares_the_field_name_owner() -> None:
    """Recursive naming and collision policy form one bounded native domain."""
    planning = ROOT / "cpp/src/planning"
    owner = planning / "field_name_sanitizer.cc"
    source = owner.read_text(encoding="utf-8")
    assert owner.is_file()
    assert len(source.splitlines()) <= 500
    assert "sanitize_logical_schema_field_names" in source
    assert "std::pmr::unordered_map<std::string_view, std::size_t" in source
    assert "base_counts(resource)" in source
    assert not (planning / "logical_field_name_sanitizer.cc").exists()


def test_logical_schema_contract_has_one_python_owner() -> None:
    """Logical-schema payloads must not live under the options subsystem."""
    core = ROOT / "src/schema_sanitizer/core_impl"
    owner = core / "logical_schema.py"
    assert owner.is_file()
    assert "class LogicalSchemaPayload" in owner.read_text(encoding="utf-8")
    assert not (core / "native_options").exists()
    assert (
        "LogicalSchemaPayload"
        not in (core / "native_options.py")
        .read_text(encoding="utf-8")
        .split("from .logical_schema", 1)[0]
    )


def test_logical_schema_wire_codec_has_one_native_owner() -> None:
    """ABI3 probes and registry methods must share one schema wire codec."""
    codec = ROOT / "cpp/src/internal/planning/options_schema_serialization.cc"
    codec_text = codec.read_text(encoding="utf-8")
    assert "serialize_logical_schema_bytes" in codec_text
    assert "std::to_underlying" in codec_text
    consumers = (
        ROOT / "cpp/src/api/python_abi3/probes/schema_probe.cc",
        ROOT / "cpp/src/api/python_abi3/registry/arrow_source_sinks.cc",
        ROOT / "cpp/src/api/python_abi3/registry/schema_registry_methods.cc",
    )
    for consumer in consumers:
        text = consumer.read_text(encoding="utf-8")
        assert "void append_logical_type" not in text
        assert "std::string encode_logical_schema" not in text
    retired = ROOT / "cpp/src/api/python_abi3/registry/schema_registry"
    assert not (retired / "payload_codec.cc").exists()
    assert not (retired / "payload_codec.hh").exists()


def test_native_probe_results_share_lazy_schema_payload_state() -> None:
    """Schema and registry probes must not duplicate lazy payload decoding."""
    owner = (ROOT / "src/schema_sanitizer/core_impl/native_results.py").read_text(encoding="utf-8")
    assert "class _LazySchemaPayloadResult" in owner
    assert "class SchemaProbeResult(_LazySchemaPayloadResult)" in owner
    assert "class RegistryProbeResult(_LazySchemaPayloadResult)" in owner
    assert owner.count("def field_names") == 1
    assert owner.count("def schema_payload") == 1


def test_native_schema_contract_codec_has_explicit_owners() -> None:
    """Logical payload conversion and option-wire encoding stay separate."""
    assert not (ROOT / "src/schema_sanitizer/core_impl/native_options").exists()
    core = ROOT / "src/schema_sanitizer/core_impl"
    logical_schema = (core / "logical_schema.py").read_text(encoding="utf-8")
    option_wire = (core / "native_options.py").read_text(encoding="utf-8")
    assert "class LogicalSchemaPayload" in logical_schema
    assert "def encode_arrow_schema_payload" in logical_schema
    assert "def pyarrow_schema_from_payload" in logical_schema
    assert "def _append_schema" in option_wire
    assert "def _append_schema" not in logical_schema


def test_provider_schema_merge_reuses_chunk_vectors() -> None:
    """Chunked registry probes should retain vector capacity across chunks."""
    registry = ROOT / "cpp/src/api/python_abi3/registry"
    arrow = (registry / "arrow_source_provider.cc").read_text(encoding="utf-8")
    paths = (registry / "path_source_provider.cc").read_text(encoding="utf-8")
    assert arrow.count("std::vector<ArrowSourceSpec> sources;") == 1
    assert paths.count("std::vector<PathSourceSpec> sources;") == 1
    assert "std::vector<ArrowSourceSpec> next_sources;" not in arrow
    assert "decref_arrow_sources(&state->sources);" in arrow


def test_schema_evolution_and_ordering_share_one_indexed_owner() -> None:
    """Schema reconciliation must stay linear and avoid a second ordering TU."""
    owner = ROOT / "cpp/src/planning/schema_evolution.cc"
    text = owner.read_text(encoding="utf-8")
    assert len(text.splitlines()) <= 500
    assert "BorrowedStringLookupMap" in text
    assert text.count("build_field_map(") >= 4
    assert "find_field(" not in text
    assert not (owner.parent / "schema_field_order.cc").exists()


def test_schema_probe_abi_has_one_visible_translation_unit() -> None:
    """Schema probe implementation should match its actual compilation unit."""
    package = ROOT / "cpp/src/api/python_abi3/probes"
    assert {path.name for path in package.iterdir() if path.is_file()} == {
        "schema_probe.cc",
        "schema_probe_methods.cc",
        "schema_probe_internal.hh",
    }
    implementation = (package / "schema_probe.cc").read_text(encoding="utf-8")
    methods = (package / "schema_probe_methods.cc").read_text(encoding="utf-8")
    assert "merge_path_source_schemas" in implementation
    assert "py_context_registry_probe_from_path_sources" in methods
    assert "py_context_registry_probe_from_path_source_chunk_provider" in methods
    assert len(implementation.splitlines()) <= 500
    assert len(methods.splitlines()) <= 500
    assert not list(package.rglob("*.inc"))


def test_schema_probe_matches_its_real_translation_unit() -> None:
    """The ABI3 probe unit must not hide its implementation in include fragments."""
    package = ROOT / "cpp/src/api/python_abi3/probes"
    owners = tuple(
        (
            package / name
            for name in ("schema_probe.cc", "schema_probe_methods.cc", "schema_probe_internal.hh")
        )
    )
    assert {path.name for path in package.iterdir() if path.is_file()} == {
        owner.name for owner in owners
    }
    assert all((len(owner.read_text(encoding="utf-8").splitlines()) <= 500 for owner in owners))
    assert not list(package.rglob("*.inc"))


def test_versioned_field_names_have_one_exact_renderer() -> None:
    """Registry generation and numeric canonicalization share one renderer."""
    helper = (CPP / "internal/planning/variant_field_names.cc").read_text(encoding="utf-8")
    header = (CPP / "internal/planning/variant_field_names.hh").read_text(encoding="utf-8")
    registry = (CPP / "schema_registry/schema_registry.cc").read_text(encoding="utf-8")
    numeric = (CPP / "schema_registry/schema_registry_numeric.cc").read_text(encoding="utf-8")
    assert "make_versioned_field_name" in helper
    assert "make_versioned_field_name" in header
    assert "std::unreachable()" in helper
    assert "std::to_string(version)" not in registry
    assert 'canonical_name.append("_v")' not in numeric
    assert registry.count("make_versioned_field_name") == 1
    assert numeric.count("make_versioned_field_name") == 1


def test_xml_frontend_and_field_hashing_have_single_native_owners() -> None:
    """XML lifecycle stays together and field hashes are cached in the node model."""
    frontend_dir = ROOT / "cpp/src/frontends/xml"
    assert {path.name for path in frontend_dir.iterdir()} == {"frontend.cc", "frontend_internal.hh"}
    frontend = (frontend_dir / "frontend.cc").read_text(encoding="utf-8")
    document = (ROOT / "cpp/src/internal/parsing/xml/document.hh").read_text(encoding="utf-8")
    model = (ROOT / "cpp/src/internal/parsing/xml_value_model.cc").read_text(encoding="utf-8")
    assert len(frontend.splitlines()) <= 500
    assert "default_key_hash_" in frontend
    assert "std::uint64_t key_hash" in document
    assert "field.key_hash" in model
    manifest = (ROOT / "cmake/SchemaSanitizerSources.cmake").read_text(encoding="utf-8")
    assert "frontends/xml/frontend.cc" in manifest
    assert "frontends/xml/frontend_batch.cc" not in manifest
    assert "frontends/xml/frontend_lifecycle.cc" not in manifest
