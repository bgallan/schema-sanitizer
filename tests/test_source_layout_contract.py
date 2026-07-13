"""Protect source ownership boundaries established by maintenance passes."""

from __future__ import annotations

import re
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def test_removed_flat_python_modules_stay_absent() -> None:
    """Removed provider and source-plan compatibility surfaces stay absent."""
    removed = (
        "src/schema_sanitizer/api_impl/remote",
        "src/schema_sanitizer/adapters/parquet/status/schema_support.py",
        "src/schema_sanitizer/api_impl/source_plan/prepared_composite.py",
        "src/schema_sanitizer/api_impl/source_plan/prepared_local.py",
        "src/schema_sanitizer/api_impl/source_plan/prepared_plan.py",
        "src/schema_sanitizer/api_impl/source_plan/prepared_sources.py",
        "src/schema_sanitizer/api_impl/source_plan/opened_registry.py",
        "src/schema_sanitizer/api_impl/source_plan/registry_drifts.py",
        "src/schema_sanitizer/api_impl/source_plan/registry_file_output.py",
        "src/schema_sanitizer/api_impl/source_plan/registry_output.py",
        "src/schema_sanitizer/api_impl/source_plan/registry_stream.py",
        "src/schema_sanitizer/api_impl/source_plan/probe_remote.py",
        "src/schema_sanitizer/api_impl/source_plan/remote_provider.py",
        "src/schema_sanitizer/api_impl/source_plan/remote_registry_streams.py",
    )
    assert not [relative for relative in removed if (ROOT / relative).exists()]


def test_cpp_source_names_describe_ownership() -> None:
    """Production C++ fragments must not regress to numbered or anonymous names."""
    disallowed = re.compile(r"(?:anonymous|part\d+|public_\d+)", re.IGNORECASE)
    offenders = [
        path.relative_to(ROOT).as_posix()
        for path in (ROOT / "cpp" / "src").rglob("*")
        if path.is_file() and disallowed.search(path.name)
    ]
    assert not offenders


def test_cpp_fragments_are_not_include_forwarders() -> None:
    """A C++ implementation fragment must contain behavior, not only includes."""
    offenders: list[str] = []
    for path in (ROOT / "cpp" / "src").rglob("*.cc.inc"):
        meaningful = [
            line.strip()
            for line in path.read_text(encoding="utf-8").splitlines()
            if line.strip() and not line.lstrip().startswith(("//", "/*", "*", "*/"))
        ]
        if meaningful and all(line.startswith("#include") for line in meaningful):
            offenders.append(path.relative_to(ROOT).as_posix())
    assert not offenders


def test_split_parquet_fragments_stay_retired() -> None:
    """Parquet phases must not collapse back into mixed implementation fragments."""
    removed = (
        "cpp/src/internal/parquet/stream_writer/stream_writer_collect_encode.cc.inc",
        "cpp/src/internal/parquet/footer_reader/footer_reader_dictionary_pages.cc.inc",
    )
    assert not [relative for relative in removed if (ROOT / relative).exists()]


def test_native_options_are_owned_by_one_cohesive_core_module() -> None:
    """Catalog, enum validation, wire encoding, and capsule reuse share one owner."""
    core = ROOT / "src/schema_sanitizer/core_impl"
    native_options = core / "native_options.py"
    assert native_options.is_file()
    assert not (core / "native_options").exists()
    text = native_options.read_text(encoding="utf-8")
    assert "class OptionSpec" in text
    assert "class Options" in text
    assert "def _encode_options_bytes" in text
    assert "def _options_capsule" in text
    assert "_prepared_capsule" in text
    assert not list(core.glob("options_*.py"))
    assert not (ROOT / "src/schema_sanitizer/options_impl/catalog.py").exists()
    core_text = "\n".join(path.read_text(encoding="utf-8") for path in core.rglob("*.py"))
    assert "options_impl" not in core_text


def test_footer_reader_fragments_are_grouped_by_responsibility() -> None:
    """The footer-reader root must remain an entry point, not a flat fragment dump."""
    footer_reader = ROOT / "cpp/src/internal/parquet/footer_reader"
    assert {path.name for path in footer_reader.iterdir() if path.is_file()} == {
        "api.hh",
        "footer_reader.cc",
        "footer_reader_schema.cc.inc",
    }
    for directory in (
        "model",
        "native_stream",
        "pages",
        "reporting",
        "runtime",
        "thrift",
    ):
        assert (footer_reader / directory).is_dir()


def test_retired_mixed_footer_reader_fragments_stay_absent() -> None:
    """Independent codecs and recursive phases must not collapse into mixed files."""
    removed = (
        "cpp/src/internal/parquet/footer_reader/pages/footer_reader_delta_byte_stream.cc.inc",
        "cpp/src/internal/parquet/footer_reader/native_stream/schema/native_stream_structure_validation.cc.inc",
    )
    assert not [relative for relative in removed if (ROOT / relative).exists()]


def test_schema_registry_is_not_owned_by_api_implementation() -> None:
    """Shared registry services must remain independent of the public API orchestration layer."""
    registry = ROOT / "src/schema_sanitizer/core_impl/schema_registry.py"
    assert registry.is_file()
    assert not (ROOT / "src/schema_sanitizer/schema_registry_impl").exists()
    assert not (ROOT / "src/schema_sanitizer/api_impl/schema_registry").exists()
    assert not (ROOT / "src/schema_sanitizer/api_impl/core_errors.py").exists()
    assert not (ROOT / "src/schema_sanitizer/api_impl/file_conversion/metadata.py").exists()


def test_json_ondemand_parser_is_a_cohesive_subsystem() -> None:
    """On-demand JSON parsing must not return to the flat mixed parsing directory."""
    parser = ROOT / "cpp/src/internal/parsing/json/ondemand"
    assert {path.name for path in parser.iterdir() if path.is_file()} == {
        "array_iteration.cc",
        "document.cc",
        "document.hh",
        "lex.cc",
        "object_iteration.cc",
        "scalar.cc",
        "scan.cc",
        "scan.hh",
        "value.cc",
        "value_iteration.cc",
    }
    removed = (
        "cpp/src/internal/parsing/json_ondemand.hh",
        "cpp/src/internal/parsing/json_ondemand_iteration.cc",
        "cpp/src/internal/parsing/json_ondemand_lex.cc",
        "cpp/src/internal/parsing/json_ondemand_scan.cc",
        "cpp/src/internal/parsing/json_ondemand_scan.hh",
        "cpp/src/internal/parsing/json_ondemand_value.cc",
        "cpp/src/internal/parsing/json_string_decode.cc",
        "cpp/src/internal/parsing/json_string_decode.hh",
    )
    assert not [relative for relative in removed if (ROOT / relative).exists()]


def test_ingest_stream_is_split_by_runtime_responsibility() -> None:
    """The ingest stream must keep construction separate from its batching loop."""
    stream = ROOT / "cpp/src/internal/materialization/ingest_stream"
    assert {path.name for path in stream.iterdir() if path.is_file()} == {
        "batching.cc",
        "source.cc",
        "source.hh",
        "source_internal.hh",
    }
    assert not (ROOT / "cpp/src/internal/materialization/stream_output.cc").exists()
    assert not (ROOT / "cpp/src/internal/materialization/stream_output.hh").exists()


def test_remote_io_is_owned_by_a_neutral_subsystem() -> None:
    """Remote providers and cohesive staging must not live under API orchestration."""
    remote = ROOT / "src/schema_sanitizer/remote_impl"
    assert {
        path.name for path in remote.iterdir() if path.is_dir() and path.name != "__pycache__"
    } == {"providers"}
    providers = remote / "providers"
    assert {path.name for path in providers.glob("*.py")} == {
        "__init__.py",
        "azure.py",
        "gcs.py",
        "s3.py",
    }
    assert not [
        path for path in providers.iterdir() if path.is_dir() and path.name != "__pycache__"
    ]
    staging = remote / "staging.py"
    assert staging.is_file()
    assert len(staging.read_text(encoding="utf-8").splitlines()) <= 500
    assert not (remote / "staging").exists()
    assert not (ROOT / "src/schema_sanitizer/api_impl/remote").exists()
    for retired in ("gcs", "s3", "azure", "http.py", "local_staging.py", "output.py"):
        assert not (remote / retired).exists()


def test_materialization_is_grouped_by_phase() -> None:
    """Arrow builders and value conversion expose cohesive phase owners."""
    materialization = ROOT / "cpp/src/internal/materialization"
    builders = materialization / "builders"
    assert {path.name for path in builders.iterdir()} == {
        "detail.hh",
        "factory.cc",
        "nested.cc",
        "scalar.cc",
    }
    assert not (builders / "scalar").exists()
    assert len((builders / "scalar.cc").read_text(encoding="utf-8").splitlines()) <= 500

    conversion = materialization / "conversion"
    assert {path.name for path in conversion.iterdir()} == {
        "detail.hh",
        "object_fields.cc",
        "object_fields.hh",
        "scalar.cc",
        "scalar_text.cc",
        "scalar_text.hh",
        "struct.cc",
        "struct.hh",
        "object_struct",
        "value.cc",
        "variants.cc",
        "variants.hh",
    }
    assert not (conversion / "scalar").exists()
    assert len((conversion / "scalar.cc").read_text(encoding="utf-8").splitlines()) <= 500
    assert {path.name for path in (conversion / "object_struct").iterdir()} == {
        "api.hh",
        "conversion.cc",
        "fields.cc",
        "fields.hh",
    }
    assert not list(materialization.rglob("*.cc.inc"))
    retired = (
        "column_builder_detail.hh",
        "column_builders.cc",
        "nested_column_builders.cc",
        "scalar_column_builders.cc",
        "conversion_detail.hh",
        "scalar_conversion.cc",
        "value_conversion.cc",
        "row_field_snapshot.cc",
        "row_field_snapshot.hh",
        "struct_conversion.cc",
        "struct_conversion.hh",
        "variant_routing.cc",
        "variant_routing.hh",
        "value_format.cc",
        "value_format.hh",
    )
    assert not [name for name in retired if (materialization / name).exists()]


def test_path_source_provider_state_has_one_visible_owner() -> None:
    """Path registry state and public method groups have explicit owners."""
    registry = ROOT / "cpp/src/api/python_abi3/registry"
    runtime = registry / "path_source_sinks.cc"
    provider = registry / "path_source_provider.cc"
    methods = (
        registry / "path_source_input_methods.cc",
        registry / "path_source_registry_methods.cc",
        registry / "path_source_auto_methods.cc",
    )
    state = registry / "path_source_sinks_internal.hh"

    assert runtime.is_file()
    assert provider.is_file()
    assert all(method.is_file() for method in methods)
    assert state.is_file()
    assert not (registry / "path_source_methods.cc").exists()
    assert not (registry / "path_source_sinks").exists()
    state_text = state.read_text(encoding="utf-8")
    runtime_text = runtime.read_text(encoding="utf-8")
    provider_text = provider.read_text(encoding="utf-8")
    assert "struct NativePathSourcesStreamState" in state_text
    assert "load_next_provider_chunk" in provider_text
    assert "merge_path_source_provider_schemas" in provider_text
    assert len(runtime_text.splitlines()) <= 500
    assert len(provider_text.splitlines()) <= 500
    assert all(len(method.read_text(encoding="utf-8").splitlines()) <= 500 for method in methods)


def test_input_discovery_is_owned_by_one_neutral_module() -> None:
    """Pipeline discovery remains neutral without a package of forwarding phases."""
    discovery = ROOT / "src/schema_sanitizer/input_impl/directory_inputs.py"
    assert discovery.is_file()
    assert not (ROOT / "src/schema_sanitizer/input_impl/directory_inputs").exists()
    assert len(discovery.read_text(encoding="utf-8").splitlines()) <= 500
    assert not (ROOT / "src/schema_sanitizer/api_impl/input/folder_files.py").exists()
    assert not (ROOT / "src/schema_sanitizer/api_impl/input/discovered_inputs.py").exists()
    pipeline_text = (ROOT / "src/schema_sanitizer/pipeline/source_discovery.py").read_text(
        encoding="utf-8"
    )
    assert "api_impl.input" not in pipeline_text


def test_native_recursive_layout_helpers_have_bounded_direct_owners() -> None:
    """Array shells stay local while repeated-path logic has one schema owner."""
    native_stream = ROOT / "cpp/src/internal/parquet/footer_reader/native_stream"
    materialization = native_stream / "materialization"
    layout = materialization / "layout"
    assert {path.name for path in layout.glob("*.cc.inc")} == {
        "native_stream_array_shells.cc.inc",
    }
    repeated = materialization / "native_stream_page_layout.cc.inc"
    assert repeated.is_file()
    assert len(repeated.read_text(encoding="utf-8").splitlines()) <= 500
    assert not (native_stream / "schema/native_stream_repeated_layout_validation.cc.inc").exists()
    assert not (materialization / "native_stream_materialize_layout.cc.inc").exists()


def test_parquet_contract_gates_are_grouped_by_verdict() -> None:
    """Static verdict reducers stay grouped while runtime status owns composition."""
    gates = ROOT / "src/schema_sanitizer/adapters/parquet/contract_gates"
    assert {path.name for path in gates.glob("*.py")} == {
        "__init__.py",
        "native.py",
    }
    parquet = ROOT / "src/schema_sanitizer/adapters/parquet"
    status = (parquet / "status.py").read_text(encoding="utf-8")
    assert "def _parquet_contract_runtime_readiness_status_from_capabilities" in status
    assert "def _parquet_preflight_contract_status_from_writer_status" in status
    assert "def _parquet_contract_certification_status_from_parts" in status
    assert not (parquet / "contracts.py").exists()
    assert not (parquet / "nested_contracts.py").exists()


def test_arrow_source_provider_registry_endpoints_share_result_builder() -> None:
    """Arrow provider endpoints and stream assembly have explicit owners."""
    registry = ROOT / "cpp/src/api/python_abi3/registry"
    methods = registry / "arrow_source_provider_methods.cc"
    runtime = registry / "arrow_source_sinks.cc"

    assert methods.is_file()
    assert runtime.is_file()
    assert not (registry / "arrow_source_methods.cc").exists()
    assert not (registry / "arrow_source_sinks").exists()
    methods_text = methods.read_text(encoding="utf-8")
    runtime_text = runtime.read_text(encoding="utf-8")
    assert "py_context_to_registry_sink_arrow_source_chunk_provider_auto_registry" in methods_text
    assert "pack_arrow_source_provider_registry_stream" in runtime_text


def test_runtime_streams_are_owned_by_api_runtime() -> None:
    """Source plans and API results share one direct stream-runtime owner."""
    stream_impl = ROOT / "src/schema_sanitizer/api_impl/streams.py"
    assert stream_impl.is_file()
    assert not (ROOT / "src/schema_sanitizer/stream_impl.py").exists()
    assert not (ROOT / "src/schema_sanitizer/api_impl/streams").exists()
    assert len(stream_impl.read_text(encoding="utf-8").splitlines()) <= 500
    results = ROOT / "src/schema_sanitizer/api_impl/results.py"
    assert results.is_file()
    assert not (ROOT / "src/schema_sanitizer/api_impl/results").exists()
    ingest = ROOT / "src/schema_sanitizer/api_impl/ingest.py"
    assert ingest.is_file()
    assert not ingest.with_suffix("").exists()
    assert len(ingest.read_text(encoding="utf-8").splitlines()) <= 500
    source_plan_text = "\n".join(
        path.read_text(encoding="utf-8")
        for path in (ROOT / "src/schema_sanitizer/api_impl/source_plan").rglob("*.py")
    )
    assert "api_impl.ingest" not in source_plan_text
    assert "ingest.types" not in source_plan_text


def test_jsonl_scalar_writers_are_split_by_value_domain() -> None:
    """Numeric and logical Arrow values must not collapse into one scalar writer."""
    output = ROOT / "cpp/src/internal/json_output"
    assert not (output / "jsonl_value_writer_scalar.cc").exists()
    for name in (
        "jsonl_value_writer_integer.cc",
        "jsonl_value_writer_floating.cc",
        "jsonl_value_writer_logical.cc",
    ):
        assert (output / name).is_file()


def test_path_source_chunk_provider_endpoints_share_stream_assembly() -> None:
    """Path provider endpoints and stream assembly have explicit owners."""
    registry = ROOT / "cpp/src/api/python_abi3/registry"
    methods = registry / "path_source_auto_methods.cc"
    runtime = registry / "path_source_sinks.cc"

    assert methods.is_file()
    assert runtime.is_file()
    assert not (registry / "path_source_methods.cc").exists()
    assert not (registry / "path_source_sinks").exists()
    methods_text = methods.read_text(encoding="utf-8")
    runtime_text = runtime.read_text(encoding="utf-8")
    assert (
        "py_context_to_registry_sink_from_path_source_chunk_provider_auto_registry" in methods_text
    )
    assert "pack_chunk_provider_registry_stream" in runtime_text


def test_native_parquet_reader_has_one_direct_owner() -> None:
    """Native reader contracts, preflight, and opening remain one cohesive owner."""
    parquet = ROOT / "src/schema_sanitizer/adapters/parquet"
    reader = parquet / "native_reader.py"
    assert reader.is_file()
    assert not reader.with_suffix("").exists()
    assert len(reader.read_text(encoding="utf-8").splitlines()) <= 500
    assert not (parquet / "direct_native.py").exists()


def test_native_row_group_materialization_has_one_bounded_owner() -> None:
    """The short consecutive row-group phases remain one readable owner."""
    materialization = ROOT / "cpp/src/internal/parquet/footer_reader/native_stream/materialization"
    row_group = materialization / "row_group"
    owners = list(row_group.glob("*.cc.inc"))
    assert {owner.name for owner in owners} == {"native_stream_row_group.cc.inc"}
    assert len(owners[0].read_text(encoding="utf-8").splitlines()) <= 500
    assert not (materialization / "native_stream_row_group.cc.inc").exists()


def test_projection_audits_are_grouped_by_composition_mode() -> None:
    """Each projection-audit mode has one bounded owner without deep fragments."""
    projection = ROOT / "src/schema_sanitizer/adapters/parquet/projection"
    audits = projection / "audits"
    assert {path.name for path in projection.glob("*.py")} == {"__init__.py"}
    assert {path.name for path in audits.glob("*.py")} == {
        "__init__.py",
        "composition.py",
        "coverage.py",
        "partitions.py",
        "subset.py",
        "summary.py",
    }
    assert not (audits / "subset").exists()
    for owner in audits.glob("*.py"):
        assert len(owner.read_text(encoding="utf-8").splitlines()) <= 500

    for retired in (
        "single.py",
        "chain.py",
        "partition.py",
        "coverage.py",
        "coverage_inputs.py",
        "coverage_consistency.py",
    ):
        assert not (projection / retired).exists()


def test_native_stream_diagnostics_do_not_span_fragment_boundaries() -> None:
    """Diagnostics and page layout helpers must be complete, separately owned units."""
    native_stream = ROOT / "cpp/src/internal/parquet/footer_reader/native_stream"
    diagnostics = native_stream / "diagnostics"
    assert {path.name for path in diagnostics.glob("*.cc.inc")} == {
        "native_stream_output_layout.cc.inc",
        "native_stream_recursive_diagnostics.cc.inc",
    }
    assert (native_stream / "materialization/native_stream_page_layout.cc.inc").is_file()
    assert not (native_stream / "schema/native_stream_path_diagnostics.cc.inc").exists()
    assert not (native_stream / "schema/native_stream_materialization_layout.cc.inc").exists()

    output_layout = (diagnostics / "native_stream_output_layout.cc.inc").read_text(encoding="utf-8")
    page_layout = (native_stream / "materialization/native_stream_page_layout.cc.inc").read_text(
        encoding="utf-8"
    )
    assert output_layout.lstrip().startswith(
        "void append_native_recursive_output_layout_diagnostics("
    )
    assert output_layout.rstrip().endswith("}")
    assert page_layout.lstrip().startswith("sanitize::Status materialization_payload(")


def test_registry_path_sink_calls_share_one_direct_owner() -> None:
    """Local collections and lazy providers share one direct owner module."""
    core = ROOT / "src/schema_sanitizer/core_impl"
    owner = core / "registry_sinks.py"
    assert owner.is_file()
    assert not (core / "registry").exists()
    source = owner.read_text(encoding="utf-8")
    assert "class _RegistryPathProviderSinkMethods" in source
    assert "class _RegistryPathSourceSinkMethods" in source
    assert "native_core as _native" in source


def test_json_streaming_parser_is_grouped_by_phase() -> None:
    """JSON stream state, cross-chunk scanning, and buffering remain separate units."""
    streaming = ROOT / "cpp/src/internal/parsing/streaming"
    json_streaming = streaming / "json"
    assert {path.name for path in json_streaming.iterdir() if path.is_file()} == {
        "scanner.hh",
        "scanner_flow.cc",
        "scanner_state.cc",
        "scanner_value.cc",
        "value_span_buffer.cc",
        "value_span_scanner.cc",
        "value_span_scanner.hh",
    }
    for retired in (
        "json_streaming_scanner.cc",
        "json_streaming_scanner.hh",
        "json_value_span_scanner.cc",
    ):
        assert not (streaming / retired).exists()

    json_frontend = ROOT / "cpp/src/frontends/json"
    assert not (json_frontend / "text").exists()
    assert not list(json_frontend.glob("*.cc.inc"))
    entrypoint = (json_frontend / "text_frontend.cc").read_text(encoding="utf-8")
    assert "class JsonTextRows" in entrypoint
    assert "class JsonTextFrontend" in entrypoint
    assert "class JsonArrayGroupFrontend" in entrypoint
    assert entrypoint.index("class JsonTextRows") < entrypoint.index("class JsonTextFrontend")
    assert entrypoint.index("class JsonTextFrontend") < entrypoint.index(
        "class JsonArrayGroupFrontend"
    )

    sources = (ROOT / "cmake/SchemaSanitizerSources.cmake").read_text(encoding="utf-8")
    assert "internal/parsing/streaming/json/scanner.cc" not in sources
    assert "internal/parsing/streaming/json/scanner_flow.cc" in sources
    assert "internal/parsing/streaming/json/scanner_state.cc" in sources
    assert "internal/parsing/streaming/json/scanner_value.cc" in sources
    assert "internal/parsing/streaming/json/value_span_scanner.cc" in sources
    assert "internal/parsing/streaming/json/value_span_buffer.cc" in sources
    assert "internal/parsing/streaming/json_streaming_scanner.cc" not in sources


def test_pipeline_execution_and_warm_up_are_owned_subsystems() -> None:
    """Pipeline execution and warm-up use cohesive direct owners."""
    pipeline = ROOT / "src/schema_sanitizer/pipeline"
    assert not (pipeline / "runner.py").exists()
    assert not (pipeline / "registry_bootstrap.py").exists()
    owner = pipeline / "partition_execution.py"
    assert owner.is_file()
    assert len(owner.read_text(encoding="utf-8").splitlines()) <= 500
    assert not (pipeline / "execution").exists()
    warm_up = pipeline / "registry_warmup.py"
    assert warm_up.is_file()
    assert len(warm_up.read_text(encoding="utf-8").splitlines()) <= 500
    assert not (pipeline / "registry_warmup").exists()


def test_schema_registry_abi3_has_one_bounded_method_owner() -> None:
    """Registry query and merge methods share one unit, separate from payload codec."""
    registry = ROOT / "cpp/src/api/python_abi3/registry"
    owner = registry / "schema_registry_methods.cc"
    assert owner.is_file()
    assert len(owner.read_text(encoding="utf-8").splitlines()) <= 500
    assert not (registry / "_core_abi3_schema_registry.cc").exists()
    assert not (registry / "schema_registry").exists()
    logical_schema = registry.parent / "logical_schema"
    assert {path.name for path in logical_schema.iterdir() if path.is_file()} == {
        "payload.cc",
        "payload.hh",
    }
    sources = (ROOT / "cmake/SchemaSanitizerSources.cmake").read_text(encoding="utf-8")
    assert "registry/_core_abi3_schema_registry.cc" not in sources
    assert "registry/schema_registry_methods.cc" in sources
    assert "registry/schema_registry/" not in sources
    assert "api/python_abi3/logical_schema/payload.cc" in sources


def test_parquet_page_payload_has_one_cohesive_phase_owner() -> None:
    """Page selection, writing, estimation, and slicing stay in one bounded block."""
    writer = ROOT / "cpp/src/internal/parquet/stream_writer"
    owner = writer / "stream_writer_pages.cc.inc"
    assert owner.is_file()
    assert len(owner.read_text(encoding="utf-8").splitlines()) <= 500
    assert not (writer / "pages").exists()
    assert not (writer / "stream_writer_page_slicing.cc.inc").exists()
