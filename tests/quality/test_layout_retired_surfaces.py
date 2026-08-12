"""Contracts keeping retired facades and fragmented surfaces absent."""

from __future__ import annotations

import importlib.util
from pathlib import Path

from schema_sanitizer.pipeline.advanced import (
    SchemaDriftDiff,
    read_parquet_schema,
)

ROOT = Path(__file__).resolve().parents[2]

FOOTER = ROOT / "cpp/src/internal/parquet/footer_reader"

PARQUET_INTERNAL_CPP = ROOT / "cpp/src/internal/parquet"

SRC = ROOT / "src/schema_sanitizer"

WRITER = ROOT / "cpp/src/internal/parquet/stream_writer"


def test_cloud_provider_packages_stay_flat_and_facade_free() -> None:
    """Cloud backends remain direct modules without retired package surfaces."""
    providers = ROOT / "src/schema_sanitizer/remote_impl/providers"
    provider_limits = {"gcs": 550, "s3": 500, "azure": 650}
    for name, line_limit in provider_limits.items():
        owner = providers / f"{name}.py"
        assert owner.is_file()
        assert not (providers / name).exists()
        assert len(owner.read_text(encoding="utf-8").splitlines()) <= line_limit
    production = "\n".join(
        (path.read_text(encoding="utf-8") for path in (ROOT / "src/schema_sanitizer").rglob("*.py"))
    )
    for retired in (
        "providers.gcs.client",
        "providers.gcs.objects",
        "providers.gcs.direct_listing",
        "providers.gcs.bulk_discovery",
        "providers.s3.client",
        "providers.s3.discovery",
        "providers.s3.objects",
        "providers.azure.client",
        "providers.azure.discovery",
        "providers.azure.objects",
    ):
        assert retired not in production


def test_cohesive_python_domains_are_modules_not_micro_packages() -> None:
    """Small single-purpose packages stay consolidated below the 500-line target."""
    package = ROOT / "src/schema_sanitizer"
    owners = (
        package / "core_impl/native_options.py",
        package / "core_impl/execution.py",
        package / "api_impl/parquet/multisource.py",
    )
    retired = (
        package / "core_impl/native_options",
        package / "core_impl/execution",
        package / "api_impl/parquet/multisource",
    )
    assert all((path.is_file() for path in owners))
    owner_limits = (600, 500, 500)
    assert all(
        (
            len(path.read_text(encoding="utf-8").splitlines()) <= limit
            for path, limit in zip(owners, owner_limits, strict=True)
        )
    )
    assert all((not path.exists() for path in retired))
    package_text = "\n".join((path.read_text(encoding="utf-8") for path in package.rglob("*.py")))
    assert "core_impl.native_options." not in package_text
    assert "core_impl.execution." not in package_text
    assert "parquet.multisource." not in package_text


def test_current_native_routes_do_not_keep_capability_facades() -> None:
    """Required ABI3 routes are direct calls, not optional compatibility probes."""
    core = ROOT / "src/schema_sanitizer/core_impl"
    assert not (core / "native_cache.py").exists()
    assert not (core / "registry/capabilities.py").exists()
    remote = ROOT / "src/schema_sanitizer/api_impl/source_plan/remote.py"
    assert remote.is_file()
    assert not remote.with_suffix("").exists()
    owners = (
        core / "native_symbols.py",
        core / "execution.py",
        ROOT / "src/schema_sanitizer/api_impl/source_plan/registry.py",
        remote,
    )
    owner_text = "\n".join((path.read_text(encoding="utf-8") for path in owners))
    assert "NativeFunctionCache" not in owner_text
    assert "supports_" not in owner_text
    assert "getattr(_native" not in owner_text


def test_dead_environment_facades_stay_removed() -> None:
    """Environment notes without production callers must not become modules again."""
    package = ROOT / "src/schema_sanitizer"
    assert not (package / "pipeline/source_discovery_environment.py").exists()
    assert not (package / "remote_impl/environment.py").exists()


def test_directory_preparation_package_stays_retired() -> None:
    """Directory preparation must not regain storage-role facade modules."""
    assert importlib.util.find_spec("schema_sanitizer.api_impl.input.directories") is None
    owner = ROOT / "src/schema_sanitizer/api_impl/input/directory_preparation.py"
    source = owner.read_text(encoding="utf-8")
    assert source.count("native_text_encoding_supported(") == 1
    assert source.count('len(csv_delimiter.encode("utf-8"))') == 1
    assert "suffix=FORMAT_SUFFIXES[input_format]" in source
    assert "def _attach_native_directory_manifest" not in source
    assert "def _attach_remote_native_directory_manifest" not in source


def test_execution_registry_probes_do_not_use_pass_through_helpers() -> None:
    """Registry probe methods should call the ABI directly from their owner."""
    owner = ROOT / "src/schema_sanitizer/core_impl/probes.py"
    text = owner.read_text(encoding="utf-8")
    assert "probe_dependencies" not in text
    assert "registry_probe_arrow_sources(" in text
    assert "registry_probe_path_source_chunk_provider(" in text


def test_hive_planning_has_one_bounded_owner_without_package_facades() -> None:
    """Closely coupled Hive planning must not return to a micro-package."""
    owner = SRC / "pipeline/hive.py"
    assert owner.is_file()
    assert not (SRC / "pipeline/hive").exists()
    text = owner.read_text(encoding="utf-8")
    assert "class HiveRangeConfig" in text
    assert "def build_hive_range_plan" in text
    assert "def build_warm_up_hive_range_plan_from_namespace" in text
    assert len(text.splitlines()) <= 500


def test_input_selection_has_one_bounded_owner_without_facades() -> None:
    """Closely coupled selector and path rules must have one neutral owner."""
    owner = SRC / "input_impl/selection.py"
    retired = (
        SRC / "input_impl/selection",
        SRC / "input_impl/path_inputs.py",
        SRC / "input_impl/errors.py",
    )
    assert owner.is_file()
    assert len(owner.read_text(encoding="utf-8").splitlines()) <= 500
    assert all((not path.exists() for path in retired))


def test_options_have_two_bounded_owners_without_helper_facades() -> None:
    """Catalog options and per-call options remain cohesive direct modules."""
    package = ROOT / "src/schema_sanitizer/options_impl"
    owners = {"options.py", "call_options.py", "__init__.py"}
    assert {path.name for path in package.iterdir() if path.is_file()} == owners
    assert len((package / "options.py").read_text(encoding="utf-8").splitlines()) <= 500
    assert len((package / "call_options.py").read_text(encoding="utf-8").splitlines()) <= 500
    production = "\n".join(
        (path.read_text(encoding="utf-8") for path in (ROOT / "src/schema_sanitizer").rglob("*.py"))
    )
    for retired in ("options_groups", "call_option_validators", "options_impl.native_call"):
        assert retired not in production


def test_parquet_micro_fragments_are_consolidated_by_runtime_phase() -> None:
    """Consecutive Parquet phases remain in bounded cohesive owners."""
    owners = {
        FOOTER / "runtime/native_stream_readiness.cc.inc": 500,
        FOOTER / "native_stream/materialization/native_stream_validity.cc.inc": 500,
        FOOTER / "native_stream/materialization/row_group/native_stream_row_group.cc.inc": 500,
        FOOTER
        / "native_stream/materialization/row_group/native_stream_parallel_columns.cc.inc": 500,
        FOOTER
        / "native_stream/materialization/row_group/native_stream_retained_budget.cc.inc": 500,
    }
    for owner, line_limit in owners.items():
        assert owner.is_file()
        assert len(owner.read_text(encoding="utf-8").splitlines()) <= line_limit
    assert not (FOOTER / "runtime/readiness").exists()
    assert not (FOOTER / "native_stream/materialization/validity").exists()
    assert {
        path.name for path in (FOOTER / "native_stream/materialization/row_group").glob("*.cc.inc")
    } == {
        "native_stream_retained_budget.cc.inc",
        "native_stream_row_group.cc.inc",
        "native_stream_parallel_columns.cc.inc",
    }
    translation_unit = (FOOTER / "footer_reader.cc").read_text(encoding="utf-8")
    for owner in owners:
        relative = owner.relative_to(FOOTER).as_posix()
        assert translation_unit.count(f'#include "{relative}"') == 1
    for retired in (
        "runtime/readiness/",
        "native_stream/materialization/validity/",
        "row_group/native_stream_array.cc.inc",
        "row_group/native_stream_column.cc.inc",
        "row_group/native_stream_flat_column.cc.inc",
        "row_group/native_stream_output_field.cc.inc",
        "row_group/native_stream_repeated_column.cc.inc",
    ):
        assert retired not in translation_unit


def test_parquet_writer_micro_fragments_are_owned_by_their_only_callers() -> None:
    """Single-purpose helpers must stay with collection and stream entry owners."""
    writer = PARQUET_INTERNAL_CPP / "stream_writer"
    collection = (writer / "stream_writer_collection.cc.inc").read_text(encoding="utf-8")
    api = (writer / "stream_writer_api.cc.inc").read_text(encoding="utf-8")
    assert "void emit_nulls_for_subtree" in collection
    assert "std::string stream_error_message" in api
    assert len(collection.splitlines()) <= 500
    assert len(api.splitlines()) <= 500
    assert not (writer / "stream_writer_null_collection.cc.inc").exists()
    assert not (writer / "stream_writer_stream_errors.cc.inc").exists()


def test_partition_execution_has_one_direct_owner_without_package_facade() -> None:
    """Partition loop, state result, and bootstrap stay in one bounded module."""
    pipeline = ROOT / "src/schema_sanitizer/pipeline"
    owner = pipeline / "partition_execution.py"
    assert importlib.util.find_spec("schema_sanitizer.pipeline.execution") is None
    assert owner.is_file()
    assert not (pipeline / "execution").exists()
    source = owner.read_text(encoding="utf-8")
    assert len(source.splitlines()) <= 500
    assert "class PartitionPipelineResult" in source
    assert "def _compile_native_registry_state" in source
    assert "def run_partitioned_to_parquet_registry_json" in source


def test_passthrough_python_modules_stay_folded_into_real_owners() -> None:
    """Single-call facades must not return after their behavior gained a domain owner."""
    package = ROOT / "src/schema_sanitizer"
    retired = (
        package / "adapters/pyarrow/csv_native.py",
        package / "adapters/pyarrow/csv_values.py",
        package / "api_impl/analytical/invocation.py",
        package / "api_impl/file_conversion/writer_lifecycle.py",
        package / "api_impl/parquet/folder_reader.py",
        package / "integrations/bigquery/partition_defaults.py",
    )
    owners = (
        package / "adapters/pyarrow/csv_sink.py",
        package / "api_impl/analytical.py",
        package / "api_impl/stream_output.py",
        package / "api_impl/input/directory_preparation.py",
        package / "integrations/bigquery/external_table.py",
    )
    assert all((not path.exists() for path in retired))
    assert all((path.is_file() for path in owners))
    owner_limits = (550, 600, 550, 600, 550)
    assert all(
        (
            len(path.read_text(encoding="utf-8").splitlines()) <= limit
            for path, limit in zip(owners, owner_limits, strict=True)
        )
    )
    csv_sink = owners[0].read_text(encoding="utf-8")
    parquet_directory = owners[3].read_text(encoding="utf-8")
    assert "def native_csv_nested_reader" in csv_sink
    assert "return any(" in csv_sink
    assert "nested_column_indices" not in csv_sink
    assert "folder_files(" in parquet_directory
    assert "prepare_parquet_directory_from_files(" in parquet_directory


def test_pipeline_schema_operations_have_one_owner_without_facades() -> None:
    """Parquet schema loading and drift comparison stay in one bounded module."""
    owner = SRC / "pipeline/schemas.py"
    assert owner.is_file()
    assert not (SRC / "pipeline/parquet.py").exists()
    assert not (SRC / "pipeline/schema_drift.py").exists()
    text = owner.read_text(encoding="utf-8")
    assert "def read_parquet_schema" in text
    assert "def diff_arrow_schemas" in text
    assert read_parquet_schema.__module__ == "schema_sanitizer.pipeline.schemas"
    assert SchemaDriftDiff.__module__ == "schema_sanitizer.pipeline.schemas"
    assert len(text.splitlines()) <= 500


def test_public_conversion_implementations_are_not_nested_namespaces() -> None:
    """Public conversion implementations use bounded direct modules, not shell packages."""
    api_impl = ROOT / "src/schema_sanitizer/api_impl"
    analytical = api_impl / "analytical.py"
    analytical_output = api_impl / "results.py"
    file_conversion = api_impl / "file_conversion"
    assert analytical.is_file()
    assert analytical_output.is_file()
    assert not (api_impl / "analytical").exists()
    assert not (file_conversion / "public").exists()
    assert (file_conversion / "converters.py").is_file()
    for name in ("delimited.py", "parquet.py", "invocation.py"):
        assert not (file_conversion / name).exists()
    package_text = "\n".join(
        (path.read_text(encoding="utf-8") for path in (ROOT / "src/schema_sanitizer").rglob("*.py"))
    )
    assert ".api_impl.analytical" in package_text
    assert ".api_impl.analytical.public" not in package_text
    assert "file_conversion.public" not in package_text


def test_python_micro_packages_remain_direct_modules() -> None:
    """Small cohesive domains must not regress into pass-through packages."""
    package = ROOT / "src/schema_sanitizer"
    owners = (
        package / "core_impl/probes.py",
        package / "input_impl/selection.py",
        package / "api_impl/file_conversion/converters.py",
        package / "api_impl/source_plan/remote.py",
    )
    owner_limits = (500, 500, 600, 950)
    for owner, limit in zip(owners, owner_limits, strict=True):
        assert owner.is_file()
        assert not owner.with_suffix("").exists()
        assert len(owner.read_text(encoding="utf-8").splitlines()) <= limit


def test_python_owners_do_not_reexport_unrelated_symbols() -> None:
    """File writers and call options each have one direct internal owner."""
    from schema_sanitizer.api_impl.file_conversion import writers
    from schema_sanitizer.options_impl import call_options

    assert hasattr(writers, "write_parquet_native_first_stream")
    assert hasattr(writers, "write_jsonl_native_first_stream")
    assert Path(writers.__file__).name == "writers.py"
    assert not Path(writers.__file__).with_suffix("").exists()
    assert hasattr(call_options, "normalize_call_options")
    assert hasattr(call_options, "_CallOptions")
    assert Path(call_options.__file__).name == "call_options.py"


def test_retired_ingest_modules_stay_absent() -> None:
    """Selection and execution-context modules must not return as ingest facades."""
    owner = ROOT / "src/schema_sanitizer/api_impl/ingest.py"
    assert owner.is_file()
    assert not owner.with_suffix("").exists()
    retired = (
        "context.py",
        "context_operations.py",
        "pool.py",
        "selectors.py",
        "text_input.py",
        "lifecycle.py",
        "types.py",
        "streams.py",
        "diagnostics.py",
    )
    assert not [name for name in retired if (owner.with_suffix("") / name).exists()]
    assert importlib.util.find_spec("schema_sanitizer.api_impl.input.text_encoding") is None


def test_retired_input_and_conversion_modules_stay_absent() -> None:
    """Removed mixed modules must not return as compatibility facades."""
    for module_name in (
        "schema_sanitizer.api_impl.input.prepare",
        "schema_sanitizer.api_impl.input.types",
    ):
        assert importlib.util.find_spec(module_name) is None


def test_retired_metadata_stream_micro_units_stay_absent() -> None:
    """Old stream phases and UTF-8 subpackage must not return as compatibility files."""
    stream = ROOT / "cpp/src/api/python_abi3/metadata/stream"
    retired = {
        "api.hh",
        "array.cc",
        "builder_parts.hh",
        "builders.hh",
        "callbacks.cc",
        "callbacks.hh",
        "column.cc",
        "release.cc",
        "schema.cc",
        "state.hh",
        "wrapper.cc",
    }
    assert all((not (stream / name).exists() for name in retired))
    assert not (ROOT / "cpp/src/api/python_abi3/metadata/utf8").exists()


def test_retired_path_input_modules_stay_absent() -> None:
    """Removed path and directory modules must not return as compatibility facades."""
    for module_name in (
        "schema_sanitizer.api_impl.input.constants",
        "schema_sanitizer.api_impl.input.paths",
        "schema_sanitizer.api_impl.input.directory_native",
        "schema_sanitizer.api_impl.input.local_directories",
        "schema_sanitizer.api_impl.input.remote_directories",
    ):
        assert importlib.util.find_spec(module_name) is None


def test_retired_python_facades_stay_absent() -> None:
    """Retired mixed-responsibility modules must not return as facades."""
    for module_name in (
        "schema_sanitizer.api_impl.file_conversion.api",
        "schema_sanitizer.adapters.parquet.observability",
        "schema_sanitizer.adapters.parquet.stream_factory",
    ):
        assert importlib.util.find_spec(module_name) is None


def test_retired_python_modules_stay_absent() -> None:
    """Old option and native-output locations must not return as facades."""
    for module_name in (
        "schema_sanitizer.options_impl.call_option_model",
        "schema_sanitizer.api_impl.file_conversion.native_writers",
        "schema_sanitizer.api_impl.native_output",
        "schema_sanitizer.api_impl.input.remote_chunks",
    ):
        assert importlib.util.find_spec(module_name) is None


def test_retired_python_owners_have_no_importers() -> None:
    """No source module may preserve old implementation paths as aliases."""
    retired_names = (
        "input_impl.path_inputs",
        "input_impl.errors",
        "input_impl.selection.",
        "api_impl.input.json_array",
        "api_impl.file_conversion.stream_writers",
    )
    source = "\n".join((path.read_text(encoding="utf-8") for path in SRC.rglob("*.py")))
    assert not [name for name in retired_names if name in source]


def test_retired_python_service_modules_stay_absent() -> None:
    """Mixed discovery and multi-source sink modules must not return as facades."""
    discovery = importlib.util.find_spec("schema_sanitizer.pipeline.source_discovery")
    assert discovery is not None and discovery.submodule_search_locations is None
    assert not (ROOT / "src/schema_sanitizer/api_impl/parquet/multisource").exists()


def test_retired_schema_and_python_fragments_stay_absent() -> None:
    """Removed internal paths must not return as compatibility facades."""
    removed = (
        "src/schema_sanitizer/api_impl/ingest/binary.py",
        "src/schema_sanitizer/api_impl/ingest/plan.py",
        "src/schema_sanitizer/api_impl/source_plan/remote/chunk_provider.py",
        "src/schema_sanitizer/api_impl/source_plan/remote/native_probe.py",
        "cpp/src/internal/parquet/footer_reader/schema/footer_reader_arrow_formats.cc.inc",
        "cpp/src/internal/parquet/footer_reader/schema/footer_reader_leaf_schema.cc.inc",
        "cpp/src/internal/parquet/footer_reader/schema/footer_reader_schema_levels.cc.inc",
    )
    assert not [relative for relative in removed if (ROOT / relative).exists()]


def test_retired_single_call_input_facades_stay_absent() -> None:
    """Private stream and JSON-array wrappers must not regain ownership."""
    assert not (SRC / "api_impl/file_conversion/stream_writers.py").exists()
    assert not (SRC / "api_impl/input/json_array.py").exists()


def test_root_private_facades_moved_to_domain_owners() -> None:
    """Root-level implementation modules must not return as compatibility facades."""
    package = ROOT / "src/schema_sanitizer"
    owners = (
        package / "core_impl/schema_registry.py",
        package / "input_impl/source_plan.py",
        package / "api_impl/streams.py",
    )
    retired = (
        package / "schema_registry_impl.py",
        package / "source_plan_impl.py",
        package / "stream_impl.py",
        package / "api_impl/schema_registry.py",
        package / "api_impl/source_plan/plan.py",
    )
    assert all((path.is_file() for path in owners))
    assert all((len(path.read_text(encoding="utf-8").splitlines()) <= 500 for path in owners))
    assert all((not path.exists() for path in retired))


def test_small_cpp_domains_do_not_use_hidden_include_fragments() -> None:
    """Metadata streams and path sources remain visible in normal source files."""
    metadata_stream = ROOT / "cpp/src/api/python_abi3/metadata/stream"
    path_sources = ROOT / "cpp/src/api/python_abi3/path_sources"
    assert {path.name for path in metadata_stream.iterdir() if path.is_file()} == {
        "array_builder.cc",
        "stream.cc",
        "stream.hh",
    }
    assert {path.name for path in path_sources.iterdir() if path.is_file()} == {
        "csv_source_projections.cc",
        "path_sources.cc",
        "path_source_plan.cc",
        "path_source_probe.cc",
        "path_sources.hh",
    }
    assert not list(metadata_stream.rglob("*.inc"))
    assert not list(path_sources.rglob("*.inc"))
    assert (
        len((path_sources / "csv_source_projections.cc").read_text(encoding="utf-8").splitlines())
        <= 500
    )


def test_writer_micro_fragments_are_consolidated_but_bounded() -> None:
    """Closely coupled encoding and page helpers stay together below 500 lines."""
    values = WRITER / "stream_writer_arrow_values.cc.inc"
    pages = WRITER / "stream_writer_pages.cc.inc"
    assert "append_plain_primitive_value" in values.read_text(encoding="utf-8")
    assert "append_dictionary_value" in values.read_text(encoding="utf-8")
    assert "write_column_pages" in pages.read_text(encoding="utf-8")
    assert len(values.read_text(encoding="utf-8").splitlines()) <= 500
    assert len(pages.read_text(encoding="utf-8").splitlines()) <= 500
    for retired in (
        "stream_writer_plain_values.cc.inc",
        "stream_writer_dictionary_values.cc.inc",
        "stream_writer_column_chunks.cc.inc",
    ):
        assert not (WRITER / retired).exists()
