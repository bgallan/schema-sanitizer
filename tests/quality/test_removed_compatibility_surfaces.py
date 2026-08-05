"""Ensure removed compatibility names and forwarding modules stay removed."""

from __future__ import annotations

import importlib.util
from pathlib import Path

import pytest

from schema_sanitizer.adapters.parquet.compression import normalize_parquet_compression
from schema_sanitizer.input_impl.directory_inputs import DirectoryDiscovery
from schema_sanitizer.options_impl.call_options import normalize_call_options
from schema_sanitizer.remote_impl.providers import gcs


def test_forwarding_modules_are_removed() -> None:
    """Internal callers must use owning modules instead of compatibility facades."""
    assert importlib.util.find_spec("schema_sanitizer.api_impl.file_api") is None
    assert importlib.util.find_spec("schema_sanitizer.api_impl.folder_listing") is None
    assert importlib.util.find_spec("schema_sanitizer.api_impl.json_array_reader") is None
    assert importlib.util.find_spec("schema_sanitizer.api_impl.native_directory_errors") is None
    assert importlib.util.find_spec("schema_sanitizer.api_impl.source_batch") is None
    assert importlib.util.find_spec("schema_sanitizer.api_impl.conversion_call_options") is None
    assert importlib.util.find_spec("schema_sanitizer.options_impl.call_option_model") is None
    assert (
        importlib.util.find_spec("schema_sanitizer.api_impl.file_conversion.native_writers") is None
    )
    assert importlib.util.find_spec("schema_sanitizer.api_impl.core_errors") is None
    assert importlib.util.find_spec("schema_sanitizer.schema_registry_impl") is None
    assert importlib.util.find_spec("schema_sanitizer.source_plan_impl") is None
    assert importlib.util.find_spec("schema_sanitizer.stream_impl") is None
    assert importlib.util.find_spec("schema_sanitizer.api_impl.schema_registry") is None
    assert importlib.util.find_spec("schema_sanitizer.api_impl.file_conversion.metadata") is None
    assert importlib.util.find_spec("schema_sanitizer.public_impl") is None
    assert importlib.util.find_spec("schema_sanitizer.adapters._optional") is None
    assert importlib.util.find_spec("schema_sanitizer.adapters.pyarrow.dependency") is None
    assert (
        importlib.util.find_spec("schema_sanitizer.api_impl.file_conversion.output_metadata")
        is None
    )
    assert importlib.util.find_spec("schema_sanitizer.api_impl.parquet.compression_options") is None
    assert importlib.util.find_spec("schema_sanitizer.options_impl.catalog") is None
    assert importlib.util.find_spec("schema_sanitizer.core_impl.options_bytes") is None
    assert importlib.util.find_spec("schema_sanitizer.core_impl.options_bytes_codec") is None
    assert importlib.util.find_spec("schema_sanitizer.core_impl.options_bytes_defaults") is None
    assert importlib.util.find_spec("schema_sanitizer.core_impl.options_enum_metadata") is None
    assert importlib.util.find_spec("schema_sanitizer.core_impl.options_logical_schema") is None
    assert importlib.util.find_spec("schema_sanitizer.adapters.pyarrow_parquet_direct") is None
    assert importlib.util.find_spec("schema_sanitizer.pipeline.discovery") is None
    assert importlib.util.find_spec("schema_sanitizer.integrations.bigquery.namespace") is None
    assert importlib.util.find_spec("schema_sanitizer.api_impl.remote") is None
    assert importlib.util.find_spec("schema_sanitizer.remote_impl.gcs") is None
    assert importlib.util.find_spec("schema_sanitizer.remote_impl.s3") is None
    assert importlib.util.find_spec("schema_sanitizer.remote_impl.azure") is None
    assert importlib.util.find_spec("schema_sanitizer.remote_impl.http") is None
    assert importlib.util.find_spec("schema_sanitizer.remote_impl.local_staging") is None
    assert importlib.util.find_spec("schema_sanitizer.remote_impl.output") is None
    assert importlib.util.find_spec("schema_sanitizer.adapters.parquet.direct") is None
    assert importlib.util.find_spec("schema_sanitizer.adapters.parquet.direct_native") is None
    assert importlib.util.find_spec("schema_sanitizer.api_impl.direct_native_file_output") is None
    assert importlib.util.find_spec("schema_sanitizer.api_impl.source_plan.streams") is None
    assert importlib.util.find_spec("schema_sanitizer.core_impl.runtime_context_probes") is None
    assert importlib.util.find_spec("schema_sanitizer.core_impl.runtime_probes") is None
    assert importlib.util.find_spec("schema_sanitizer.core_impl.runtime_registry") is not None
    assert importlib.util.find_spec("schema_sanitizer.core_impl.runtime_registry_arrow") is None
    assert (
        importlib.util.find_spec("schema_sanitizer.core_impl.runtime_registry_arrow_sinks") is None
    )
    assert (
        importlib.util.find_spec("schema_sanitizer.core_impl.runtime_registry_path_sinks") is None
    )
    assert (
        importlib.util.find_spec("schema_sanitizer.core_impl.runtime_registry_sink_capabilities")
        is None
    )
    assert (
        importlib.util.find_spec("schema_sanitizer.core_impl.runtime_registry_source_sinks") is None
    )
    assert importlib.util.find_spec("schema_sanitizer.core_impl.runtime_support") is None
    assert importlib.util.find_spec("schema_sanitizer.core_impl.runtime") is None
    assert importlib.util.find_spec("schema_sanitizer.core_impl.runtime_sinks") is None
    probes_spec = importlib.util.find_spec("schema_sanitizer.core_impl.probes")
    assert probes_spec is not None
    assert probes_spec.submodule_search_locations is None
    assert importlib.util.find_spec("schema_sanitizer.api_impl.source_plan.common") is None
    assert importlib.util.find_spec("schema_sanitizer.api_impl.source_plan.prepared") is None
    assert importlib.util.find_spec("schema_sanitizer.api_impl.stream_writer_core") is None
    assert importlib.util.find_spec("schema_sanitizer.core_impl.native_functions") is None
    assert importlib.util.find_spec("schema_sanitizer.adapters.parquet.common") is None
    assert not (
        Path(__file__).resolve().parents[2]
        / "src/schema_sanitizer/api_impl/native_output/common.py"
    ).exists()
    assert importlib.util.find_spec("schema_sanitizer.integrations.bigquery.common") is None
    assert importlib.util.find_spec("schema_sanitizer.api_impl.input.public") is None
    assert importlib.util.find_spec("schema_sanitizer.adapters.parquet.projection.base") is None
    assert importlib.util.find_spec("schema_sanitizer.adapters.pyarrow_schema_support") is None
    parquet_status = importlib.util.find_spec("schema_sanitizer.adapters.parquet.status")
    assert parquet_status is not None
    assert parquet_status.submodule_search_locations is None
    assert not (
        Path(__file__).resolve().parents[2]
        / "src/schema_sanitizer/adapters/parquet/status/schema_support.py"
    ).exists()
    assert importlib.util.find_spec("schema_sanitizer.adapters.pyarrow_common") is None
    assert importlib.util.find_spec("schema_sanitizer.adapters.pyarrow_streams") is None
    assert importlib.util.find_spec("schema_sanitizer.adapters.pyarrow_csv_sink") is None
    assert importlib.util.find_spec("schema_sanitizer.adapters.pyarrow_jsonl_sink") is None
    assert importlib.util.find_spec("schema_sanitizer.adapters.pyarrow_metadata_native") is None
    assert importlib.util.find_spec("schema_sanitizer.adapters.schema_decision_cache") is None
    assert importlib.util.find_spec("schema_sanitizer.adapters.parquet.direct_helpers") is None
    assert importlib.util.find_spec("schema_sanitizer.adapters.parquet.contracts") is None
    assert importlib.util.find_spec("schema_sanitizer.adapters.parquet.nested_contracts") is None
    assert importlib.util.find_spec("schema_sanitizer.api_impl.shared") is None
    assert importlib.util.find_spec("schema_sanitizer.api_impl.native_folder_common") is None
    assert importlib.util.find_spec("schema_sanitizer.api_impl.registry_file_writers") is None
    assert (
        importlib.util.find_spec("schema_sanitizer.api_impl.registry_file_writer_helpers") is None
    )
    assert importlib.util.find_spec("schema_sanitizer.core_impl.byte_reader_base") is None
    assert importlib.util.find_spec("schema_sanitizer.api_impl.ingest_runtime_binary") is None
    assert importlib.util.find_spec("schema_sanitizer.api_impl.ingest_runtime_selectors") is None
    assert importlib.util.find_spec("schema_sanitizer.api_impl.ingest_runtime_streams") is None
    assert importlib.util.find_spec("schema_sanitizer.api_impl.ingest_runtime_types") is None
    assert importlib.util.find_spec("schema_sanitizer.api_impl.ingest_diagnostics") is None
    assert importlib.util.find_spec("schema_sanitizer.api_impl.ingest_lifecycle") is None
    assert importlib.util.find_spec("schema_sanitizer.api_impl.ingest_input_prepare") is None
    assert importlib.util.find_spec("schema_sanitizer.api_impl.native_ingest_plan") is None
    assert importlib.util.find_spec("schema_sanitizer.api_impl.context") is None
    assert importlib.util.find_spec("schema_sanitizer.api_impl.context_ops") is None
    assert importlib.util.find_spec("schema_sanitizer.api_impl.pool") is None
    assert importlib.util.find_spec("schema_sanitizer.api_impl.file_convert_api") is None
    assert importlib.util.find_spec("schema_sanitizer.api_impl.file_convert_core") is None
    assert importlib.util.find_spec("schema_sanitizer.api_impl.file_conversion_metadata") is None
    assert importlib.util.find_spec("schema_sanitizer.api_impl.file_output_metadata") is None
    assert importlib.util.find_spec("schema_sanitizer.api_impl.native_file_output") is None
    assert importlib.util.find_spec("schema_sanitizer.api_impl.stream_file_writers") is None
    assert importlib.util.find_spec("schema_sanitizer.api_impl.stream_writer_lifecycle") is None
    assert importlib.util.find_spec("schema_sanitizer.api_impl.analytical_api") is None
    assert importlib.util.find_spec("schema_sanitizer.api_impl.analytical_core") is None
    assert importlib.util.find_spec("schema_sanitizer.api_impl.table_output") is None
    assert importlib.util.find_spec("schema_sanitizer.api_impl.table_diagnostics") is None
    assert importlib.util.find_spec("schema_sanitizer.pipeline.hive_paths") is None
    assert importlib.util.find_spec("schema_sanitizer.pipeline.hive_namespace") is None
    assert importlib.util.find_spec("schema_sanitizer.pipeline.registry_bootstrap") is None
    assert importlib.util.find_spec("schema_sanitizer.pipeline.runner") is None
    assert importlib.util.find_spec("schema_sanitizer.adapters.parquet.direct_status") is None
    assert not (
        Path(__file__).resolve().parents[2] / "src/schema_sanitizer/api_impl/registry_output"
    ).exists()
    assert importlib.util.find_spec("schema_sanitizer.api_impl.source_plan.probe") is None
    assert importlib.util.find_spec("schema_sanitizer.api_impl.source_plan.prepared_plan") is None
    assert importlib.util.find_spec("schema_sanitizer.api_impl.source_plan.prepared_local") is None
    assert (
        importlib.util.find_spec("schema_sanitizer.api_impl.source_plan.prepared_composite") is None
    )
    assert (
        importlib.util.find_spec("schema_sanitizer.api_impl.source_plan.prepared_sources") is None
    )
    assert importlib.util.find_spec("schema_sanitizer.api_impl.source_plan.opened_registry") is None
    assert importlib.util.find_spec("schema_sanitizer.api_impl.source_plan.registry_stream") is None
    assert importlib.util.find_spec("schema_sanitizer.api_impl.source_plan.registry_output") is None
    assert (
        importlib.util.find_spec("schema_sanitizer.api_impl.source_plan.registry_file_output")
        is None
    )
    assert importlib.util.find_spec("schema_sanitizer.api_impl.source_plan.registry_drifts") is None
    assert importlib.util.find_spec("schema_sanitizer.api_impl.source_plan.probe_remote") is None
    assert importlib.util.find_spec("schema_sanitizer.api_impl.source_plan.remote_provider") is None
    assert (
        importlib.util.find_spec("schema_sanitizer.api_impl.source_plan.remote_registry_streams")
        is None
    )
    assert importlib.util.find_spec("schema_sanitizer.api_impl.source_plan.batch") is None
    assert importlib.util.find_spec("schema_sanitizer.api_impl.source_plan.model") is None
    assert importlib.util.find_spec("schema_sanitizer.api_impl.source_plan.path_sources") is None
    assert importlib.util.find_spec("schema_sanitizer.api_impl.source_plan.native_sinks") is None
    assert (
        importlib.util.find_spec("schema_sanitizer.api_impl.source_plan.route_diagnostics") is None
    )
    ingest_owner = Path(__file__).resolve().parents[2] / "src/schema_sanitizer/api_impl/ingest.py"
    assert ingest_owner.is_file()
    assert not ingest_owner.with_suffix("").exists()
    assert importlib.util.find_spec("schema_sanitizer.api_impl.input.directory_errors") is None
    assert importlib.util.find_spec("schema_sanitizer.core_impl.registry") is None
    assert importlib.util.find_spec("schema_sanitizer.core_impl.registry_arrow") is None
    assert importlib.util.find_spec("schema_sanitizer.core_impl.registry_paths") is None
    assert importlib.util.find_spec("schema_sanitizer.core_impl.registry_sinks") is not None
    assert importlib.util.find_spec("schema_sanitizer.core_impl.registry_sources") is None
    assert importlib.util.find_spec("schema_sanitizer.adapters.parquet.projection.single") is None
    assert importlib.util.find_spec("schema_sanitizer.adapters.parquet.projection.chain") is None
    assert (
        importlib.util.find_spec("schema_sanitizer.adapters.parquet.projection.partition") is None
    )
    assert importlib.util.find_spec("schema_sanitizer.adapters.parquet.projection.coverage") is None
    assert (
        importlib.util.find_spec("schema_sanitizer.adapters.parquet.projection.coverage_inputs")
        is None
    )
    assert (
        importlib.util.find_spec(
            "schema_sanitizer.adapters.parquet.projection.coverage_consistency"
        )
        is None
    )


def test_provider_packages_do_not_reexport_removed_facades() -> None:
    """Provider packages must expose submodules, not the old flat GCS surface."""
    for name in (
        "_gcs_list",
        "_gcs_directories_containing_files",
        "_gcs_download_file",
        "_gcs_upload_file",
        "_gcs_token",
    ):
        assert not hasattr(gcs, name)


def test_new_internal_packages_do_not_become_reexport_facades() -> None:
    """Owning packages must not flatten their implementation symbols."""
    from schema_sanitizer.adapters import pyarrow
    from schema_sanitizer.adapters.parquet import status as parquet_status
    from schema_sanitizer.adapters.parquet.projection import audits as projection_audits
    from schema_sanitizer.api_impl import (
        analytical,
        file_conversion,
        ingest,
        registry_output,
        stream_output,
    )
    from schema_sanitizer.api_impl import (
        source_plan as source_plan_package,
    )
    from schema_sanitizer.api_impl.parquet import arrow_sources, multisource
    from schema_sanitizer.api_impl.source_plan import preparation as source_plan_preparation
    from schema_sanitizer.api_impl.source_plan import probing as source_plan_probing
    from schema_sanitizer.api_impl.source_plan import registry as source_plan_registry
    from schema_sanitizer.api_impl.source_plan import remote as source_plan_remote
    from schema_sanitizer.core_impl import (
        execution,
        native_results,
        registry_sinks,
        schema_registry,
    )
    from schema_sanitizer.input_impl import source_plan as input_source_plan
    from schema_sanitizer.pipeline import hive, partition_execution, registry_warmup
    from schema_sanitizer.remote_impl.providers import azure, s3

    assert Path(partition_execution.__file__).name == "partition_execution.py"
    assert hasattr(partition_execution, "run_partitioned_to_parquet")
    assert hasattr(partition_execution, "PartitionPipelineResult")
    assert Path(registry_warmup.__file__).name == "registry_warmup.py"
    assert hasattr(registry_warmup, "infer_warm_up_schema_registry")
    assert hasattr(registry_warmup, "prepare_schema_warm_up_input")

    # These are now cohesive owner modules, not package-level re-export shells.
    assert Path(execution.__file__).name == "execution.py"
    assert hasattr(execution, "ExecutionContext")
    assert Path(multisource.__file__).name == "multisource.py"
    assert hasattr(multisource, "infer_parquet_multisource_registry")
    assert Path(arrow_sources.__file__).name == "arrow_sources.py"
    assert hasattr(arrow_sources, "ParquetArrowSource")
    assert hasattr(arrow_sources, "parquet_arrow_stream_factory_or_none")
    assert Path(input_source_plan.__file__).name == "source_plan.py"
    assert hasattr(input_source_plan, "NativeSourcePlan")
    assert hasattr(input_source_plan, "PreparedSourceBatch")
    assert Path(native_results.__file__).name == "native_results.py"
    assert hasattr(native_results, "SinkOutput")
    assert Path(schema_registry.__file__).name == "schema_registry.py"
    assert hasattr(schema_registry, "merge_schema_registry")
    assert hasattr(schema_registry, "SchemaRegistryMergeResult")
    assert Path(registry_sinks.__file__).name == "registry_sinks.py"
    assert hasattr(registry_sinks, "_RegistryArrowSinkMethods")
    assert hasattr(registry_sinks, "_RegistryPathProviderSinkMethods")
    assert hasattr(registry_sinks, "_RegistryPathSourceSinkMethods")
    assert Path(registry_output.__file__).name == "registry_output.py"
    assert hasattr(registry_output, "write_registry_raw_stream_to_file")
    assert hasattr(registry_output, "write_parquet_registry_file")
    assert Path(stream_output.__file__).name == "stream_output.py"
    assert hasattr(stream_output, "write_raw_stream_to_file")
    assert hasattr(stream_output, "write_table_or_stream")
    assert Path(source_plan_registry.__file__).name == "registry.py"
    assert hasattr(source_plan_registry, "open_source_plan_registry_stream")
    assert hasattr(source_plan_registry, "OpenedSourcePlanRegistryStream")
    assert Path(source_plan_probing.__file__).name == "probing.py"
    assert hasattr(source_plan_probing, "probe_source_plan_registry")
    assert hasattr(source_plan_probing, "probe_prepared_source_plan_registry")
    assert Path(source_plan_preparation.__file__).name == "preparation.py"
    assert hasattr(source_plan_preparation, "source_plan_from_prepared_inputs")
    assert not hasattr(source_plan_preparation, "prepared_native_sources")
    assert Path(source_plan_remote.__file__).name == "remote.py"
    assert hasattr(source_plan_remote, "RemotePathSourceChunkProvider")
    assert hasattr(source_plan_remote, "probe_remote_registry")
    assert Path(ingest.__file__).name == "ingest.py"
    assert hasattr(ingest, "NativeIngestPlan")
    assert hasattr(ingest, "normalize_options")
    assert hasattr(ingest, "native_ingest_plan")
    assert Path(parquet_status.__file__).name == "status.py"
    assert hasattr(parquet_status, "native_parquet_footer_info")
    assert hasattr(parquet_status, "parquet_contract_certification_status")
    assert hasattr(parquet_status, "parquet_schema_is_direct_native_eligible")

    assert Path(hive.__file__).name == "hive.py"
    assert hasattr(hive, "HiveRangeConfig")
    assert hasattr(hive, "build_hive_range_plan")
    assert hasattr(hive, "parse_iso_date")

    assert Path(analytical.__file__).name == "analytical.py"
    for name in ("to_pyarrow", "convert_analytical_with_options"):
        assert hasattr(analytical, name)
    assert not hasattr(analytical, "convert_arrow_table_output")

    for provider, names in (
        (s3, ("open_client", "list_files", "download_file")),
        (azure, ("open_service", "list_files", "download_file")),
    ):
        assert Path(provider.__file__).suffix == ".py"
        for name in names:
            assert hasattr(provider, name)

    for package, names in (
        (pyarrow, ("ensure_pyarrow", "write_csv_stream", "write_jsonl_stream")),
        (
            projection_audits,
            (
                "_native_recursive_projection_contract_audit_from_summaries",
                "_native_recursive_projection_chain_contract_audit_from_summaries",
                "_native_recursive_projection_partition_contract_audit_from_summaries",
                "_native_recursive_projection_coverage_contract_audit_from_summaries",
            ),
        ),
        (source_plan_package, ("NativeSourcePlan", "OpenedSourcePlanRegistryStream")),
        (input_source_plan, ("open_source_plan_sink_stream_or_none",)),
        (
            file_conversion,
            ("to_parquet", "convert_file_with_options", "write_csv_native_first_stream"),
        ),
    ):
        for name in names:
            assert not hasattr(package, name)


def test_removed_enum_spellings_are_rejected() -> None:
    """C++-style and historical enum spellings are not accepted."""
    for key, value in (
        ("schema_mode", "kStrict"),
        ("column_order", "sorted"),
        ("column_order", "schema-contract-first"),
    ):
        with pytest.raises(ValueError, match=key):
            normalize_call_options(**{key: value})


def test_removed_timestamp_precision_aliases_are_rejected() -> None:
    """Timestamp precision accepts only the documented canonical names."""
    for value in ("ms", "micros", "nanoseconds", "timestamp-micros"):
        with pytest.raises(ValueError, match="timestamp_precision"):
            normalize_call_options(timestamp_precision=value)


def test_removed_compression_aliases_are_rejected() -> None:
    """Compression accepts only gzip, snappy, and uncompressed."""
    for value in ("gz", "none", "no_compression"):
        with pytest.raises(ValueError, match="parquet_compression"):
            normalize_parquet_compression(value)


def test_removed_text_encoding_aliases_are_rejected() -> None:
    """Text encodings accept only the shared Python/C++ canonical names."""
    for value in ("utf8", "UTF-8", "latin-1", "cp819", "utf-8-sig"):
        with pytest.raises(ValueError, match="input_text_encoding"):
            normalize_call_options(input_text_encoding=value)


def test_discovery_results_are_explicit_value_objects() -> None:
    """Bulk discovery results must not emulate the removed mapping contract."""
    assert not issubclass(DirectoryDiscovery, dict)


def test_repository_tools_use_current_internal_owners() -> None:
    """Benchmarks and wheel smoke tests must not import removed modules."""
    root = Path(__file__).resolve().parents[2]
    checked = [
        *sorted((root / ".github" / "workflows").glob("*.yml")),
        *sorted((root / "benchmarks").glob("*.py")),
    ]
    text = "\n".join(path.read_text(encoding="utf-8") for path in checked)
    for removed_name in (
        "schema_sanitizer.api_impl.native_file_output",
        "schema_sanitizer.api_impl.file_output_metadata",
        "schema_sanitizer.api_impl.file_conversion.output_metadata",
        "schema_sanitizer.api_impl.file_conversion.metadata",
        "schema_sanitizer.api_impl.core_errors",
        "schema_sanitizer.core_impl.schema_registry",
        "schema_sanitizer.api_impl.parquet.compression_options",
        "schema_sanitizer.public_impl",
        "schema_sanitizer.api_impl.parquet_direct",
        "schema_sanitizer.adapters.pyarrow_jsonl_sink",
        "schema_sanitizer.adapters.pyarrow_csv_values",
        "schema_sanitizer.core_impl.runtime",
    ):
        assert removed_name not in text


def test_transcoding_sources_use_current_layout() -> None:
    """The old monolithic transcoding source must not return as a build input."""
    root = Path(__file__).resolve().parents[2]
    old_source = root / "cpp" / "src" / "ingest" / "chunk_source_transcoding.cc"
    sources = (root / "cmake" / "SchemaSanitizerSources.cmake").read_text(encoding="utf-8")

    assert not old_source.exists()
    assert "cpp/src/ingest/chunk_source_transcoding.cc" not in sources
    assert "cpp/src/ingest/transcoding/decoder.cc" in sources
    assert "cpp/src/ingest/transcoding/chunk_source.cc" in sources


def test_source_plan_owner_does_not_depend_on_conversion_orchestration() -> None:
    """Canonical source-plan mechanics avoid higher-level conversion owners."""
    root = Path(__file__).resolve().parents[2]
    source = (root / "src/schema_sanitizer/input_impl/source_plan.py").read_text(encoding="utf-8")

    assert "file_conversion" not in source
    assert "analytical" not in source


def test_path_source_and_scalar_builder_layouts_are_cohesive() -> None:
    """Path sources and scalar builders expose their real translation units."""
    root = Path(__file__).resolve().parents[2]
    path_sources = root / "cpp" / "src" / "api" / "python_abi3" / "path_sources"
    scalar = root / "cpp" / "src" / "internal" / "materialization" / "builders"

    assert {path.name for path in path_sources.iterdir() if path.is_file()} == {
        "csv_source_projections.cc",
        "path_sources.cc",
        "path_source_plan.cc",
        "path_source_probe.cc",
        "path_sources.hh",
    }
    assert not (path_sources / "private").exists()
    assert not list(path_sources.rglob("*.cc.inc"))
    assert all(
        len(path.read_text(encoding="utf-8").splitlines()) <= 500
        for path in path_sources.iterdir()
        if path.is_file()
    )

    owner = scalar / "scalar.cc"
    text = owner.read_text(encoding="utf-8")
    assert not (scalar / "scalar").exists()
    assert not list(scalar.rglob("*.cc.inc"))
    assert len(text.splitlines()) <= 500
    assert "payload->f64 = std::move(values_)" in text
    assert "payload->i64 = std::move(values_)" in text
    assert "payload->i32 = std::move(values_)" in text
    assert ".assign(values_.begin" not in text
