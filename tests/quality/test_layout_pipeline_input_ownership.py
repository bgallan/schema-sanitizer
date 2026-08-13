"""Ownership and layout contracts for input and source-plan pipelines."""

from __future__ import annotations

import importlib.util
from datetime import date
from pathlib import Path

import pytest

from schema_sanitizer.input_impl.selection import FORMAT_SUFFIXES, input_format_extensions
from schema_sanitizer.pipeline.advanced import (
    discover_existing_source_plans,
)
from schema_sanitizer.pipeline.source_discovery import _unique_source_locations
from schema_sanitizer.pipeline.types import PartitionRunPlan

ROOT = Path(__file__).resolve().parents[2]

SRC = ROOT / "src/schema_sanitizer"


def test_csv_projection_has_one_owner_and_cached_column_metadata() -> None:
    """CSV projection must not re-scan every planned column for every row."""
    frontend = ROOT / "cpp/src/frontends/csv"
    owner = frontend / "column_projection.cc"
    assert owner.is_file()
    assert not (frontend / "column_projection").exists()
    source = owner.read_text(encoding="utf-8")
    assert len(source.splitlines()) <= 500
    assert "std::ranges::equal" in source
    assert "std::to_chars" in source
    assert "keep_mask_ready_ && keep_mask_.size() >= column_count" in source
    assert "first_new_column = keep_mask_.size()" in source
    assert "ensure_column_hashes(cells.size())" in source
    assert ".key_hash = column_hashes_[i]" in source
    header = (frontend / "column_projection.hh").read_text(encoding="utf-8")
    assert "std::vector<std::uint64_t> column_hashes_" in header
    manifest = (ROOT / "cmake/SchemaSanitizerSources.cmake").read_text(encoding="utf-8")
    assert "frontends/csv/column_projection.cc" in manifest
    assert "frontends/csv/column_projection/" not in manifest


def test_csv_projection_is_one_bounded_cohesive_unit() -> None:
    """Small projection lifecycle, header mapping, and row filtering share one owner."""
    csv = ROOT / "cpp/src/frontends/csv"
    owner = csv / "column_projection.cc"
    assert owner.is_file()
    assert not (csv / "column_projection").exists()
    assert len(owner.read_text(encoding="utf-8").splitlines()) <= 500


def test_directory_discovery_has_one_input_model() -> None:
    """Local and remote discovery share one typed result and accumulator."""
    owner = SRC / "input_impl/directory_inputs.py"
    source = owner.read_text(encoding="utf-8")
    assert "class DirectoryDiscovery" in source
    assert "class DirectoryDiscoveryBuilder" in source
    assert "from ..sources.models import RemoteFile as _RemoteFile" in source
    assert "def split_parent_child" in source
    remote_files = SRC / "sources/models.py"
    remote_source = remote_files.read_text(encoding="utf-8")
    assert remote_files.is_file()
    assert "class RemoteFile" in remote_source
    assert "def remote_file_sort_key" in remote_source
    assert len(remote_source.splitlines()) <= 500
    assert not (SRC / "input_impl/remote_files.py").exists()
    assert not (SRC / "input_impl/source_manifest.py").exists()
    assert "class RemoteDirectoryDiscovery" not in source
    assert "class LocalDirectoryDiscovery" not in source
    staging = (SRC / "remote_impl/staging.py").read_text(encoding="utf-8")
    assert "RemoteFile" in staging
    assert "DirectoryDiscovery" not in staging
    assert not (SRC / "remote_impl/types.py").exists()
    pipeline = (SRC / "pipeline/source_discovery.py").read_text(encoding="utf-8")
    assert "DirectoryDiscovery[FolderFile]" in pipeline
    assert "LocalDirectoryDiscovery" not in pipeline


def test_directory_input_and_pipeline_discovery_have_distinct_single_owners() -> None:
    """Reusable folder input state and pipeline orchestration must not share a vague name."""
    directory_inputs = SRC / "input_impl/directory_inputs.py"
    source_discovery = SRC / "pipeline/source_discovery.py"
    assert directory_inputs.is_file()
    assert source_discovery.is_file()
    assert not (SRC / "input_impl/discovery.py").exists()
    assert not (SRC / "pipeline/source_discovery").exists()
    assert "def folder_files" in directory_inputs.read_text(encoding="utf-8")
    discovery_text = source_discovery.read_text(encoding="utf-8")
    directory_text = directory_inputs.read_text(encoding="utf-8")
    assert "def discover_existing_source_plans_async" in discovery_text
    assert "DirectoryDiscoveryBuilder" in discovery_text
    assert "dict.fromkeys" in directory_text
    assert discover_existing_source_plans.__module__ == "schema_sanitizer.pipeline.source_discovery"


def test_directory_preparation_has_one_bounded_owner() -> None:
    """Directory preparation stays cohesive without a storage-role micro-package."""
    owner = ROOT / "src/schema_sanitizer/api_impl/input/directory_preparation.py"
    retired = ROOT / "src/schema_sanitizer/api_impl/input/directories"
    assert owner.is_file()
    assert not retired.exists()
    source = owner.read_text(encoding="utf-8")
    assert "def prepare_directory(" in source
    assert "class RemoteNativeDirectorySourceManifest" in source
    assert "def prepare_single_parquet_file(" in source
    assert len(source.splitlines()) <= 550


def test_execution_context_has_one_cohesive_owner() -> None:
    """Context, sink routing, table materialization, and pooling share one owner."""
    owner = ROOT / "src/schema_sanitizer/api_impl/execution_context.py"
    assert owner.is_file()
    assert not (ROOT / "src/schema_sanitizer/api_impl/execution_context").exists()
    assert len(owner.read_text(encoding="utf-8").splitlines()) <= 600
    from schema_sanitizer.api_impl import execution_context

    assert hasattr(execution_context, "ExecutionContext")
    assert hasattr(execution_context, "default_pool")


def test_ingest_preparation_is_split_by_phase() -> None:
    """Inference, schema resolution, and PreparedIngest assembly stay independent."""
    ingest = ROOT / "cpp/src/ingest"
    assert not (ingest / "prepare.cc").exists()
    package = ingest / "prepare"
    assert {path.name for path in package.iterdir()} == {
        "inference.cc",
        "prepare.cc",
        "prepare_internal.hh",
        "schema.cc",
    }
    assert "scan_shapes_row" not in (package / "prepare.cc").read_text(encoding="utf-8")
    assert "compile_plan" not in (package / "inference.cc").read_text(encoding="utf-8")


def test_input_extension_catalog_has_one_owner() -> None:
    """Hive planning and discovery derive extensions from selector metadata."""
    assert input_format_extensions("parquet") == ("parquet", "pq")
    assert input_format_extensions("jsonl") == ("jsonl",)
    assert FORMAT_SUFFIXES["ndjson"] == (".ndjson",)
    hive = (SRC / "pipeline/hive.py").read_text(encoding="utf-8")
    discovery = (SRC / "pipeline/source_discovery.py").read_text(encoding="utf-8")
    assert "FORMAT_EXTENSIONS" not in hive + discovery
    assert "input_format_extensions" in hive
    assert "input_format_extensions" in discovery


def test_input_preparation_and_discovery_have_direct_owners() -> None:
    """Input preparation and discovery must not return to micro-packages."""
    preparation = ROOT / "src/schema_sanitizer/api_impl/input/preparation.py"
    discovery = ROOT / "src/schema_sanitizer/input_impl/directory_inputs.py"
    assert preparation.is_file()
    assert discovery.is_file()
    assert not preparation.with_suffix("").is_dir()
    assert not discovery.with_suffix("").is_dir()
    assert len(preparation.read_text(encoding="utf-8").splitlines()) <= 500
    assert len(discovery.read_text(encoding="utf-8").splitlines()) <= 500


def test_input_selection_has_one_bounded_owner() -> None:
    """Closely coupled selector rules stay in one bounded domain owner."""
    owner = ROOT / "src/schema_sanitizer/input_impl/selection.py"
    retired = ROOT / "src/schema_sanitizer/input_impl/selection"
    assert owner.is_file()
    assert not retired.exists()
    assert len(owner.read_text(encoding="utf-8").splitlines()) <= 600
    from schema_sanitizer.input_impl import selection

    assert hasattr(selection, "resolve_source_and_format")
    assert hasattr(selection, "prepare_native_text_data")


def test_local_path_source_plan_has_one_canonical_batch_and_capsule() -> None:
    """Local directories do not retain parallel file lists or reconstructed ABI tuples."""
    prepared = (ROOT / "src/schema_sanitizer/input_impl/prepared.py").read_text(encoding="utf-8")
    plan = (ROOT / "src/schema_sanitizer/input_impl/source_plan.py").read_text(encoding="utf-8")
    execution = (ROOT / "src/schema_sanitizer/core_impl/execution.py").read_text(encoding="utf-8")
    manifest_body = prepared.split("class NativeDirectorySourceManifest:", 1)[1].split(
        "class StagedNativeDirectoryManifest:", 1
    )[0]
    assert "source_batch: PreparedSourceBatch" in manifest_body
    assert "files:" not in manifest_body
    assert "options:" not in manifest_body
    assert "_path_source_tuples_from_plan" not in plan
    assert "_accepts_native_path_source_plan" not in execution
    assert "native_payload=native_payload" in plan


def test_path_input_helpers_share_the_selector_owner() -> None:
    """Path validation and selector metadata remain in the neutral input domain."""
    owner = ROOT / "src/schema_sanitizer/input_impl/selection.py"
    assert owner.is_file()
    assert not (ROOT / "src/schema_sanitizer/input_impl/path_inputs.py").exists()
    from schema_sanitizer.input_impl import selection

    assert hasattr(selection, "normalize_public_input_format")
    assert hasattr(selection, "display_source_file")
    assert "api_impl" not in owner.read_text(encoding="utf-8")
    assert len(owner.read_text(encoding="utf-8").splitlines()) <= 500


def test_path_source_probe_borrows_capsule_storage() -> None:
    """Immediate probes must not copy every descriptor from reusable native plans."""
    owner = (ROOT / "cpp/src/api/python_abi3/path_sources/path_source_plan.cc").read_text(
        encoding="utf-8"
    )
    methods = (ROOT / "cpp/src/api/python_abi3/probes/schema_probe_methods.cc").read_text(
        encoding="utf-8"
    )
    implementation = (ROOT / "cpp/src/api/python_abi3/probes/schema_probe.cc").read_text(
        encoding="utf-8"
    )
    assert "bool parse_path_sources_view(" in owner
    assert "out->borrowed = &plan->sources" in owner
    assert methods.count("parse_path_sources_view(sources_obj") == 4
    assert "parse_path_sources_view(chunk_sources, &parsed_sources)" in implementation
    assert methods.count("parsed_sources.get()") == 4
    assert "parsed_sources.get()" in implementation
    assert "std::vector<PathSourceSpec> sources;" not in methods


def test_path_source_probes_use_the_execution_probe_owner() -> None:
    """Path collections, best-effort, and providers share one direct ABI owner."""
    owner = ROOT / "src/schema_sanitizer/core_impl/probes.py"
    text = owner.read_text(encoding="utf-8")
    assert not (ROOT / "src/schema_sanitizer/core_impl/probes").exists()
    assert "registry_probe_path_sources" in text
    assert "registry_probe_path_sources_best_effort" in text
    assert "registry_probe_path_source_chunk_provider" in text


def test_path_source_sink_endpoints_are_grouped_as_public_units() -> None:
    """Path-source ABI3 endpoints use small compilation units by operation."""
    registry = ROOT / "cpp/src/api/python_abi3/registry"
    owners = (
        registry / "path_source_input_methods.cc",
        registry / "path_source_registry_methods.cc",
        registry / "path_source_auto_methods.cc",
    )
    source = "\n".join((owner.read_text(encoding="utf-8") for owner in owners))
    assert "py_context_to_sink_from_path_sources" in source
    assert "py_context_to_sink_from_path_source_chunk_provider" in source
    assert "py_context_to_registry_sink_from_path_sources" in source
    assert not (registry / "path_source_methods.cc").exists()
    assert not (registry / "path_source_sinks").exists()
    assert all((len(owner.read_text(encoding="utf-8").splitlines()) <= 500 for owner in owners))


def test_path_source_size_validation_is_native_and_one_time() -> None:
    """Local plans validate file sizes while creating their reusable C++ capsule."""
    python_owner = (ROOT / "src/schema_sanitizer/input_impl/source_plan.py").read_text(
        encoding="utf-8"
    )
    cpp_owner = (ROOT / "cpp/src/api/python_abi3/path_sources/path_source_plan.cc").read_text(
        encoding="utf-8"
    )
    input_owner = (ROOT / "cpp/src/api/python_abi3/path_sources/path_sources.cc").read_text(
        encoding="utf-8"
    )
    assert "_check_path_source_sizes" not in python_owner
    assert "os.path.getsize" not in python_owner
    assert "check_document_size" not in python_owner
    assert "validate_path_source_sizes" in cpp_owner
    assert "std::ifstream input(source.path, std::ios::binary | std::ios::ate)" in cpp_owner
    assert "input.tellg()" in cpp_owner
    assert "std::filesystem" not in cpp_owner
    assert "memory_limit_bytes limit exceeded during" in cpp_owner
    assert '"OLs:path_source_plan_create"' in cpp_owner
    assert '"O|Ls:path_source_plan_create"' not in cpp_owner
    assert "std::find(kDirectPathSourceFrontends.cbegin()" in input_owner
    assert "std::ranges::contains" not in input_owner


def test_prepared_input_value_objects_have_neutral_owner() -> None:
    """Prepared-input contracts remain available without importing API implementation."""
    from schema_sanitizer.input_impl import prepared

    assert hasattr(prepared, "PreparedPublicInput")
    assert not hasattr(prepared, "prepare_public_input")


def test_prepared_manifest_sources_are_not_round_tripped_through_list() -> None:
    """Existing immutable descriptors must stay immutable through planning."""
    owner = (ROOT / "src/schema_sanitizer/api_impl/source_plan/preparation.py").read_text(
        encoding="utf-8"
    )
    assert "return manifest.source_batch.sources" in owner
    assert "list(manifest.source_batch.sources)" not in owner


def test_projection_audit_modes_have_one_owner_each() -> None:
    """Closely coupled audit phases stay in bounded mode-specific modules."""
    audits = SRC / "adapters/parquet/projection/audits"
    assert {path.name for path in audits.glob("*.py")} == {
        "__init__.py",
        "composition.py",
        "coverage.py",
        "partitions.py",
        "subset.py",
        "summary.py",
    }
    assert not (audits / "subset").exists()
    for retired in (
        "coverage_inputs.py",
        "coverage_consistency.py",
        "partition_inputs.py",
        "partition_recomposition.py",
    ):
        assert not (audits / retired).exists()
    assert (
        max((len(path.read_text(encoding="utf-8").splitlines()) for path in audits.glob("*.py")))
        <= 500
    )


def test_projection_duplicate_detection_has_one_linear_owner() -> None:
    """All projection audits share one Counter-based duplicate-name helper."""
    audits = ROOT / "src/schema_sanitizer/adapters/parquet/projection/audits"
    summary = (audits / "summary.py").read_text(encoding="utf-8")
    assert "def duplicate_names" in summary
    assert "Counter(values)" in summary
    for name in ("subset.py", "composition.py", "coverage.py", "partitions.py"):
        text = (audits / name).read_text(encoding="utf-8")
        assert "duplicate_names(" in text
        assert ".count(name)" not in text
        assert "Counter(projection)" not in text
        assert "Counter(partition)" not in text


def test_projection_duplicate_detection_is_linear() -> None:
    """Projection audits use one Counter pass instead of repeated list.count scans."""
    audits = ROOT / "src/schema_sanitizer/adapters/parquet/projection/audits"
    for name in ("partitions.py", "coverage.py"):
        text = (audits / name).read_text(encoding="utf-8")
        assert "Counter(" in text
        assert ".count(name)" not in text


def test_projection_subset_audit_has_one_bounded_owner() -> None:
    """Subset audit phases stay cohesive without a five-file micro-package."""
    assert (
        importlib.util.find_spec("schema_sanitizer.adapters.parquet.projection.audits.subset")
        is not None
    )
    owner = ROOT / "src/schema_sanitizer/adapters/parquet/projection/audits/subset.py"
    assert owner.is_file()
    assert len(owner.read_text(encoding="utf-8").splitlines()) <= 500
    assert not owner.with_suffix("").exists()


def test_public_input_preparation_has_one_direct_owner() -> None:
    """Discovery reuse, target resolution, and orchestration share one small owner."""
    owner = ROOT / "src/schema_sanitizer/api_impl/input/preparation.py"
    retired = ROOT / "src/schema_sanitizer/api_impl/input/preparation"
    assert owner.is_file()
    assert not retired.exists()
    assert len(owner.read_text(encoding="utf-8").splitlines()) <= 500
    from schema_sanitizer.api_impl.input import preparation

    assert hasattr(preparation, "prepare_public_input")


def test_source_discovery_has_one_bounded_pipeline_owner() -> None:
    """Closely coupled discovery phases must not return to a micro-package."""
    owner = ROOT / "src/schema_sanitizer/pipeline/source_discovery.py"
    assert owner.is_file()
    assert not owner.with_suffix("").exists()
    text = owner.read_text(encoding="utf-8")
    assert "def _discover_directories" in text
    assert "def _discover_source" in text
    assert "def discover_existing_source_plans_async" in text
    assert len(text.splitlines()) <= 600


def test_source_plan_deduplication_keeps_only_first_seen_uris() -> None:
    """Discovery must not allocate a list of plan positions for every URI."""
    plans = [
        PartitionRunPlan(date(2026, 1, 1), "gs://bucket/a", "out-a"),
        PartitionRunPlan(date(2026, 1, 2), "gs://bucket/b", "out-b"),
        PartitionRunPlan(date(2026, 1, 3), "gs://bucket/a", "out-c"),
    ]
    assert _unique_source_locations(plans) == {"gs://bucket/a": "gcs", "gs://bucket/b": "gcs"}
    with pytest.raises(ValueError, match="Unsupported source URI scheme: 'hdfs'"):
        _unique_source_locations([PartitionRunPlan(date(2026, 1, 1), "hdfs://cluster/a", "out")])
    owner = (ROOT / "src/schema_sanitizer/pipeline/source_discovery.py").read_text(encoding="utf-8")
    unique_owner = owner[
        owner.index("def _unique_source_locations") : owner.index("\ndef _partition_plans")
    ]
    assert "def _source_indices" not in unique_owner
    assert "defaultdict" not in unique_owner
    assert ".append(index)" not in unique_owner
    assert "location_kind(source_uri)" in unique_owner
    assert "source_uri in locations" in unique_owner


def test_source_plan_is_owned_by_the_input_domain() -> None:
    """Prepared-input models must not depend back on API orchestration."""
    package = ROOT / "src/schema_sanitizer"
    owner = package / "input_impl/source_plan.py"
    assert owner.is_file()
    assert len(owner.read_text(encoding="utf-8").splitlines()) <= 500
    assert not (package / "api_impl/source_plan/plan.py").exists()
    assert not (package / "input_impl/source_plan").exists()
    input_sources = "\n".join(
        (path.read_text(encoding="utf-8") for path in (package / "input_impl").rglob("*.py"))
    )
    assert "api_impl" not in input_sources


def test_source_plan_probing_has_one_direct_owner() -> None:
    """Probe dispatch and sequence accumulation stay in one cohesive module."""
    owner = ROOT / "src/schema_sanitizer/api_impl/source_plan/probing.py"
    assert owner.is_file()
    assert not owner.with_suffix("").exists()
    text = owner.read_text(encoding="utf-8")
    assert "def probe_source_plan_registry" in text
    assert "def _probe_sequence_registry" in text
    assert "def probe_prepared_source_plan_registry" in text
    assert "probe_child" not in text
    assert len(text.splitlines()) <= 500


def test_source_plan_sink_opening_is_owned_by_execution_context() -> None:
    """High-level sink routing owns its only source-plan opening helper."""
    owner = SRC / "api_impl/execution_context.py"
    source = owner.read_text(encoding="utf-8")
    assert "def _open_source_plan_sink_stream_or_none" in source
    assert not (SRC / "api_impl/source_plan/sink_stream.py").exists()
    assert len(source.splitlines()) <= 600


def test_text_transcoding_belongs_to_input_selection() -> None:
    """Encoding policy and its path transcoder have one bounded owner."""
    selection = ROOT / "src/schema_sanitizer/input_impl/selection.py"
    preparation = ROOT / "src/schema_sanitizer/api_impl/input/preparation.py"
    selection_source = selection.read_text(encoding="utf-8")
    preparation_source = preparation.read_text(encoding="utf-8")
    assert "class TranscodingPathByteReader" in selection_source
    assert "def prepare_native_text_data" in selection_source
    assert "prepare_native_text_data(" in preparation_source
    assert "TranscodingPathByteReader(" not in preparation_source
    assert importlib.util.find_spec("schema_sanitizer.core_impl.transcoding_reader") is None
    assert len(selection_source.splitlines()) <= 500
