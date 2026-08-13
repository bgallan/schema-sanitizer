"""Ownership and layout contracts for output and conversion paths."""

from __future__ import annotations

import importlib.util
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]

FOOTER = ROOT / "cpp/src/internal/parquet/footer_reader"

PARQUET_STREAM_SCHEMA = ROOT / "cpp/src/internal/parquet/footer_reader/native_stream/schema"

SRC = ROOT / "src/schema_sanitizer"

PARQUET_FOOTER_SCHEMA = FOOTER / "native_stream/schema"


def test_analytical_conversion_has_direct_bounded_owners() -> None:
    """Analytical orchestration is direct and old package routes stay removed."""
    api_impl = ROOT / "src/schema_sanitizer/api_impl"
    owner = api_impl / "analytical.py"
    output = api_impl / "results.py"
    assert owner.is_file()
    assert output.is_file()
    assert not (api_impl / "analytical").exists()
    assert len(owner.read_text(encoding="utf-8").splitlines()) <= 600
    source = owner.read_text(encoding="utf-8")
    assert "call_options_from_locals(options, ANALYTICAL_HELPER_KEYS)" in source
    assert "call_options_from_locals(dict(options)" not in source


def test_analytical_output_conversion_belongs_to_results() -> None:
    """Table conversion and analytical result wrappers share one bounded owner."""
    api_impl = ROOT / "src/schema_sanitizer/api_impl"
    owner = api_impl / "results.py"
    source = owner.read_text(encoding="utf-8")
    assert "TABLE_ADAPTER_FORMATS" in source
    assert "def normalize_table_output_format" in source
    assert "def convert_arrow_table_output" in source
    assert "class Result" in source
    assert "class SinkResult" in source
    assert not (api_impl / "analytical_output.py").exists()
    assert len(source.splitlines()) <= 800
    assert "reserve_finalizer_cleanup" in source
    assert "defer_prepared_finalizer_cleanup" in source


def test_analytical_public_backends_have_direct_owners() -> None:
    """Analytical wrappers and orchestration share one bounded direct owner."""
    api_impl = ROOT / "src/schema_sanitizer/api_impl"
    owner = api_impl / "analytical.py"
    output = api_impl / "results.py"
    retired = api_impl / "analytical"
    assert owner.is_file()
    assert output.is_file()
    assert not retired.exists()
    assert len(owner.read_text(encoding="utf-8").splitlines()) <= 600
    assert len(output.read_text(encoding="utf-8").splitlines()) <= 800
    for name in ("duckdb.py", "pandas.py", "polars.py", "pyarrow.py"):
        assert not (api_impl / name).exists()


def test_c_sink_bridge_is_split_by_operation() -> None:
    """Shared sink helpers, input calls, and diagnostics remain independent."""
    api = ROOT / "cpp/src/api/c"
    for name in (
        "schema_sanitizer_c_sink_common.cc",
        "schema_sanitizer_c_sink_input.cc",
        "schema_sanitizer_c_sink_diagnostics.cc",
    ):
        assert (api / name).is_file()
    assert not (api / "schema_sanitizer_c_sink.cc").exists()


def test_call_option_filtering_has_one_owner_per_conversion_route() -> None:
    """Invocation wrappers pass raw locals; execution filters them exactly once."""
    core = ROOT / "src/schema_sanitizer/options_impl/call_options.py"
    core_text = core.read_text(encoding="utf-8")
    assert "FILE_CONVERSION_HELPER_KEYS" in core_text
    assert "CONVERTER_HELPER_KEYS" not in core_text
    assert "PARQUET_WRITER_OPTION_KEYS" not in core_text
    analytical = ROOT / "src/schema_sanitizer/api_impl/analytical.py"
    file_conversion = ROOT / "src/schema_sanitizer/api_impl/file_conversion"
    analytical_text = analytical.read_text(encoding="utf-8")
    public_start = analytical_text.index("def to_duckdb")
    assert "call_options_from_locals" not in analytical_text[public_start:]
    converters_text = (file_conversion / "converters.py").read_text(encoding="utf-8")
    assert analytical_text.count("call_options_from_locals(") == 1
    assert converters_text.count("call_options_from_locals(") == 1
    assert not (file_conversion / "execution.py").exists()


def test_direct_native_writers_live_with_file_conversion() -> None:
    """Native direct output must stay beside its only orchestration consumer."""
    owner = SRC / "api_impl/file_conversion/direct_writers.py"
    consumer = SRC / "api_impl/file_conversion/writers.py"
    assert owner.is_file()
    assert "try_write_parquet_direct_native" in owner.read_text(encoding="utf-8")
    assert "from . import direct_writers as _native_output" in consumer.read_text(encoding="utf-8")
    assert len(owner.read_text(encoding="utf-8").splitlines()) <= 500
    assert not (SRC / "api_impl/native_output.py").exists()


def test_file_conversion_execution_has_one_small_orchestration_owner() -> None:
    """Target lifecycle, source-plan routing, and public entry points share one owner."""
    assert importlib.util.find_spec("schema_sanitizer.api_impl.file_conversion.core") is None
    owner = ROOT / "src/schema_sanitizer/api_impl/file_conversion/converters.py"
    assert owner.is_file()
    assert not owner.with_suffix("").exists()
    text = owner.read_text(encoding="utf-8")
    assert "def try_convert_source_plan_with_options" in text
    assert "def convert_file_with_options" in text
    assert len(text.splitlines()) <= 600


def test_file_conversion_orchestration_has_one_bounded_owner() -> None:
    """Public converters and target lifecycle must not split into one-consumer modules."""
    package = ROOT / "src/schema_sanitizer/api_impl/file_conversion"
    owner = package / "converters.py"
    source = owner.read_text(encoding="utf-8")
    assert "def try_convert_source_plan_with_options" in source
    assert "def convert_file_with_options" in source
    assert "def _convert_public_file" in source
    assert not (package / "execution.py").exists()
    assert len(source.splitlines()) <= 600


def test_file_conversion_reuses_normalized_writer_options() -> None:
    """Conversion must not clone or recreate writer options in each routing phase."""
    owner = ROOT / "src/schema_sanitizer/api_impl/file_conversion/converters.py"
    text = owner.read_text(encoding="utf-8")
    assert "resolved_writer_options = writer_options or {}" in text
    assert "dict(writer_options or {})" not in text
    assert text.count("(writer_options or {}).get") == 0


def test_file_converters_have_one_direct_public_owner() -> None:
    """CSV, JSONL, and Parquet wrappers share one owner without old facades."""
    package = ROOT / "src/schema_sanitizer/api_impl/file_conversion"
    owner = package / "converters.py"
    source = owner.read_text(encoding="utf-8")
    assert owner.is_file()
    assert len(source.splitlines()) <= 600
    for name in ("to_csv", "to_jsonl", "to_parquet"):
        assert f"def {name}(" in source
    for removed in ("delimited.py", "parquet.py", "invocation.py"):
        assert not (package / removed).exists()
    root_api = (ROOT / "src/schema_sanitizer/__init__.py").read_text(encoding="utf-8")
    assert root_api.count(".api_impl.file_conversion.converters") == 4
    assert ".api_impl.file_conversion.delimited" not in root_api
    assert ".api_impl.file_conversion.parquet" not in root_api


def test_file_output_metadata_has_one_cohesive_owner() -> None:
    """Metadata planning, lifecycle, and route state share one small owner."""
    assert importlib.util.find_spec("schema_sanitizer.adapters.pyarrow.output_metadata") is None
    owner = ROOT / "src/schema_sanitizer/adapters/pyarrow/file_metadata.py"
    assert owner.is_file()
    assert not (ROOT / "src/schema_sanitizer/adapters/pyarrow/file_metadata").exists()
    assert len(owner.read_text(encoding="utf-8").splitlines()) <= 500


def test_file_writers_have_one_direct_owner() -> None:
    """Native-first file writers must not return to per-format micro-modules."""
    owner = ROOT / "src/schema_sanitizer/api_impl/file_conversion/writers.py"
    text = owner.read_text(encoding="utf-8")
    assert len(text.splitlines()) <= 550
    for symbol in (
        "write_csv_native_first_stream",
        "write_jsonl_native_first_stream",
        "write_parquet_native_first_stream",
        "try_write_raw_native_file_output",
    ):
        assert f"def {symbol}(" in text
    assert not owner.with_suffix("").exists()


def test_native_file_outputs_have_one_direct_owner() -> None:
    """Small native writer routes stay cohesive instead of regaining format facades."""
    owner = ROOT / "src/schema_sanitizer/api_impl/file_conversion/direct_writers.py"
    assert owner.is_file()
    assert not owner.with_suffix("").exists()
    source = owner.read_text(encoding="utf-8")
    for name in (
        "try_write_csv_direct_native",
        "try_write_jsonl_direct_native",
        "try_write_parquet_direct_native",
    ):
        assert f"def {name}" in source
    assert len(source.splitlines()) <= 500


def test_native_output_stays_one_cohesive_module() -> None:
    """Native file-output dispatch must not regress into per-format facade packages."""
    owner = ROOT / "src/schema_sanitizer/api_impl/file_conversion/direct_writers.py"
    retired = owner.with_suffix("")
    assert owner.is_file()
    assert not retired.exists()
    source = owner.read_text(encoding="utf-8")
    assert len(source.splitlines()) <= 500
    assert "def _call_native_writer" in source
    assert "def try_write_csv_direct_native" in source
    assert "def try_write_jsonl_direct_native" in source
    assert "def try_write_parquet_direct_native" in source


def test_native_results_remain_one_cohesive_module() -> None:
    """Typed ABI results must not regress into a package of forwarding modules."""
    owner = ROOT / "src/schema_sanitizer/core_impl/native_results.py"
    assert owner.is_file()
    assert not owner.with_suffix("").exists()
    source = owner.read_text(encoding="utf-8")
    for result_type in (
        "IngestDiagnostics",
        "SchemaProbeResult",
        "RegistryProbeResult",
        "SinkOutput",
    ):
        assert f"class {result_type}" in source
    assert len(source.splitlines()) <= 500


def test_object_struct_conversion_has_a_focused_subdomain() -> None:
    """Field lookup and object materialization must remain separate units."""
    conversion = ROOT / "cpp/src/internal/materialization/conversion"
    assert not (conversion / "struct_object.cc").exists()
    assert not (conversion / "struct_object.hh").exists()
    package = conversion / "object_struct"
    assert {path.name for path in package.iterdir()} == {
        "api.hh",
        "conversion.cc",
        "fields.cc",
        "fields.hh",
    }
    assert (
        "find_strict_extra_field"
        not in (package / "conversion.cc")
        .read_text(encoding="utf-8")
        .split("Status convert_object_struct", maxsplit=1)[0]
    )


def test_output_diagnostics_and_source_preparation_are_direct_owners() -> None:
    """Retired micro-packages must not return as forwarding surfaces."""
    api_impl = ROOT / "src/schema_sanitizer/api_impl"
    output_owner = api_impl / "output_diagnostics.py"
    preparation_owner = api_impl / "source_plan/preparation.py"
    assert output_owner.is_file()
    assert preparation_owner.is_file()
    assert not (api_impl / "output_diagnostics").exists()
    assert not (api_impl / "source_plan/preparation").exists()
    assert len(output_owner.read_text(encoding="utf-8").splitlines()) <= 500
    assert len(preparation_owner.read_text(encoding="utf-8").splitlines()) <= 500


def test_output_diagnostics_are_not_owned_by_analytical_backends() -> None:
    """File and table diagnostics must remain an output concern."""
    analytical = ROOT / "src/schema_sanitizer/api_impl/analytical.py"
    assert analytical.is_file()
    assert not (ROOT / "src/schema_sanitizer/api_impl/analytical").exists()
    assert "patch_table_diagnostics" not in analytical.read_text(encoding="utf-8")
    owner = ROOT / "src/schema_sanitizer/api_impl/output_diagnostics.py"
    assert owner.is_file()
    assert not (ROOT / "src/schema_sanitizer/api_impl/output_diagnostics").exists()
    text = owner.read_text(encoding="utf-8")
    assert "def patch_file_output_diagnostics" in text
    assert "def patch_table_diagnostics" in text


def test_pipeline_runtime_dependencies_are_not_owned_by_file_conversion() -> None:
    """Registry state, probe options, and low-level context pooling stay neutral."""
    core = ROOT / "src/schema_sanitizer/core_impl"
    assert (core / "probes.py").is_file()
    assert (core / "execution.py").is_file()
    assert not (core / "execution").exists()
    assert (ROOT / "src/schema_sanitizer/core_impl/schema_registry.py").is_file()
    execution = ROOT / "src/schema_sanitizer/api_impl/file_conversion/converters.py"
    combined = execution.read_text(encoding="utf-8")
    assert "ContextVar" not in combined
    assert "def options_for_schema_probe" not in combined
    assert "def schema_registry_native_state_context" not in combined


def test_recursive_output_layout_builds_and_counts_once() -> None:
    """Each leaf tree is built once and subtree counts are reused by Arrow setup."""
    owner = PARQUET_FOOTER_SCHEMA / "native_stream_output_layout.cc.inc"
    source = owner.read_text(encoding="utf-8")
    model = (PARQUET_FOOTER_SCHEMA / "native_stream_recursive_model.cc.inc").read_text(
        encoding="utf-8"
    )
    schema_root = (PARQUET_FOOTER_SCHEMA / "native_stream_arrow_schema_root.cc.inc").read_text(
        encoding="utf-8"
    )
    assert source.count("build_native_recursive_materialization_tree(") == 1
    assert "plan_native_recursive_path(path" not in source
    assert "recursive_subtree_counts" in source
    assert "recursive_subtree_counts" in model
    assert "const auto &subtree_counts = field.recursive_subtree_counts" in schema_root
    assert "count_native_recursive_materialization_subtree_resources(" not in schema_root
    assert not (PARQUET_FOOTER_SCHEMA / "native_stream_output_field_layout.cc.inc").exists()
    assert not (PARQUET_FOOTER_SCHEMA / "native_stream_metadata_validation.cc.inc").exists()
    assert len(source.splitlines()) <= 500


def test_recursive_output_layout_defers_counts_and_avoids_tree_copies() -> None:
    """Wide schemas must not copy and recount the merged tree for every leaf."""
    layout = (PARQUET_STREAM_SCHEMA / "native_stream_output_layout.cc.inc").read_text(
        encoding="utf-8"
    )
    validation = layout
    assert "auto merged_tree = field.recursive_tree" not in layout
    assert "count_native_recursive_materialization_resources" not in layout
    assert "recursive_tree, &field.recursive_tree" in layout
    assert "finalize_native_output_layout" in validation
    assert "plan_native_recursive_materialization_tree(" in validation
    assert "recursive_subtree_counts" in validation
    assert validation.count("return finalize_native_output_layout(") == 1
    assert "plan_native_recursive_layout_columns" in validation
    assert "SAN_RETURN_NOT_OK(finalize_native_output_layout" in validation


def test_recursive_output_validation_is_linear_and_portable() -> None:
    """Recursive output validation avoids copying and unsupported range algorithms."""
    layout = FOOTER / "native_stream/schema/native_stream_output_layout.cc.inc"
    text = layout.read_text(encoding="utf-8")
    assert "enum class LeafState" in text
    assert "std::find(leaf_states.cbegin(), leaf_states.cend(), LeafState::Unseen)" in text
    assert "std::ranges::contains" not in text
    assert "std::ranges::sort(recursive_leaf_columns)" not in text
    assert "expected_leaf_columns" not in text
    model = FOOTER / "native_stream/schema/native_stream_recursive_model.cc.inc"
    model_text = model.read_text(encoding="utf-8")
    assert "tree->leaf_column_indices.reserve(tree->nodes.size())" in model_text
    assert "pending.reserve(tree->nodes.size())" in model_text
    assert "std::views::reverse" in model_text
    assert not (
        FOOTER / "native_stream/schema/native_stream_recursive_leaf_columns.cc.inc"
    ).exists()


def test_stream_output_has_one_direct_owner_and_one_normalization() -> None:
    """File stream output must not return to per-phase gateway modules."""
    api = ROOT / "src/schema_sanitizer/api_impl"
    owner = api / "stream_output.py"
    source = owner.read_text(encoding="utf-8")
    assert owner.is_file()
    assert len(source.splitlines()) <= 500
    assert not (api / "stream_output").exists()
    assert source.count("normalize_options(") == 1
    assert "call_options: Options | None" in source
    assert "options=call_options" in source
