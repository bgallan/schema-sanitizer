"""Ownership and layout contracts for BigQuery, Hive, and partition planning."""

from __future__ import annotations

import importlib.util
from contextlib import nullcontext
from datetime import date
from pathlib import Path
from types import SimpleNamespace

import pytest

import schema_sanitizer.integrations.bigquery.external_table as external_table_owner
import schema_sanitizer.pipeline.partition_execution as partition_execution
from schema_sanitizer.core_impl import hive_uris
from schema_sanitizer.pipeline.advanced import HiveRangeConfig, build_hive_range_plan
from schema_sanitizer.pipeline.types import PartitionRunPlan, SchemaRegistryState

ROOT = Path(__file__).resolve().parents[2]

SRC = ROOT / "src/schema_sanitizer"


def test_bigquery_external_table_and_namespace_owners_are_flat() -> None:
    """BigQuery no longer has parallel singular and package-shaped owners."""
    bigquery = SRC / "integrations/bigquery"
    assert (bigquery / "external_table.py").is_file()
    assert (bigquery / "namespace_ops.py").is_file()
    assert not (bigquery / "external_tables").exists()
    assert not (bigquery / "namespaces").exists()
    assert len((bigquery / "external_table.py").read_text(encoding="utf-8").splitlines()) <= 500
    assert len((bigquery / "namespace_ops.py").read_text(encoding="utf-8").splitlines()) <= 500


def test_bigquery_external_table_helpers_have_one_bounded_owner() -> None:
    """External-table model, URI, partition, and DDL helpers stay cohesive."""
    bigquery = ROOT / "src/schema_sanitizer/integrations/bigquery"
    owner = bigquery / "external_table.py"
    assert owner.is_file()
    assert not (bigquery / "external_tables").exists()
    source = owner.read_text(encoding="utf-8")
    assert "class ExternalTableSpec" in source
    assert "def external_table_ddl" in source
    assert "def external_table_spec_from_namespace" in source
    assert len(source.splitlines()) <= 500


def test_bigquery_namespace_workflows_have_one_direct_owner() -> None:
    """Namespace-derived client and table workflows stay in one bounded module."""
    bigquery = ROOT / "src/schema_sanitizer/integrations/bigquery"
    owner = bigquery / "namespace_ops.py"
    registry = bigquery / "registry.py"
    assert owner.is_file()
    assert not (bigquery / "namespaces").exists()
    source = owner.read_text(encoding="utf-8")
    assert "def import_bigquery_adbc" in source
    assert "def create_or_replace_external_bigquery_table_from_namespace" in source
    assert len(source.splitlines()) <= 500
    assert registry.is_file()
    assert not registry.with_suffix("").exists()
    registry_source = registry.read_text(encoding="utf-8")
    assert "def fetch_latest_schema_registry" in registry_source
    assert "def prepare_existing_schema_registry_from_namespace" in registry_source
    assert len(registry_source.splitlines()) <= 500


def test_bigquery_registry_and_sidecar_have_one_owner_each() -> None:
    """Embedded-registry and sidecar operations stay cohesive and bounded."""
    package = ROOT / "src/schema_sanitizer/integrations/bigquery"
    registry = package / "registry.py"
    sidecar = package / "sidecar.py"
    assert registry.is_file() and (not registry.with_suffix("").exists())
    assert sidecar.is_file() and (not sidecar.with_suffix("").exists())
    registry_text = registry.read_text(encoding="utf-8")
    sidecar_text = sidecar.read_text(encoding="utf-8")
    for symbol in (
        "fetch_latest_schema_registry",
        "prepare_existing_schema_registry_from_namespace",
        "latest_schema_registry_query",
    ):
        assert f"def {symbol}" in registry_text
    for symbol in (
        "fetch_sidecar_last_ingested_partition",
        "update_registry_sidecar_table",
        "sidecar_upsert_query",
    ):
        assert f"def {symbol}" in sidecar_text
    assert len(registry_text.splitlines()) <= 500
    assert len(sidecar_text.splitlines()) <= 500


def test_bigquery_registry_has_bounded_direct_owners() -> None:
    """Registry and sidecar workflows should not be fragmented into micro-packages."""
    package = ROOT / "src/schema_sanitizer/integrations/bigquery"
    registry = package / "registry.py"
    sidecar = package / "sidecar.py"
    assert registry.is_file() and sidecar.is_file()
    assert not registry.with_suffix("").exists()
    assert not sidecar.with_suffix("").exists()
    assert len(registry.read_text(encoding="utf-8").splitlines()) <= 500
    assert len(sidecar.read_text(encoding="utf-8").splitlines()) <= 500


def test_bigquery_sidecar_has_one_bounded_owner() -> None:
    """BigQuery sidecar SQL, lookup, and mutation stay in one cohesive module."""
    assert (
        importlib.util.find_spec("schema_sanitizer.integrations.bigquery.registry_sidecar") is None
    )
    owner = ROOT / "src/schema_sanitizer/integrations/bigquery/sidecar.py"
    assert owner.is_file()
    assert not owner.with_suffix("").exists()
    source = owner.read_text(encoding="utf-8")
    assert "def sidecar_table_ddl" in source
    assert "def fetch_sidecar_last_ingested_partition" in source
    assert "def update_registry_sidecar_table" in source
    assert len(source.splitlines()) <= 500


def test_bigquery_sql_helpers_have_one_owner() -> None:
    """Quoting and canonical names must not return to parallel tiny modules."""
    package = ROOT / "src/schema_sanitizer/integrations/bigquery"
    owner = package / "sql.py"
    source = owner.read_text(encoding="utf-8")
    assert owner.is_file()
    assert "_BQ_TYPE_SYNONYMS =" in source
    assert source.count("_BQ_TYPE_SYNONYMS =") == 1
    assert "def _validate_identifier_component" in source
    assert not (package / "identifiers.py").exists()
    assert not (package / "type_normalization.py").exists()


def test_external_table_spec_resolves_partition_location_once(monkeypatch) -> None:
    """Spec construction must not recompute partition columns and URI prefixes."""
    calls = 0
    original = external_table_owner.external_table_hive_uri_prefix

    def counted_prefix(**kwargs: object) -> str:
        """Count Hive-prefix resolutions while preserving behavior."""
        nonlocal calls
        calls += 1
        return original(**kwargs)

    monkeypatch.setattr(external_table_owner, "external_table_hive_uri_prefix", counted_prefix)
    spec = external_table_owner.external_table_spec_from_namespace(
        SimpleNamespace(
            silver_parquet_prefix="gs://bucket/table",
            partition_granularity="hourly",
            external_table_source_uri=None,
        )
    )
    assert calls == 1
    assert spec.hive_uri_prefix == "gs://bucket/table"
    assert spec.source_uris == ["gs://bucket/table/*"]
    assert spec.partition_columns[-1] == ("hour", "INT64")


def test_hive_output_validation_is_linear_and_reports_duplicates() -> None:
    """Duplicate-output validation must use one pass rather than nested scans."""
    from schema_sanitizer.pipeline.hive import _validate_unique_outputs

    plans = [PartitionRunPlan(None, f"input-{index}", f"output-{index % 2}") for index in range(4)]
    with pytest.raises(ValueError, match="output-0.*output-1"):
        _validate_unique_outputs(plans)
    source = (SRC / "pipeline/hive.py").read_text(encoding="utf-8")
    function = source[
        source.index("def _validate_unique_outputs") : source.index(
            "\ndef ", source.index("def _validate_unique_outputs") + 1
        )
    ]
    assert "sum(" not in function
    assert "seen:" in function


def test_hive_partition_points_are_streamed_into_the_plan() -> None:
    """Range planning must not allocate a second list of partition points."""
    source = (SRC / "pipeline/hive.py").read_text(encoding="utf-8")
    assert "def _iter_partition_points" in source
    assert "yield logical_date" in source
    assert "list[tuple[date, int | None]]" not in source
    plans = build_hive_range_plan(
        HiveRangeConfig(
            source_prefix="gs://bucket/raw/events",
            output_prefix="gs://bucket/silver/events",
            start_date=date(2026, 7, 1),
            end_date=date(2026, 7, 2),
            partition_granularity="hourly",
            start_hour=22,
            end_hour=23,
            input_format="jsonl",
        )
    )
    assert len(plans) == 4
    assert plans[0].source_uri.endswith("events_20260701_22.jsonl")
    assert plans[-1].output_uri.endswith("events_20260702_23.parquet")


def test_hive_template_parsing_is_cached_per_template() -> None:
    """Repeated partitions must reuse placeholder and normalization analysis."""
    hive_uris._template_fields.cache_clear()
    hive_uris.normalize_uri_prefix.cache_clear()
    template = "gs://bucket/year={year}/month={month}/date={date}/part_{yyyymmdd}.jsonl"
    for day in range(1, 5):
        hive_uris.render_uri_for_partition(template, date(2026, 7, day), None)
    assert hive_uris._template_fields.cache_info().hits >= 3
    for day in range(1, 5):
        hive_uris.build_partition_directory_uri(
            "gs://bucket/table/", date(2026, 7, day), logical_hour=None
        )
    assert hive_uris.normalize_uri_prefix.cache_info().hits >= 3


def test_hive_uri_helpers_have_one_cached_owner() -> None:
    """Hive URI values and rendering must not return to a micro-package."""
    owner = SRC / "core_impl/hive_uris.py"
    assert owner.is_file()
    assert not owner.with_suffix("").exists()
    text = owner.read_text(encoding="utf-8")
    assert "@lru_cache(maxsize=4096)" in text
    assert "def _partition_directory_uri" in text
    assert len(text.splitlines()) <= 500
    hive_uris._uri_template_values.cache_clear()
    hive_uris.build_partitioned_file_uri(
        "gs://bucket/table",
        date(2026, 7, 12),
        logical_hour=3,
        file_name_prefix="part",
        extension="parquet",
    )
    hive_uris.build_partition_directory_uri("gs://bucket/table", date(2026, 7, 12), logical_hour=3)
    assert hive_uris._uri_template_values.cache_info().hits >= 1


def test_hive_uri_helpers_have_one_neutral_bounded_owner() -> None:
    """BigQuery and pipeline share one neutral Hive URI implementation."""
    assert not (ROOT / "src/schema_sanitizer/pipeline/hive").exists()
    owner = ROOT / "src/schema_sanitizer/core_impl/hive_uris.py"
    assert owner.is_file()
    assert len(owner.read_text(encoding="utf-8").splitlines()) <= 500
    assert not owner.with_suffix("").exists()
    bigquery_text = "\n".join(
        (
            path.read_text(encoding="utf-8")
            for path in (ROOT / "src/schema_sanitizer/integrations/bigquery").rglob("*.py")
        )
    )
    assert "pipeline.hive.uris" not in bigquery_text


def test_partition_audit_has_one_bounded_owner() -> None:
    """Closely coupled partition inputs and recomposition share one owner."""
    package = ROOT / "src/schema_sanitizer/adapters/parquet/projection/audits"
    owner = package / "partitions.py"
    assert owner.is_file()
    assert len(owner.read_text(encoding="utf-8").splitlines()) <= 500
    assert not (package / "partition_inputs.py").exists()
    assert not (package / "partition_recomposition.py").exists()


def test_partition_execution_has_one_bounded_owner() -> None:
    """The small partition loop, result, and registry bootstrap share one owner."""
    assert importlib.util.find_spec("schema_sanitizer.pipeline.execution") is None
    owner = ROOT / "src/schema_sanitizer/pipeline/partition_execution.py"
    source = owner.read_text(encoding="utf-8")
    assert owner.is_file()
    assert len(source.splitlines()) <= 500
    assert "def _compile_native_registry_state" in source
    assert "class PartitionPipelineResult" in source
    assert "def run_partitioned_to_parquet_registry_json" in source


def test_registry_sidecar_partition_is_parsed_once_per_query() -> None:
    """The fetch path delegates validation to the query builder exactly once."""
    owner = ROOT / "src/schema_sanitizer/integrations/bigquery/registry.py"
    text = owner.read_text(encoding="utf-8")
    fetch_body = text.split("def fetch_latest_schema_registry(", 1)[1].split(
        "def fetch_latest_schema_registry_from_namespace(", 1
    )[0]
    assert "partition_filter_sql(partition_key, partition_columns)" not in fetch_body
    assert "except ValueError:" in fetch_body


def test_static_partition_kwargs_avoid_redundant_copy_and_remain_live(
    monkeypatch, tmp_path: Path
) -> None:
    """Static kwargs avoid an extra dict copy while preserving live mutations."""
    options = {"input_format": "jsonl", "field_name_policy": "preserve"}
    initial_state = object()
    seen_policies: list[str] = []
    monkeypatch.setattr(
        partition_execution, "native_registry_state_context", lambda _state: nullcontext()
    )
    monkeypatch.setattr(
        partition_execution,
        "discovered_directory_input_context",
        lambda _uri, _discovered: nullcontext(),
    )

    def fake_to_parquet(*_args, **kwargs):
        """Record the current mapping value for each partition."""
        seen_policies.append(kwargs["field_name_policy"])
        return SimpleNamespace(
            stats={},
            schema_registry_json=None,
            schema_drifts_json="[]",
            native_registry_state=initial_state,
        )

    def mutate_options(*_args) -> None:
        """Change the shared mapping after the first partition."""
        options["field_name_policy"] = "lower_snake"

    monkeypatch.setattr(partition_execution, "to_parquet", fake_to_parquet)
    plans = [
        PartitionRunPlan(None, f"source-{index}", str(tmp_path / f"{index}.parquet"))
        for index in range(2)
    ]
    partition_execution.run_partitioned_to_parquet_registry_json(
        plans,
        initial_schema_registry_json="{}",
        initial_schema_registry_state=SchemaRegistryState(
            schema_registry_json="{}", native_registry_state=initial_state
        ),
        to_parquet_kwargs=options,
        after_partition=mutate_options,
    )
    source = (ROOT / "src/schema_sanitizer/pipeline/partition_execution.py").read_text(
        encoding="utf-8"
    )
    assert "dict(to_parquet_kwargs)" not in source
    assert seen_policies == ["preserve", "lower_snake"]
