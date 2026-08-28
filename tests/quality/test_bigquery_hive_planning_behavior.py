"""Behavioral efficiency contracts for BigQuery and Hive planning."""

from __future__ import annotations

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


def test_external_table_spec_resolves_partition_location_once(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Spec construction computes partition columns and URI prefixes once."""
    calls = 0
    original = external_table_owner.external_table_hive_uri_prefix

    def counted_prefix(**kwargs: object) -> str:
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


def test_hive_output_validation_reports_each_duplicate() -> None:
    """Duplicate outputs fail with the complete conflicting set."""
    from schema_sanitizer.pipeline.hive import _validate_unique_outputs

    plans = [PartitionRunPlan(None, f"input-{index}", f"output-{index % 2}") for index in range(4)]

    with pytest.raises(ValueError, match="output-0.*output-1"):
        _validate_unique_outputs(plans)


def test_hive_partition_points_flow_into_the_plan() -> None:
    """Hourly range planning preserves endpoints across multiple days."""
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


def test_hive_template_analysis_is_cached() -> None:
    """Repeated partitions reuse template and URI-prefix analysis."""
    hive_uris._template_fields.cache_clear()
    hive_uris.normalize_uri_prefix.cache_clear()
    template = "gs://bucket/year={year}/month={month}/date={date}/part_{yyyymmdd}.jsonl"

    for day in range(1, 5):
        hive_uris.render_uri_for_partition(template, date(2026, 7, day), None)
        hive_uris.build_partition_directory_uri(
            "gs://bucket/table/",
            date(2026, 7, day),
            logical_hour=None,
        )

    assert hive_uris._template_fields.cache_info().hits >= 3
    assert hive_uris.normalize_uri_prefix.cache_info().hits >= 3


def test_hive_uri_value_expansion_is_shared() -> None:
    """File and directory routes reuse one partition-value expansion."""
    hive_uris._uri_template_values.cache_clear()
    hive_uris.build_partitioned_file_uri(
        "gs://bucket/table",
        date(2026, 7, 12),
        logical_hour=3,
        file_name_prefix="part",
        extension="parquet",
    )
    hive_uris.build_partition_directory_uri(
        "gs://bucket/table",
        date(2026, 7, 12),
        logical_hour=3,
    )

    assert hive_uris._uri_template_values.cache_info().hits >= 1


def test_static_partition_kwargs_remain_live(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """A caller can deliberately mutate shared options between partitions."""
    options = {"input_format": "jsonl", "field_name_policy": "preserve"}
    initial_state = object()
    seen_policies: list[str] = []
    monkeypatch.setattr(
        partition_execution,
        "native_registry_state_context",
        lambda _state: nullcontext(),
    )
    monkeypatch.setattr(
        partition_execution,
        "discovered_directory_input_context",
        lambda _uri, _discovered: nullcontext(),
    )

    def fake_to_parquet(*_args: object, **kwargs: object) -> SimpleNamespace:
        seen_policies.append(str(kwargs["field_name_policy"]))
        return SimpleNamespace(
            stats={},
            schema_registry_json=None,
            schema_drifts_json="[]",
            native_registry_state=initial_state,
        )

    def mutate_options(*_args: object) -> None:
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
            schema_registry_json="{}",
            native_registry_state=initial_state,
        ),
        to_parquet_kwargs=options,
        after_partition=mutate_options,
    )

    assert seen_policies == ["preserve", "lower_snake"]
