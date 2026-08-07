"""Protect consolidation and hot-path ownership from maintenance layout 92."""

from __future__ import annotations

from datetime import date
from pathlib import Path

from schema_sanitizer.core_impl import hive_uris
from schema_sanitizer.pipeline.advanced import HiveRangeConfig, build_hive_range_plan

ROOT = Path(__file__).resolve().parents[2]
SRC = ROOT / "src/schema_sanitizer"
CPP = ROOT / "cpp/src"


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
            "gs://bucket/table/",
            date(2026, 7, day),
            logical_hour=None,
        )
    assert hive_uris.normalize_uri_prefix.cache_info().hits >= 3


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
        path.read_text(encoding="utf-8") for path in (CPP / "api/python_abi3/registry").glob("*.cc")
    )
    assert registry_text.count("append_registry_metadata_columns") == 13
    assert "append_first_row_columns_from_dict" not in registry_text
    assert "append_timestamp_columns" not in registry_text
    manifest = (ROOT / "cmake/SchemaSanitizerSources.cmake").read_text(encoding="utf-8")
    assert manifest.count("metadata/columns/columns.cc") == 1
    assert "metadata/columns/spans.cc" not in manifest
    assert "metadata/columns/values.cc" not in manifest
