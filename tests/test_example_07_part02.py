"""Regression tests for example 07 range-prefix pipeline helpers."""

from __future__ import annotations

import importlib.util
import json
import logging
import sys
from datetime import date
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest


def _load_example_08() -> Any:
    """Load example 07 despite its numeric filename prefix."""
    path = (
        Path(__file__).resolve().parents[1]
        / "examples"
        / "example_07"
        / "07_gcs_jsonl_to_silver_parquet_range_prefix.py"
    )
    spec = importlib.util.spec_from_file_location("schema_sanitizer_example_08", path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"Could not load example module from {path}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def _load_example_07_runtime_support() -> Any:
    """Load the canonical runtime helper module for example 07."""
    from examples.example_07 import runtime_support

    return runtime_support


# Split from test_example_07.py: test_example_07_warm_up_logs_progress, test_example_07_warm_up_supports_json_directory_input, test_example_07_source_discovery_skips_missing_dates, ...


def test_example_07_warm_up_logs_progress(caplog, tmp_path: Path) -> None:
    """Verify example warm-up emits concise source preparation and scan progress."""
    example = _load_example_07_runtime_support()
    first = tmp_path / "first.jsonl"
    second = tmp_path / "second.jsonl"
    first.write_text('{"alpha": 1}\n', encoding="utf-8")
    second.write_text('{"beta": 2}\n', encoding="utf-8")

    args = SimpleNamespace(
        input_format="jsonl",
        input_mode="single_file",
        schema_mode="strict",
        column_order="alphabetically",
        field_name_policy="lower_snake",
        timestamp_precision="TIMESTAMP_MICROS",
        parse_integers=True,
        parse_floats=True,
        parse_float_decimal_separator=".",
        parse_float_thousands_separator=",",
        parse_iso_timestamps=True,
        parse_iso_dates=True,
        parse_iso_times=True,
        on_error="emit_null_row",
        memory_limit_bytes=64 * 1024 * 1024,
        arrow_max_depth=32,
        parquet_max_depth=15,
        input_text_encoding="utf-8",
    )
    plans = [
        example.DateRunPlan(date(2026, 1, 1), str(first), str(tmp_path / "first.parquet")),
        example.DateRunPlan(date(2026, 1, 2), str(second), str(tmp_path / "second.parquet")),
    ]

    with caplog.at_level(logging.INFO, logger="gcs_input_to_silver_parquet"):
        registry = example._infer_warm_up_schema_registry(
            args,
            plans,
            example._new_schema_registry(),
        )

    assert example.registry_has_canonical_schema(registry)
    assert "Warm-up scan starting partitions=2 mode=additive" in caplog.text
    assert "Warm-up prepare 1/2" in caplog.text
    assert "Warm-up prepare 2/2" in caplog.text
    assert "Warm-up scan finished partitions=2" in caplog.text
    assert "canonical_schema=True" in caplog.text


def test_example_07_logs_partition_cpu_and_io_wait_summary(caplog) -> None:
    """Each materialized partition log must compare CPU with estimated I/O wait."""
    from examples.example_07 import runtime_reporting

    plan = runtime_reporting.DateRunPlan(date(2026, 7, 16), "source", "output.parquet")
    run = runtime_reporting.DateRunResult(
        plan=plan,
        output_schema=None,
        stats={},
        schema_drifts_json="[]",
        wall_seconds=5.0,
        cpu_seconds=2.0,
        io_wait_seconds=3.0,
    )

    with caplog.at_level(logging.INFO, logger="gcs_input_to_silver_parquet"):
        runtime_reporting._log_one_parquet_processed(
            index=1,
            total=1,
            plan=plan,
            run_result=run,
            run_seconds=5.0,
            pipeline_start_time=0.0,
            registry_updated=True,
        )

    assert "duration=5.0s cpu=2.0s io_wait_est=3.0s" in caplog.text
    assert "cpu_share=40.0% io_wait_share=60.0%" in caplog.text
    assert "drifts=0" in caplog.text


def test_example_07_prints_all_additive_drifts_with_triggering_partition(capsys) -> None:
    """The final additive summary must attribute every drift to its partition."""
    from examples.example_07 import runtime_reporting

    first_plan = runtime_reporting.DateRunPlan(date(2026, 7, 15), "source-a", "a.parquet")
    second_plan = runtime_reporting.DateRunPlan(date(2026, 7, 16), "source-b", "b.parquet")
    runs = [
        runtime_reporting.DateRunResult(
            plan=first_plan,
            output_schema=None,
            stats={},
            schema_drifts_json=json.dumps(
                [
                    {
                        "source_path": "new_field",
                        "output_name": "new_field",
                        "drift_type": "newly_added",
                        "previous_schema": None,
                        "new_schema": "int64",
                    },
                    {
                        "source_path": "percentage",
                        "output_name": "percentage",
                        "drift_type": "type_promoted",
                        "previous_schema": "int64",
                        "new_schema": "double",
                    },
                ]
            ),
        ),
        runtime_reporting.DateRunResult(
            plan=second_plan,
            output_schema=None,
            stats={},
            schema_drifts_json=json.dumps(
                [
                    {
                        "source_path": "value",
                        "output_name": "value_v2_string",
                        "drift_type": "new_version_generated",
                        "previous_schema": "int64",
                        "new_schema": "string",
                    }
                ]
            ),
        ),
    ]

    runtime_reporting._print_schema_drift_summary(runs, schema_mode="additive")

    output = capsys.readouterr().out
    assert "Total schema drift(s): 3" in output
    assert "partition=2026-07-15 change=new_column_added" in output
    assert "partition=2026-07-15 change=column_type_promoted" in output
    assert "partition=2026-07-16 change=new_column_version" in output
    assert "output_column=value_v2_string" in output

    runtime_reporting._print_schema_drift_summary(runs, schema_mode="strict")
    assert capsys.readouterr().out == ""


def test_example_07_warm_up_supports_json_directory_input(tmp_path: Path) -> None:
    """Verify JSON directory warm-up scans all partitions as one additive registry."""
    example = _load_example_07_runtime_support()
    first = tmp_path / "hour=00"
    second = tmp_path / "hour=01"
    first.mkdir()
    second.mkdir()
    (first / "a.json").write_text('{"alpha": 1}', encoding="utf-8")
    (second / "b.json").write_text('{"beta": 2}', encoding="utf-8")

    args = SimpleNamespace(
        input_format="json",
        input_mode="directory",
        schema_mode="strict",
        column_order="alphabetically",
        field_name_policy="lower_snake",
        timestamp_precision="TIMESTAMP_MICROS",
        parse_integers=True,
        parse_floats=True,
        parse_float_decimal_separator=".",
        parse_float_thousands_separator=",",
        parse_iso_timestamps=True,
        parse_iso_dates=True,
        parse_iso_times=True,
        on_error="emit_null_row",
        memory_limit_bytes=64 * 1024 * 1024,
        arrow_max_depth=32,
        parquet_max_depth=15,
        input_text_encoding="utf-8",
    )
    plans = [
        example.DateRunPlan(
            logical_date=date(2026, 1, 1),
            source_uri=str(first),
            output_uri=str(tmp_path / "first.parquet"),
        ),
        example.DateRunPlan(
            logical_date=date(2026, 1, 1),
            logical_hour=1,
            source_uri=str(second),
            output_uri=str(tmp_path / "second.parquet"),
        ),
    ]

    registry = example._infer_warm_up_schema_registry(args, plans, example._new_schema_registry())

    assert example.registry_has_canonical_schema(registry)
    fields = registry["canonical_schema"]["fields"]
    assert {field["name"] for field in fields} >= {"alpha", "beta"}


def test_example_07_source_discovery_skips_missing_dates(monkeypatch) -> None:
    """Verify date-range source discovery removes missing files before conversion."""
    import schema_sanitizer.pipeline.source_discovery as source_discovery_mod
    from schema_sanitizer.pipeline import discover_existing_source_plans
    from schema_sanitizer.pipeline.types import PartitionRunPlan

    plans = [
        PartitionRunPlan(
            logical_date=date(2026, 1, day),
            source_uri=(
                f"gs://raw/year=2026/month=01/date=2026-01-0{day}/events_2026010{day}.json"
            ),
            output_uri=(
                f"gs://silver/year=2026/month=01/date=2026-01-0{day}/events_2026010{day}.parquet"
            ),
        )
        for day in (1, 2, 3)
    ]

    async def fake_remote_file_exists(uri: str, *, memory_limit_bytes: int | None = None) -> bool:
        """Return false for one missing generated object."""
        return not uri.endswith("20260102.json")

    monkeypatch.setattr(source_discovery_mod.routing, "remote_file_exists", fake_remote_file_exists)

    discovery = discover_existing_source_plans(plans)

    assert [plan.label for plan in discovery.existing_plans] == ["2026-01-01", "2026-01-03"]
    assert [plan.label for plan in discovery.skipped_plans] == ["2026-01-02"]


def test_example_07_daily_single_file_prefix_plan() -> None:
    """Verify daily single-file mode generates one source and output object per date."""
    from examples.example_07.cli import build_parser
    from schema_sanitizer.pipeline import build_hive_range_plan_from_namespace

    args = build_parser().parse_args(
        [
            "--source-jsonl-prefix",
            "gs://raw/events/rt",
            "--silver-parquet-prefix",
            "gs://silver/events/rt",
            "--start-date",
            "2026-06-25",
            "--end-date",
            "2026-06-25",
            "--input-format",
            "jsonl",
            "--target-table",
            "project.dataset.table",
        ]
    )

    plans = build_hive_range_plan_from_namespace(args)

    assert len(plans) == 1
    assert plans[0].source_uri == (
        "gs://raw/events/rt/year=2026/month=06/date=2026-06-25/events_20260625.jsonl"
    )
    assert plans[0].output_uri == (
        "gs://silver/events/rt/year=2026/month=06/date=2026-06-25/events_20260625.parquet"
    )


def test_example_07_hourly_directory_prefix_plan() -> None:
    """Verify hourly directory mode aggregates each source partition separately."""
    from examples.example_07.cli import build_parser
    from schema_sanitizer.integrations.bigquery import hive_partition_columns
    from schema_sanitizer.pipeline import build_hive_range_plan_from_namespace

    args = build_parser().parse_args(
        [
            "--source-jsonl-prefix",
            "gs://raw/events/rt",
            "--silver-parquet-prefix",
            "gs://silver/events/rt",
            "--start-date",
            "2026-06-25",
            "--end-date",
            "2026-06-25",
            "--start-hour",
            "8",
            "--end-hour",
            "9",
            "--partition-granularity",
            "hourly",
            "--input-format",
            "ndjson",
            "--input-mode",
            "directory",
            "--target-table",
            "project.dataset.table",
        ]
    )

    plans = build_hive_range_plan_from_namespace(args)

    assert [plan.label for plan in plans] == [
        "2026-06-25/hour=08",
        "2026-06-25/hour=09",
    ]
    assert plans[0].source_uri == ("gs://raw/events/rt/year=2026/month=06/date=2026-06-25/hour=08")
    assert plans[0].output_uri == (
        "gs://silver/events/rt/year=2026/month=06/date=2026-06-25/hour=08/"
        "events_20260625_08.parquet"
    )
    assert hive_partition_columns(
        args.hive_partition_column,
        partition_granularity=args.partition_granularity,
    ) == (
        ("year", "INT64"),
        ("month", "INT64"),
        ("date", "DATE"),
        ("hour", "INT64"),
    )


def test_example_07_rejects_source_extension_mismatched_with_input_format() -> None:
    """Verify prefix planning enforces the public input format extension contract."""
    from examples.example_07.cli import build_parser
    from schema_sanitizer.pipeline import build_hive_range_plan_from_namespace

    args = build_parser().parse_args(
        [
            "--source-jsonl-prefix",
            "gs://raw/events/rt",
            "--silver-parquet-prefix",
            "gs://silver/events/rt",
            "--start-date",
            "2026-06-25",
            "--end-date",
            "2026-06-25",
            "--input-format",
            "jsonl",
            "--source-file-extension",
            "json",
            "--target-table",
            "project.dataset.table",
        ]
    )

    with pytest.raises(ValueError, match="requires extension"):
        build_hive_range_plan_from_namespace(args)


def test_example_07_hourly_uri_templates_render_hour_and_filename() -> None:
    """Verify explicit hourly URI templates render partition and filename placeholders."""
    from examples.example_07.cli import build_parser
    from schema_sanitizer.pipeline import build_hive_range_plan_from_namespace

    args = build_parser().parse_args(
        [
            "--source-jsonl-uri",
            "gs://raw/year={year}/month={month}/date={date}/hour={hour}",
            "--silver-parquet-uri",
            "gs://silver/year={year}/month={month}/date={date}/hour={hour}/"
            "events_{yyyymmddhh}.parquet",
            "--start-date",
            "2026-06-25",
            "--end-date",
            "2026-06-25",
            "--start-hour",
            "8",
            "--end-hour",
            "8",
            "--partition-granularity",
            "hourly",
            "--input-format",
            "jsonl",
            "--input-mode",
            "directory",
            "--target-table",
            "project.dataset.table",
        ]
    )

    plan = build_hive_range_plan_from_namespace(args)[0]

    assert plan.source_uri == ("gs://raw/year=2026/month=06/date=2026-06-25/hour=08")
    assert plan.output_uri == (
        "gs://silver/year=2026/month=06/date=2026-06-25/hour=08/events_2026062508.parquet"
    )


def test_example_07_directory_discovery_skips_empty_partitions(monkeypatch) -> None:
    """Verify directory mode lists partitions and skips those without matching files."""
    import schema_sanitizer.pipeline.source_discovery as source_discovery_mod
    from schema_sanitizer.pipeline import discover_existing_source_plans
    from schema_sanitizer.pipeline.types import PartitionRunPlan

    plans = [
        PartitionRunPlan(
            logical_date=date(2026, 6, 25),
            logical_hour=hour,
            source_uri=(f"gs://raw/year=2026/month=06/date=2026-06-25/hour={hour:02d}"),
            output_uri=(
                "gs://silver/year=2026/month=06/date=2026-06-25/"
                f"hour={hour:02d}/events_20260625_{hour:02d}.parquet"
            ),
        )
        for hour in (8, 9)
    ]
    captured: dict[str, object] = {}

    async def fake_bulk_directory_check(
        uris: list[str],
        extensions: tuple[str, ...],
        *,
        memory_limit_bytes: int | None = None,
    ):
        """Capture directory discovery configuration."""
        from schema_sanitizer.input_impl.directory_inputs import DirectoryDiscovery

        captured["extensions"] = extensions
        exists_by_uri = {uri: uri.endswith("hour=08") for uri in uris}
        return DirectoryDiscovery(
            exists_by_uri=exists_by_uri,
            files_by_uri={uri: [] for uri in uris},
        )

    monkeypatch.setattr(
        source_discovery_mod.gcs,
        "directories_containing_files",
        fake_bulk_directory_check,
    )

    discovery = discover_existing_source_plans(
        plans,
        input_mode="directory",
        input_format="parquet",
    )

    assert captured["extensions"] == ("parquet", "pq")
    assert [plan.label for plan in discovery.existing_plans] == ["2026-06-25/hour=08"]
    assert [plan.label for plan in discovery.skipped_plans] == ["2026-06-25/hour=09"]
