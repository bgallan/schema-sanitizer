"""Regression tests for example 07 range-prefix pipeline helpers."""

from __future__ import annotations

import json
import logging
import re
from datetime import date
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest


def _load_example_07_runtime_support() -> Any:
    """Load the canonical runtime helper module for example 07."""
    from examples.example_07 import runtime_support

    return runtime_support


# Split from test_example_07.py: test_example_07_warm_up_logs_progress, test_example_07_warm_up_supports_json_directory_input, test_example_07_source_discovery_skips_missing_dates, ...


def test_example_07_warm_up_logs_progress(caplog, tmp_path: Path) -> None:
    """Warm-up progress must match normal-run timing fields and cadence."""
    example = _load_example_07_runtime_support()
    first = tmp_path / "first.jsonl"
    second = tmp_path / "second.jsonl"
    first.write_text('{"alpha": 1}\n', encoding="utf-8")
    second.write_text('{"beta": 2}\n', encoding="utf-8")

    args = SimpleNamespace(
        input_format="jsonl",
        input_mode="single_file",
        schema_mode="additive",
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
    warm_up_drift_runs: list[example.DateRunResult] = []

    with caplog.at_level(logging.INFO, logger="gcs_input_to_silver_parquet"):
        registry = example._infer_warm_up_schema_registry(
            args,
            plans,
            example._new_schema_registry(),
            warm_up_drift_runs=warm_up_drift_runs,
        )

    assert example.registry_has_canonical_schema(registry)
    assert "run=warmup progress=1/2 label=2026-01-01" in caplog.text
    assert "run=warmup progress=2/2 label=2026-01-02" in caplog.text
    progress_records = [
        record.message for record in caplog.records if record.message.startswith("run=warmup ")
    ]
    assert len(progress_records) == 2
    for message in progress_records:
        assert " duration=" in message
        percentages = re.search(r" cpu=(\d+\.\d)% io=(\d+\.\d)%", message)
        assert percentages is not None
        assert float(percentages.group(1)) + float(percentages.group(2)) == 100.0
        assert " io_wait_est=" not in message
        assert " cpu_share=" not in message
        assert " io_wait_share=" not in message
        assert " avg=" not in message
        assert " eta=" not in message
        assert " source=" not in message
        assert " output=" not in message
        assert " registry_updated=" not in message
        assert " drifts=" not in message
        assert " source_files=1" in message
        assert " source_size_mb=0.000" in message

    caplog.clear()
    from examples.example_07 import runtime_reporting

    with caplog.at_level(logging.INFO, logger="gcs_input_to_silver_parquet"):
        runtime_reporting._log_schema_drift_summary(
            [],
            schema_mode="additive",
            warm_up_runs=warm_up_drift_runs,
        )

    assert "Schema drift summary mode=additive total=2 warmup=2 parquet=0" in caplog.text
    assert (
        "Schema drift run=warmup partition=2026-01-01 change=new_column_added source_path=alpha"
    ) in caplog.text
    assert (
        "Schema drift run=warmup partition=2026-01-02 change=new_column_added source_path=beta"
    ) in caplog.text


def test_example_07_logs_partition_cpu_and_io_percentages(caplog, monkeypatch) -> None:
    """Each materialized partition log must show complementary CPU/I/O shares."""
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
        )

    assert (
        "run=parquet progress=1/1 label=2026-07-16 duration=5.0s cpu=40.0% io=60.0% "
        "source_files=unknown source_size_mb=unknown"
    ) in caplog.text
    assert "io_wait_est=" not in caplog.text
    assert "cpu_share=" not in caplog.text
    assert "io_wait_share=" not in caplog.text
    assert "avg=" not in caplog.text
    assert "eta=" not in caplog.text
    assert "registry_updated=" not in caplog.text
    assert "drifts=" not in caplog.text
    assert "output=" not in caplog.text

    caplog.clear()
    parallel_run = runtime_reporting.DateRunResult(
        plan=plan,
        output_schema=None,
        stats={},
        schema_drifts_json="[]",
        wall_seconds=5.0,
        cpu_seconds=8.0,
        io_wait_seconds=0.0,
    )
    with caplog.at_level(logging.INFO, logger="gcs_input_to_silver_parquet"):
        runtime_reporting._log_one_parquet_processed(
            index=1,
            total=1,
            plan=plan,
            run_result=parallel_run,
            run_seconds=5.0,
        )

    assert "duration=5.0s cpu=100.0% io=0.0%" in caplog.text
    assert "cpu=8.0s" not in caplog.text

    class ExistingWindowsPath:
        """Stand in for an existing Windows source while running on any host."""

        def __init__(self, value: str):
            """Validate the canonical Windows path reaches pathlib unchanged."""
            assert value == r"C:\source\events.jsonl"

        def is_file(self) -> bool:
            """Report that the synthetic source exists."""
            return True

        def stat(self) -> SimpleNamespace:
            """Return deterministic source size metadata."""
            return SimpleNamespace(st_size=2_500_000)

    monkeypatch.setattr(runtime_reporting, "Path", ExistingWindowsPath)
    windows_plan = runtime_reporting.DateRunPlan(
        date(2026, 7, 17),
        r"C:\source\events.jsonl",
        r"C:\target\events.parquet",
    )
    assert runtime_reporting._source_metrics(windows_plan) == (1, 2_500_000)


def test_example_07_logs_all_drifts_with_triggering_partition(caplog) -> None:
    """The final summary must attribute every drift to its partition."""
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

    with caplog.at_level(logging.INFO, logger="gcs_input_to_silver_parquet"):
        runtime_reporting._log_schema_drift_summary(runs, schema_mode="additive")

    assert "Schema drift summary mode=additive total=3" in caplog.text
    assert "warmup=" not in caplog.text
    assert "run=parquet partition=2026-07-15 change=new_column_added" in caplog.text
    assert "run=parquet partition=2026-07-15 change=column_type_promoted" in caplog.text
    assert "run=parquet partition=2026-07-16 change=new_column_version" in caplog.text
    assert "output_column=value_v2_string" in caplog.text


def test_example_07_logs_full_filesystem_prefixes_once(caplog) -> None:
    """Run startup must expose unabridged source and target prefixes."""
    from examples.example_07 import runtime_reporting

    args = SimpleNamespace(
        source_jsonl_prefix="gs://raw-bucket/a/very/long/source/prefix",
        source_jsonl_uri=None,
        silver_parquet_prefix="gs://silver-bucket/a/very/long/target/prefix",
        silver_parquet_uri=None,
    )
    with caplog.at_level(logging.INFO, logger="gcs_input_to_silver_parquet"):
        runtime_reporting._log_run_filesystem_prefixes(args)

    assert caplog.messages == [
        "Run filesystems "
        "source_prefix=gs://raw-bucket/a/very/long/source/prefix "
        "target_prefix=gs://silver-bucket/a/very/long/target/prefix"
    ]


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
        schema_mode="additive",
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
    import schema_sanitizer.pipeline.source_discovery_sync as source_discovery_mod
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

    def fake_remote_file_metadata(
        uri: str,
        *,
        memory_limit_bytes: int | None = None,
    ) -> object | None:
        """Return false for one missing generated object."""
        if uri.endswith("20260102.json"):
            return None
        return RemoteFile(uri, uri.rsplit("/", 1)[-1], 42)

    from schema_sanitizer.input_impl.directory_inputs import RemoteFile

    monkeypatch.setattr(
        source_discovery_mod.sync_backend,
        "remote_file_metadata",
        fake_remote_file_metadata,
    )

    discovery = discover_existing_source_plans(plans)

    assert [plan.label for plan in discovery.existing_plans] == ["2026-01-01", "2026-01-03"]
    assert [plan.label for plan in discovery.skipped_plans] == ["2026-01-02"]
    assert [plan.source_file_count for plan in discovery.existing_plans] == [1, 1]
    assert [plan.source_bytes for plan in discovery.existing_plans] == [42, 42]


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
    import schema_sanitizer.pipeline.source_discovery_sync as source_discovery_mod
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

    def fake_bulk_directory_check(
        provider: str,
        uris: list[str],
        extensions: tuple[str, ...],
        *,
        memory_limit_bytes: int | None = None,
    ):
        """Capture directory discovery configuration."""
        from schema_sanitizer.input_impl.directory_inputs import DirectoryDiscovery

        assert provider == "gcs"
        captured["extensions"] = extensions
        exists_by_uri = {uri: uri.endswith("hour=08") for uri in uris}
        return DirectoryDiscovery(
            exists_by_uri=exists_by_uri,
            files_by_uri={uri: [] for uri in uris},
        )

    monkeypatch.setattr(
        source_discovery_mod.sync_backend,
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
