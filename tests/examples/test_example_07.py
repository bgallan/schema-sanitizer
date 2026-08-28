"""Example 07 CLI, planning, discovery, execution, and reporting contracts."""

from __future__ import annotations

import json
import logging
import re
import subprocess
import sys
from datetime import date
from importlib.util import module_from_spec, spec_from_file_location
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest


def _load_example_07_runtime_support() -> Any:
    """Load the canonical runtime helper module for example 07."""
    from examples.example_07 import runtime_support

    return runtime_support


def test_example_07_always_forces_additive_normal_runs(monkeypatch) -> None:
    """Normal conversion must remain additive even for a conflicting namespace."""
    pa = pytest.importorskip("pyarrow")
    example = _load_example_07_runtime_support()
    captured: dict[str, Any] = {}

    def fake_to_parquet(input_path: str, output_path: str, **kwargs: Any) -> Any:
        """Capture converter kwargs without writing a Parquet file."""
        captured["input_path"] = input_path
        captured["output_path"] = output_path
        captured.update(kwargs)
        return SimpleNamespace(
            stats={},
            schema_registry={"canonical_schema": {"fields": []}},
            schema_drifts=[],
        )

    monkeypatch.setattr(example.ss, "to_parquet", fake_to_parquet)
    monkeypatch.setattr(example, "read_parquet_schema", lambda _uri: pa.schema([]))
    monkeypatch.setattr(example, "log_schema_drift_from_namespace", lambda *_args, **_kwargs: None)

    args = SimpleNamespace(
        input_format="json_array",
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
        multi_threading=True,
        memory_limit_bytes=64 * 1024 * 1024,
        arrow_max_depth=32,
        parquet_max_depth=15,
        parquet_compression="gzip",
        parquet_gzip_level=None,
        input_text_encoding="utf-8",
    )
    plan = example.DateRunPlan(
        logical_date=date(2026, 1, 2),
        source_uri="gs://raw/year=2026/month=01/date=2026-01-02/asset_20260102.json",
        output_uri="gs://silver/year=2026/month=01/date=2026-01-02/asset_20260102.parquet",
    )

    example._run_one_date(
        args,
        plan,
        example._new_schema_registry(),
        None,
        enable_parquet_schema_drift_logging=False,
    )

    assert captured["schema_mode"] == "additive"
    assert captured["schema_registry"] == example._new_schema_registry()
    assert captured["input_format"] == "json_array"
    assert captured["input_mode"] == "single_file"
    assert captured["parse_integers"] is True
    assert captured["parse_floats"] is True
    assert captured["parse_iso_timestamps"] is True
    assert captured["parse_iso_dates"] is True
    assert captured["parse_iso_times"] is True
    assert captured["multi_threading"] is True


def test_example_07_parser_lives_in_cli_module() -> None:
    from examples.example_07.cli import build_parser
    from schema_sanitizer.integrations.bigquery.advanced import (
        hive_partition_columns,
        registry_order_sql,
    )

    parser = build_parser()
    args = parser.parse_args(
        [
            "--source-jsonl-prefix",
            "gs://raw/events/rt",
            "--silver-parquet-prefix",
            "gs://silver/events/rt",
            "--start-date",
            "2026-01-01",
            "--end-date",
            "2026-01-01",
            "--target-table",
            "project.dataset.table",
            "--bigquery-registry-sidecar-table",
            "project.dataset.registry_state",
        ]
    )

    assert args.schema_mode == "additive"
    schema_mode_action = next(action for action in parser._actions if action.dest == "schema_mode")
    assert schema_mode_action.choices == ("additive",)
    assert args.field_name_policy == "lower_snake"
    assert args.input_format == "json_array"
    assert args.input_mode == "single_file"
    assert args.partition_granularity == "daily"
    assert args.start_date_warm_up is None
    assert args.end_date_warm_up is None
    assert not _load_example_07_runtime_support()._schema_warm_up_requested(args)
    assert args.start_hour is None
    assert args.end_hour is None
    assert args.start_hour_warm_up is None
    assert args.end_hour_warm_up is None
    assert args.bigquery_registry_sidecar_table == "project.dataset.registry_state"
    assert args.source_file_extension is None
    assert args.parse_integers is True
    assert args.parse_floats is True
    assert args.parse_iso_timestamps is True
    assert args.parse_iso_dates is True
    assert args.parse_iso_times is True
    assert args.parquet_enable_list_inference is True
    assert args.multi_threading is False
    assert args.memory_limit_bytes is None
    registry_order = registry_order_sql(
        hive_partition_columns(
            args.hive_partition_column,
            partition_granularity=args.partition_granularity,
        )
    )
    assert "SAFE_CAST(`ingestion_timestamp` AS TIMESTAMP) DESC" in registry_order
    assert "JSON_VALUE(`schema_registry`, '$.schema_generation')" in registry_order


def test_example_07_cli_help_runs_without_cloud_credentials() -> None:
    """The real CLI help path must remain an offline, side-effect-free smoke test."""
    repository_root = Path(__file__).resolve().parents[2]
    script = (
        repository_root
        / "examples"
        / "example_07"
        / "07_gcs_jsonl_to_silver_parquet_range_prefix.py"
    )

    completed = subprocess.run(
        [sys.executable, "-B", str(script), "--help"],
        cwd=repository_root,
        capture_output=True,
        text=True,
        check=False,
    )

    assert completed.returncode == 0, completed.stderr
    assert "--multi-threading | --no-multi-threading" in completed.stdout
    assert "--memory-limit-bytes" in completed.stdout
    assert "--target-table" in completed.stdout


def test_example_07_concurrency_option_reaches_conversion_and_discovery(monkeypatch) -> None:
    """One public boolean must select bounded concurrency across pipeline stages."""
    from examples.example_07.cli import build_parser

    example = _load_example_07_runtime_support()
    args = build_parser().parse_args(
        [
            "--source-jsonl-prefix",
            "gs://raw/events/rt",
            "--silver-parquet-prefix",
            "gs://silver/events/rt",
            "--start-date",
            "2026-01-01",
            "--end-date",
            "2026-01-01",
            "--target-table",
            "project.dataset.table",
            "--multi-threading",
        ]
    )
    plan = example.DateRunPlan(
        date(2026, 1, 1),
        "gs://raw/events/rt/year=2026/month=01/date=2026-01-01/events.json",
        "gs://silver/events/rt/year=2026/month=01/date=2026-01-01/events.parquet",
    )
    captured: dict[str, Any] = {}

    def fake_discovery(plans, **kwargs):
        """Capture the public discovery policy without touching GCS."""
        captured.update(kwargs)
        return SimpleNamespace(existing_plans=list(plans), skipped_plans=[])

    monkeypatch.setattr(example, "discover_existing_source_plans", fake_discovery)

    assert example._build_to_parquet_kwargs(args)["multi_threading"] is True
    assert example._filter_available_date_plans(
        [plan],
        args=args,
        skipped_log_sample_size=1,
    ) == [plan]
    assert captured["threading_mode"] == "multi"
    assert captured["memory_limit_bytes"] is None


def test_example_07_rejects_non_positive_memory_limit_at_cli_boundary() -> None:
    """Reject invalid operation budgets before source or cloud initialization."""
    from examples.example_07.cli import build_parser

    with pytest.raises(SystemExit):
        build_parser().parse_args(
            [
                "--source-jsonl-uri",
                "input.jsonl",
                "--silver-parquet-uri",
                "output.parquet",
                "--target-table",
                "project.dataset.table",
                "--memory-limit-bytes",
                "0",
            ]
        )


def test_example_07_warm_up_prefix_plan_uses_warm_up_range() -> None:
    from examples.example_07.cli import build_parser
    from schema_sanitizer.pipeline.advanced import (
        build_warm_up_hive_range_plan_from_namespace,
    )

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
            "--start-date-warm-up",
            "2026-06-20",
            "--end-date-warm-up",
            "2026-06-21",
            "--input-format",
            "jsonl",
            "--target-table",
            "project.dataset.table",
        ]
    )

    plans = build_warm_up_hive_range_plan_from_namespace(args)

    assert _load_example_07_runtime_support()._schema_warm_up_requested(args)
    assert [plan.label for plan in plans] == ["2026-06-20", "2026-06-21"]
    assert plans[0].source_uri == (
        "gs://raw/events/rt/year=2026/month=06/date=2026-06-20/events_20260620.jsonl"
    )


def test_example_07_warm_up_hourly_plan_uses_warm_up_hours() -> None:
    from examples.example_07.cli import build_parser
    from schema_sanitizer.pipeline.advanced import (
        build_warm_up_hive_range_plan_from_namespace,
    )

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
            "10",
            "--end-hour",
            "11",
            "--start-date-warm-up",
            "2026-06-24",
            "--end-date-warm-up",
            "2026-06-24",
            "--start-hour-warm-up",
            "1",
            "--end-hour-warm-up",
            "2",
            "--partition-granularity",
            "hourly",
            "--input-format",
            "jsonl",
            "--target-table",
            "project.dataset.table",
        ]
    )

    plans = build_warm_up_hive_range_plan_from_namespace(args)

    assert [plan.label for plan in plans] == [
        "2026-06-24/hour=01",
        "2026-06-24/hour=02",
    ]


def test_example_07_hour_flags_require_hourly_granularity() -> None:
    from examples.example_07.cli import build_parser

    parser = build_parser()

    with pytest.raises(SystemExit):
        parser.parse_args(
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
                "--input-format",
                "jsonl",
                "--target-table",
                "project.dataset.table",
            ]
        )


def test_example_07_warm_up_hour_flags_require_warm_up_dates() -> None:
    from examples.example_07.cli import build_parser

    parser = build_parser()

    with pytest.raises(SystemExit):
        parser.parse_args(
            [
                "--source-jsonl-prefix",
                "gs://raw/events/rt",
                "--silver-parquet-prefix",
                "gs://silver/events/rt",
                "--start-date",
                "2026-06-25",
                "--end-date",
                "2026-06-25",
                "--partition-granularity",
                "hourly",
                "--start-hour-warm-up",
                "1",
                "--end-hour-warm-up",
                "2",
                "--input-format",
                "jsonl",
                "--target-table",
                "project.dataset.table",
            ]
        )


def test_example_07_hourly_plan_defaults_to_full_day_when_hours_omitted() -> None:
    from examples.example_07.cli import build_parser
    from schema_sanitizer.pipeline.advanced import build_hive_range_plan_from_namespace

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
            "--partition-granularity",
            "hourly",
            "--input-format",
            "jsonl",
            "--target-table",
            "project.dataset.table",
        ]
    )

    plans = build_hive_range_plan_from_namespace(args)

    assert len(plans) == 24
    assert plans[0].label == "2026-06-25/hour=00"
    assert plans[-1].label == "2026-06-25/hour=23"


def test_example_07_warm_up_hourly_plan_defaults_to_full_day_when_hours_omitted() -> None:
    from examples.example_07.cli import build_parser
    from schema_sanitizer.pipeline.advanced import (
        build_warm_up_hive_range_plan_from_namespace,
    )

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
            "--partition-granularity",
            "hourly",
            "--start-date-warm-up",
            "2026-06-24",
            "--end-date-warm-up",
            "2026-06-24",
            "--input-format",
            "jsonl",
            "--target-table",
            "project.dataset.table",
        ]
    )

    plans = build_warm_up_hive_range_plan_from_namespace(args)

    assert len(plans) == 24
    assert plans[0].label == "2026-06-24/hour=00"
    assert plans[-1].label == "2026-06-24/hour=23"


def test_example_07_warm_up_infers_one_additive_registry(tmp_path: Path) -> None:
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
        multi_threading=True,
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
            logical_date=date(2026, 1, 2),
            source_uri=str(second),
            output_uri=str(tmp_path / "second.parquet"),
        ),
    ]

    registry = example._infer_warm_up_schema_registry(args, plans, example._new_schema_registry())

    assert example.registry_has_canonical_schema(registry)
    fields = registry["canonical_schema"]["fields"]
    assert {field["name"] for field in fields} >= {"alpha", "beta"}


def test_example_07_manual_preflight_stabilizes_integer_float_parquet(
    tmp_path: Path,
) -> None:
    """Manual warm-up must widen integer and float observations before writes."""
    pa = pytest.importorskip("pyarrow")
    pq = pytest.importorskip("pyarrow.parquet")
    from schema_sanitizer.pipeline.advanced import run_partitioned_to_parquet_registry_state

    example = _load_example_07_runtime_support()
    integer_source = tmp_path / "integer.jsonl"
    float_source = tmp_path / "float.jsonl"
    integer_source.write_text('{"exitscreen":{"timeelapsed":0}}\n', encoding="utf-8")
    float_source.write_text('{"exitscreen":{"timeelapsed":0.0}}\n', encoding="utf-8")
    plans = [
        example.DateRunPlan(
            logical_date=date(2026, 6, 20),
            source_uri=str(integer_source),
            output_uri=str(tmp_path / "integer.parquet"),
        ),
        example.DateRunPlan(
            logical_date=date(2026, 6, 21),
            source_uri=str(float_source),
            output_uri=str(tmp_path / "float.parquet"),
        ),
    ]
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
        multi_threading=True,
        memory_limit_bytes=64 * 1024 * 1024,
        arrow_max_depth=32,
        parquet_max_depth=15,
        input_text_encoding="utf-8",
        parquet_compression="uncompressed",
        parquet_gzip_level=None,
    )

    preflight = example._schema_warm_up_plan_for_run(plans)
    state = example._infer_warm_up_schema_registry_state(
        args,
        preflight,
        example.SchemaRegistryState(
            schema_registry_json=json.dumps(example._new_schema_registry(), separators=(",", ":"))
        ),
    )
    run_partitioned_to_parquet_registry_state(
        plans,
        initial_schema_registry_state=state,
        to_parquet_kwargs=example._build_to_parquet_kwargs(args),
    )

    for plan in plans:
        nested = pq.read_schema(plan.output_uri).field("exitscreen").type
        assert nested.field("timeelapsed").type == pa.float64()


def test_example_07_without_preflight_keeps_per_partition_numeric_types(
    tmp_path: Path,
) -> None:
    """No warm-up leaves an earlier integer file unchanged after float promotion."""
    pa = pytest.importorskip("pyarrow")
    pq = pytest.importorskip("pyarrow.parquet")
    from schema_sanitizer.pipeline.advanced import run_partitioned_to_parquet_registry_state

    example = _load_example_07_runtime_support()
    integer_source = tmp_path / "integer.jsonl"
    float_source = tmp_path / "float.jsonl"
    integer_source.write_text('{"exitscreen":{"timeelapsed":0}}\n', encoding="utf-8")
    float_source.write_text('{"exitscreen":{"timeelapsed":0.0}}\n', encoding="utf-8")
    plans = [
        example.DateRunPlan(
            logical_date=date(2026, 6, 20),
            source_uri=str(integer_source),
            output_uri=str(tmp_path / "integer.parquet"),
        ),
        example.DateRunPlan(
            logical_date=date(2026, 6, 21),
            source_uri=str(float_source),
            output_uri=str(tmp_path / "float.parquet"),
        ),
    ]
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
        parquet_compression="uncompressed",
        parquet_gzip_level=None,
    )

    assert example._schema_warm_up_plan_for_run([]) == []
    result = run_partitioned_to_parquet_registry_state(
        plans,
        initial_schema_registry_state=example.SchemaRegistryState(
            schema_registry_json=json.dumps(example._new_schema_registry(), separators=(",", ":"))
        ),
        to_parquet_kwargs=example._build_to_parquet_kwargs(args),
    )

    integer_nested = pq.read_schema(plans[0].output_uri).field("exitscreen").type
    float_nested = pq.read_schema(plans[1].output_uri).field("exitscreen").type
    assert integer_nested.field("timeelapsed").type == pa.int64()
    assert float_nested.field("timeelapsed").type == pa.float64()
    final_registry = json.loads(result.final_schema_registry_json)
    exitscreen = next(
        field
        for field in final_registry["canonical_schema"]["fields"]
        if field["name"] == "exitscreen"
    )
    timeelapsed = next(
        field for field in exitscreen["type"]["fields"] if field["name"] == "timeelapsed"
    )
    assert timeelapsed["type"] == {"kind": "float64"}


def test_example_07_preflight_only_uses_requested_warm_up() -> None:
    """Normal ranges must not be scanned unless warm-up was requested."""
    example = _load_example_07_runtime_support()
    requested = [example.DateRunPlan(date(2026, 6, 20), "warm", "warm.parquet")]

    assert example._schema_warm_up_plan_for_run(requested) == requested
    assert example._schema_warm_up_plan_for_run([]) == []


def _load_example_07_entrypoint() -> Any:
    """Load the numbered CLI script without executing its ``__main__`` guard."""
    script = (
        Path(__file__).resolve().parents[2]
        / "examples"
        / "example_07"
        / "07_gcs_jsonl_to_silver_parquet_range_prefix.py"
    )
    spec = spec_from_file_location("example_07_entrypoint_test", script)
    assert spec is not None and spec.loader is not None
    module = module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_example_07_reporting_result_releases_heavy_lifecycle_owners() -> None:
    """Reporting history must not retain native state or discovered-input owners."""
    example = _load_example_07_runtime_support()
    owner = object()
    plan = example.DateRunPlan(
        date(2026, 1, 1),
        "input.jsonl",
        "output.parquet",
        discovered_input=owner,
        _metadata_owner=owner,
        source_file_count=1,
        source_bytes=42,
    )
    result = example.DateRunResult(
        plan=plan,
        output_schema=owner,
        stats={"rows": 1},
        schema_registry={"canonical_schema": {}},
        schema_drifts=[{"drift_type": "newly_added"}],
        schema_registry_json='{"canonical_schema":{}}',
        schema_drifts_json='[{"drift_type":"newly_added"}]',
        native_registry_state=owner,
    )

    retained = example._run_result_for_reporting(result)

    assert retained.plan.source_file_count == 1
    assert retained.plan.source_bytes == 42
    assert retained.plan.discovered_input is None
    assert retained.plan._metadata_owner is None
    assert retained.output_schema is None
    assert retained.schema_registry is None
    assert retained.schema_registry_json is None
    assert retained.native_registry_state is None
    assert retained.stats == {"rows": 1}
    assert retained.schema_drifts_json == '[{"drift_type":"newly_added"}]'


def test_example_07_main_runs_offline_with_mocked_cloud_boundaries(
    monkeypatch,
    tmp_path: Path,
) -> None:
    """Exercise the complete CLI orchestration without GCS or BigQuery credentials."""
    from examples.example_07.cli import build_parser

    module = _load_example_07_entrypoint()
    source = tmp_path / "input.jsonl"
    output = tmp_path / "output.parquet"
    source.write_text('{"value": 1}\n', encoding="utf-8")
    args = build_parser().parse_args(
        [
            "--source-jsonl-uri",
            str(source),
            "--silver-parquet-uri",
            str(output),
            "--input-format",
            "jsonl",
            "--target-table",
            "project.dataset.table",
            "--multi-threading",
        ]
    )
    from schema_sanitizer.pipeline import PartitionRunPlan, PartitionRunResult

    run_plan = [PartitionRunPlan(None, str(source), str(output))]
    native_owner = object()
    captured: dict[str, Any] = {}

    monkeypatch.setattr(
        module,
        "build_parser",
        lambda: SimpleNamespace(parse_args=lambda: args),
    )
    monkeypatch.setattr(module, "build_hive_range_plan_from_namespace", lambda _args: run_plan)
    monkeypatch.setattr(
        module,
        "_filter_available_date_plans",
        lambda plans, **_kwargs: plans,
    )
    monkeypatch.setattr(
        module,
        "warn_if_output_uri_not_covered_by_external_source_uris",
        lambda _args: None,
    )
    monkeypatch.setattr(
        module,
        "prepare_existing_schema_registry_from_namespace",
        lambda _args, _table_ref: {},
    )

    def fake_pipeline(plans, **kwargs):
        """Complete one partition and expose the requested retention policy."""
        captured["plans"] = plans
        captured["pipeline_kwargs"] = kwargs
        result = PartitionRunResult(
            plan=plans[0],
            output_schema="final-schema",
            stats={"rows": 1},
            schema_drifts_json="[]",
            wall_seconds=0.01,
            cpu_seconds=0.005,
            io_wait_seconds=0.005,
            native_registry_state=native_owner,
        )
        kwargs["after_partition"](1, 1, result, 0.01, None, True)
        return SimpleNamespace(completed_runs=[], final_native_registry_state=native_owner)

    monkeypatch.setattr(module, "run_partitioned_to_parquet_registry_state", fake_pipeline)
    monkeypatch.setattr(
        module,
        "create_or_replace_external_bigquery_table_from_namespace",
        lambda _args, _table_ref, schema, **_kwargs: captured.update(final_schema=schema),
    )
    monkeypatch.setattr(module, "_print_stats_summary", lambda runs, **_kwargs: None)
    monkeypatch.setattr(module, "_log_schema_drift_summary", lambda runs, **_kwargs: None)
    monkeypatch.setattr(
        module,
        "_print_run_outputs_summary",
        lambda runs, **_kwargs: captured.update(reporting_runs=runs),
    )

    assert module.main() == 0
    pipeline_kwargs = captured["pipeline_kwargs"]
    assert pipeline_kwargs["result_retention"] == "streaming"
    assert pipeline_kwargs["to_parquet_kwargs"]["multi_threading"] is True
    assert captured["final_schema"] == "final-schema"
    retained = captured["reporting_runs"][0]
    assert retained.native_registry_state is None
    assert retained.output_schema is None


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
            assert value == r"C:\source\events.jsonl"

        def is_file(self) -> bool:
            return True

        def stat(self) -> SimpleNamespace:
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


def test_example_07_source_discovery_skips_missing_dates(tmp_path: Path) -> None:
    from schema_sanitizer.pipeline import PartitionRunPlan
    from schema_sanitizer.pipeline.advanced import discover_existing_source_plans

    sources = [tmp_path / f"events_2026010{day}.json" for day in (1, 2, 3)]
    sources[0].write_text('{"value": 1}', encoding="utf-8")
    sources[2].write_text('{"value": 3}', encoding="utf-8")
    plans = [
        PartitionRunPlan(
            logical_date=date(2026, 1, day),
            source_uri=str(sources[index]),
            output_uri=str(tmp_path / f"events_2026010{day}.parquet"),
        )
        for index, day in enumerate((1, 2, 3))
    ]

    discovery = discover_existing_source_plans(plans)

    assert [plan.label for plan in discovery.existing_plans] == ["2026-01-01", "2026-01-03"]
    assert [plan.label for plan in discovery.skipped_plans] == ["2026-01-02"]
    assert [plan.source_file_count for plan in discovery.existing_plans] == [1, 1]
    assert [plan.source_bytes for plan in discovery.existing_plans] == [12, 12]


def test_example_07_daily_single_file_prefix_plan() -> None:
    from examples.example_07.cli import build_parser
    from schema_sanitizer.pipeline.advanced import build_hive_range_plan_from_namespace

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
    from examples.example_07.cli import build_parser
    from schema_sanitizer.integrations.bigquery.advanced import hive_partition_columns
    from schema_sanitizer.pipeline.advanced import build_hive_range_plan_from_namespace

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
            "jsonl",
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
    from examples.example_07.cli import build_parser
    from schema_sanitizer.pipeline.advanced import build_hive_range_plan_from_namespace

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
    from examples.example_07.cli import build_parser
    from schema_sanitizer.pipeline.advanced import build_hive_range_plan_from_namespace

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


def test_example_07_directory_discovery_skips_empty_partitions(tmp_path: Path) -> None:
    from schema_sanitizer.pipeline import PartitionRunPlan
    from schema_sanitizer.pipeline.advanced import discover_existing_source_plans

    source_directories = [tmp_path / f"hour={hour:02d}" for hour in (8, 9)]
    for directory in source_directories:
        directory.mkdir()
    (source_directories[0] / "part.parquet").write_bytes(b"parquet-placeholder")
    (source_directories[1] / "ignored.csv").write_text("value\n1\n", encoding="utf-8")
    plans = [
        PartitionRunPlan(
            logical_date=date(2026, 6, 25),
            logical_hour=hour,
            source_uri=str(source_directories[index]),
            output_uri=str(tmp_path / f"events_20260625_{hour:02d}.parquet"),
        )
        for index, hour in enumerate((8, 9))
    ]

    discovery = discover_existing_source_plans(
        plans,
        input_mode="directory",
        input_format="parquet",
    )

    assert [plan.label for plan in discovery.existing_plans] == ["2026-06-25/hour=08"]
    assert [plan.label for plan in discovery.skipped_plans] == ["2026-06-25/hour=09"]
