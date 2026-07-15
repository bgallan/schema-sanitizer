"""Regression tests for example 07 range-prefix pipeline helpers."""

from __future__ import annotations

import importlib.util
import json
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


# Split from test_example_07.py: test_example_07_embedded_registry_preserves_additive_bootstrap_mode, test_example_07_parser_lives_in_cli_module, test_example_07_warm_up_prefix_plan_uses_warm_up_range, ...


def test_example_07_embedded_registry_preserves_additive_bootstrap_mode(monkeypatch) -> None:
    """Verify first registry-backed range runs can bootstrap with additive mode."""
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


def test_example_07_parser_lives_in_cli_module() -> None:
    """Verify the extracted parser preserves core range-prefix defaults."""
    from examples.example_07.cli import build_parser
    from schema_sanitizer.integrations.bigquery import hive_partition_columns, registry_order_sql

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
            "--bigquery-registry-sidecar-table",
            "project.dataset.registry_state",
        ]
    )

    assert args.schema_mode == "strict"
    assert args.field_name_policy == "lower_snake"
    assert args.input_format == "json_array"
    assert args.input_mode == "single_file"
    assert args.partition_granularity == "daily"
    assert args.start_date_warm_up is None
    assert args.end_date_warm_up is None
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
    registry_order = registry_order_sql(
        hive_partition_columns(
            args.hive_partition_column,
            partition_granularity=args.partition_granularity,
        )
    )
    assert "SAFE_CAST(`ingestion_timestamp` AS TIMESTAMP) DESC" in registry_order
    assert "JSON_VALUE(`schema_registry`, '$.schema_generation')" in registry_order


def test_example_07_warm_up_prefix_plan_uses_warm_up_range() -> None:
    """Verify schema warm-up range options render their own source partitions."""
    from examples.example_07.cli import build_parser
    from schema_sanitizer.pipeline import build_warm_up_hive_range_plan_from_namespace

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

    assert [plan.label for plan in plans] == ["2026-06-20", "2026-06-21"]
    assert plans[0].source_uri == (
        "gs://raw/events/rt/year=2026/month=06/date=2026-06-20/events_20260620.jsonl"
    )


def test_example_07_warm_up_hourly_plan_uses_warm_up_hours() -> None:
    """Verify hourly schema warm-up uses dedicated warm-up hour bounds."""
    from examples.example_07.cli import build_parser
    from schema_sanitizer.pipeline import build_warm_up_hive_range_plan_from_namespace

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
    """Verify hour bounds require explicit hourly partition granularity."""
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
    """Verify warm-up hour bounds are only valid with a warm-up date range."""
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
    """Verify explicit hourly mode defaults omitted normal hours to 00..23."""
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
    """Verify warm-up hourly mode defaults omitted warm-up hours to 00..23."""
    from examples.example_07.cli import build_parser
    from schema_sanitizer.pipeline import build_warm_up_hive_range_plan_from_namespace

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
    """Verify warm-up scans multiple sources as one additive registry inference."""
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


def test_example_07_additive_preflight_stabilizes_integer_float_parquet(
    tmp_path: Path,
) -> None:
    """All additive partitions must use the final widened numeric schema."""
    pa = pytest.importorskip("pyarrow")
    pq = pytest.importorskip("pyarrow.parquet")
    from schema_sanitizer.pipeline import run_partitioned_to_parquet_registry_state

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

    preflight = example._schema_warm_up_plan_for_run(args, plans, [])
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


def test_example_07_strict_preflight_only_uses_requested_warm_up() -> None:
    """Strict runs must not silently absorb current-range drift."""
    example = _load_example_07_runtime_support()
    current = [example.DateRunPlan(date(2026, 6, 21), "current", "current.parquet")]
    requested = [example.DateRunPlan(date(2026, 6, 20), "warm", "warm.parquet")]

    assert (
        example._schema_warm_up_plan_for_run(
            SimpleNamespace(schema_mode="strict"), current, requested
        )
        == requested
    )
