"""Public contracts for the compact pipeline facade.

It runs local Hive conversion, inspects generated cloud plans, renders modified-time
outputs, and verifies delegation to the shared engine.
"""

from __future__ import annotations

from datetime import date
from pathlib import Path
from types import SimpleNamespace

import pytest

import schema_sanitizer as ss
from schema_sanitizer.core_impl.uris import local_path_from_file_uri
from schema_sanitizer.pipeline import (
    HivePartitions,
    ModifiedTimePartitions,
    ParquetPipeline,
    high_level,
)


def test_hive_pipeline_executes_local_end_to_end(tmp_path: Path, require_native: None) -> None:
    """The high-level facade discovers and writes one real local partition."""
    pytest.importorskip("pyarrow")
    logical_date = date(2026, 1, 2)
    source_root = tmp_path / "raw"
    source = source_root / "year=2026" / "month=01" / "date=2026-01-02"
    source.mkdir(parents=True)
    (source / "events_20260102.csv").write_text("id,name\n1,Ada\n", encoding="utf-8")
    output_root = tmp_path / "clean"
    job = ParquetPipeline(
        source=source_root.as_uri(),
        output=output_root.as_uri(),
        partitions=HivePartitions.daily(
            logical_date,
            logical_date,
            file_name_prefix="events",
            source_file_extension="csv",
        ),
        options=ss.SanitizeOptions(
            input_format="csv",
            parsing=ss.ParsingOptions(integers=True),
        ),
    )

    result = job.run()

    assert len(result.completed_runs) == 1
    output_path = Path(local_path_from_file_uri(result.completed_runs[0].plan.output_uri))
    assert output_path.is_file()


def test_hive_pipeline_plan_uses_public_configuration(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """One compact job declaration expands into deterministic daily paths."""
    monkeypatch.setattr(
        high_level,
        "discover_existing_source_plans",
        lambda plans, **_kwargs: SimpleNamespace(existing_plans=plans),
    )
    job = ParquetPipeline(
        source="gs://bucket/raw/events",
        output="gs://bucket/clean/events",
        partitions=HivePartitions.daily(
            date(2026, 1, 1),
            date(2026, 1, 2),
            file_name_prefix="events",
        ),
        options=ss.SanitizeOptions(input_format="csv", input_mode="directory"),
    )

    plans = job.plan()

    assert [plan.logical_date for plan in plans] == [date(2026, 1, 1), date(2026, 1, 2)]
    assert plans[0].source_uri.endswith("year=2026/month=01/date=2026-01-01")
    assert plans[0].output_uri.endswith(
        "year=2026/month=01/date=2026-01-01/events_20260101.parquet"
    )


def test_modified_time_pipeline_renders_one_output_per_day(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Modified-time discovery accepts a date template without manual plan wiring."""
    logical_date = date(2026, 2, 3)
    partition_plan = SimpleNamespace(logical_date=logical_date)

    class Window:
        source_window = SimpleNamespace(logical_date=logical_date)

        def to_partition_run_plan(self, output_uri: str) -> object:
            """Attach the rendered output URI to the partition run plan."""
            partition_plan.output_uri = output_uri
            return partition_plan

    monkeypatch.setattr(
        high_level,
        "plan_gcs_modified_time_windows",
        lambda *_args, **_kwargs: (Window(),),
    )
    job = ParquetPipeline(
        source="gs://bucket/raw",
        output="gs://bucket/clean/{date}.parquet",
        partitions=ModifiedTimePartitions.daily(logical_date, logical_date),
    )

    assert job.plan() == [partition_plan]
    assert partition_plan.output_uri == "gs://bucket/clean/2026-02-03.parquet"


def test_pipeline_run_passes_reusable_options_to_existing_engine(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The facade delegates execution without creating a second pipeline engine."""
    captured: dict[str, object] = {}
    expected = object()

    def run(plans: object, **kwargs: object) -> object:
        """Capture delegated plans and options, then return the sentinel result."""
        captured["plans"] = plans
        captured.update(kwargs)
        return expected

    monkeypatch.setattr(high_level, "run_partitioned_to_parquet_registry_json", run)
    plan = SimpleNamespace(output_uri="gs://bucket/clean/day.parquet")
    monkeypatch.setattr(ParquetPipeline, "plan", lambda _self: [plan])
    job = ParquetPipeline(
        source="gs://bucket/raw",
        output="gs://bucket/clean",
        partitions=HivePartitions.daily(date(2026, 1, 1), date(2026, 1, 1)),
        options=ss.SanitizeOptions(
            input_format="csv",
            resources=ss.ResourceOptions(multi_threading=True),
        ),
        parquet=ss.ParquetOptions(compression="zstd"),
    )

    assert job.run() is expected
    assert captured["plans"] == [plan]
    kwargs = captured["to_parquet_kwargs"]
    assert isinstance(kwargs, dict)
    assert kwargs["input_format"] == "csv"
    assert kwargs["multi_threading"] is True
    assert kwargs["parquet_compression"] == "zstd"
