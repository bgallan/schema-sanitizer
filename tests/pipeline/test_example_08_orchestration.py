"""Example-08 orchestration and event-header contracts.

It covers CLI configuration, event-header normalization, Hive schema and paths, per-day
execution, and external-table publication ordering.
"""

from __future__ import annotations

from contextlib import nullcontext
from datetime import UTC, date, datetime
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest

from examples.example_08 import runtime_support
from examples.example_08.cli import build_parser
from examples.example_08.event_normalization import (
    detect_event_columns,
    parse_event_column,
)
from examples.example_08.hive_output import (
    partitioned_output_uri,
    prepare_hive_parquet_schema,
    write_hive_parquet_dataset,
)
from examples.example_08.runtime_support import (
    DayRunResult,
    Example08Config,
    run_modified_time_csv_workflow,
)
from schema_sanitizer.sources import RemoteFile


def _remote(
    name: str,
    *,
    updated: datetime,
    generation: str,
    size: int = 10,
) -> RemoteFile:
    """Build one exact GCS object identity for orchestration tests."""
    return RemoteFile(
        uri=f"gs://source/csv/{name}",
        name=name,
        size=size,
        updated=updated,
        generation=generation,
    )


class FakeListingClient:
    """Minimal fake GCS client used before optional analytical dependencies."""

    def __init__(self, files: tuple[RemoteFile, ...]) -> None:
        """Initialize fake listing client state for files and list calls."""
        self.files = files
        self.list_calls = 0

    def list_csv_objects(
        self,
        _source_prefix: str,
        *,
        memory_limit_bytes: int | None,
    ) -> tuple[RemoteFile, ...]:
        """Return the configured CSV object listing for the requested window."""
        del memory_limit_bytes
        self.list_calls += 1
        return self.files

    def schema_sanitizer_download_scope(self):
        """Open the fake provider download scope for schema-sanitizer."""
        return nullcontext()

    def publish_file_atomic(
        self,
        _local_path: str,
        _destination_uri: str,
        *,
        memory_limit_bytes: int | None,
    ) -> int:
        """Record an atomic publication without writing to cloud storage."""
        del memory_limit_bytes
        raise AssertionError("stubbed day execution must not publish")


class FakeBigQueryClient:
    """Minimal fake target service with observable replacement calls."""

    def __init__(self) -> None:
        """Initialize fake BigQuery client state for read calls and replace calls."""
        self.read_calls = 0
        self.replace_calls: list[dict[str, Any]] = []

    def read_target_schema(self, _target_table: str) -> object:
        """Return the in-memory target schema while recording the lookup when needed."""
        self.read_calls += 1
        return SimpleNamespace(
            names=[
                "event",
                "event_timestamp",
                "source_file",
                "ingestion_timestamp",
                "schema_registry",
                "schema_drifts",
            ]
        )

    def replace_external_table(self, target_table: str, **kwargs: Any) -> None:
        """Record the requested external-table replacement."""
        self.replace_calls.append({"target_table": target_table, **kwargs})


def _config() -> Example08Config:
    """Return the standard two-day example configuration."""
    return Example08Config(
        source_csv_prefix="gs://source/csv",
        silver_parquet_prefix="gs://silver/output",
        start_date=date(2026, 7, 1),
        end_date=date(2026, 7, 2),
        target_table="project.dataset.records",
        partition_timestamp_column="event_timestamp",
        parquet_file_prefix="records",
    )


def test_example_08_parser_exposes_required_contract() -> None:
    """The CLI exposes source, dates, target, event, and operational knobs."""
    args = build_parser().parse_args(
        [
            "--source-csv-prefix",
            "gs://source/csv",
            "--silver-parquet-prefix",
            "gs://silver/output",
            "--start-date",
            "2026-07-01",
            "--end-date",
            "2026-07-02",
            "--target-table",
            "p.d.t",
            "--partition-timestamp-column",
            "event_timestamp",
            "--parquet-file-prefix",
            "records",
            "--bigquery-project",
            "p",
            "--bigquery-location",
            "EU",
            "--event-separator",
            "|",
            "--event-column",
            "event_items",
            "--omit-null-payloads",
            "--on-error",
            "skip_row",
            "--memory-limit-bytes",
            "4096",
            "--multi-threading",
            "--field-name-policy",
            "preserve",
            "--log-level",
            "DEBUG",
        ]
    )
    assert args.start_date == date(2026, 7, 1)
    assert args.end_date == date(2026, 7, 2)
    assert args.event_separator == "|"
    assert args.event_column == "event_items"
    assert args.partition_timestamp_column == "event_timestamp"
    assert args.parquet_file_prefix == "records"
    assert args.omit_null_payloads is True
    assert args.multi_threading is True


def test_event_header_splits_only_on_first_separator() -> None:
    """A slash inside the event text remains part of the final text."""
    parsed = parse_event_column("17/Path / nested / value")
    assert parsed is not None
    assert parsed.event_id == 17
    assert parsed.event_text == "Path / nested / value"
    assert parse_event_column("not-an-id/event") is None
    assert parse_event_column("18/") is None


def test_event_detection_preserves_unicode_and_column_order() -> None:
    """Unicode and renamed event remain distinct deterministic columns."""
    detected = detect_event_columns(["country", "2/Métrica Δ", "1/状態変更", "1/Renamed/event"])
    assert [(item.event_id, item.event_text) for item in detected] == [
        (2, "Métrica Δ"),
        (1, "状態変更"),
        (1, "Renamed/event"),
    ]


def test_partitioned_output_uri_preserves_the_validated_hive_path() -> None:
    """Hive object names remain below the configured silver prefix."""
    assert partitioned_output_uri(
        "gs://silver/root/",
        "year=2026/month=7/day=1/records_20260701_20260703.gz.parquet",
    ) == ("gs://silver/root/year=2026/month=7/day=1/records_20260701_20260703.gz.parquet")
    with pytest.raises(ValueError, match="invalid relative Parquet path"):
        partitioned_output_uri("gs://silver/root", "../outside.parquet")


def test_hive_schema_requires_a_real_timestamp_and_removes_path_fields() -> None:
    """Partition path fields stay out of Parquet while the source timestamp remains."""
    pa = pytest.importorskip("pyarrow")
    target = pa.schema(
        [
            pa.field("record_id", pa.string()),
            pa.field("event_timestamp", pa.timestamp("us", tz="UTC")),
            pa.field("year", pa.int64()),
            pa.field("month", pa.int64()),
            pa.field("day", pa.int64()),
        ]
    )

    parquet_schema = prepare_hive_parquet_schema(target, "event_timestamp")
    assert parquet_schema.names == ["record_id", "event_timestamp"]
    with pytest.raises(ValueError, match="must be a timestamp"):
        prepare_hive_parquet_schema(
            pa.schema([pa.field("event_timestamp", pa.string())]),
            "event_timestamp",
        )


def test_hive_writer_rejects_null_partition_timestamps(tmp_path: Path) -> None:
    """Rows without a timestamp cannot silently enter an ambiguous Hive partition."""
    pa = pytest.importorskip("pyarrow")
    pytest.importorskip("polars")
    schema = pa.schema([pa.field("event_timestamp", pa.timestamp("us", tz="UTC"))])
    table = pa.Table.from_arrays(
        [pa.array([None], type=schema.field("event_timestamp").type)],
        schema=schema,
    )

    with pytest.raises(ValueError, match="contains 1 null value"):
        write_hive_parquet_dataset(
            table,
            schema,
            tmp_path,
            file_prefix="records",
            timestamp_column="event_timestamp",
            source_window_date=date(2026, 7, 1),
        )


def test_config_rejects_non_preserving_event_policy() -> None:
    """Event patterns cannot survive a field-name rewrite policy."""
    with pytest.raises(ValueError, match="requires field_name_policy='preserve'"):
        Example08Config(
            source_csv_prefix="gs://source/csv",
            silver_parquet_prefix="gs://silver/output",
            start_date=date(2026, 7, 1),
            end_date=date(2026, 7, 1),
            target_table="p.d.t",
            partition_timestamp_column="event_timestamp",
            parquet_file_prefix="records",
            field_name_policy="lower_snake",
        )
    with pytest.raises(ValueError, match="must not be year, month, or day"):
        Example08Config(
            source_csv_prefix="gs://source/csv",
            silver_parquet_prefix="gs://silver/output",
            start_date=date(2026, 7, 1),
            end_date=date(2026, 7, 1),
            target_table="p.d.t",
            partition_timestamp_column="year",
            parquet_file_prefix="records",
        )
    with pytest.raises(ValueError, match="parquet_file_prefix"):
        Example08Config(
            source_csv_prefix="gs://source/csv",
            silver_parquet_prefix="gs://silver/output",
            start_date=date(2026, 7, 1),
            end_date=date(2026, 7, 1),
            target_table="p.d.t",
            partition_timestamp_column="event_timestamp",
            parquet_file_prefix="../records",
        )


def test_workflow_lists_once_and_groups_three_objects_into_two_days(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """One complete listing feeds one multi-object call for each non-empty day."""
    files = (
        _remote(
            "a.csv",
            updated=datetime(2026, 7, 1, 1, tzinfo=UTC),
            generation="1",
            size=11,
        ),
        _remote(
            "b.csv",
            updated=datetime(2026, 7, 1, 23, 59, tzinfo=UTC),
            generation="2",
            size=13,
        ),
        _remote(
            "c.csv",
            updated=datetime(2026, 7, 2, 0, 0, tzinfo=UTC),
            generation="3",
            size=17,
        ),
    )
    gcs = FakeListingClient(files)
    bigquery = FakeBigQueryClient()
    seen_manifests: list[tuple[tuple[str, str | None], ...]] = []

    monkeypatch.setattr(
        runtime_support.ss,
        "project_ingress_scalar_schema",
        lambda schema: ("ingress", schema),
    )
    monkeypatch.setattr(
        runtime_support,
        "prepare_hive_parquet_schema",
        lambda schema, _timestamp_column: schema,
    )
    monkeypatch.setattr(
        runtime_support.ss,
        "schema_registry_from_arrow_schema",
        lambda schema, **_kwargs: {"schema": repr(schema)},
    )

    def fake_day(
        _config_value: Example08Config,
        plan: Any,
        **_kwargs: Any,
    ) -> DayRunResult:
        """Record each complete manifest and return deterministic telemetry."""
        seen_manifests.append(plan.source_manifest.content_identities)
        return DayRunResult(
            logical_date=plan.source_window.logical_date,
            output_uris=(
                partitioned_output_uri(
                    "gs://silver/output",
                    "year=2026/month=7/day=1/"
                    "records_20260701_"
                    f"{plan.source_window.logical_date:%Y%m%d}.gz.parquet",
                ),
            ),
            source_object_count=plan.selected_object_count,
            input_bytes=plan.total_bytes,
            row_count=plan.selected_object_count,
            event_column_count=1,
            partition_count=1,
            output_bytes=100,
            conversion_seconds=0.1,
            normalization_seconds=0.1,
            parquet_seconds=0.1,
            upload_seconds=0.1,
        )

    monkeypatch.setattr(runtime_support, "_run_one_day", fake_day)
    result = run_modified_time_csv_workflow(
        _config(),
        gcs_client=gcs,
        bigquery_client=bigquery,
        to_polars=lambda *_args, **_kwargs: SimpleNamespace(clean_data=None),
    )

    assert gcs.list_calls == 1
    assert seen_manifests == [
        (("gs://source/csv/a.csv", "1"), ("gs://source/csv/b.csv", "2")),
        (("gs://source/csv/c.csv", "3"),),
    ]
    assert [day.source_object_count for day in result.completed_days] == [2, 1]
    assert len(bigquery.replace_calls) == 1
    replace_call = bigquery.replace_calls[0]
    assert replace_call["reference_file_schema_uri"].endswith(
        "records_20260701_20260702.gz.parquet"
    )
    assert replace_call["hive_uri_prefix"] == "gs://silver/output"
    assert replace_call["partition_columns"] == (
        ("year", "INT64"),
        ("month", "INT64"),
        ("day", "INT64"),
    )


def test_external_table_is_not_updated_when_a_day_fails(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A failed validation or publication prevents the BigQuery replacement."""
    gcs = FakeListingClient(
        (
            _remote(
                "a.csv",
                updated=datetime(2026, 7, 1, 1, tzinfo=UTC),
                generation="1",
            ),
        )
    )
    bigquery = FakeBigQueryClient()
    monkeypatch.setattr(
        runtime_support,
        "prepare_hive_parquet_schema",
        lambda schema, _timestamp_column: schema,
    )
    monkeypatch.setattr(runtime_support.ss, "project_ingress_scalar_schema", lambda value: value)
    monkeypatch.setattr(
        runtime_support.ss,
        "schema_registry_from_arrow_schema",
        lambda _schema, **_kwargs: {},
    )
    monkeypatch.setattr(
        runtime_support,
        "_run_one_day",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(ValueError("validation failed")),
    )

    with pytest.raises(ValueError, match="validation failed"):
        run_modified_time_csv_workflow(
            _config(),
            gcs_client=gcs,
            bigquery_client=bigquery,
        )
    assert bigquery.replace_calls == []
