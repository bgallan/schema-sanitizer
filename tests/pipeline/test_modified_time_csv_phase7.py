"""Example-08 orchestration and question-header contracts."""

from __future__ import annotations

from contextlib import nullcontext
from datetime import UTC, date, datetime
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest

from examples.example_08 import runtime_support
from examples.example_08.cli import build_parser
from examples.example_08.question_normalization import (
    detect_question_columns,
    parse_question_column,
)
from examples.example_08.runtime_support import (
    DayRunResult,
    Example08Config,
    output_uri_for_day,
    run_modified_time_csv_workflow,
)
from schema_sanitizer.input_impl.remote_files import RemoteFile


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
        """Store one immutable listing and observable call counters."""
        self.files = files
        self.list_calls = 0

    def list_csv_objects(
        self,
        _source_prefix: str,
        *,
        memory_limit_bytes: int | None,
    ) -> tuple[RemoteFile, ...]:
        """Return the configured listing exactly once per workflow call."""
        del memory_limit_bytes
        self.list_calls += 1
        return self.files

    def schema_sanitizer_download_scope(self):
        """Return a no-op scope because day execution is stubbed here."""
        return nullcontext()

    def publish_file_atomic(
        self,
        _local_path: str,
        _destination_uri: str,
        *,
        memory_limit_bytes: int | None,
    ) -> int:
        """Reject accidental publication from the orchestration-only test."""
        del memory_limit_bytes
        raise AssertionError("stubbed day execution must not publish")


class FakeBigQueryClient:
    """Minimal fake target service with observable replacement calls."""

    def __init__(self) -> None:
        """Initialize call tracking."""
        self.read_calls = 0
        self.replace_calls: list[dict[str, Any]] = []

    def read_target_schema(self, _target_table: str) -> object:
        """Return an opaque schema consumed by monkeypatched helpers."""
        self.read_calls += 1
        return SimpleNamespace(
            names=[
                "questions",
                "source_file",
                "ingestion_timestamp",
                "schema_registry",
                "schema_drifts",
            ]
        )

    def replace_external_table(self, target_table: str, **kwargs: Any) -> None:
        """Record one post-publication external-table replacement."""
        self.replace_calls.append({"target_table": target_table, **kwargs})


def _config() -> Example08Config:
    """Return the standard two-day example configuration."""
    return Example08Config(
        source_csv_prefix="gs://source/csv",
        silver_parquet_prefix="gs://silver/output",
        start_date=date(2026, 7, 1),
        end_date=date(2026, 7, 2),
        target_table="project.dataset.responses",
    )


def test_example_08_parser_exposes_required_contract() -> None:
    """The CLI exposes source, dates, target, questions, and operational knobs."""
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
            "--bigquery-project",
            "p",
            "--bigquery-location",
            "EU",
            "--question-separator",
            "|",
            "--questions-column",
            "answers",
            "--omit-null-answers",
            "--on-error",
            "skip_row",
            "--memory-limit-bytes",
            "4096",
            "--multi-threading",
            "--parquet-compression",
            "gzip",
            "--field-name-policy",
            "preserve",
            "--log-level",
            "DEBUG",
        ]
    )
    assert args.start_date == date(2026, 7, 1)
    assert args.end_date == date(2026, 7, 2)
    assert args.question_separator == "|"
    assert args.questions_column == "answers"
    assert args.omit_null_answers is True
    assert args.multi_threading is True
    assert args.parquet_compression == "gzip"


def test_question_header_splits_only_on_first_separator() -> None:
    """A slash inside the question text remains part of the final text."""
    parsed = parse_question_column("17/Path / nested / value")
    assert parsed is not None
    assert parsed.question_id == 17
    assert parsed.question_text == "Path / nested / value"
    assert parse_question_column("not-an-id/question") is None
    assert parse_question_column("18/") is None


def test_question_detection_preserves_unicode_and_column_order() -> None:
    """Unicode and renamed questions remain distinct deterministic columns."""
    detected = detect_question_columns(
        ["country", "2/¿Cómo estás?", "1/旧しい質問", "1/Renamed/question"]
    )
    assert [(item.question_id, item.question_text) for item in detected] == [
        (2, "¿Cómo estás?"),
        (1, "旧しい質問"),
        (1, "Renamed/question"),
    ]


def test_output_uri_is_one_deterministic_object_per_day() -> None:
    """Daily output names do not depend on source file names or ordering."""
    assert output_uri_for_day("gs://silver/root/", date(2026, 7, 1)) == (
        "gs://silver/root/2026-07-01.parquet"
    )


def test_config_rejects_non_preserving_question_policy() -> None:
    """Question patterns cannot survive a field-name rewrite policy."""
    with pytest.raises(ValueError, match="requires field_name_policy='preserve'"):
        Example08Config(
            source_csv_prefix="gs://source/csv",
            silver_parquet_prefix="gs://silver/output",
            start_date=date(2026, 7, 1),
            end_date=date(2026, 7, 1),
            target_table="p.d.t",
            field_name_policy="lower_snake",
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
            output_uri=output_uri_for_day("gs://silver/output", plan.source_window.logical_date),
            source_object_count=plan.selected_object_count,
            input_bytes=plan.total_bytes,
            row_count=plan.selected_object_count,
            question_column_count=1,
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
    assert bigquery.replace_calls[0]["reference_file_schema_uri"].endswith("2026-07-02.parquet")


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


def test_example_08_files_remain_small_and_separated() -> None:
    """The example keeps CLI, transformation, and runtime responsibilities split."""
    root = Path(__file__).parents[2] / "examples" / "example_08"
    expected = {
        "08_gcs_csv_modified_window_to_polars_parquet.py",
        "__init__.py",
        "cli.py",
        "question_normalization.py",
        "runtime_support.py",
    }
    assert expected <= {path.name for path in root.glob("*.py")}
    for path in root.glob("*.py"):
        assert len(path.read_text(encoding="utf-8").splitlines()) <= 500
