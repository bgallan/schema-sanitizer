"""Fake-cloud integration coverage for example 08."""

from __future__ import annotations

import json
from contextlib import contextmanager
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any

import pytest

import schema_sanitizer as ss
from examples.example_08.runtime_support import (
    Example08Config,
    run_modified_time_csv_workflow,
)
from schema_sanitizer.remote_impl import sync_backend
from schema_sanitizer.sources import RemoteFile


class FakeGcsClient:
    """In-memory exact-generation GCS client for one integration process."""

    def __init__(self, objects: list[tuple[RemoteFile, bytes]]) -> None:
        """Store source generations and initialize publication telemetry."""
        self._objects = {remote.content_identity: payload for remote, payload in objects}
        self._listing = tuple(remote for remote, _payload in objects)
        self.list_calls = 0
        self.published: dict[str, bytes] = {}

    def list_csv_objects(
        self,
        _source_prefix: str,
        *,
        memory_limit_bytes: int | None,
    ) -> tuple[RemoteFile, ...]:
        """Return all configured source generations in one listing call."""
        del memory_limit_bytes
        self.list_calls += 1
        return self._listing

    @contextmanager
    def schema_sanitizer_download_scope(self):
        """Route schema-sanitizer's synchronous staging into this fake store."""
        original = sync_backend.download_files_to_directory

        def download(
            files: list[RemoteFile],
            directory: str,
            *,
            memory_limit_bytes: int | None,
        ) -> None:
            """Materialize exact requested generations under staging names."""
            del memory_limit_bytes
            root = Path(directory)
            for remote in files:
                (root / remote.name).write_bytes(self._objects[remote.content_identity])

        sync_backend.download_files_to_directory = download
        try:
            yield
        finally:
            sync_backend.download_files_to_directory = original

    def publish_file_atomic(
        self,
        local_path: str,
        destination_uri: str,
        *,
        memory_limit_bytes: int | None,
    ) -> int:
        """Commit bytes only after the complete local file has been read."""
        del memory_limit_bytes
        payload = Path(local_path).read_bytes()
        self.published[destination_uri] = payload
        return len(payload)


class FakeBigQueryClient:
    """In-memory target schema and external-table update recorder."""

    def __init__(self, schema: Any) -> None:
        """Store the expected final schema."""
        self.schema = schema
        self.replace_calls: list[dict[str, Any]] = []

    def read_target_schema(self, _target_table: str) -> Any:
        """Return the configured existing table schema."""
        return self.schema

    def replace_external_table(self, target_table: str, **kwargs: Any) -> None:
        """Record the one allowed post-publication table update."""
        self.replace_calls.append({"target_table": target_table, **kwargs})


def _remote(name: str, updated: datetime, generation: str, payload: bytes) -> RemoteFile:
    """Build one fake object with exact generation metadata."""
    return RemoteFile(
        uri=f"gs://source/csv/{name}",
        name=name,
        size=len(payload),
        updated=updated,
        generation=generation,
    )


def _final_schema(pa: Any) -> Any:
    """Return the target analytical schema used by both integration cases."""
    question = pa.struct(
        [
            pa.field("question_id", pa.int64(), nullable=False),
            pa.field("question_text", pa.string(), nullable=False),
            pa.field("answer", pa.string()),
        ]
    )
    return pa.schema(
        [
            pa.field("response_id", pa.string()),
            pa.field("country", pa.string()),
            pa.field("questions", pa.list_(question)),
            pa.field("source_file", pa.string()),
            pa.field("ingestion_timestamp", pa.timestamp("us", tz="UTC")),
            pa.field("schema_registry", pa.string()),
            pa.field("schema_drifts", pa.string()),
        ]
    )


def _config() -> Example08Config:
    """Return the fake-cloud two-day workflow configuration."""
    return Example08Config(
        source_csv_prefix="gs://source/csv",
        silver_parquet_prefix="gs://silver/output",
        start_date=date(2026, 7, 1),
        end_date=date(2026, 7, 2),
        target_table="project.dataset.responses",
        omit_null_answers=True,
        parquet_compression="none",
    )


def test_example_08_fake_cloud_end_to_end() -> None:
    """Three heterogeneous CSVs become two validated nested Parquet objects."""
    pa = pytest.importorskip("pyarrow")
    pq = pytest.importorskip("pyarrow.parquet")
    pytest.importorskip("polars")

    a = ('response_id,country,1/How are you?,2/Path/with/slash\nr1,ES,Bien,"A/B"\n').encode()
    b = ("3/¿Nueva pregunta?,response_id,country,1/How are you?\nSí,r2,MX,\n").encode()
    c = ('response_id,country,1/How do you feel?\nr3,FR,"Très bien, merci"\n').encode()
    gcs = FakeGcsClient(
        [
            (_remote("a.csv", datetime(2026, 7, 1, 1, tzinfo=UTC), "11", a), a),
            (_remote("b.csv", datetime(2026, 7, 1, 18, tzinfo=UTC), "12", b), b),
            (_remote("c.csv", datetime(2026, 7, 2, 0, tzinfo=UTC), "13", c), c),
        ]
    )
    bigquery = FakeBigQueryClient(_final_schema(pa))
    calls: list[Any] = []

    def convert(manifest: Any, **kwargs: Any) -> Any:
        """Spy on one real schema-sanitizer call per complete daily manifest."""
        calls.append((manifest, kwargs))
        return ss.to_polars(manifest, **kwargs)

    result = run_modified_time_csv_workflow(
        _config(),
        gcs_client=gcs,
        bigquery_client=bigquery,
        to_polars=convert,
    )

    assert gcs.list_calls == 1
    assert [manifest.object_count for manifest, _kwargs in calls] == [2, 1]
    assert all(kwargs["csv_header_mode"] == "union" for _manifest, kwargs in calls)
    assert all(kwargs["schema_mode"] == "additive" for _manifest, kwargs in calls)
    assert all(kwargs["field_name_policy"] == "preserve" for _manifest, kwargs in calls)
    assert [day.row_count for day in result.completed_days] == [2, 1]
    assert set(gcs.published) == {
        "gs://silver/output/2026-07-01.parquet",
        "gs://silver/output/2026-07-02.parquet",
    }

    first = pq.read_table(pa.BufferReader(gcs.published[result.completed_days[0].output_uri]))
    second = pq.read_table(pa.BufferReader(gcs.published[result.completed_days[1].output_uri]))
    assert first.schema.equals(bigquery.schema, check_metadata=False)
    assert second.schema.equals(bigquery.schema, check_metadata=False)
    assert first.column_names == bigquery.schema.names
    assert all("/" not in name for name in first.column_names)
    first_questions = first["questions"].to_pylist()
    assert first_questions[0] == [
        {"question_id": 1, "question_text": "How are you?", "answer": "Bien"},
        {"question_id": 2, "question_text": "Path/with/slash", "answer": "A/B"},
    ]
    assert first_questions[1] == [
        {"question_id": 3, "question_text": "¿Nueva pregunta?", "answer": "Sí"}
    ]
    assert second["questions"].to_pylist()[0] == [
        {
            "question_id": 1,
            "question_text": "How do you feel?",
            "answer": "Très bien, merci",
        }
    ]
    assert first["source_file"].to_pylist() == [
        "gs://source/csv/a.csv",
        "gs://source/csv/b.csv",
    ]
    registry = json.loads(first["schema_registry"][0].as_py())
    registry_names = [field["name"] for field in registry["canonical_schema"]["fields"]]
    assert "questions" in registry_names
    assert all("/" not in name for name in registry_names)
    assert len(bigquery.replace_calls) == 1
    assert bigquery.replace_calls[0]["reference_file_schema_uri"] == (
        "gs://silver/output/2026-07-02.parquet"
    )


def test_example_08_validation_failure_publishes_nothing() -> None:
    """A final-schema mismatch prevents both object publication and table update."""
    pa = pytest.importorskip("pyarrow")
    pytest.importorskip("polars")
    payload = b"response_id,country,1/Question\nr1,ES,yes\n"
    remote = _remote("a.csv", datetime(2026, 7, 1, 1, tzinfo=UTC), "1", payload)
    gcs = FakeGcsClient([(remote, payload)])
    schema = _final_schema(pa).insert(2, pa.field("missing_scalar", pa.string()))
    bigquery = FakeBigQueryClient(schema)

    with pytest.raises(ValueError, match="missing final scalar fields"):
        run_modified_time_csv_workflow(
            _config(),
            gcs_client=gcs,
            bigquery_client=bigquery,
        )
    assert gcs.published == {}
    assert bigquery.replace_calls == []
