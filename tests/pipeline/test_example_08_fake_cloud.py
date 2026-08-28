"""Fake-cloud integration coverage for example 08.

It runs the two-day workflow against in-memory GCS and BigQuery clients and proves
validation failures publish nothing.
"""

from __future__ import annotations

import json
from contextlib import contextmanager
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any

import pytest

import schema_sanitizer as ss
from examples.example_08.hive_output import prepare_hive_parquet_schema
from examples.example_08.runtime_support import (
    Example08Config,
    run_modified_time_csv_workflow,
)
from schema_sanitizer.remote_impl import directory_downloads, sync_backend
from schema_sanitizer.sources import RemoteFile


class FakeGcsClient:
    """In-memory exact-generation GCS client for one integration process."""

    def __init__(self, objects: list[tuple[RemoteFile, bytes]]) -> None:
        """Initialize fake GCS client state for objects, listing, and list calls."""
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
        """Return the configured CSV object listing for the requested window."""
        del memory_limit_bytes
        self.list_calls += 1
        return self._listing

    @contextmanager
    def schema_sanitizer_download_scope(self):
        """Open the fake provider download scope for schema-sanitizer."""
        original_sync = sync_backend.download_files_to_directory
        original_open = directory_downloads.provider_client_for_downloads
        original_close = directory_downloads.close_provider_client
        original_download = directory_downloads.download_file_to_path

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

        async def open_async(
            _files: Any,
            *,
            memory_limit_bytes: int | None,
            threading_mode: str,
        ) -> object:
            """Return one inert shared client for multi-threaded staging."""
            del memory_limit_bytes
            assert threading_mode == "multi"
            return object()

        async def close_async(_client: object) -> None:
            """Close the inert fake client."""

        async def download_async(
            _client: object,
            remote: RemoteFile,
            local_path: str,
            *,
            storage_reservation: Any = None,
        ) -> None:
            """Materialize one exact generation for multi-threaded staging."""
            del storage_reservation
            Path(local_path).write_bytes(self._objects[remote.content_identity])

        sync_backend.download_files_to_directory = download
        directory_downloads.provider_client_for_downloads = open_async
        directory_downloads.close_provider_client = close_async
        directory_downloads.download_file_to_path = download_async
        try:
            yield
        finally:
            sync_backend.download_files_to_directory = original_sync
            directory_downloads.provider_client_for_downloads = original_open
            directory_downloads.close_provider_client = original_close
            directory_downloads.download_file_to_path = original_download

    def publish_file_atomic(
        self,
        local_path: str,
        destination_uri: str,
        *,
        memory_limit_bytes: int | None,
    ) -> int:
        """Record an atomic publication without writing to cloud storage."""
        del memory_limit_bytes
        payload = Path(local_path).read_bytes()
        self.published[destination_uri] = payload
        return len(payload)


class FakeBigQueryClient:
    """In-memory target schema and external-table update recorder."""

    def __init__(self, schema: Any) -> None:
        """Initialize fake BigQuery client state for schema and replace calls."""
        self.schema = schema
        self.replace_calls: list[dict[str, Any]] = []

    def read_target_schema(self, _target_table: str) -> Any:
        """Return the in-memory target schema while recording the lookup when needed."""
        return self.schema

    def replace_external_table(self, target_table: str, **kwargs: Any) -> None:
        """Record the requested external-table replacement."""
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
    event = pa.struct(
        [
            pa.field("event_id", pa.int64(), nullable=False),
            pa.field("event_text", pa.string(), nullable=False),
            pa.field("payload", pa.string()),
        ]
    )
    return pa.schema(
        [
            pa.field("record_id", pa.string()),
            pa.field("country", pa.string()),
            pa.field("event_timestamp", pa.timestamp("us", tz="UTC")),
            pa.field("event", pa.list_(event)),
            pa.field("source_file", pa.string()),
            pa.field("ingestion_timestamp", pa.timestamp("us", tz="UTC")),
            pa.field("schema_registry", pa.string()),
            pa.field("schema_drifts", pa.string()),
            pa.field("year", pa.int64()),
            pa.field("month", pa.int64()),
            pa.field("day", pa.int64()),
        ]
    )


def _config(*, multi_threading: bool = False) -> Example08Config:
    """Return the fake-cloud two-day workflow configuration."""
    return Example08Config(
        source_csv_prefix="gs://source/csv",
        silver_parquet_prefix="gs://silver/output",
        start_date=date(2026, 7, 1),
        end_date=date(2026, 7, 2),
        target_table="project.dataset.records",
        partition_timestamp_column="event_timestamp",
        parquet_file_prefix="records",
        omit_null_payloads=True,
        multi_threading=multi_threading,
    )


@pytest.mark.parametrize("multi_threading", [False, True])
def test_example_08_fake_cloud_end_to_end(multi_threading: bool) -> None:
    """Three heterogeneous CSVs become validated timestamp-partitioned Parquet."""
    pa = pytest.importorskip("pyarrow")
    pq = pytest.importorskip("pyarrow.parquet")
    pytest.importorskip("polars")

    a = (
        "record_id,country,event_timestamp,1/Created,2/Path/with/slash\n"
        'r1,ES,2026-06-30T23:30:00Z,active,"A/B"\n'
    ).encode()
    b = (
        "3/Estado Δ,record_id,country,event_timestamp,1/Created\n"
        "listo,r2,MX,2026-07-01T01:00:00Z,\n"
    ).encode()
    c = (
        "record_id,country,event_timestamp,1/Updated\n"
        'r3,FR,2026-07-02T01:30:00+02:00,"revision, complete"\n'
    ).encode()
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
        _config(multi_threading=multi_threading),
        gcs_client=gcs,
        bigquery_client=bigquery,
        to_polars=convert,
    )

    assert gcs.list_calls == 1
    assert [manifest.object_count for manifest, _kwargs in calls] == [2, 1]
    assert all(kwargs["csv_header_mode"] == "union" for _manifest, kwargs in calls)
    assert all(kwargs["schema_mode"] == "additive" for _manifest, kwargs in calls)
    assert all(kwargs["field_name_policy"] == "preserve" for _manifest, kwargs in calls)
    assert all(kwargs["multi_threading"] is multi_threading for _manifest, kwargs in calls)
    assert [day.row_count for day in result.completed_days] == [2, 1]
    assert [day.partition_count for day in result.completed_days] == [2, 1]
    assert set(gcs.published) == {
        "gs://silver/output/year=2026/month=6/day=30/records_20260630_20260701.gz.parquet",
        "gs://silver/output/year=2026/month=7/day=1/records_20260701_20260701.gz.parquet",
        "gs://silver/output/year=2026/month=7/day=1/records_20260701_20260702.gz.parquet",
    }
    july_first_outputs = sorted(uri for uri in gcs.published if "/year=2026/month=7/day=1/" in uri)
    assert [uri.rsplit("/", 1)[-1] for uri in july_first_outputs] == [
        "records_20260701_20260701.gz.parquet",
        "records_20260701_20260702.gz.parquet",
    ]

    parquet_schema = prepare_hive_parquet_schema(bigquery.schema, "event_timestamp")
    parquet_files = [
        pq.ParquetFile(pa.BufferReader(payload)) for _uri, payload in sorted(gcs.published.items())
    ]
    assert {
        parquet_file.metadata.row_group(row_group).column(column).compression
        for parquet_file in parquet_files
        for row_group in range(parquet_file.metadata.num_row_groups)
        for column in range(parquet_file.metadata.num_columns)
    } == {"GZIP"}
    tables = [parquet_file.read() for parquet_file in parquet_files]
    assert all(table.schema.equals(parquet_schema, check_metadata=False) for table in tables)
    assert all(table.column_names == parquet_schema.names for table in tables)
    combined = pa.concat_tables(tables)
    rows = {row["record_id"]: row for row in combined.to_pylist()}
    assert rows["r1"]["event"] == [
        {"event_id": 1, "event_text": "Created", "payload": "active"},
        {"event_id": 2, "event_text": "Path/with/slash", "payload": "A/B"},
    ]
    assert rows["r2"]["event"] == [{"event_id": 3, "event_text": "Estado Δ", "payload": "listo"}]
    assert rows["r3"]["event"] == [
        {
            "event_id": 1,
            "event_text": "Updated",
            "payload": "revision, complete",
        }
    ]
    assert {row["source_file"] for row in rows.values()} == {
        "gs://source/csv/a.csv",
        "gs://source/csv/b.csv",
        "gs://source/csv/c.csv",
    }
    registry = json.loads(combined["schema_registry"][0].as_py())
    registry_names = [field["name"] for field in registry["canonical_schema"]["fields"]]
    assert "event" in registry_names
    assert not {"year", "month", "day"} & set(registry_names)
    assert all("/" not in name for name in registry_names)
    assert len(bigquery.replace_calls) == 1
    assert bigquery.replace_calls[0]["reference_file_schema_uri"] == (
        "gs://silver/output/year=2026/month=7/day=1/records_20260701_20260702.gz.parquet"
    )
    assert bigquery.replace_calls[0]["hive_uri_prefix"] == "gs://silver/output"
    assert bigquery.replace_calls[0]["source_uri_pattern"] == "gs://silver/output/*"


def test_example_08_validation_failure_publishes_nothing() -> None:
    """A final-schema mismatch prevents both object publication and table update."""
    pa = pytest.importorskip("pyarrow")
    pytest.importorskip("polars")
    payload = b"record_id,country,1/Event\nr1,ES,yes\n"
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
