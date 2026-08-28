"""Remote discovery, staging, registry, and source-plan contracts.

It spans provider pagination and bulk discovery, lazy staging, registry probes,
source-plan reuse, memory limits, retry classification, and cleanup ownership.
"""

from __future__ import annotations

import asyncio
import json
import threading
from pathlib import Path
from types import SimpleNamespace

import pytest
from _support.remote_harness import BoundedResponse as FakeResponse
from conftest import read_test_jsonl

import schema_sanitizer as ss

_BULK_OBJECT_NAMES = (
    "events/date=2026-01-01/hour=00/a.json",
    "events/date=2026-01-01/hour=00/nested/ignored.json",
    "events/date=2026-01-01/hour=01/b.txt",
    "events/date=2026-01-01/hour=02/c.json",
)


def _bulk_directory_uris(base: str) -> list[str]:
    """Build the three remote directory URIs used by bulk-listing cases."""
    return [f"{base}/hour={hour}" for hour in ("00", "01", "02")]


def _assert_bulk_directory_result(result: object, base: str) -> None:
    """Assert existence results for all three bulk directory URIs."""
    uris = _bulk_directory_uris(base)
    assert result.exists_by_uri == {
        uris[0]: True,
        uris[1]: False,
        uris[2]: True,
    }


def test_remote_gcs_directory_listing_reads_all_pages(monkeypatch) -> None:
    """Verify remote GCS directory listing reads all pages."""
    from schema_sanitizer.remote_impl.async_bridge import run_sync
    from schema_sanitizer.remote_impl.providers import gcs as gcs_listing
    from schema_sanitizer.sources import RemoteFile

    class FakeSession:
        """Minimal aiohttp-like session with paginated GCS responses."""

        def __init__(self):
            """Initialize fake session state for params and pages."""
            self.params: list[dict[str, str]] = []
            self.pages = [
                {"items": [{"name": "events/ignore.txt"}], "nextPageToken": "page-2"},
                {"items": [{"name": "events/row.json", "size": "7"}]},
            ]

        async def __aenter__(self):
            """Return the managed fake session value from context entry."""
            return self

        async def __aexit__(self, exc_type, exc, traceback):
            """Finalize the fake session context without suppressing exceptions."""
            return None

        def get(self, _url, *, params):
            """Return the configured response for the requested provider object."""
            self.params.append(dict(params))
            return FakeResponse(self.pages.pop(0))

    fake_session = FakeSession()

    async def fake_session_factory(headers, *, memory_limit_bytes=None, threading_mode="single"):
        """Return the fake GCS session."""
        assert headers["Authorization"] == "Bearer token"
        return fake_session

    monkeypatch.setattr(gcs_listing, "access_token", lambda: "token")
    monkeypatch.setattr(gcs_listing, "open_aiohttp_session", fake_session_factory)

    files = run_sync(gcs_listing.list_directory("gs://bucket/events/", (".json",)))

    assert files == [RemoteFile("gs://bucket/events/row.json", "row.json", 7)]
    assert fake_session.params[0]["prefix"] == "events/"
    assert "pageToken" not in fake_session.params[0]
    assert fake_session.params[1]["pageToken"] == "page-2"


def test_remote_gcs_bulk_directory_discovery_groups_parent_prefixes(monkeypatch) -> None:
    """Verify remote GCS bulk directory discovery groups parent prefixes."""
    from schema_sanitizer.remote_impl.async_bridge import run_sync
    from schema_sanitizer.remote_impl.providers import gcs as gcs_bulk_discovery

    class FakeSession:
        """Minimal aiohttp-like session with one parent-prefix listing."""

        def __init__(self):
            """Initialize fake session state for params."""
            self.params: list[dict[str, str]] = []

        async def __aenter__(self):
            """Return the managed fake session value from context entry."""
            return self

        async def __aexit__(self, exc_type, exc, traceback):
            """Finalize the fake session context without suppressing exceptions."""
            return None

        def get(self, _url, *, params):
            """Return the configured response for the requested provider object."""
            self.params.append(dict(params))
            return FakeResponse({"items": [{"name": name} for name in _BULK_OBJECT_NAMES]})

    fake_session = FakeSession()

    async def fake_session_factory(headers, *, memory_limit_bytes=None, threading_mode="single"):
        """Return the fake GCS session."""
        assert headers["Authorization"] == "Bearer token"
        return fake_session

    monkeypatch.setattr(gcs_bulk_discovery, "access_token", lambda: "token")
    monkeypatch.setattr(gcs_bulk_discovery, "open_aiohttp_session", fake_session_factory)

    result = run_sync(
        gcs_bulk_discovery.directories_containing_files(
            _bulk_directory_uris("gs://bucket/events/date=2026-01-01"),
            (".json",),
        )
    )

    _assert_bulk_directory_result(result, "gs://bucket/events/date=2026-01-01")
    assert len(fake_session.params) == 1
    assert fake_session.params[0]["prefix"] == "events/date=2026-01-01/"


def test_remote_s3_bulk_directory_discovery_groups_parent_prefixes(monkeypatch) -> None:
    """Verify remote S3 bulk directory discovery groups parent prefixes."""
    from schema_sanitizer.remote_impl.async_bridge import run_sync
    from schema_sanitizer.remote_impl.providers import s3 as s3_discovery

    class FakeS3Client:
        """Minimal async S3 client with one parent-prefix listing."""

        def __init__(self):
            """Initialize fake S3 client state for calls."""
            self.calls: list[dict[str, object]] = []

        async def __aenter__(self):
            """Return the managed fake S3 client value from context entry."""
            return self

        async def __aexit__(self, exc_type, exc, traceback):
            """Finalize the fake S3 client context without suppressing exceptions."""
            return None

        async def list_objects_v2(self, **kwargs):
            """Return the configured page of S3 object listings."""
            self.calls.append(dict(kwargs))
            return {
                "Contents": [{"Key": name} for name in _BULK_OBJECT_NAMES],
                "IsTruncated": False,
            }

    fake_client = FakeS3Client()

    async def fake_s3_client():
        """Return the fake S3 client."""
        return fake_client

    monkeypatch.setattr(s3_discovery, "open_client", fake_s3_client)

    result = run_sync(
        s3_discovery.directories_containing_files(
            _bulk_directory_uris("s3://bucket/events/date=2026-01-01"),
            (".json",),
        )
    )

    _assert_bulk_directory_result(result, "s3://bucket/events/date=2026-01-01")
    assert fake_client.calls == [
        {
            "Bucket": "bucket",
            "Prefix": "events/date=2026-01-01/",
            "MaxKeys": 1000,
        }
    ]


def test_remote_azure_bulk_directory_discovery_groups_parent_prefixes(monkeypatch) -> None:
    """Verify remote azure bulk directory discovery groups parent prefixes."""
    from types import SimpleNamespace

    from schema_sanitizer.remote_impl.async_bridge import run_sync
    from schema_sanitizer.remote_impl.providers import azure as azure_discovery

    class FakeContainer:
        """Minimal async Azure container client."""

        def __init__(self):
            """Initialize fake container state for prefixes."""
            self.prefixes: list[str] = []

        async def list_blobs(self, *, name_starts_with):
            """Return the configured Azure blob listing."""
            self.prefixes.append(name_starts_with)
            for name in _BULK_OBJECT_NAMES:
                yield SimpleNamespace(name=name)

    class FakeService:
        """Minimal async Azure blob service."""

        def __init__(self):
            """Initialize fake service state for container and closed."""
            self.container = FakeContainer()
            self.closed = False

        def get_container_client(self, container_name):
            """Return the sole container client after validating its name."""
            assert container_name == "container"
            return self.container

        async def close(self):
            """Close the fake service and update closed."""
            self.closed = True

    fake_service = FakeService()

    async def fake_azure_service(ref):
        """Return the fake Azure service."""
        assert ref.account_url == "https://account.blob.core.windows.net"
        return fake_service

    monkeypatch.setattr(azure_discovery, "open_service", fake_azure_service)

    result = run_sync(
        azure_discovery.directories_containing_files(
            _bulk_directory_uris("az://account/container/events/date=2026-01-01"),
            (".json",),
        )
    )

    _assert_bulk_directory_result(result, "az://account/container/events/date=2026-01-01")
    assert fake_service.container.prefixes == ["events/date=2026-01-01/"]
    assert fake_service.closed is True


def test_remote_s3_directory_listing_reads_all_pages(monkeypatch) -> None:
    """Verify remote S3 directory listing reads all pages."""
    from schema_sanitizer.remote_impl.async_bridge import run_sync
    from schema_sanitizer.remote_impl.providers import s3 as s3_discovery
    from schema_sanitizer.sources import RemoteFile

    class FakeS3Client:
        """Minimal async S3 client with paginated list_objects_v2 responses."""

        def __init__(self):
            """Initialize fake S3 client state for calls."""
            self.calls: list[dict[str, object]] = []

        async def __aenter__(self):
            """Return the managed fake S3 client value from context entry."""
            return self

        async def __aexit__(self, exc_type, exc, traceback):
            """Finalize the fake S3 client context without suppressing exceptions."""
            return None

        async def list_objects_v2(self, **kwargs):
            """Return the configured page of S3 object listings."""
            self.calls.append(kwargs)
            if "ContinuationToken" not in kwargs:
                return {
                    "IsTruncated": True,
                    "NextContinuationToken": "page-2",
                    "Contents": [{"Key": "events/ignore.txt", "Size": 4}],
                }
            assert kwargs["ContinuationToken"] == "page-2"
            return {
                "IsTruncated": False,
                "Contents": [{"Key": "events/row.json", "Size": 7}],
            }

    fake_client = FakeS3Client()

    async def fake_s3_client():
        """Return the fake S3 client context manager."""
        return fake_client

    monkeypatch.setattr(s3_discovery, "open_client", fake_s3_client)

    files = run_sync(s3_discovery.list_files("s3://bucket/events/", (".json",)))

    assert files == [RemoteFile("s3://bucket/events/row.json", "row.json", 7)]
    assert fake_client.calls[0]["Prefix"] == "events/"
    assert "ContinuationToken" not in fake_client.calls[0]
    assert fake_client.calls[1]["ContinuationToken"] == "page-2"


class _Stage:
    """Minimal staged-path stand-in that preserves its local file."""

    def __init__(self, path: Path):  # noqa: F405
        """Initialize stage state for path."""
        self.path = str(path)

    def close(self) -> None:
        """Close the stage and release its retained resources."""
        pass


def test_uri_input_uses_async_local_staging(monkeypatch, tmp_path, require_native: None) -> None:
    """Verify URI input uses async local staging."""
    pytest.importorskip("pyarrow")

    staged_paths: list[str] = []

    def fake_stage(
        uri: str,
        *,
        memory_limit_bytes: int | None,
        threading_mode: str = "single",
        operation_context=None,
    ) -> _Stage:
        """Write the remote payload to a local staged file."""
        del memory_limit_bytes
        assert uri == "s3://bucket/events.jsonl"
        path = tmp_path / "staged.jsonl"
        path.write_bytes(b'{"a": 1}\n{"a": 2}\n')
        staged_paths.append(str(path))
        return _Stage(path)

    from schema_sanitizer.remote_impl import staging as remote_staging

    monkeypatch.setattr(remote_staging, "stage_remote_single_file", fake_stage)

    result = read_test_jsonl("s3://bucket/events.jsonl", memory_limit_bytes=1 << 20)

    assert result.clean_data.to_pylist() == [{"a": 1}, {"a": 2}]
    assert staged_paths == [str(tmp_path / "staged.jsonl")]


def test_uri_input_staging_works_with_converters(
    monkeypatch, tmp_path, require_native: None
) -> None:
    """Verify URI input staging works with converters."""
    pytest.importorskip("pyarrow")

    out = tmp_path / "out.jsonl"

    def fake_stage(
        uri: str,
        *,
        memory_limit_bytes: int | None,
        threading_mode: str = "single",
        operation_context=None,
    ) -> _Stage:
        """Write one remote payload to a local staged file."""
        del memory_limit_bytes
        assert uri == "s3://bucket/events.jsonl"
        path = tmp_path / "converter-staged.jsonl"
        path.write_bytes(b'{"a": 1}\n{"a": 2}\n')
        return _Stage(path)

    from schema_sanitizer.remote_impl import staging as remote_staging

    monkeypatch.setattr(remote_staging, "stage_remote_single_file", fake_stage)

    ss.to_jsonl(
        "s3://bucket/events.jsonl",
        out,
        input_format="jsonl",
        memory_limit_bytes=1 << 20,
    )

    rows = [json.loads(line) for line in out.read_text(encoding="utf-8").splitlines()]
    assert [
        {
            k: v
            for k, v in row.items()
            if k
            not in {
                "schema_registry",
                "schema_drifts",
                "source_file",
                "ingestion_timestamp",
            }
        }
        for row in rows
    ] == [{"a": 1}, {"a": 2}]


def test_uri_input_allows_non_utf8_after_local_staging(
    monkeypatch, tmp_path, require_native: None
) -> None:
    """Verify URI input allows non utf8 after local staging."""
    pytest.importorskip("pyarrow")

    def fake_stage(
        uri: str,
        *,
        memory_limit_bytes: int | None,
        threading_mode: str = "single",
        operation_context=None,
    ) -> _Stage:
        """Write Latin-1 JSONL to a local staged file."""
        del memory_limit_bytes
        assert uri == "s3://bucket/events.jsonl"
        path = tmp_path / "latin1.jsonl"
        path.write_bytes('{"name":"café"}\n'.encode("latin-1"))
        return _Stage(path)

    from schema_sanitizer.remote_impl import staging as remote_staging

    monkeypatch.setattr(remote_staging, "stage_remote_single_file", fake_stage)

    result = read_test_jsonl("s3://bucket/events.jsonl", input_text_encoding="iso8859-1")
    assert result.clean_data.to_pylist() == [{"name": "café"}]


def test_remote_parquet_directory_stages_children_synchronously(monkeypatch, tmp_path) -> None:
    """Verify remote Parquet directory stages children synchronously."""
    from schema_sanitizer.remote_impl import staging as remote_staging
    from schema_sanitizer.sources import RemoteFile

    def fake_list(uri, suffixes, *, memory_limit_bytes=None):
        """Return deterministic remote Parquet children."""
        assert uri == "s3://bucket/partition/"
        assert ".parquet" in suffixes
        return [
            RemoteFile("s3://bucket/partition/a.parquet", "a.parquet", None),
            RemoteFile("s3://bucket/partition/b.parquet", "b.parquet", None),
        ]

    def fake_download(files, directory, *, memory_limit_bytes):
        """Write staged payloads serially in canonical file order."""
        assert memory_limit_bytes is None
        for file in files:
            (Path(directory) / file.name).write_bytes(file.name.encode("utf-8"))

    monkeypatch.setattr(remote_staging.sync_backend, "list_remote_directory", fake_list)
    monkeypatch.setattr(remote_staging.sync_backend, "download_files_to_directory", fake_download)

    staged = remote_staging.stage_remote_parquet_directory(
        "s3://bucket/partition/",
        suffixes=(".parquet",),
        memory_limit_bytes=None,
    )
    try:
        root = Path(staged.path)
        assert (root / "a.parquet").read_bytes() == b"a.parquet"
        assert (root / "b.parquet").read_bytes() == b"b.parquet"
    finally:
        staged.close()


def test_remote_parquet_directory_public_reader_uses_staged_arrow_path(
    monkeypatch,
    tmp_path,
    require_native: None,
) -> None:
    """Verify remote Parquet directory public reader uses staged arrow path."""
    pa = pytest.importorskip("pyarrow")
    pq = pytest.importorskip("pyarrow.parquet")
    from schema_sanitizer.remote_impl import staging as remote_staging
    from schema_sanitizer.remote_impl.staging import StagedPath

    def fake_stage_remote_parquet_directory(
        uri, *, suffixes, memory_limit_bytes, threading_mode="single", operation_context=None
    ):
        """Return a local staged Parquet directory for a remote URI."""
        assert uri == "s3://bucket/partition/"
        assert suffixes == (".parquet",)
        assert isinstance(memory_limit_bytes, int) and memory_limit_bytes > 0
        staged_dir = tmp_path / "staged-parquet"
        staged_dir.mkdir()
        pq.write_table(pa.table({"id": [1, 2]}), staged_dir / "a.parquet")
        pq.write_table(pa.table({"id": [3]}), staged_dir / "b.parquet")
        return StagedPath(
            str(staged_dir),
            is_dir=True,
            source_file_by_name={
                "a.parquet": "s3://bucket/partition/a.parquet",
                "b.parquet": "s3://bucket/partition/b.parquet",
            },
        )

    monkeypatch.setattr(
        remote_staging,
        "stage_remote_parquet_directory",
        fake_stage_remote_parquet_directory,
    )

    result = ss.to_pyarrow(
        "s3://bucket/partition/",
        input_format="parquet",
        input_mode="directory",
    )

    rows = result.clean_data.to_pylist()
    assert [{key: row[key] for key in ("id",)} for row in rows] == [
        {"id": 1},
        {"id": 2},
        {"id": 3},
    ]
    assert [row["source_file"] for row in rows] == [
        "s3://bucket/partition/a.parquet",
        "s3://bucket/partition/a.parquet",
        "s3://bucket/partition/b.parquet",
    ]


def test_remote_parquet_single_file_public_reader_uses_staged_arrow_path(
    monkeypatch,
    tmp_path,
    require_native: None,
) -> None:
    """Verify remote Parquet single file public reader uses staged arrow path."""
    pa = pytest.importorskip("pyarrow")
    pq = pytest.importorskip("pyarrow.parquet")
    from schema_sanitizer.remote_impl import staging as remote_staging
    from schema_sanitizer.remote_impl.staging import StagedPath

    def fake_stage_remote_single_file(
        uri, *, memory_limit_bytes, threading_mode="single", operation_context=None
    ):
        """Return a local staged Parquet file for a remote URI."""
        assert uri == "s3://bucket/events.parquet"
        assert isinstance(memory_limit_bytes, int) and memory_limit_bytes > 0
        staged_file = tmp_path / "events.parquet"
        pq.write_table(pa.table({"id": [1, 2]}), staged_file)
        return StagedPath(str(staged_file))

    monkeypatch.setattr(
        remote_staging,
        "stage_remote_single_file",
        fake_stage_remote_single_file,
    )

    result = ss.to_pyarrow("s3://bucket/events.parquet", input_format="parquet")

    rows = result.clean_data.to_pylist()
    assert [{key: row[key] for key in ("id",)} for row in rows] == [
        {"id": 1},
        {"id": 2},
    ]
    assert [row["source_file"] for row in rows] == [
        "s3://bucket/events.parquet",
        "s3://bucket/events.parquet",
    ]


def test_remote_parquet_schema_probe_retires_readers_before_staged_cleanup(
    monkeypatch,
    tmp_path,
    require_native: None,
) -> None:
    """A schema-only probe must not defer PyArrow handles past staged cleanup."""
    pa = pytest.importorskip("pyarrow")
    pq = pytest.importorskip("pyarrow.parquet")
    from schema_sanitizer.adapters.parquet import record_batch_factory
    from schema_sanitizer.core_impl.finalizer_cleanup import drain_finalizer_cleanup
    from schema_sanitizer.remote_impl import staging as remote_staging
    from schema_sanitizer.remote_impl.staging import StagedPath

    staged_file = tmp_path / "probe-events.parquet"
    pq.write_table(pa.table({"id": [1, 2]}), staged_file)
    deferred_stream_owners: list[tuple[object, ...]] = []
    original_defer = record_batch_factory.defer_prepared_finalizer_cleanup

    def record_deferred_stream_owner(capsule) -> bool:
        """Record only Parquet stream graphs handed to the finalizer escrow."""
        if capsule.callback is record_batch_factory._cleanup_parquet_stream_owner_capsule:
            deferred_stream_owners.append(tuple(capsule.arg0 or ()))
        return original_defer(capsule)

    class OrderedStagedPath(StagedPath):
        """Assert external readers retire before the staging owner is closed."""

        def close(self) -> None:
            """Close the ordered staged path and release its retained resources."""
            assert deferred_stream_owners == []
            super().close()

    def fake_stage_remote_single_file(
        uri, *, memory_limit_bytes, threading_mode="single", operation_context=None
    ):
        """Return one PyArrow-written staged file that requires fallback decoding."""
        assert uri == "s3://bucket/probe-events.parquet"
        assert isinstance(memory_limit_bytes, int) and memory_limit_bytes > 0
        return OrderedStagedPath(str(staged_file))

    monkeypatch.setattr(
        record_batch_factory,
        "defer_prepared_finalizer_cleanup",
        record_deferred_stream_owner,
    )
    monkeypatch.setattr(
        remote_staging,
        "stage_remote_single_file",
        fake_stage_remote_single_file,
    )
    try:
        result = ss.to_pyarrow(
            "s3://bucket/probe-events.parquet",
            input_format="parquet",
        )
    finally:
        # Keeps this regression hermetic when run against an older extension:
        # any captured owner remains authoritative until a governed safe point.
        drain_finalizer_cleanup()

    assert [{"id": row["id"]} for row in result.clean_data.to_pylist()] == [
        {"id": 1},
        {"id": 2},
    ]
    assert deferred_stream_owners == []
    assert not staged_file.exists()


def test_remote_parquet_single_file_writer_uses_staged_arrow_path(
    monkeypatch,
    tmp_path,
    require_native: None,
) -> None:
    """Verify remote Parquet single file writer uses staged arrow path."""
    pa = pytest.importorskip("pyarrow")
    pq = pytest.importorskip("pyarrow.parquet")
    from schema_sanitizer.remote_impl import staging as remote_staging
    from schema_sanitizer.remote_impl.staging import StagedPath

    staged_file = tmp_path / "events.parquet"
    pq.write_table(pa.table({"id": [1, 2]}), staged_file)

    def fake_stage_remote_single_file(
        uri, *, memory_limit_bytes, threading_mode="single", operation_context=None
    ):
        """Return a local staged Parquet file for a remote URI."""
        assert uri == "s3://bucket/events.parquet"
        assert isinstance(memory_limit_bytes, int) and memory_limit_bytes > 0
        return StagedPath(str(staged_file))

    monkeypatch.setattr(
        remote_staging,
        "stage_remote_single_file",
        fake_stage_remote_single_file,
    )

    out_path = tmp_path / "out.parquet"
    result = ss.to_parquet(
        "s3://bucket/events.parquet",
        out_path,
        input_format="parquet",
    )

    rows = pq.read_table(out_path).to_pylist()
    assert [{key: row[key] for key in ("id",)} for row in rows] == [
        {"id": 1},
        {"id": 2},
    ]
    assert [row["source_file"] for row in rows] == [
        "s3://bucket/events.parquet",
        "s3://bucket/events.parquet",
    ]
    assert result.stats["inferred_rows"] == 2
    assert result.stats["materialized_rows"] == 2


def test_remote_text_directory_stages_child_sources_synchronously(monkeypatch) -> None:
    """Verify remote text directory stages child sources synchronously."""
    from schema_sanitizer.remote_impl import staging as remote_staging
    from schema_sanitizer.sources import RemoteFile

    files = [
        RemoteFile("s3://bucket/partition/a.jsonl", "a.jsonl", None),
        RemoteFile("s3://bucket/partition/b.jsonl", "b.jsonl", None),
    ]

    def fake_download(files, directory, *, memory_limit_bytes):
        """Write staged payloads serially in canonical file order."""
        assert memory_limit_bytes is None
        for file in files:
            (Path(directory) / file.name).write_bytes(file.uri.encode("utf-8"))

    monkeypatch.setattr(remote_staging.sync_backend, "download_files_to_directory", fake_download)

    staged = remote_staging.stage_remote_files_to_directory(
        files,
        memory_limit_bytes=None,
    )
    try:
        root = Path(staged.path)
        assert (root / "a.jsonl").read_bytes() == b"s3://bucket/partition/a.jsonl"
        assert (root / "b.jsonl").read_bytes() == b"s3://bucket/partition/b.jsonl"
        assert staged.source_file_by_name == {
            "a.jsonl": "s3://bucket/partition/a.jsonl",
            "b.jsonl": "s3://bucket/partition/b.jsonl",
        }
    finally:
        staged.close()


def test_remote_json_directory_preparation_uses_lazy_native_source_stage(
    monkeypatch, tmp_path
) -> None:
    """Verify remote JSON directory preparation uses lazy native source stage."""
    from schema_sanitizer.api_impl.input import preparation as public_input
    from schema_sanitizer.api_impl.source_plan.attached import (
        remote_native_multisource_manifest_from_data,
    )
    from schema_sanitizer.input_impl.source_plan import path_source_tuples
    from schema_sanitizer.remote_impl import staging as remote_staging
    from schema_sanitizer.remote_impl import sync_backend
    from schema_sanitizer.remote_impl.staging import StagedPath
    from schema_sanitizer.sources import RemoteFile

    staged_calls = []

    def fake_list_remote_directory_files(uri, suffixes, *, memory_limit_bytes=None):
        """Return deterministic remote children without staging them."""
        assert uri == "s3://bucket/partition/"
        assert suffixes == (".json",)
        return [
            RemoteFile("s3://bucket/partition/a.json", "a.json", 8),
            RemoteFile("s3://bucket/partition/b.json", "b.json", 8),
        ]

    def fake_stage_remote_files_to_directory(files, **kwargs):
        """Stage only the requested remote chunk."""
        assert kwargs["memory_limit_bytes"] is None
        staged_calls.append([file.name for file in files])
        staged_dir = tmp_path / f"staged-{len(staged_calls)}"
        staged_dir.mkdir()
        for file in files:
            (staged_dir / file.name).write_text('{"a":1}\n', encoding="utf-8")
        return StagedPath(
            str(staged_dir),
            is_dir=True,
            source_file_by_name={file.name: file.uri for file in files},
        )

    monkeypatch.setattr(
        sync_backend,
        "list_remote_directory",
        fake_list_remote_directory_files,
    )
    monkeypatch.setattr(
        remote_staging,
        "stage_remote_files_to_directory",
        fake_stage_remote_files_to_directory,
    )

    prepared = public_input.prepare_public_input(
        "s3://bucket/partition/",
        input_format="json",
        input_mode="directory",
        input_text_encoding="utf-8",
        xml_row_tag=None,
        csv_delimiter=",",
        csv_has_header=True,
        memory_limit_bytes=None,
    )
    try:
        assert prepared.format == "json"
        assert prepared.source == "stream"
        manifest = remote_native_multisource_manifest_from_data(prepared.data)
        assert manifest is not None
        assert staged_calls == []
        first = manifest.stage_chunk(0)
        assert first is not None
        assert staged_calls == [["a.json", "b.json"]]
        assert first.manifest.source_batch is not None
        assert path_source_tuples(first.manifest.source_batch) == [
            (
                "json",
                str(tmp_path / "staged-1" / "a.json"),
                "s3://bucket/partition/a.json",
            ),
            (
                "json",
                str(tmp_path / "staged-1" / "b.json"),
                "s3://bucket/partition/b.json",
            ),
        ]
        first.close()
    finally:
        prepared.close()


def test_discovered_remote_json_directory_uses_same_lazy_source_plan(
    monkeypatch,
) -> None:
    """Verify discovered remote JSON directory uses same lazy source plan."""
    import schema_sanitizer.input_impl.source_plan as source_plan_model
    from schema_sanitizer.api_impl.input import preparation as public_input
    from schema_sanitizer.api_impl.input import remote_directory_preparation
    from schema_sanitizer.api_impl.source_plan import attached as source_plan_attached
    from schema_sanitizer.input_impl.directory_inputs import (
        DiscoveredDirectoryInput,
        discovered_directory_inputs,
    )
    from schema_sanitizer.remote_impl import sync_backend
    from schema_sanitizer.sources import RemoteFile

    files = (
        RemoteFile("s3://bucket/partition/a.json", "a.json", 8),
        RemoteFile("s3://bucket/partition/b.json", "b.json", 8),
    )

    def fail_listing(*_args, **_kwargs):
        """Fail if discovered remote inputs are listed again."""
        raise AssertionError("discovered remote directory should not be relisted")

    packet_policies = []
    real_packet_policy = remote_directory_preparation.remote_staging_packet_policy

    def capture_packet_policy(memory_limit_bytes):
        """Record the exact dynamic policy consumed by preparation."""
        policy = real_packet_policy(memory_limit_bytes)
        packet_policies.append((memory_limit_bytes, policy))
        return policy

    monkeypatch.setattr(sync_backend, "list_remote_directory", fail_listing)
    monkeypatch.setattr(
        remote_directory_preparation,
        "remote_staging_packet_policy",
        capture_packet_policy,
    )

    with discovered_directory_inputs(
        {
            "s3://bucket/partition/": DiscoveredDirectoryInput(
                input_format="json",
                remote_files=files,
            )
        }
    ):
        prepared = public_input.prepare_public_input(
            "s3://bucket/partition/",
            input_format="json",
            input_mode="directory",
            input_text_encoding="utf-8",
            xml_row_tag=None,
            csv_delimiter=",",
            csv_has_header=True,
            memory_limit_bytes=None,
        )

    try:
        plan = source_plan_attached.source_plan_from_data(prepared.data)
        manifest = source_plan_attached.remote_native_multisource_manifest_from_data(prepared.data)
        assert prepared.format == "json"
        assert prepared.source == "stream"
        assert plan is not None
        assert plan.kind == source_plan_model.REMOTE_CHUNKS
        assert plan.route_name == "remote_native_manifest_chunks"
        assert manifest is not None
        assert manifest.files == tuple(files)
        assert len(packet_policies) == 1
        memory_limit_bytes, packet_policy = packet_policies[0]
        assert memory_limit_bytes is None
        assert manifest.chunk_size == packet_policy.max_files
        assert manifest.chunk_target_bytes == packet_policy.target_bytes
    finally:
        prepared.close()


def _remote_plan():
    """Build a two-file remote source plan."""
    import schema_sanitizer.input_impl.source_plan as source_plan_model
    from schema_sanitizer.api_impl.input.directory_preparation import (
        RemoteNativeDirectorySourceManifest,
    )
    from schema_sanitizer.sources import RemoteFile

    manifest = RemoteNativeDirectorySourceManifest(
        [
            RemoteFile("s3://bucket/a.json", "a.json", None),
            RemoteFile("s3://bucket/b.json", "b.json", None),
        ],
        input_format="json",
        chunk_size=1,
    )
    return source_plan_model.NativeSourcePlan(
        kind=source_plan_model.REMOTE_CHUNKS,
        payload=manifest,
        input_format="json",
        route_name="remote_native_manifest_chunks",
    )


def _fake_staging(events: list[str], label: str):
    """Build a context factory yielding two staged one-file manifests."""
    from schema_sanitizer.input_impl.prepared import NativeDirectorySourceManifest
    from schema_sanitizer.input_impl.source_plan import PreparedSourceBatch, SourceDescriptor

    class FakeStage:
        """One staged remote source owned by the test context."""

        def __init__(self, name: str) -> None:
            """Initialize fake stage state for name and manifest."""
            self.name = name
            self.manifest = NativeDirectorySourceManifest(
                PreparedSourceBatch(
                    (
                        SourceDescriptor(
                            "json",
                            f"/tmp/{name}.json",
                            f"s3://bucket/{name}.json",
                        ),
                    ),
                    input_format="json",
                )
            )

        def close(self) -> None:
            """Close the fake stage and release its retained resources."""
            events.append(f"close:{self.name}")

    class FakeStagedChunks:
        """Context manager yielding deterministic staged chunks."""

        def __enter__(self):
            """Return the managed fake staged chunks value from context entry."""
            events.append(f"enter:{label}")
            return iter([FakeStage(f"{label}-a"), FakeStage(f"{label}-b")])

        def __exit__(self, _exc_type, _exc, _tb) -> bool:
            """Finalize the fake staged chunks context without suppressing exceptions."""
            events.append(f"exit:{label}")
            return False

    return FakeStagedChunks()


def _patch_native_plan(monkeypatch, chunks: list[tuple[tuple[str, str, str], ...]]) -> None:
    """Expose provider chunks without constructing a real native capsule."""
    import schema_sanitizer.input_impl.source_plan as path_sources_impl

    def create(sources, *_args):
        """Record one provider chunk and return a stand-in plan."""
        chunk = tuple(sources)
        chunks.append(chunk)
        return ("native-plan", chunk)

    monkeypatch.setattr(path_sources_impl, "PATH_SOURCE_PLAN_CREATE", create)


def test_remote_registry_stream_uses_current_native_auto_provider(monkeypatch) -> None:
    """Remote registry output uses one paired-provider native route."""
    from schema_sanitizer.api_impl.source_plan import registry as source_plan_registry_stream
    from schema_sanitizer.api_impl.source_plan import remote as source_plan_remote_staging

    events: list[str] = []
    native_chunks: list[tuple[tuple[str, str, str], ...]] = []
    _patch_native_plan(monkeypatch, native_chunks)

    class FakeRaw:
        """Opened native result retaining the streaming provider."""

        diagnostics = {"route": "auto-provider"}
        native_registry_state = "auto-state"
        schema_registry_json = '{"auto":true}'
        schema_drifts_json = "[]"

        def __init__(self, stream_provider) -> None:
            """Initialize fake raw state for stream provider."""
            self._stream_provider = stream_provider

        def close(self) -> None:
            """Close the fake raw and release its retained resources."""
            events.append("raw-close")
            self._stream_provider.close()

    class FakeRawContext:
        """Current ABI context consuming paired remote providers."""

        auto_calls = 0

        def to_registry_sink_path_source_chunk_provider_auto_registry(
            self,
            sink,
            probe_provider,
            stream_provider,
            call_options,
            **options,
        ):
            """Validate and open the paired native registry providers."""
            assert sink == "stream"
            assert call_options == "options"
            assert options == {
                "registry_json": "{}",
                "field_name_policy": "lower_snake",
                "schema_mode": "additive",
                "first_row_columns": {},
                "timestamp_columns": ("ingestion_timestamp",),
                "native_registry_state": None,
                "skip_invalid_json_sources": True,
            }
            self.auto_calls += 1
            while probe_provider.next_sources() is not None:
                pass
            probe_provider.close()
            return FakeRaw(stream_provider)

    contexts = [
        _fake_staging(events, "probe"),
        _fake_staging(events, "stream"),
    ]
    monkeypatch.setattr(
        source_plan_remote_staging,
        "open_staged_remote_chunks",
        lambda _manifest, **_kwargs: contexts.pop(0),
    )
    raw_context = FakeRawContext()

    opened = source_plan_registry_stream.open_source_plan_registry_stream(
        raw_context,
        _remote_plan(),
        "options",
        registry_json="{}",
        field_name_policy="lower_snake",
        schema_mode="additive",
        first_row_columns={},
        timestamp_columns=("ingestion_timestamp",),
    )

    assert opened.schema_registry_json == '{"auto":true}'
    assert opened.native_registry_state == "auto-state"
    assert raw_context.auto_calls == 1
    assert len(native_chunks) == 2
    assert events == [
        "enter:probe",
        "close:probe-b",
        "exit:probe",
    ]
    opened.close()
    assert events == [
        "enter:probe",
        "close:probe-b",
        "exit:probe",
        "raw-close",
        "close:probe-a",
    ]


def test_remote_registry_probe_is_owned_by_native_chunk_provider(monkeypatch) -> None:
    """Registry inference makes one native call and lets it pull every chunk."""
    from schema_sanitizer.api_impl.source_plan import probing as source_plan_probe
    from schema_sanitizer.api_impl.source_plan import remote as source_plan_remote_staging

    events: list[str] = []
    native_chunks: list[tuple[tuple[str, str, str], ...]] = []
    _patch_native_plan(monkeypatch, native_chunks)

    class FakeRawContext:
        """Current ABI context consuming one registry probe provider."""

        def registry_probe_path_source_chunk_provider(
            self,
            provider,
            call_options,
            **options,
        ):
            """Return the configured source chunk for registry probing."""
            assert call_options == "options"
            assert options["skip_invalid_json_sources"] is True
            while provider.next_sources() is not None:
                pass
            provider.close()
            return SimpleNamespace(
                schema_registry_json='{"native":true}',
                schema_drifts_json="[]",
                conversion_timestamp="2026-07-11T00:00:00Z",
                field_names=("id",),
                native_registry_state="compiled-state",
            )

    monkeypatch.setattr(
        source_plan_remote_staging,
        "open_staged_remote_chunks",
        lambda _manifest, **_kwargs: _fake_staging(events, "probe"),
    )
    raw = source_plan_probe.probe_source_plan_registry(
        FakeRawContext(),
        _remote_plan(),
        "options",
        registry_json="{}",
        field_name_policy="lower_snake",
        schema_mode="additive",
    )

    assert raw.schema_registry_json == '{"native":true}'
    assert raw.native_registry_state == "compiled-state"
    assert len(native_chunks) == 2
    assert events == [
        "enter:probe",
        "close:probe-a",
        "close:probe-b",
        "exit:probe",
    ]


def test_remote_auto_provider_failure_closes_both_providers(monkeypatch) -> None:
    """A native opening failure releases active probe and stream staging contexts."""
    from schema_sanitizer.api_impl.source_plan import registry as source_plan_registry_stream
    from schema_sanitizer.api_impl.source_plan import remote as source_plan_remote_staging

    events: list[str] = []
    native_chunks: list[tuple[tuple[str, str, str], ...]] = []
    _patch_native_plan(monkeypatch, native_chunks)

    class FakeRawContext:
        """Current ABI context failing after opening both providers."""

        def to_registry_sink_path_source_chunk_provider_auto_registry(
            self,
            _sink,
            probe_provider,
            stream_provider,
            _call_options,
            **_options,
        ):
            """Open both providers before simulating native failure."""
            assert probe_provider.next_sources() is not None
            assert stream_provider.next_sources() is not None
            raise RuntimeError("native open failed")

    contexts = [
        _fake_staging(events, "probe"),
        _fake_staging(events, "stream"),
    ]
    monkeypatch.setattr(
        source_plan_remote_staging,
        "open_staged_remote_chunks",
        lambda _manifest, **_kwargs: contexts.pop(0),
    )

    with pytest.raises(RuntimeError, match="native open failed"):
        source_plan_registry_stream.open_source_plan_registry_stream(
            FakeRawContext(),
            _remote_plan(),
            "options",
            registry_json="{}",
            field_name_policy="lower_snake",
            schema_mode="additive",
            first_row_columns={},
            timestamp_columns=(),
        )

    assert sorted(event for event in events if event.startswith("close:")) == [
        "close:probe-a",
        "close:stream-a",
    ]
    assert "exit:probe" in events
    assert "exit:stream" in events


def test_remote_probe_prefix_resume_uses_exact_file_offset(monkeypatch) -> None:
    """Streaming reuses the probe prefix and resumes at the first unstaged file."""
    from schema_sanitizer.api_impl.source_plan import remote as remote_source_plan
    from schema_sanitizer.input_impl.prepared import NativeDirectorySourceManifest
    from schema_sanitizer.input_impl.source_plan import PreparedSourceBatch, SourceDescriptor

    opened_at: list[int] = []
    closed: list[str] = []
    native_chunks: list[tuple[tuple[str, str, str], ...]] = []
    _patch_native_plan(monkeypatch, native_chunks)
    manifest = _remote_plan().payload

    class FakeStage:
        """Own one staged source from the requested manifest suffix."""

        def __init__(self, source_index: int) -> None:
            """Initialize fake stage state for name and manifest."""
            source = manifest.files[source_index]
            self.name = source.name
            self.manifest = NativeDirectorySourceManifest(
                PreparedSourceBatch(
                    (
                        SourceDescriptor(
                            "json",
                            f"/tmp/{source.name}",
                            source.uri,
                        ),
                    ),
                    input_format="json",
                )
            )

        def close(self) -> None:
            """Close the fake stage and release its retained resources."""
            closed.append(self.name)

    class FakeStagedChunks:
        """Yield the exact manifest suffix requested by the provider."""

        def __init__(self, start: int) -> None:
            """Initialize fake staged chunks state for start."""
            self.start = start

        def __enter__(self):
            """Return the managed fake staged chunks value from context entry."""
            return iter(FakeStage(index) for index in range(self.start, len(manifest.files)))

        def __exit__(self, _exc_type, _exc, _tb) -> bool:
            """Finalize the fake staged chunks context without suppressing exceptions."""
            return False

    def open_chunks(_manifest, *, start=0):
        """Record the exact resume offset requested by each provider."""
        opened_at.append(start)
        return FakeStagedChunks(start)

    monkeypatch.setattr(remote_source_plan, "open_staged_remote_chunks", open_chunks)
    probe = remote_source_plan.RemotePathSourceChunkProvider(
        retained_chunks=[],
        remaining_manifest=manifest,
        retain_consumed_chunks=1,
    )
    stream = remote_source_plan.RemotePathSourceChunkProvider(
        retained_chunks=[],
        remaining_manifest=manifest,
        retained_chunk_donor=probe,
    )

    while probe.next_sources() is not None:
        pass
    probe.close()
    probe_chunks = tuple(native_chunks)
    native_chunks.clear()

    while stream.next_sources() is not None:
        pass
    stream.close()

    assert opened_at == [0, 1]
    assert [chunk[0][2] for chunk in probe_chunks] == [
        "s3://bucket/a.json",
        "s3://bucket/b.json",
    ]
    assert [chunk[0][2] for chunk in native_chunks] == [
        "s3://bucket/a.json",
        "s3://bucket/b.json",
    ]
    assert sorted(closed) == ["a.json", "b.json", "b.json"]


def test_remote_chunk_prefetch_iterator_stages_next_chunk_and_cleans_up() -> None:
    """Verify remote chunk prefetch iterator stages next chunk and cleans up."""
    from schema_sanitizer.api_impl.source_plan.remote import open_staged_remote_chunks

    class FakeStaged:
        """Fake staged chunk with cleanup tracking."""

        def __init__(self, start: int):
            """Initialize fake staged state for start and closed."""
            self.start = start
            self.closed = False

        def close(self) -> None:
            """Close the fake staged and update closed."""
            self.closed = True

    class FakeManifest:
        """Fake remote manifest with chunk staging hooks."""

        chunk_size = 1
        files = [object(), object()]
        input_format = "json"
        memory_limit_bytes = 1
        threading_mode = "single"

        def __init__(self) -> None:
            """Initialize fake manifest state for calls, staged, and second started."""
            self.calls: list[int] = []
            self.staged: dict[int, FakeStaged] = {}
            self.second_started = threading.Event()

        def stage_chunk(self, start: int) -> FakeStaged:
            """Record the requested chunk and return its tracked staged value."""
            self.calls.append(start)
            if start == 1:
                self.second_started.set()
            staged = FakeStaged(start)
            self.staged[start] = staged
            return staged

        @staticmethod
        def next_chunk_start(start: int) -> int:
            """Return the next configured remote chunk boundary."""
            return start + 1

    manifest = FakeManifest()
    with open_staged_remote_chunks(manifest) as chunks:
        first = next(chunks)
        assert first.start == 0
        assert manifest.second_started.wait(timeout=2.0)
        assert manifest.calls == [0, 1]
        assert manifest.staged[1].closed is False

    assert manifest.staged[0].closed is False
    assert manifest.staged[1].closed is True


def test_remote_json_directory_to_jsonl_uses_bounded_registry_staging(
    monkeypatch,
    tmp_path,
    require_native: None,
) -> None:
    """Verify remote JSON directory to JSONL uses bounded registry staging."""
    pytest.importorskip("pyarrow")
    from schema_sanitizer.remote_impl import staging as remote_staging
    from schema_sanitizer.remote_impl import sync_backend
    from schema_sanitizer.remote_impl.staging import StagedPath
    from schema_sanitizer.sources import RemoteFile

    staged_calls = []

    def fake_list_remote_directory_files(uri, suffixes, *, memory_limit_bytes=None):
        """Return two remote JSON children."""
        assert uri == "s3://bucket/partition/"
        assert suffixes == (".json",)
        return [
            RemoteFile("s3://bucket/partition/a.json", "a.json", 8),
            RemoteFile("s3://bucket/partition/b.json", "b.json", 8),
        ]

    def fake_stage_remote_files_to_directory(files, **kwargs):
        """Stage only the requested chunk into local child files."""
        assert isinstance(kwargs["memory_limit_bytes"], int)
        assert kwargs["memory_limit_bytes"] > 0
        staged_calls.append([file.name for file in files])
        staged_dir = tmp_path / f"remote-stage-{len(staged_calls)}"
        staged_dir.mkdir()
        for file in files:
            value = file.name.removesuffix(".json")
            (staged_dir / file.name).write_text(
                json.dumps({"id": value}) + "\n",
                encoding="utf-8",
            )
        return StagedPath(
            str(staged_dir),
            is_dir=True,
            source_file_by_name={file.name: file.uri for file in files},
        )

    monkeypatch.setattr(
        sync_backend,
        "list_remote_directory",
        fake_list_remote_directory_files,
    )
    monkeypatch.setattr(
        remote_staging,
        "stage_remote_files_to_directory",
        fake_stage_remote_files_to_directory,
    )

    out_path = tmp_path / "out.jsonl"
    ss.to_jsonl(
        "s3://bucket/partition/",
        out_path,
        input_format="json",
        input_mode="directory",
    )

    rows = [json.loads(line) for line in out_path.read_text(encoding="utf-8").splitlines()]
    assert [row["id"] for row in rows] == ["a", "b"]
    assert [row["source_file"] for row in rows] == [
        "s3://bucket/partition/a.json",
        "s3://bucket/partition/b.json",
    ]
    assert staged_calls == [["a.json", "b.json"]]


def test_remote_json_directory_to_pyarrow_uses_bounded_registry_staging(
    monkeypatch,
    tmp_path,
    require_native: None,
) -> None:
    """Verify remote JSON directory to PyArrow uses bounded registry staging."""
    pytest.importorskip("pyarrow")
    from schema_sanitizer.remote_impl import staging as remote_staging
    from schema_sanitizer.remote_impl import sync_backend
    from schema_sanitizer.remote_impl.staging import StagedPath
    from schema_sanitizer.sources import RemoteFile

    staged_calls = []

    def fake_list_remote_directory_files(uri, suffixes, *, memory_limit_bytes=None):
        """Return two remote JSON children."""
        assert uri == "s3://bucket/partition/"
        assert suffixes == (".json",)
        return [
            RemoteFile("s3://bucket/partition/a.json", "a.json", 8),
            RemoteFile("s3://bucket/partition/b.json", "b.json", 8),
        ]

    def fake_stage_remote_files_to_directory(files, **kwargs):
        """Stage each requested chunk into local child files."""
        assert isinstance(kwargs["memory_limit_bytes"], int)
        assert kwargs["memory_limit_bytes"] > 0
        staged_calls.append([file.name for file in files])
        staged_dir = tmp_path / f"remote-arrow-stage-{len(staged_calls)}"
        staged_dir.mkdir()
        for file in files:
            value = file.name.removesuffix(".json")
            (staged_dir / file.name).write_text(
                json.dumps({"id": value}) + "\n",
                encoding="utf-8",
            )
        return StagedPath(
            str(staged_dir),
            is_dir=True,
            source_file_by_name={file.name: file.uri for file in files},
        )

    monkeypatch.setattr(
        sync_backend,
        "list_remote_directory",
        fake_list_remote_directory_files,
    )
    monkeypatch.setattr(
        remote_staging,
        "stage_remote_files_to_directory",
        fake_stage_remote_files_to_directory,
    )

    result = ss.to_pyarrow(
        "s3://bucket/partition/",
        input_format="json",
        input_mode="directory",
    )

    rows = result.clean_data.to_pylist()
    assert [row["id"] for row in rows] == ["a", "b"]
    assert [row["source_file"] for row in rows] == [
        "s3://bucket/partition/a.json",
        "s3://bucket/partition/b.json",
    ]
    assert staged_calls == [["a.json", "b.json"]]


def test_remote_json_directory_to_jsonl_uses_bounded_staging_with_registry(
    monkeypatch,
    tmp_path,
    require_native: None,
) -> None:
    """Verify remote JSON directory to JSONL uses bounded staging with registry."""
    pytest.importorskip("pyarrow")
    from schema_sanitizer.api_impl.source_plan import registry as source_plan_registry_stream
    from schema_sanitizer.remote_impl import staging as remote_staging
    from schema_sanitizer.remote_impl import sync_backend
    from schema_sanitizer.remote_impl.staging import StagedPath
    from schema_sanitizer.sources import RemoteFile

    registry_seed = tmp_path / "registry-seed.json"
    registry_seed.write_text('{"id": "seed"}\n', encoding="utf-8")
    registry_json = ss.to_pyarrow(registry_seed, input_format="json").schema_registry_json
    staged_calls = []

    def fake_list_remote_directory_files(uri, suffixes, *, memory_limit_bytes=None):
        """Return two remote JSON children."""
        assert uri == "s3://bucket/partition/"
        assert suffixes == (".json",)
        return [
            RemoteFile("s3://bucket/partition/a.json", "a.json", 8),
            RemoteFile("s3://bucket/partition/b.json", "b.json", 8),
        ]

    def fake_stage_remote_files_to_directory(files, **kwargs):
        """Stage each requested remote chunk into local child files."""
        assert isinstance(kwargs["memory_limit_bytes"], int)
        assert kwargs["memory_limit_bytes"] > 0
        staged_calls.append([file.name for file in files])
        staged_dir = tmp_path / f"remote-single-pass-stage-{len(staged_calls)}"
        staged_dir.mkdir()
        for file in files:
            value = file.name.removesuffix(".json")
            (staged_dir / file.name).write_text(
                json.dumps({"id": value}) + "\n",
                encoding="utf-8",
            )
        return StagedPath(
            str(staged_dir),
            is_dir=True,
            source_file_by_name={file.name: file.uri for file in files},
        )

    registry_stream_calls = 0
    real_registry_stream = source_plan_registry_stream.open_source_plan_registry_stream

    def tracking_registry_stream(*args, **kwargs):
        """Record registry-stream use before delegating to the real implementation."""
        nonlocal registry_stream_calls
        registry_stream_calls += 1
        return real_registry_stream(*args, **kwargs)

    monkeypatch.setattr(
        sync_backend,
        "list_remote_directory",
        fake_list_remote_directory_files,
    )
    monkeypatch.setattr(
        remote_staging,
        "stage_remote_files_to_directory",
        fake_stage_remote_files_to_directory,
    )
    monkeypatch.setattr(
        source_plan_registry_stream,
        "open_source_plan_registry_stream",
        tracking_registry_stream,
    )

    out_path = tmp_path / "remote-single-pass.jsonl"
    ss.to_jsonl(
        "s3://bucket/partition/",
        out_path,
        input_format="json",
        input_mode="directory",
        schema_registry=registry_json,
    )

    rows = [json.loads(line) for line in out_path.read_text(encoding="utf-8").splitlines()]
    assert registry_stream_calls == 1
    assert [row["id"] for row in rows] == ["a", "b"]
    assert staged_calls == [["a.json", "b.json"]]


def test_source_plan_sequence_probe_flattens_path_sources_once() -> None:
    """Pure path-source sequences should use one native probe, not a Python merge loop."""
    from types import SimpleNamespace

    import schema_sanitizer.input_impl.source_plan as path_sources_impl
    import schema_sanitizer.input_impl.source_plan as source_plan_model
    from schema_sanitizer.api_impl.source_plan import probing as source_plan_probe
    from schema_sanitizer.input_impl.source_plan import (
        PreparedSourceBatch,
        SourceDescriptor,
        _native_path_source_plan,
    )

    monkeypatch = pytest.MonkeyPatch()
    monkeypatch.setattr(
        path_sources_impl, "PATH_SOURCE_PLAN_CREATE", lambda sources, *_args: tuple(sources)
    )
    calls: list[list[tuple[str, str, str]]] = []

    class FakeRawContext:
        """Raw context that captures one native path-source probe."""

        def registry_probe_path_sources_best_effort(
            self,
            sources,
            _call_options,
            *,
            registry_json,
            field_name_policy,
            schema_mode,
        ):
            """Return the best-effort source set captured by the registry probe."""
            calls.append(list(sources))
            assert registry_json == "{}"
            assert field_name_policy == "lower_snake"
            assert schema_mode == "additive"
            return SimpleNamespace(
                schema_registry_json='{"flattened":true}',
                schema_drifts_json="[]",
                conversion_timestamp="2026-07-05T00:00:00Z",
                field_names=("id",),
            )

    def child(path: str, source_file: str, route: str):
        """Build one child plan for the sequence fixture."""
        return _native_path_source_plan(
            source_batch=PreparedSourceBatch(
                (SourceDescriptor("json", path, source_file),),
                input_format="json",
            ),
            input_format="json",
            route_name=route,
        )

    plan = source_plan_model.NativeSourcePlan(
        kind=source_plan_model.SEQUENCE,
        payload=(
            child("/tmp/a.json", "gs://bucket/a.json", "child-a"),
            child("/tmp/b.json", "gs://bucket/b.json", "child-b"),
        ),
        input_format="json",
        route_name="sequence",
    )

    raw = source_plan_probe.probe_source_plan_registry(
        FakeRawContext(),
        plan,
        "options",
        registry_json="{}",
        field_name_policy="lower_snake",
        schema_mode="additive",
    )

    assert raw.schema_registry_json == '{"flattened":true}'
    assert calls == [
        [
            ("json", "/tmp/a.json", "gs://bucket/a.json"),
            ("json", "/tmp/b.json", "gs://bucket/b.json"),
        ]
    ]
    monkeypatch.undo()


def test_source_plan_plain_stream_uses_native_path_source_payload() -> None:
    """Plain stream source plans should pass the reusable native payload capsule."""
    import schema_sanitizer.input_impl.source_plan as source_plan_model
    from schema_sanitizer.api_impl import execution_context as execution_context_impl

    native_payload = object()
    captured_sources: list[object] = []

    class FakeRawContext:
        """Raw context that supports the native path-source capsule."""

        def to_sink_path_sources(
            self,
            _sink,
            sources,
            _call_options,
            *,
            include_source_file,
            first_row_columns,
            timestamp_columns,
        ):
            """Capture the native path-source capsule passed to the sink."""
            assert include_source_file is True
            assert first_row_columns == {}
            assert timestamp_columns == ()
            captured_sources.append(sources)
            return "raw-stream"

    from schema_sanitizer.input_impl.source_plan import PreparedSourceBatch, SourceDescriptor

    plan = source_plan_model.NativeSourcePlan(
        kind=source_plan_model.PATH_SOURCES,
        payload=None,
        input_format="json",
        route_name="native_manifest_paths",
        source_batch=PreparedSourceBatch(
            (SourceDescriptor("json", "/tmp/a.json", "gs://bucket/a.json"),),
            input_format="json",
        ),
        native_payload=native_payload,
    )

    raw = execution_context_impl._open_source_plan_sink_stream_or_none(
        FakeRawContext(),
        plan,
        "options",
        sink="stream",
        include_source_file=True,
    )

    assert raw == "raw-stream"
    assert captured_sources == [native_payload]


def test_remote_source_plan_stream_uses_native_chunk_provider(monkeypatch) -> None:
    """Remote source-plan streams should pull chunks lazily instead of flattening all chunks."""
    import schema_sanitizer.input_impl.source_plan as source_plan_model
    from schema_sanitizer.api_impl import execution_context as execution_context_impl
    from schema_sanitizer.api_impl.input.directory_preparation import (
        RemoteNativeDirectorySourceManifest,
    )
    from schema_sanitizer.api_impl.source_plan import remote as source_plan_remote_staging
    from schema_sanitizer.input_impl.prepared import NativeDirectorySourceManifest
    from schema_sanitizer.input_impl.source_plan import PreparedSourceBatch, SourceDescriptor
    from schema_sanitizer.sources import RemoteFile

    events: list[str] = []
    native_chunks: list[tuple[tuple[str, str, str], ...]] = []

    def fake_path_source_plan_create(sources, *_args):
        """Return a visible native-plan stand-in."""
        chunk = tuple(sources)
        native_chunks.append(chunk)
        return ("native-plan", chunk)

    import schema_sanitizer.input_impl.source_plan as path_sources_impl

    monkeypatch.setattr(
        path_sources_impl,
        "PATH_SOURCE_PLAN_CREATE",
        fake_path_source_plan_create,
    )

    class FakeStage:
        """Fake staged remote chunk."""

        def __init__(self, name: str) -> None:
            """Initialize fake stage state for name and manifest."""
            self.name = name
            self.manifest = NativeDirectorySourceManifest(
                PreparedSourceBatch(
                    (
                        SourceDescriptor(
                            "json",
                            f"/tmp/{name}.json",
                            f"s3://bucket/{name}.json",
                        ),
                    ),
                    input_format="json",
                )
            )

        def close(self) -> None:
            """Close the fake stage and release its retained resources."""
            events.append(f"close:{self.name}")

    class FakeStagedChunks:
        """Context manager yielding fake staged chunks."""

        def __init__(self, stages: list[FakeStage]) -> None:
            """Initialize fake staged chunks state for stages."""
            self._stages = stages

        def __enter__(self):
            """Return the managed fake staged chunks value from context entry."""
            events.append("enter")
            return iter(self._stages)

        def __exit__(self, _exc_type, _exc, _tb) -> bool:
            """Finalize the fake staged chunks context without suppressing exceptions."""
            events.append("exit")
            return False

        def __iter__(self):
            """Iterate over the configured values."""
            return iter(self._stages)

    class FakeRaw:
        """Fake raw sink that owns the provider like native does."""

        def __init__(self, provider) -> None:
            """Initialize fake raw state for provider."""
            self.provider = provider

        def close(self) -> None:
            """Close the fake raw and release its retained resources."""
            self.provider.close()
            events.append("raw-close")

    class FakeRawContext:
        """Raw context exposing the plain native chunk-provider sink."""

        def to_sink_path_source_chunk_provider(
            self,
            sink,
            provider,
            call_options,
            *,
            include_source_file,
            first_row_columns,
            timestamp_columns,
        ):
            """Validate the chunk-provider sink request and retain its provider."""
            assert sink == "stream"
            assert call_options == "options"
            assert include_source_file is True
            assert first_row_columns == {}
            assert timestamp_columns == ()
            events.append("provider-open")
            return FakeRaw(provider)

    stages = [FakeStage("a"), FakeStage("b")]
    plan = source_plan_model.NativeSourcePlan(
        kind=source_plan_model.REMOTE_CHUNKS,
        payload=RemoteNativeDirectorySourceManifest(
            [
                RemoteFile("s3://bucket/a.json", "a.json", None),
                RemoteFile("s3://bucket/b.json", "b.json", None),
            ],
            input_format="json",
            chunk_size=1,
        ),
        input_format="json",
        route_name="remote_native_manifest_chunks",
    )
    monkeypatch.setattr(
        source_plan_remote_staging,
        "open_staged_remote_chunks",
        lambda _manifest, **_kwargs: FakeStagedChunks(stages),
    )

    raw = execution_context_impl._open_source_plan_sink_stream_or_none(
        FakeRawContext(),
        plan,
        "options",
        sink="stream",
        include_source_file=True,
    )

    assert isinstance(raw, FakeRaw)
    assert events == ["provider-open"]
    assert raw.provider.next_sources() == (
        "native-plan",
        (("json", "/tmp/a.json", "s3://bucket/a.json"),),
    )
    assert events == ["provider-open", "enter"]
    assert raw.provider.next_sources() == (
        "native-plan",
        (("json", "/tmp/b.json", "s3://bucket/b.json"),),
    )
    assert events == ["provider-open", "enter", "close:a"]
    assert native_chunks == [
        (("json", "/tmp/a.json", "s3://bucket/a.json"),),
        (("json", "/tmp/b.json", "s3://bucket/b.json"),),
    ]
    raw.close()
    assert events == [
        "provider-open",
        "enter",
        "close:a",
        "close:b",
        "exit",
        "raw-close",
    ]


def test_remote_json_directory_preparation_allows_native_non_utf8_directory(
    monkeypatch,
) -> None:
    """Verify remote JSON directory preparation allows native non utf8 directory."""
    from schema_sanitizer.api_impl.input import preparation as public_input
    from schema_sanitizer.api_impl.source_plan import attached as source_plan_attached
    from schema_sanitizer.remote_impl import staging as remote_staging
    from schema_sanitizer.remote_impl import sync_backend
    from schema_sanitizer.sources import RemoteFile

    def fail_native_stage(*args, **kwargs):
        """Fail if remote directories stage eagerly during preparation."""
        raise AssertionError("remote directories should not stage during preparation")

    def fake_list_remote_directory(*_args, **_kwargs):
        """Return one remote child through the synchronous backend."""
        return (RemoteFile("s3://bucket/partition/row.json", "row.json"),)

    monkeypatch.setattr(
        sync_backend,
        "list_remote_directory",
        fake_list_remote_directory,
    )
    monkeypatch.setattr(
        remote_staging,
        "stage_remote_files_to_directory",
        fail_native_stage,
    )

    prepared = public_input.prepare_public_input(
        "s3://bucket/partition/",
        input_format="json",
        input_mode="directory",
        input_text_encoding="iso8859-1",
        xml_row_tag=None,
        csv_delimiter=",",
        csv_has_header=True,
        memory_limit_bytes=None,
    )

    manifest = source_plan_attached.remote_native_multisource_manifest_from_data(prepared.data)
    assert manifest is not None
    assert manifest.input_text_encoding == "iso8859-1"


def test_remote_directory_staging_respects_download_concurrency(monkeypatch) -> None:
    """Verify remote directory staging respects download concurrency."""
    from schema_sanitizer.remote_impl import directory_downloads as remote_downloads
    from schema_sanitizer.remote_impl import staging as remote_staging
    from schema_sanitizer.sources import RemoteFile

    active_downloads = 0
    max_active_downloads = 0
    full_window = asyncio.Event()

    files = [
        RemoteFile(f"s3://bucket/partition/{index}.jsonl", f"{index}.jsonl") for index in range(5)
    ]

    async def fake_client(files, *, memory_limit_bytes, threading_mode="single"):
        """Return a reusable fake provider client from bounded provider metadata."""
        assert len(files) == 1
        assert files[0].uri == "s3://bucket/partition/0.jsonl"
        assert memory_limit_bytes == 32 * 1024 * 1024
        return object()

    async def fake_close(client):
        """Accept closing the fake provider client."""
        assert client is not None

    async def fake_download(
        client,
        file,
        local_path,
        *,
        storage_reservation=None,
    ):
        """Write a payload while tracking active download count."""
        assert storage_reservation is not None
        nonlocal active_downloads, max_active_downloads
        assert client is not None
        active_downloads += 1
        max_active_downloads = max(max_active_downloads, active_downloads)
        if active_downloads == 2:
            full_window.set()
        try:
            await asyncio.wait_for(full_window.wait(), timeout=5)
            Path(local_path).write_text(f'{{"file":"{file.name}"}}\n', encoding="utf-8")
        finally:
            active_downloads -= 1

    monkeypatch.setattr(remote_downloads, "provider_client_for_downloads", fake_client)
    monkeypatch.setattr(remote_downloads, "close_provider_client", fake_close)
    monkeypatch.setattr(remote_downloads, "download_file_to_path", fake_download)

    staged = remote_staging.stage_remote_files_to_directory(
        files,
        memory_limit_bytes=32 * 1024 * 1024,
        threading_mode="multi",
    )
    try:
        assert max_active_downloads == 2
        root = Path(staged.path)
        assert [
            (root / f"{index}.jsonl").read_text(encoding="utf-8").strip() for index in range(5)
        ] == [
            '{"file":"0.jsonl"}',
            '{"file":"1.jsonl"}',
            '{"file":"2.jsonl"}',
            '{"file":"3.jsonl"}',
            '{"file":"4.jsonl"}',
        ]
    finally:
        staged.close()


def test_remote_directory_staging_does_not_retry_memory_limit_failure(monkeypatch) -> None:
    """Verify remote directory staging does not retry memory limit failure."""
    from contextlib import contextmanager

    from schema_sanitizer.errors import SchemaSanitizerResourceError
    from schema_sanitizer.remote_impl import staging as remote_staging
    from schema_sanitizer.remote_impl import sync_backend
    from schema_sanitizer.remote_impl.providers import s3_sync
    from schema_sanitizer.sources import RemoteFile

    downloads = 0

    files = [RemoteFile("s3://bucket/partition/row.jsonl", "row.jsonl", None)]

    @contextmanager
    def fake_client():
        """Yield one inert blocking S3 client."""
        yield object()

    def fake_download(
        _context,
        file,
        local_path,
        *,
        memory_limit_bytes,
        storage_reservation=None,
    ):
        """Write an oversized payload through the current streaming contract."""
        assert storage_reservation is not None
        nonlocal downloads
        assert memory_limit_bytes == 8
        assert file.name == "row.jsonl"
        downloads += 1
        Path(local_path).write_bytes(b'{"payload":"too large"}\n')

    monkeypatch.setattr(s3_sync, "open_client", fake_client)
    monkeypatch.setattr(sync_backend, "_download_with_context", fake_download)

    with pytest.raises(SchemaSanitizerResourceError, match="memory_limit_bytes"):
        remote_staging.stage_remote_files_to_directory(
            files,
            memory_limit_bytes=8,
        )
    assert downloads == 1
