"""Remote input staging tests, part one."""

# ruff: noqa: F405

from __future__ import annotations

from input_contract_shared import *  # noqa: F403


class _Stage:
    """Minimal staged-path stand-in that preserves its local file."""

    def __init__(self, path: Path):  # noqa: F405
        """Store the local staged path."""
        self.path = str(path)

    def close(self) -> None:
        """Keep the staged file available for test assertions."""
        pass


# Split from test_input_remote_staging.py: test_uri_input_uses_async_local_staging, test_uri_input_staging_works_with_converters, test_uri_input_allows_non_utf8_after_local_staging, ...


def test_uri_input_uses_async_local_staging(monkeypatch, tmp_path) -> None:
    """Verify URI inputs are staged to a replayable local file."""
    pytest.importorskip("pyarrow")
    require_native()

    staged_paths: list[str] = []

    def fake_stage(uri: str, *, memory_limit_bytes: int | None) -> _Stage:
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


def test_uri_input_staging_works_with_converters(monkeypatch, tmp_path) -> None:
    """Verify converter inputs stage remote files before native conversion."""
    pytest.importorskip("pyarrow")
    require_native()

    out = tmp_path / "out.jsonl"

    def fake_stage(uri: str, *, memory_limit_bytes: int | None) -> _Stage:
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


def test_uri_input_allows_non_utf8_after_local_staging(monkeypatch, tmp_path) -> None:
    """Verify URI text inputs can be transcoded after local staging."""
    require_native()

    def fake_stage(uri: str, *, memory_limit_bytes: int | None) -> _Stage:
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


def test_remote_parquet_directory_stages_children_concurrently(monkeypatch, tmp_path) -> None:
    """Verify remote Parquet directory staging downloads every listed child."""
    from schema_sanitizer.input_impl.directory_inputs import RemoteFile
    from schema_sanitizer.remote_impl import staging as remote_staging

    async def fake_list(uri, suffixes, *, memory_limit_bytes=None):
        """Return deterministic remote Parquet children."""
        assert uri == "s3://bucket/partition/"
        assert ".parquet" in suffixes
        return [
            RemoteFile("s3://bucket/partition/a.parquet", "a.parquet", None),
            RemoteFile("s3://bucket/partition/b.parquet", "b.parquet", None),
        ]

    async def fake_client(files, *, memory_limit_bytes=None):
        """Return a reusable fake provider client."""
        assert len(files) == 2
        return object()

    async def fake_close(client):
        """Accept closing the fake provider client."""
        assert client is not None

    async def fake_download(client, file, local_path):
        """Write a staged payload for one remote child."""
        assert client is not None
        Path(local_path).write_bytes(file.name.encode("utf-8"))

    monkeypatch.setattr(remote_staging.routing, "list_remote_directory", fake_list)
    monkeypatch.setattr(remote_staging, "provider_client_for_downloads", fake_client)
    monkeypatch.setattr(remote_staging, "close_provider_client", fake_close)
    monkeypatch.setattr(remote_staging, "download_file_to_path", fake_download)

    staged = remote_staging.stage_remote_parquet_directory(
        "s3://bucket/partition/",
        suffixes=(".parquet", ".pq"),
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
) -> None:
    """Verify remote Parquet directories stage locally and preserve source URIs."""
    pa = pytest.importorskip("pyarrow")
    pq = pytest.importorskip("pyarrow.parquet")
    from schema_sanitizer.remote_impl import staging as remote_staging
    from schema_sanitizer.remote_impl.staging import StagedPath

    require_native()

    def fake_stage_remote_parquet_directory(uri, *, suffixes, memory_limit_bytes):
        """Return a local staged Parquet directory for a remote URI."""
        assert uri == "s3://bucket/partition/"
        assert suffixes == (".parquet", ".pq")
        assert memory_limit_bytes is None
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
) -> None:
    """Verify remote Parquet single files stage locally and preserve source URI."""
    pa = pytest.importorskip("pyarrow")
    pq = pytest.importorskip("pyarrow.parquet")
    from schema_sanitizer.remote_impl import staging as remote_staging
    from schema_sanitizer.remote_impl.staging import StagedPath

    require_native()

    def fake_stage_remote_single_file(uri, *, memory_limit_bytes):
        """Return a local staged Parquet file for a remote URI."""
        assert uri == "s3://bucket/events.parquet"
        assert memory_limit_bytes is None
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


def test_remote_parquet_single_file_writer_uses_staged_arrow_path(
    monkeypatch,
    tmp_path,
) -> None:
    """Verify remote Parquet single-file writes preserve staged source URI spans."""
    pa = pytest.importorskip("pyarrow")
    pq = pytest.importorskip("pyarrow.parquet")
    from schema_sanitizer.remote_impl import staging as remote_staging
    from schema_sanitizer.remote_impl.staging import StagedPath

    require_native()

    staged_file = tmp_path / "events.parquet"
    pq.write_table(pa.table({"id": [1, 2]}), staged_file)

    def fake_stage_remote_single_file(uri, *, memory_limit_bytes):
        """Return a local staged Parquet file for a remote URI."""
        assert uri == "s3://bucket/events.parquet"
        assert memory_limit_bytes is None
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


def test_remote_text_directory_stages_child_sources_concurrently(monkeypatch) -> None:
    """Verify remote text directory staging preserves child files and source URIs."""
    from schema_sanitizer.input_impl.directory_inputs import RemoteFile
    from schema_sanitizer.remote_impl import staging as remote_staging

    files = [
        RemoteFile("s3://bucket/partition/a.jsonl", "a.jsonl", None),
        RemoteFile("s3://bucket/partition/b.jsonl", "b.jsonl", None),
    ]

    async def fake_client(files, *, memory_limit_bytes=None):
        """Return a reusable fake provider client."""
        assert len(files) == 2
        return object()

    async def fake_close(client):
        """Accept closing the fake provider client."""
        assert client is not None

    async def fake_download(client, file, local_path):
        """Write a staged payload for one remote child."""
        assert client is not None
        Path(local_path).write_bytes(file.uri.encode("utf-8"))

    monkeypatch.setattr(remote_staging, "provider_client_for_downloads", fake_client)
    monkeypatch.setattr(remote_staging, "close_provider_client", fake_close)
    monkeypatch.setattr(remote_staging, "download_file_to_path", fake_download)

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
