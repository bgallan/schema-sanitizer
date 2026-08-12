"""Ownership and layout contracts for remote and cloud backends."""

from __future__ import annotations

import asyncio
import tomllib
from pathlib import Path
from typing import Any

from schema_sanitizer.core_impl.uris import location_kind, looks_like_supported_uri, remote_provider
from schema_sanitizer.input_impl.directory_inputs import (
    DirectoryDiscoveryBuilder,
    split_parent_child,
)
from schema_sanitizer.input_impl.selection import looks_like_uri_string
from schema_sanitizer.sources import RemoteFile

ROOT = Path(__file__).resolve().parents[2]

SRC = ROOT / "src/schema_sanitizer"


def test_arrow_provider_chunk_flow_has_one_visible_owner() -> None:
    """Small provider phases stay visible in the real compilation unit."""
    registry = ROOT / "cpp/src/api/python_abi3/registry"
    provider = registry / "arrow_source_provider.cc"
    runtime = registry / "arrow_source_sinks.cc"
    assert provider.is_file() and runtime.is_file()
    assert not (registry / "arrow_source_sinks/provider_chunks").exists()
    provider_text = provider.read_text(encoding="utf-8")
    runtime_text = runtime.read_text(encoding="utf-8")
    assert "merge_arrow_source_provider_schemas" in provider_text
    for symbol in (
        "finish_opened_source_metadata",
        "try_open_passthrough_arrow_source",
        "ingest_arrow_source_with_registry_plan",
    ):
        assert symbol in runtime_text
    assert len(provider_text.splitlines()) <= 500
    assert len(runtime_text.splitlines()) <= 500


def test_azure_directory_downloads_reuse_one_service(monkeypatch, tmp_path) -> None:
    """A staged Azure directory must not create one SDK client per child."""
    from schema_sanitizer.remote_impl import directory_downloads
    from schema_sanitizer.remote_impl.providers import azure
    from schema_sanitizer.sources import RemoteFile

    opened: list[str] = []
    downloaded: list[tuple[str, str]] = []
    requested_concurrency: list[int] = []

    class FakeStream:
        """Minimal Azure download stream."""

        async def chunks(self):
            """Yield one deterministic chunk."""
            yield b"payload"

    class FakeBlob:
        """Minimal Azure blob client."""

        def __init__(self, container: str, blob: str):
            """Store the selected Azure container and object name."""
            self.container = container
            self.blob = blob

        async def download_blob(self, *, max_concurrency: int = 1) -> FakeStream:
            """Record the object selected through the shared service."""
            requested_concurrency.append(max_concurrency)
            downloaded.append((self.container, self.blob))
            return FakeStream()

    class FakeService:
        """Reusable Azure service stand-in."""

        closed = False

        def get_blob_client(self, container: str, blob: str) -> FakeBlob:
            """Return a blob client without opening another service."""
            return FakeBlob(container, blob)

        async def close(self) -> None:
            """Record service shutdown."""
            self.closed = True

    service = FakeService()

    async def fake_open_service(ref: Any) -> FakeService:
        """Return one service for the complete directory batch."""
        opened.append(ref.account_url)
        return service

    monkeypatch.setattr(azure, "open_service", fake_open_service)
    files = [
        RemoteFile("https://acct.blob.core.windows.net/container/a.parquet", "a.parquet"),
        RemoteFile("https://acct.blob.core.windows.net/container/b.parquet", "b.parquet"),
    ]

    async def exercise() -> None:
        """Download both files through one opened provider context."""
        context = await directory_downloads.provider_client_for_downloads(files)
        assert context is not None
        for file in files:
            await directory_downloads.download_file_to_path(
                context, file, str(tmp_path / file.name)
            )
        await directory_downloads.close_provider_client(context)

    asyncio.run(exercise())
    assert opened == ["https://acct.blob.core.windows.net"]
    assert downloaded == [("container", "a.parquet"), ("container", "b.parquet")]
    assert requested_concurrency == [1, 1]
    assert service.closed is True
    assert (tmp_path / "a.parquet").read_bytes() == b"payload"
    assert (tmp_path / "b.parquet").read_bytes() == b"payload"


def test_bulk_discovery_reuses_unique_uri_classifications(monkeypatch) -> None:
    """Directory grouping consumes preclassified locations without reparsing."""
    import schema_sanitizer.pipeline.source_discovery as source_discovery

    source_locations = {
        "https://example.test/a.json": "http",
        "https://example.test/b.json": "http",
        "https://example.test/c.json": "http",
    }

    def fail_classification(_uri: str):
        """Fail if grouped discovery reparses a location."""
        raise AssertionError("location classification must be reused")

    monkeypatch.setattr(source_discovery, "location_kind", fail_classification)
    checked = asyncio.run(
        source_discovery._discover_directories(
            source_locations,
            extensions=("json",),
            input_format="json",
            exists_by_uri={},
            discovered_by_uri={},
        )
    )
    assert checked == set()


def test_bulk_remote_discovery_avoids_requested_child_set_copies() -> None:
    """Provider scans perform one child lookup instead of building a set per group."""
    owners = (
        ROOT / "src/schema_sanitizer/remote_impl/providers/gcs.py",
        ROOT / "src/schema_sanitizer/remote_impl/providers/s3.py",
        ROOT / "src/schema_sanitizer/remote_impl/providers/azure.py",
    )
    for owner in owners:
        source = owner.read_text(encoding="utf-8")
        assert "requested_children = set(children)" not in source
        assert "children.get(child)" in source


def test_cloud_extra_declares_direct_blocking_s3_dependency() -> None:
    """The sync S3 owner may import Botocore without relying on transitive luck."""
    pyproject = tomllib.loads((ROOT / "pyproject.toml").read_text(encoding="utf-8"))
    extras = pyproject["project"]["optional-dependencies"]
    assert "botocore>=1.34" in extras["cloud"]
    assert "botocore>=1.34" in extras["all"]


def test_cloud_providers_have_one_bounded_backend_owner() -> None:
    """Each cloud backend keeps discovery and transfer in one bounded module."""
    providers = ROOT / "src/schema_sanitizer/remote_impl/providers"
    provider_limits = {"gcs": 550, "s3": 500, "azure": 650}
    for name, line_limit in provider_limits.items():
        owner = providers / f"{name}.py"
        assert owner.is_file()
        assert not (providers / name).exists()
        source = owner.read_text(encoding="utf-8")
        assert "async def directories_containing_files" in source
        assert "async def file_exists" in source
        assert len(source.splitlines()) <= line_limit
    gcs_source = (providers / "gcs.py").read_text(encoding="utf-8")
    gcs_objects = providers / "gcs_objects.py"
    object_source = gcs_objects.read_text(encoding="utf-8")
    assert gcs_objects.is_file()
    assert "from .gcs_objects import" in gcs_source
    assert "def parse_uri" in object_source
    assert "def remote_file_from_metadata" in object_source
    assert len(object_source.splitlines()) <= 500
    for name in ("s3", "azure"):
        source = (providers / f"{name}.py").read_text(encoding="utf-8")
        assert "def parse_uri" in source


def test_coalescing_stream_matches_its_real_translation_unit() -> None:
    """Coalescing must remain one visible owner without hidden include fragments."""
    streaming = ROOT / "cpp/src/api/python_abi3/streaming"
    owners = tuple(
        (
            streaming / name
            for name in (
                "coalesce_stream.cc",
                "coalesce_schema.cc",
                "coalesce_append.cc",
                "coalesce_export.cc",
                "coalesce_stream_internal.hh",
            )
        )
    )
    assert all((owner.is_file() for owner in owners))
    assert not (streaming / "coalesce_stream").exists()
    assert all((len(owner.read_text(encoding="utf-8").splitlines()) <= 500 for owner in owners))
    assert not list(streaming.rglob("coalesce_stream*.inc"))
    source = "\n".join((owner.read_text(encoding="utf-8") for owner in owners))
    assert "std::vector<std::unique_ptr<sanitize::CArrayGuard>>" not in source
    assert "SAN_RETURN_NOT_OK(append_node(" in source


def test_coalescing_stream_releases_each_input_batch_after_copy() -> None:
    """Native coalescing should not retain or heap-box every source batch."""
    streaming = ROOT / "cpp/src/api/python_abi3/streaming"
    runtime = (streaming / "coalesce_stream.cc").read_text(encoding="utf-8")
    append = (streaming / "coalesce_append.cc").read_text(encoding="utf-8")
    schema = (streaming / "coalesce_schema.cc").read_text(encoding="utf-8")
    source = "\n".join((runtime, append, schema))
    assert all((len(part.splitlines()) <= 500 for part in (runtime, append, schema)))
    assert "sanitize::CArrayGuard batch;" in runtime
    assert "std::vector<std::unique_ptr<sanitize::CArrayGuard>>" not in source
    assert "build_coalesced_array_state" not in source
    assert "integer_width_for_format" in schema
    assert "std::find(kInteger8Formats.cbegin(), kInteger8Formats.cend(), format)" in schema
    assert "std::ranges::contains" not in schema
    assert not (streaming / "coalesce_stream").exists()


def test_dictionary_and_provider_protocols_are_not_monolithic() -> None:
    """Dictionary pages and Python providers remain split by operation."""
    pages = ROOT / "cpp/src/internal/parquet/footer_reader/pages"
    assert (pages / "footer_reader_dictionary_indices.cc.inc").is_file()
    assert (pages / "footer_reader_dictionary_page.cc.inc").is_file()
    assert not (pages / "footer_reader_dictionary_index_pages.cc.inc").exists()
    registry = ROOT / "cpp/src/api/python_abi3/registry"
    providers = registry / "arrow_source_provider.cc"
    source = providers.read_text(encoding="utf-8")
    for symbol in (
        "close_arrow_chunk_provider",
        "parse_arrow_sources",
        "load_next_arrow_provider_chunk",
    ):
        assert symbol in source
    assert len(source.splitlines()) <= 500
    assert not (registry / "arrow_source_sinks").exists()


def test_http_transport_has_one_owner() -> None:
    """HTTP session primitives and object operations share one transport owner."""
    owner = SRC / "remote_impl/transport.py"
    source = owner.read_text(encoding="utf-8")
    for symbol in ("download_http_file", "http_file_exists", "upload_http_file"):
        assert f"def {symbol}" in source
    assert not (SRC / "remote_impl/providers/http.py").exists()
    assert len(source.splitlines()) <= 700


def test_ingest_and_remote_source_plan_are_direct_modules() -> None:
    """Small orchestration domains must not regress into pass-through packages."""
    owners = (
        ROOT / "src/schema_sanitizer/api_impl/ingest.py",
        ROOT / "src/schema_sanitizer/api_impl/source_plan/remote.py",
    )
    owner_limits = (750, 950)
    for owner, limit in zip(owners, owner_limits, strict=True):
        assert owner.is_file()
        assert not owner.with_suffix("").exists()
        assert len(owner.read_text(encoding="utf-8").splitlines()) <= limit


def test_remote_chunk_prefetch_is_owned_by_the_remote_source_plan() -> None:
    """Remote plan lifecycle must not be split across the generic input package."""
    owner = SRC / "api_impl/source_plan/remote.py"
    source = owner.read_text(encoding="utf-8")
    assert "class RemoteChunkPrefetchIterator" in source
    assert "class RemotePathSourceChunkProvider" in source
    assert "deque[Future[Any]]" in source
    assert "RemoteIoCoordinator" in source
    assert "ThreadPoolExecutor" not in source
    assert ".popleft()" in source
    assert len(source.splitlines()) <= 950
    assert not (SRC / "api_impl/input/remote_chunks.py").exists()


def test_remote_chunk_provider_has_one_cohesive_owner() -> None:
    """The small provider and staging gateway should not be a micro-package."""
    owner = ROOT / "src/schema_sanitizer/api_impl/source_plan/remote.py"
    assert owner.is_file()
    assert not owner.with_suffix("").exists()
    text = owner.read_text(encoding="utf-8")
    assert "class RemotePathSourceChunkProvider" in text
    assert "def open_staged_remote_chunks" in text
    assert "deque(" in text and ".popleft()" in text
    assert "remaining_remote_manifest" not in text
    assert len(text.splitlines()) <= 950


def test_remote_directory_discovery_builder_sorts_once_at_finalization() -> None:
    """The shared accumulator preserves keys and returns deterministic file order."""
    builder = DirectoryDiscoveryBuilder[RemoteFile].from_uris(("gs://bucket/b", "gs://bucket/a"))
    builder.add(("gs://bucket/b",), RemoteFile("gs://bucket/b/z.parquet", "z.parquet", 2))
    builder.add(("gs://bucket/b",), RemoteFile("gs://bucket/b/a.parquet", "a.parquet", 1))
    result = builder.finish()
    assert result.exists_by_uri == {"gs://bucket/b": True, "gs://bucket/a": False}
    assert [file.name for file in result.files_by_uri["gs://bucket/b"]] == [
        "a.parquet",
        "z.parquet",
    ]
    assert split_parent_child("year=2026/month=07/") == ("year=2026", "month=07")


def test_remote_directory_discovery_has_one_accumulator_owner() -> None:
    """All cloud providers must share grouping output and deterministic finalization."""
    owner = SRC / "input_impl/directory_inputs.py"
    text = owner.read_text(encoding="utf-8")
    assert "class DirectoryDiscoveryBuilder" in text
    assert "def split_parent_child" in text
    assert len(text.splitlines()) <= 500
    provider_limits = {"azure.py": 650, "gcs.py": 550, "s3.py": 500}
    for name, line_limit in provider_limits.items():
        provider = SRC / "remote_impl/providers" / name
        source = provider.read_text(encoding="utf-8")
        assert "DirectoryDiscoveryBuilder[RemoteFile].from_uris(" in source
        assert "metadata_budget = current_directory_metadata_budget(memory_limit_bytes)" in source
        assert "metadata_budget=metadata_budget" in source
        assert "discovery.add(child_uris, remote_file)" in source
        assert "return discovery.finish()" in source
        assert "def _parent_child" not in source
        assert "exists_by_uri = dict.fromkeys" not in source
        assert "for files in files_by_uri.values()" not in source
        assert len(source.splitlines()) <= line_limit


def test_remote_prefetch_uses_constant_time_queue_removal() -> None:
    """Completion-order prefetch must not shift a Python list per result."""
    source = (SRC / "api_impl/source_plan/remote.py").read_text(encoding="utf-8")
    assert "from collections import deque" in source
    assert "deque[Future[Any]]" in source
    assert ".popleft()" in source
    assert ".pop(0)" not in source


def test_remote_provider_and_native_probe_avoid_linear_chunk_copies() -> None:
    """Remote chunk iteration and capsule parsing should use constant-time ownership paths."""
    provider = (
        ROOT / "src/schema_sanitizer/api_impl/source_plan/remote_runtime/provider.py"
    ).read_text(encoding="utf-8")
    probe = (ROOT / "cpp/src/api/python_abi3/probes/schema_probe.cc").read_text(encoding="utf-8")
    assert "deque(retained_chunks)" in provider
    assert "self._retained_chunks.popleft()" in provider
    assert "remaining_remote_manifest" not in provider
    assert "parse_path_sources_view(chunk_sources, &parsed_sources)" in probe
    assert "merge_path_source_schemas(\n        ctx, parsed_sources.get()" in probe


def test_remote_staging_and_directory_downloads_are_bounded_owners() -> None:
    """Temporary paths and shared directory transfers have cohesive owners."""
    remote = ROOT / "src/schema_sanitizer/remote_impl"
    staging = remote / "staging.py"
    downloads = remote / "directory_downloads.py"
    assert staging.is_file()
    assert downloads.is_file()
    assert not (remote / "staging").exists()
    assert len(staging.read_text(encoding="utf-8").splitlines()) <= 500
    source = downloads.read_text(encoding="utf-8")
    assert len(source.splitlines()) <= 500
    assert "class DownloadContext" in source
    assert "class RemoteDirectoryDownloadSession" in source
    assert "download_file_bytes" not in source
    bulk = source[source.index("async def _download_files_with_context") :]
    assert "remote_provider(file.uri)" not in bulk
    assert "await download_file_to_path(context, file" in bulk


def test_remote_staging_value_objects_have_one_owner() -> None:
    """Temporary path and output lifecycle must stay with remote staging."""
    owner = SRC / "remote_impl/staging_paths.py"
    source = owner.read_text(encoding="utf-8")
    assert "class StagedPath" in source
    assert "class RemoteOutputTarget" in source
    assert "quarantine_temporary_artifact" in source
    assert len(source.splitlines()) <= 600
    facade = (SRC / "remote_impl/staging.py").read_text(encoding="utf-8")
    assert "from .staging_paths import" in facade
    assert len(facade.splitlines()) <= 500
    assert not (SRC / "remote_impl/types.py").exists()
    production = "\n".join((path.read_text(encoding="utf-8") for path in SRC.rglob("*.py")))
    assert "remote_impl.types" not in production


def test_remote_uri_provider_has_one_canonical_owner() -> None:
    """Supported schemes and Azure endpoint recognition share one classifier."""
    assert remote_provider("gs://bucket/path") == "gcs"
    assert remote_provider("gcs://bucket/path") == "gcs"
    assert remote_provider("s3://bucket/path") == "s3"
    assert remote_provider("abfss://container@account.dfs.core.windows.net/path") == "azure"
    assert remote_provider("https://account.blob.core.windows.net/container/path") == "azure"
    assert remote_provider("https://example.test/data.json") == "http"
    assert remote_provider("hdfs://cluster/path") is None
    assert looks_like_uri_string("file:///tmp/data.json")
    assert looks_like_uri_string("wasbs://container@account.blob.core.windows.net/path")
    assert not looks_like_uri_string("hdfs://cluster/path")
    paths = (ROOT / "src/schema_sanitizer/pipeline/source_discovery.py").read_text(encoding="utf-8")
    selection = (ROOT / "src/schema_sanitizer/input_impl/selection.py").read_text(encoding="utf-8")
    routing = (ROOT / "src/schema_sanitizer/remote_impl/routing.py").read_text(encoding="utf-8")
    uris = (ROOT / "src/schema_sanitizer/core_impl/uris.py").read_text(encoding="utf-8")
    assert "REMOTE_SCHEMES" not in uris
    assert "_GCS_SCHEMES" not in paths
    assert "_AZURE_SCHEMES" not in paths
    assert "def is_gcs_uri" not in paths
    assert "_URI_SCHEMES" not in selection
    assert "remote_provider(uri)" in routing
    assert "def is_azure_blob_uri" not in routing


def test_single_remote_backend_has_blocking_provider_owners() -> None:
    """Single-mode providers stay explicit, bounded, and free of async escapes."""
    owners = (
        SRC / "remote_impl/sync_backend.py",
        SRC / "remote_impl/sync_http.py",
        SRC / "remote_impl/gcs_sync_resumable.py",
        SRC / "remote_impl/providers/gcs_sync.py",
        SRC / "remote_impl/providers/s3_sync.py",
        SRC / "remote_impl/providers/azure_sync.py",
        SRC / "pipeline/source_discovery_sync.py",
    )
    forbidden = ("import asyncio", "aiohttp", "aiobotocore", "ThreadPoolExecutor", "run_sync(")
    for owner in owners:
        source = owner.read_text(encoding="utf-8")
        assert len(source.splitlines()) <= 500
        for token in forbidden:
            assert token not in source


def test_single_remote_dispatch_cannot_submit_a_coroutine() -> None:
    """The operation boundary keeps blocking and coroutine backends disjoint."""
    context = (SRC / "api_impl/operation_context.py").read_text(encoding="utf-8")
    staging = (SRC / "remote_impl/staging.py").read_text(encoding="utf-8")
    discovery = (SRC / "pipeline/source_discovery.py").read_text(encoding="utf-8")
    assert "strict single-mode remote work must use run_remote_sync()" in context
    assert "def run_remote_sync" in context
    assert "sync_backend.remote_file_metadata" in staging
    assert "sync_backend.download_single_file" in staging
    assert "sync_backend.upload_file" in staging
    assert "discover_existing_source_plans_sync" in discovery
    assert len(staging.splitlines()) <= 500


def test_source_discovery_classifies_each_unique_uri_once() -> None:
    """Discovery carries location kinds instead of reparsing during grouping."""
    owner = (ROOT / "src/schema_sanitizer/pipeline/source_discovery.py").read_text(encoding="utf-8")
    unique = owner[
        owner.index("def _unique_source_locations") : owner.index("\ndef _partition_plans")
    ]
    grouped = owner[
        owner.index("async def _discover_directories") : owner.index("\nasync def _discover_source")
    ]
    assert unique.count("location_kind(source_uri)") == 1
    assert "dict[str, LocationKind]" in unique
    assert "remote_provider(uri)" not in grouped
    assert "for uri, kind in source_locations.items()" in grouped


def test_uri_classification_has_one_core_owner() -> None:
    """Local paths, file URIs, and remote providers share one parser and owner."""
    package = ROOT / "src/schema_sanitizer"
    owner = package / "core_impl/uris.py"
    retired = (package / "core_impl/path_uris.py", package / "remote_impl/uris.py")
    assert owner.is_file()
    assert all((not path.exists() for path in retired))
    assert location_kind("C:\\data\\rows.json") == "path"
    assert location_kind("file:///tmp/rows.json") == "file"
    assert location_kind("gs://bucket/rows.json") == "gcs"
    assert remote_provider("https://account.blob.core.windows.net/container/rows") == "azure"
    assert remote_provider("https://example.test/rows") == "http"
    assert not looks_like_supported_uri("hdfs://cluster/rows")
    production = "\n".join(
        (path.read_text(encoding="utf-8") for path in package.rglob("*.py") if path != owner)
    )
    assert "core_impl.path_uris" not in production
    assert "remote_impl.uris" not in production
