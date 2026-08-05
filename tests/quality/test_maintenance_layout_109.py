"""Maintenance contracts for layout 109 staging and recursive-tree cleanup."""

from __future__ import annotations

import asyncio
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[2]
SRC = ROOT / "src/schema_sanitizer"
SCHEMA = ROOT / "cpp/src/internal/parquet/footer_reader/native_stream/schema"


def test_remote_staging_value_objects_have_one_owner() -> None:
    """Temporary path and output lifecycle must stay with remote staging."""
    owner = SRC / "remote_impl/staging_paths.py"
    source = owner.read_text(encoding="utf-8")
    assert "class StagedPath" in source
    assert "class RemoteOutputTarget" in source
    assert "quarantine_temporary_artifact" in source
    assert len(source.splitlines()) <= 500

    facade = (SRC / "remote_impl/staging.py").read_text(encoding="utf-8")
    assert "from .staging_paths import" in facade
    assert len(facade.splitlines()) <= 500
    assert not (SRC / "remote_impl/types.py").exists()

    production = "\n".join(path.read_text(encoding="utf-8") for path in SRC.rglob("*.py"))
    assert "remote_impl.types" not in production


def test_azure_directory_downloads_reuse_one_service(monkeypatch, tmp_path) -> None:
    """A staged Azure directory must not create one SDK client per child."""
    from schema_sanitizer.input_impl.directory_inputs import RemoteFile
    from schema_sanitizer.remote_impl import directory_downloads
    from schema_sanitizer.remote_impl.providers import azure

    opened: list[str] = []
    downloaded: list[tuple[str, str]] = []

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

        async def download_blob(self) -> FakeStream:
            """Record the object selected through the shared service."""
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
        RemoteFile(
            "https://acct.blob.core.windows.net/container/a.parquet",
            "a.parquet",
        ),
        RemoteFile(
            "https://acct.blob.core.windows.net/container/b.parquet",
            "b.parquet",
        ),
    ]

    async def exercise() -> None:
        """Download both files through one opened provider context."""
        context = await directory_downloads.provider_client_for_downloads(files)
        assert context is not None
        for file in files:
            await directory_downloads.download_file_to_path(
                context,
                file,
                str(tmp_path / file.name),
            )
        await directory_downloads.close_provider_client(context)

    asyncio.run(exercise())
    assert opened == ["https://acct.blob.core.windows.net"]
    assert downloaded == [("container", "a.parquet"), ("container", "b.parquet")]
    assert service.closed is True
    assert (tmp_path / "a.parquet").read_bytes() == b"payload"
    assert (tmp_path / "b.parquet").read_bytes() == b"payload"


def test_recursive_parquet_tree_operations_are_iterative_and_bounded() -> None:
    """Path build, annotation, clone, and merge must not recurse by depth."""
    owner = SCHEMA / "native_stream_recursive_tree.cc.inc"
    source = owner.read_text(encoding="utf-8")
    assert owner.is_file()
    assert len(source.splitlines()) <= 500
    assert source.count("std::views::reverse") >= 4
    assert "pending.reserve(" in source
    assert "destination->nodes.reserve(" in source

    for retired in (
        "native_stream_schema_recursive_build.cc.inc",
        "native_stream_recursive_tree_merge.cc.inc",
    ):
        assert not (SCHEMA / retired).exists()

    for function_name in (
        "assign_native_recursive_repeated_layout_indexes",
        "clone_native_recursive_materialization_subtree",
        "merge_native_recursive_materialization_node",
    ):
        definition = source.index(f"{function_name}(")
        body_start = source.index("{", definition)
        next_definition = source.find("\n}\n\n", body_start)
        body = source[body_start:next_definition]
        assert body.count(f"{function_name}(") == 0

    entry = (SCHEMA.parents[1] / "footer_reader.cc").read_text(encoding="utf-8")
    assert "native_stream/schema/native_stream_recursive_tree.cc.inc" in entry
    assert "native_stream_schema_recursive_build.cc.inc" not in entry
    assert "native_stream_recursive_tree_merge.cc.inc" not in entry


def test_recursive_output_layout_defers_counts_and_avoids_tree_copies() -> None:
    """Wide schemas must not copy and recount the merged tree for every leaf."""
    layout = (SCHEMA / "native_stream_output_layout.cc.inc").read_text(encoding="utf-8")
    validation = layout
    assert "auto merged_tree = field.recursive_tree" not in layout
    assert "count_native_recursive_materialization_resources" not in layout
    assert "recursive_tree, &field.recursive_tree" in layout
    assert "finalize_native_output_layout" in validation
    assert "plan_native_recursive_materialization_tree(" in validation
    assert "recursive_subtree_counts" in validation
    assert validation.count("return finalize_native_output_layout(") == 1
    assert "plan_native_recursive_layout_columns" in validation
    assert "SAN_RETURN_NOT_OK(finalize_native_output_layout" in validation
