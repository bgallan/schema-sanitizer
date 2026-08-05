"""Maintenance contracts for the layout-108 ownership and writer cleanup."""

from __future__ import annotations

from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src/schema_sanitizer"
WRITER = ROOT / "cpp/src/internal/parquet/stream_writer"


def test_async_scheduler_has_a_neutral_core_owner() -> None:
    """Generic async planning must not live below the remote transport package."""
    owner = SRC / "core_impl/async_scheduler.py"
    source = owner.read_text(encoding="utf-8")
    assert owner.is_file()
    assert not (SRC / "remote_impl/scheduler.py").exists()
    assert "async def unordered_indexed_results" in source
    assert "async def ordered_indexed_results" in source
    assert "DirectoryDownloadTuning" not in source

    pipeline = (SRC / "pipeline/source_discovery.py").read_text(encoding="utf-8")
    assert "core_impl.async_scheduler" in pipeline
    assert "remote_impl.scheduler" not in pipeline

    for relative in (
        "api_impl/source_plan/remote.py",
        "api_impl/parquet/arrow_sources.py",
    ):
        consumer = (SRC / relative).read_text(encoding="utf-8")
        assert "core_impl.execution_policy" in consumer
        assert "core_impl.async_scheduler" not in consumer
        assert "remote_impl.scheduler" not in consumer


def test_directory_discovery_has_one_input_model() -> None:
    """Local and remote discovery share one typed result and accumulator."""
    owner = SRC / "input_impl/directory_inputs.py"
    source = owner.read_text(encoding="utf-8")
    assert "class DirectoryDiscovery" in source
    assert "class DirectoryDiscoveryBuilder" in source
    assert "from .remote_files import RemoteFile" in source
    assert "def split_parent_child" in source

    remote_files = SRC / "input_impl/remote_files.py"
    remote_source = remote_files.read_text(encoding="utf-8")
    assert remote_files.is_file()
    assert "class RemoteFile" in remote_source
    assert "def remote_file_sort_key" in remote_source
    assert len(remote_source.splitlines()) <= 500
    assert "class RemoteDirectoryDiscovery" not in source
    assert "class LocalDirectoryDiscovery" not in source

    staging = (SRC / "remote_impl/staging.py").read_text(encoding="utf-8")
    assert "RemoteFile" in staging
    assert "DirectoryDiscovery" not in staging
    assert not (SRC / "remote_impl/types.py").exists()

    pipeline = (SRC / "pipeline/source_discovery.py").read_text(encoding="utf-8")
    assert "DirectoryDiscovery[FolderFile]" in pipeline
    assert "LocalDirectoryDiscovery" not in pipeline


def test_remote_prefetch_uses_constant_time_queue_removal() -> None:
    """Completion-order prefetch must not shift a Python list per result."""
    source = (SRC / "api_impl/source_plan/remote.py").read_text(encoding="utf-8")
    assert "from collections import deque" in source
    assert "deque[Future[Any]]" in source
    assert ".popleft()" in source
    assert ".pop(0)" not in source


def test_parquet_writer_analyzes_schema_once_iteratively() -> None:
    """Leaf ranges, levels, paths, and subtree sizes share one bounded traversal."""
    nodes = (WRITER / "stream_writer_schema_nodes.cc.inc").read_text(encoding="utf-8")
    types = (WRITER / "stream_writer_types.cc.inc").read_text(encoding="utf-8")
    api = (WRITER / "stream_writer_api.cc.inc").read_text(encoding="utf-8")

    assert "std::vector<LeafColumn> analyze_schema(ParquetNode *root)" in nodes
    assert "std::views::reverse" in nodes
    assert "first_leaf_index" in types
    assert "leaf_count" in types
    assert "subtree_schema_element_count" in types
    assert "auto columns = analyze_schema(&root);" in api
    for retired_function in (
        "assign_leaf_indexes(&root)",
        "assign_repetition_levels(&root)",
        "collect_leaf_columns(root)",
    ):
        assert retired_function not in api
    assert not (WRITER / "stream_writer_schema_levels.cc.inc").exists()


def test_null_propagation_uses_precomputed_leaf_ranges() -> None:
    """Null collection writes leaves directly instead of recursively walking nodes."""
    source = (WRITER / "stream_writer_collection.cc.inc").read_text(encoding="utf-8")
    begin = source.index("void emit_nulls_for_subtree")
    end = source.index("\n}\n", begin) + 3
    body = source[begin:end]
    assert "node.first_leaf_index" in body
    assert "node.leaf_count" in body
    assert "for (std::size_t" in body
    assert body.count("emit_nulls_for_subtree") == 1


def test_writer_micro_fragments_are_consolidated_but_bounded() -> None:
    """Closely coupled encoding and page helpers stay together below 500 lines."""
    values = WRITER / "stream_writer_arrow_values.cc.inc"
    pages = WRITER / "stream_writer_pages.cc.inc"
    assert "append_plain_primitive_value" in values.read_text(encoding="utf-8")
    assert "append_dictionary_value" in values.read_text(encoding="utf-8")
    assert "write_column_pages" in pages.read_text(encoding="utf-8")
    assert len(values.read_text(encoding="utf-8").splitlines()) <= 500
    assert len(pages.read_text(encoding="utf-8").splitlines()) <= 500
    for retired in (
        "stream_writer_plain_values.cc.inc",
        "stream_writer_dictionary_values.cc.inc",
        "stream_writer_column_chunks.cc.inc",
    ):
        assert not (WRITER / retired).exists()
