"""Maintenance contracts for the layout-107 ownership cleanup."""

from __future__ import annotations

from pathlib import Path

from schema_sanitizer.input_impl.directory_inputs import (
    DirectoryDiscoveryBuilder,
    RemoteFile,
    split_parent_child,
)

ROOT = Path(__file__).resolve().parents[2]
SRC = ROOT / "src/schema_sanitizer"
CPP = ROOT / "cpp/src/internal/parquet"


def test_remote_directory_discovery_has_one_accumulator_owner() -> None:
    """All cloud providers must share grouping output and deterministic finalization."""
    owner = SRC / "input_impl/directory_inputs.py"
    text = owner.read_text(encoding="utf-8")
    assert "class DirectoryDiscoveryBuilder" in text
    assert "def split_parent_child" in text
    assert len(text.splitlines()) <= 500

    for name in ("azure.py", "gcs.py", "s3.py"):
        provider = SRC / "remote_impl/providers" / name
        source = provider.read_text(encoding="utf-8")
        assert "DirectoryDiscoveryBuilder[RemoteFile].from_uris(" in source
        assert "metadata_budget=current_directory_metadata_budget(memory_limit_bytes)" in source
        assert "discovery.add(child_uris, remote_file)" in source
        assert "return discovery.finish()" in source
        assert "def _parent_child" not in source
        assert "exists_by_uri = dict.fromkeys" not in source
        assert "for files in files_by_uri.values()" not in source
        assert len(source.splitlines()) <= 500


def test_remote_directory_discovery_builder_sorts_once_at_finalization() -> None:
    """The shared accumulator preserves keys and returns deterministic file order."""
    builder = DirectoryDiscoveryBuilder[RemoteFile].from_uris(("gs://bucket/b", "gs://bucket/a"))
    builder.add(
        ("gs://bucket/b",),
        RemoteFile("gs://bucket/b/z.parquet", "z.parquet", 2),
    )
    builder.add(
        ("gs://bucket/b",),
        RemoteFile("gs://bucket/b/a.parquet", "a.parquet", 1),
    )
    result = builder.finish()
    assert result.exists_by_uri == {
        "gs://bucket/b": True,
        "gs://bucket/a": False,
    }
    assert [file.name for file in result.files_by_uri["gs://bucket/b"]] == [
        "a.parquet",
        "z.parquet",
    ]
    assert split_parent_child("year=2026/month=07/") == ("year=2026", "month=07")


def test_file_output_path_and_parquet_lifecycle_have_direct_owners() -> None:
    """Output path validation is canonical and the one-use sink facade stays removed."""
    uris = (SRC / "core_impl/uris.py").read_text(encoding="utf-8")
    assert "def local_output_path_or_reject_remote" in uris
    assert not (SRC / "adapters/pyarrow/sink_lifecycle.py").exists()

    sink = (SRC / "adapters/parquet/sink.py").read_text(encoding="utf-8")
    assert 'local_output_path_or_reject_remote(out_path, sink_name="Parquet")' in sink
    assert "writer: Any | None = None" in sink
    assert "if writer is not None:" in sink
    assert "open_pyarrow_output_sink" not in sink

    for relative in (
        "api_impl/file_conversion/direct_writers.py",
        "adapters/pyarrow/csv_sink.py",
        "adapters/pyarrow/jsonl_sink.py",
    ):
        source = (SRC / relative).read_text(encoding="utf-8")
        assert "local_output_path_or_reject_remote" in source
        assert "def _local_output_path" not in source


def test_recursive_parquet_model_uses_bounded_iterative_traversal() -> None:
    """Recursive model analysis must not consume one C++ stack frame per schema node."""
    schema = CPP / "footer_reader/native_stream/schema"
    model = schema / "native_stream_recursive_model.cc.inc"
    source = model.read_text(encoding="utf-8")
    assert len(source.splitlines()) <= 500
    assert "plan_native_recursive_materialization_tree" in source
    assert "tree->leaf_column_indices.reserve(tree->nodes.size())" in source
    assert "std::vector<bool> entered(tree->nodes.size(), false)" in source
    assert "node.children | std::views::reverse" in source
    assert "std::span<const std::size_t>" in source
    assert "std::vector<std::pair<std::size_t, bool>> pending" in source

    schema_root = (schema / "native_stream_arrow_schema_root.cc.inc").read_text(encoding="utf-8")
    schema_builders = (schema / "native_stream_arrow_schema_builders.cc.inc").read_text(
        encoding="utf-8"
    )
    assert "const auto &subtree_counts = field.recursive_subtree_counts" in schema_root
    assert "count_native_recursive_materialization_subtree_resources(" not in schema_root
    assert "native_recursive_schema_resource_counts(subtree_counts" in schema_builders
    assert "native_recursive_schema_resource_counts(tree" not in schema_builders
    for retired in (
        "native_stream_recursive_types.cc.inc",
        "native_stream_recursive_resource_counts.cc.inc",
        "native_stream_recursive_leaf_columns.cc.inc",
    ):
        assert not (schema / retired).exists()


def test_parquet_writer_micro_fragments_are_owned_by_their_only_callers() -> None:
    """Single-purpose helpers must stay with collection and stream entry owners."""
    writer = CPP / "stream_writer"
    collection = (writer / "stream_writer_collection.cc.inc").read_text(encoding="utf-8")
    api = (writer / "stream_writer_api.cc.inc").read_text(encoding="utf-8")
    assert "void emit_nulls_for_subtree" in collection
    assert "std::string stream_error_message" in api
    assert len(collection.splitlines()) <= 500
    assert len(api.splitlines()) <= 500
    assert not (writer / "stream_writer_null_collection.cc.inc").exists()
    assert not (writer / "stream_writer_stream_errors.cc.inc").exists()
