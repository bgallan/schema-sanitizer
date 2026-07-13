"""Protect ownership and performance changes introduced by maintenance layout 115."""

from __future__ import annotations

from pathlib import Path

from schema_sanitizer.adapters.parquet.projection.audits.summary import duplicate_names

ROOT = Path(__file__).resolve().parents[1]


def test_projection_duplicate_detection_has_one_linear_owner() -> None:
    """All projection audits share one Counter-based duplicate-name helper."""
    audits = ROOT / "src/schema_sanitizer/adapters/parquet/projection/audits"
    summary = (audits / "summary.py").read_text(encoding="utf-8")
    assert "def duplicate_names" in summary
    assert "Counter(values)" in summary
    for name in ("subset.py", "composition.py", "coverage.py", "partitions.py"):
        text = (audits / name).read_text(encoding="utf-8")
        assert "duplicate_names(" in text
        assert ".count(name)" not in text
        assert "Counter(projection)" not in text
        assert "Counter(partition)" not in text


def test_duplicate_names_is_deterministic_and_linear_by_contract() -> None:
    """The shared duplicate detector returns sorted unique duplicate names."""
    assert duplicate_names(["b", "a", "b", "c", "a", "b"]) == ["a", "b"]
    assert duplicate_names(iter(["only", "once"])) == []


def test_repeated_path_support_has_one_iterative_cpp_owner() -> None:
    """Repeated-path planning and limits share one bounded non-recursive unit."""
    schema = ROOT / "cpp/src/internal/parquet/footer_reader/native_stream/schema"
    owner = schema / "native_stream_repeated_path_support.cc.inc"
    assert owner.is_file()
    text = owner.read_text(encoding="utf-8")
    assert len(text.splitlines()) <= 500
    assert "struct NativeRecursiveSupportValidationFrame" in text
    assert "pending.reserve(tree.nodes.size())" in text
    assert "std::views::reverse" in text
    assert "validate_native_recursive_materialization_node_supported" not in text
    assert not (schema / "native_stream_path_support.cc.inc").exists()
    assert not (schema / "native_stream_generic_repeated_limits.cc.inc").exists()


def test_recursive_struct_materialization_reuses_path_and_scalar_depth() -> None:
    """Struct materialization avoids duplicate tree walks and temporary level vectors."""
    materialization = ROOT / "cpp/src/internal/parquet/footer_reader/native_stream/materialization"
    containers = (materialization / "native_stream_recursive_containers.cc.inc").read_text(
        encoding="utf-8"
    )
    children = (materialization / "native_stream_recursive_children.cc.inc").read_text(
        encoding="utf-8"
    )
    assert "generic_list_defined_levels_from_path(candidate).size()" not in containers
    assert "candidate.max_repetition_level" not in containers
    assert "native_recursive_layout_column_for_node" in containers
    assert "native_recursive_node_path(tree, struct_node_index)" not in children
    assert "struct_node.definition_level" in children
    assert "map_column_indices" not in children
