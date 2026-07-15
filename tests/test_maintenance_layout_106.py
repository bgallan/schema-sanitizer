"""Maintenance contracts for the layout-106 cleanup."""

from __future__ import annotations

from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src/schema_sanitizer"
FOOTER = ROOT / "cpp/src/internal/parquet/footer_reader"


def _production_text() -> str:
    """Return all production Python and C++ source text."""
    return "\n".join(
        path.read_text(encoding="utf-8")
        for root in (SRC, ROOT / "cpp")
        for path in root.rglob("*")
        if path.is_file() and path.suffix in {".py", ".cc", ".hh", ".inc"}
    )


def test_optional_dependencies_have_one_canonical_owner() -> None:
    """Optional dependency loading must have one cached canonical owner."""
    dependencies = SRC / "core_impl/dependencies.py"
    text = dependencies.read_text(encoding="utf-8")

    assert len(text.splitlines()) <= 500
    assert "def ensure_optional_dependency(" in text
    assert "def ensure_pyarrow(" in text
    assert "def pyarrow_importable(" in text
    assert "@lru_cache(maxsize=1)" in text
    assert not (SRC / "core_impl/optional_dependencies.py").exists()
    assert not (SRC / "core_impl/pyarrow_dependency.py").exists()

    production = _production_text()
    assert "core_impl.optional_dependencies" not in production
    assert "core_impl.pyarrow_dependency" not in production


def test_parquet_micro_fragments_are_consolidated_by_runtime_phase() -> None:
    """Consecutive Parquet phases remain in bounded cohesive owners."""
    owners = {
        FOOTER / "runtime/native_stream_readiness.cc.inc": 500,
        FOOTER / "native_stream/materialization/native_stream_validity.cc.inc": 500,
        FOOTER / "native_stream/materialization/row_group/native_stream_row_group.cc.inc": 500,
        FOOTER
        / "native_stream/materialization/row_group/native_stream_retained_budget.cc.inc": 500,
    }
    for owner, line_limit in owners.items():
        assert owner.is_file()
        assert len(owner.read_text(encoding="utf-8").splitlines()) <= line_limit

    assert not (FOOTER / "runtime/readiness").exists()
    assert not (FOOTER / "native_stream/materialization/validity").exists()
    assert {
        path.name for path in (FOOTER / "native_stream/materialization/row_group").glob("*.cc.inc")
    } == {
        "native_stream_retained_budget.cc.inc",
        "native_stream_row_group.cc.inc",
    }

    translation_unit = (FOOTER / "footer_reader.cc").read_text(encoding="utf-8")
    for owner in owners:
        relative = owner.relative_to(FOOTER).as_posix()
        assert translation_unit.count(f'#include "{relative}"') == 1
    for retired in (
        "runtime/readiness/",
        "native_stream/materialization/validity/",
        "row_group/native_stream_array.cc.inc",
        "row_group/native_stream_column.cc.inc",
        "row_group/native_stream_flat_column.cc.inc",
        "row_group/native_stream_output_field.cc.inc",
        "row_group/native_stream_repeated_column.cc.inc",
    ):
        assert retired not in translation_unit


def test_recursive_output_validation_is_linear_and_portable() -> None:
    """Recursive output validation avoids copying and unsupported range algorithms."""
    layout = FOOTER / ("native_stream/schema/native_stream_output_layout.cc.inc")
    text = layout.read_text(encoding="utf-8")

    assert "enum class LeafState" in text
    assert "std::find(leaf_states.cbegin(), leaf_states.cend(), LeafState::Unseen)" in text
    assert "std::ranges::contains" not in text
    assert "std::ranges::sort(recursive_leaf_columns)" not in text
    assert "expected_leaf_columns" not in text

    model = FOOTER / "native_stream/schema/native_stream_recursive_model.cc.inc"
    model_text = model.read_text(encoding="utf-8")
    assert "tree->leaf_column_indices.reserve(tree->nodes.size())" in model_text
    assert "pending.reserve(tree->nodes.size())" in model_text
    assert "std::views::reverse" in model_text
    assert not (
        FOOTER / "native_stream/schema/native_stream_recursive_leaf_columns.cc.inc"
    ).exists()
