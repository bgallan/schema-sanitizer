"""Protect direct native-output and schema-registry owners from layout 73."""

from __future__ import annotations

from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]


def test_native_output_stays_one_cohesive_module() -> None:
    """Native file-output dispatch must not regress into per-format facade packages."""
    owner = ROOT / "src/schema_sanitizer/api_impl/file_conversion/direct_writers.py"
    retired = owner.with_suffix("")
    assert owner.is_file()
    assert not retired.exists()
    source = owner.read_text(encoding="utf-8")
    assert len(source.splitlines()) <= 500
    assert "def _call_native_writer" in source
    assert "def try_write_csv_direct_native" in source
    assert "def try_write_jsonl_direct_native" in source
    assert "def try_write_parquet_direct_native" in source


def test_schema_registry_merge_has_explicit_recursive_and_numeric_owners() -> None:
    """Registry recursion and numeric-family normalization stay in direct source files."""
    package = ROOT / "cpp/src/schema_registry"
    owner = package / "schema_registry.cc"
    numeric = package / "schema_registry_numeric.cc"
    assert owner.is_file()
    assert numeric.is_file()
    assert not list(package.glob("schema_registry_*.cc.inc"))
    source = owner.read_text(encoding="utf-8")
    numeric_source = numeric.read_text(encoding="utf-8")
    assert len(source.splitlines()) <= 500
    assert len(numeric_source.splitlines()) <= 500
    assert "std::ranges::find_if" in source
    assert "out.reserve(maximum_field_count)" in source
    assert "build_field_merge_index(out, maximum_field_count)" in source
    assert "StringLookupMap<VariantFamilyIndex> families" in source
    assert "normalize_integer_float_schema" not in source
    assert "void normalize_integer_float_schema" in numeric_source
    assert "std::vector<NumericFamilyPlan> plans" in numeric_source
    assert "BorrowedStringLookupMap<std::size_t> plan_index_by_family" in numeric_source
    assert "std::vector<std::size_t> plan_by_field" in numeric_source
    assert "positions_by_family" not in numeric_source
    assert "variants_by_base" not in source
    assert "next_variant_version" not in source
    entry = (package / "schema_registry_entry.cc").read_text(encoding="utf-8")
    assert len(entry.splitlines()) <= 500
    assert "drifts.reserve(input.inferred_schema.fields.size())" in entry
