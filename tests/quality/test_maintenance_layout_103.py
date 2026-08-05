"""Protect ownership and hot-path cleanups introduced by maintenance layout 103."""

from __future__ import annotations

from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]


def test_file_converters_have_one_direct_public_owner() -> None:
    """CSV, JSONL, and Parquet wrappers share one owner without old facades."""
    package = ROOT / "src/schema_sanitizer/api_impl/file_conversion"
    owner = package / "converters.py"
    source = owner.read_text(encoding="utf-8")

    assert owner.is_file()
    assert len(source.splitlines()) <= 500
    for name in ("to_csv", "to_jsonl", "to_parquet"):
        assert f"def {name}(" in source
    for removed in ("delimited.py", "parquet.py", "invocation.py"):
        assert not (package / removed).exists()

    root_api = (ROOT / "src/schema_sanitizer/__init__.py").read_text(encoding="utf-8")
    assert root_api.count(".api_impl.file_conversion.converters") == 4
    assert ".api_impl.file_conversion.delimited" not in root_api
    assert ".api_impl.file_conversion.parquet" not in root_api


def test_call_option_filter_uses_copy_and_key_removal() -> None:
    """Wrapper filtering should use C-level dict copying, not scan every item."""
    from schema_sanitizer.options_impl.call_options import call_options_from_locals

    values = {"input_path": "in", "output_path": "out", "schema_mode": "additive"}
    result = call_options_from_locals(values, frozenset({"input_path", "output_path"}))

    assert result == {"schema_mode": "additive"}
    assert values == {
        "input_path": "in",
        "output_path": "out",
        "schema_mode": "additive",
    }
    source = (ROOT / "src/schema_sanitizer/options_impl/call_options.py").read_text(
        encoding="utf-8"
    )
    assert "options = values.copy()" in source
    assert "options.pop(key, None)" in source
    assert "for key, value in values.items()" not in source


def test_field_name_planning_has_one_native_owner() -> None:
    """Policy, collision, and recursive schema naming share one implementation."""
    internal = ROOT / "cpp/src/internal/planning"
    planning = ROOT / "cpp/src/planning"
    owner = planning / "field_name_sanitizer.cc"
    contract = internal / "field_name_sanitizer.hh"
    source = owner.read_text(encoding="utf-8")

    assert owner.is_file()
    assert contract.is_file()
    assert len(source.splitlines()) <= 500
    assert "std::pmr::unordered_map<std::string_view, std::size_t" in source
    assert "base_counts(resource)" in source
    assert "std::pmr::unordered_set<std::string_view" in source
    assert "used(resource)" in source
    assert "base_counts.size() == dirty_names.size()" in source
    assert "sanitize_logical_schema_field_names" in source
    assert "out.reserve(base.size() + length)" in source

    removed = (
        internal / "field_name_collision.cc",
        internal / "field_name_collision.hh",
        internal / "field_name_policy.cc",
        internal / "field_name_policy.hh",
        planning / "logical_field_name_sanitizer.cc",
    )
    assert not [path for path in removed if path.exists()]

    manifest = (ROOT / "cmake/SchemaSanitizerSources.cmake").read_text(encoding="utf-8")
    assert "logical_field_name_sanitizer.cc" not in manifest
    assert "field_name_collision.cc" not in manifest
    assert "field_name_policy.cc" not in manifest
