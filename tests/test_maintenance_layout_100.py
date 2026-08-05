"""Protect ownership and hot-path changes introduced by layout 100."""

from __future__ import annotations

from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def test_file_writers_have_one_direct_owner() -> None:
    """Native-first file writers must not return to per-format micro-modules."""
    owner = ROOT / "src/schema_sanitizer/api_impl/file_conversion/writers.py"
    text = owner.read_text(encoding="utf-8")
    assert len(text.splitlines()) <= 550
    for symbol in (
        "write_csv_native_first_stream",
        "write_jsonl_native_first_stream",
        "write_parquet_native_first_stream",
        "try_write_raw_native_file_output",
    ):
        assert f"def {symbol}(" in text
    assert not owner.with_suffix("").exists()


def test_abi3_module_definition_has_no_fragmented_method_catalog() -> None:
    """The ABI3 initializer and table remain a single deduced static owner."""
    owner = ROOT / "cpp/src/api/python_abi3/_core_abi3_module.cc"
    text = owner.read_text(encoding="utf-8")
    assert "std::to_array<PyMethodDef>" in text
    assert "kModuleMethodCount" not in text
    assert "PyMODINIT_FUNC PyInit__core_abi3" in text
    assert len(text.splitlines()) <= 550
    for retired in ("_core_abi3.cc", "_core_abi3_module.hh", "module_methods"):
        assert not (owner.parent / retired).exists()


def test_schema_evolution_and_ordering_share_one_indexed_owner() -> None:
    """Schema reconciliation must stay linear and avoid a second ordering TU."""
    owner = ROOT / "cpp/src/planning/schema_evolution.cc"
    text = owner.read_text(encoding="utf-8")
    assert len(text.splitlines()) <= 500
    assert "BorrowedStringLookupMap" in text
    assert text.count("build_field_map(") >= 4
    assert "find_field(" not in text
    assert not (owner.parent / "schema_field_order.cc").exists()


def test_file_conversion_reuses_normalized_writer_options() -> None:
    """Conversion must not clone or recreate writer options in each routing phase."""
    owner = ROOT / "src/schema_sanitizer/api_impl/file_conversion/converters.py"
    text = owner.read_text(encoding="utf-8")
    assert "resolved_writer_options = writer_options or {}" in text
    assert "dict(writer_options or {})" not in text
    assert text.count("(writer_options or {}).get") == 0
