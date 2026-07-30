"""Protect ownership and hot-path changes introduced by maintenance layout 99."""

from __future__ import annotations

from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
_PRODUCT_SOURCE_LINE_LIMIT = 750


def test_recursive_layout_reducer_has_one_bounded_owner() -> None:
    """Reducer state, validation, and row-group folding stay in one module."""
    package = ROOT / "src/schema_sanitizer/adapters/parquet/layout"
    owner = package / "reducer.py"
    source = owner.read_text(encoding="utf-8")
    assert len(source.splitlines()) <= 500
    for retired in (
        "reducer_fingerprints.py",
        "reducer_state.py",
        "reducer_validation.py",
    ):
        assert not (package / retired).exists()
    assert "field_fingerprint_bundle" in source
    assert "bundles = [field_fingerprint_bundle(field) for field in fields]" in source
    assert "strict=True" in source
    assert "len(set(values))" not in source
    assert "any(value != values[0] for value in values[1:])" in source


def test_recursive_layout_fingerprints_are_bundled_once_per_field() -> None:
    """Row-group and final maps share one normalized fingerprint bundle."""
    package = ROOT / "src/schema_sanitizer/adapters/parquet/layout"
    fingerprints = (package / "fingerprints.py").read_text(encoding="utf-8")
    finalization = (package / "finalization.py").read_text(encoding="utf-8")
    assert "class FieldFingerprintBundle" in fingerprints
    assert "def field_fingerprint_bundle(" in fingerprints
    assert "bundle = field_fingerprint_bundle(field)" in finalization
    for duplicate_helper in (
        "leaf_contract_fingerprint_from_field",
        "leaf_contracts_from_field",
        "leaf_level_fingerprint_from_field",
        "leaf_repeated_ancestor_fingerprint_from_field",
        "leaf_repetition_path_fingerprint_from_field",
        "recursive_field_fingerprint_from_field",
        "root_contract_fingerprint_from_field",
        "root_contract_from_field",
    ):
        assert duplicate_helper not in finalization


def test_arrow_direct_values_have_one_owner_and_parsed_storage_kind() -> None:
    """Arrow values reuse schema-time storage classification without format strings."""
    package = ROOT / "cpp/src/api/python_abi3/arrow_direct"
    owner = (package / "_core_abi3_arrow_direct_values.cc").read_text(encoding="utf-8")
    model = (package / "_core_abi3_arrow_direct_model.hh").read_text(encoding="utf-8")
    parser = (package / "schema/type.cc").read_text(encoding="utf-8")
    assert len(owner.splitlines()) <= 500
    assert "enum class ArrowStorageKind" in model
    assert "ArrowStorageKind storage_kind" in model
    assert "std::string format;" not in model
    assert "node->storage_kind" in parser
    assert "ref->node->storage_kind" in owner
    assert "const std::string_view format(ref->node->format)" not in owner

    for suffix in ("dictionary", "nested", "temporal"):
        assert not (package / f"_core_abi3_arrow_direct_values_{suffix}.cc").exists()
        assert not (package / f"_core_abi3_arrow_direct_values_{suffix}.hh").exists()

    manifest = (ROOT / "cmake/SchemaSanitizerSources.cmake").read_text(encoding="utf-8")
    assert manifest.count("_core_abi3_arrow_direct_values.cc") == 1
    assert "_core_abi3_arrow_direct_values_dictionary.cc" not in manifest
    assert "_core_abi3_arrow_direct_values_nested.cc" not in manifest
    assert "_core_abi3_arrow_direct_values_temporal.cc" not in manifest


def test_product_files_remain_bounded() -> None:
    """All Python and native production owners remain explicitly bounded."""
    candidates = [
        *(ROOT / "src/schema_sanitizer").rglob("*.py"),
        *(ROOT / "cpp/src").rglob("*.cc"),
        *(ROOT / "cpp/src").rglob("*.cpp"),
        *(ROOT / "cpp/src").rglob("*.hh"),
        *(ROOT / "cpp/src").rglob("*.hpp"),
        *(ROOT / "cpp/src").rglob("*.inc"),
    ]
    oversized = {
        str(path.relative_to(ROOT)): len(path.read_text(encoding="utf-8").splitlines())
        for path in candidates
        if len(path.read_text(encoding="utf-8").splitlines()) > _PRODUCT_SOURCE_LINE_LIMIT
    }
    assert oversized == {}
