"""Protect ownership and hot-path cleanups introduced by maintenance layout 101."""

from __future__ import annotations

from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]


def test_bigquery_sql_helpers_have_one_owner() -> None:
    """Quoting and canonical names must not return to parallel tiny modules."""
    package = ROOT / "src/schema_sanitizer/integrations/bigquery"
    owner = package / "sql.py"
    source = owner.read_text(encoding="utf-8")
    assert owner.is_file()
    assert "_BQ_TYPE_SYNONYMS =" in source
    assert source.count("_BQ_TYPE_SYNONYMS =") == 1
    assert "def _validate_identifier_component" in source
    assert not (package / "identifiers.py").exists()
    assert not (package / "type_normalization.py").exists()


def test_native_directory_arguments_live_with_their_consumers() -> None:
    """A two-function argument facade must not reappear under public input."""
    package = ROOT / "src/schema_sanitizer/api_impl/input"
    directory = (package / "directory_preparation.py").read_text(encoding="utf-8")
    native_options = (ROOT / "src/schema_sanitizer/core_impl/native_options.py").read_text(
        encoding="utf-8"
    )
    assert "def _all_files_have_native_paths" in directory
    assert "def optional_memory_limit_arg" in native_options
    assert not (package / "native_arguments.py").exists()


def test_xml_folder_validation_has_one_native_owner() -> None:
    """Folder scanning and its ABI3 entry point form one translation unit."""
    package = ROOT / "cpp/src/api/python_abi3/xml"
    owner = package / "_core_abi3_xml_folder.cc"
    source = owner.read_text(encoding="utf-8")
    assert len(source.splitlines()) <= 500
    assert "PySequence_Fast" in source
    assert "PyList_GetItem" in source
    assert "PyTuple_GetItem" in source
    assert "sequence_item_borrowed_or_new" not in source
    assert not list(package.glob("*_folder_parts.*"))


def test_schema_registry_methods_share_static_empty_payloads() -> None:
    """Registry query and merge methods must share one owner and no JSON builder."""
    package = ROOT / "cpp/src/api/python_abi3/registry"
    owner = package / "schema_registry_methods.cc"
    source = owner.read_text(encoding="utf-8")
    assert len(source.splitlines()) <= 500
    assert "kEmptyRegistryPayloads" in source
    assert "std::ranges::find_if" in source
    assert "append_string_field" not in source
    assert not (package / "schema_registry").exists()
