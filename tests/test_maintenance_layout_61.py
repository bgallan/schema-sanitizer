"""Protect ownership boundaries introduced by maintenance layout 61."""

from __future__ import annotations

from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def test_execution_registry_probes_do_not_use_pass_through_helpers() -> None:
    """Registry probe methods should call the ABI directly from their owner."""
    owner = ROOT / "src/schema_sanitizer/core_impl/probes.py"
    text = owner.read_text(encoding="utf-8")
    assert "probe_dependencies" not in text
    assert "registry_probe_arrow_sources(" in text
    assert "registry_probe_path_source_chunk_provider(" in text


def test_remote_registry_stream_has_one_current_native_route() -> None:
    """Current ABI ownership must not retain auto/bounded compatibility strategies."""
    owner_remote = ROOT / "src/schema_sanitizer/api_impl/source_plan/remote.py"
    assert owner_remote.is_file()
    assert not owner_remote.with_suffix("").exists()
    owner = ROOT / "src/schema_sanitizer/api_impl/source_plan/registry.py"
    text = owner.read_text(encoding="utf-8")
    assert "to_registry_sink_path_source_chunk_provider_auto_registry" in text
    assert "getattr(" not in text
    assert len(text.splitlines()) <= 500


def test_schema_registry_json_writers_are_split_by_document_kind() -> None:
    """Registry documents and drift event arrays must use separate units."""
    package = ROOT / "cpp/src/schema_registry"
    assert not (package / "schema_registry_json_write.cc").exists()
    assert (package / "schema_registry_document_json.cc").is_file()
    assert (package / "schema_registry_drift_json.cc").is_file()
    assert "DriftEvent" not in (package / "schema_registry_document_json.cc").read_text(
        encoding="utf-8"
    )


def test_schema_probe_abi_has_one_visible_translation_unit() -> None:
    """Schema probe implementation should match its actual compilation unit."""
    package = ROOT / "cpp/src/api/python_abi3/probes"
    assert {path.name for path in package.iterdir() if path.is_file()} == {
        "schema_probe.cc",
        "schema_probe_methods.cc",
        "schema_probe_internal.hh",
    }
    implementation = (package / "schema_probe.cc").read_text(encoding="utf-8")
    methods = (package / "schema_probe_methods.cc").read_text(encoding="utf-8")
    assert "merge_path_source_schemas" in implementation
    assert "py_context_registry_probe_from_path_sources" in methods
    assert "py_context_registry_probe_from_path_source_chunk_provider" in methods
    assert len(implementation.splitlines()) <= 500
    assert len(methods.splitlines()) <= 500
    assert not list(package.rglob("*.inc"))
