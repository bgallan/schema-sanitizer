"""Protect ownership and ABI simplifications introduced by layout 76."""

from __future__ import annotations

from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def test_output_diagnostics_and_source_preparation_are_direct_owners() -> None:
    """Retired micro-packages must not return as forwarding surfaces."""
    api_impl = ROOT / "src/schema_sanitizer/api_impl"
    output_owner = api_impl / "output_diagnostics.py"
    preparation_owner = api_impl / "source_plan/preparation.py"

    assert output_owner.is_file()
    assert preparation_owner.is_file()
    assert not (api_impl / "output_diagnostics").exists()
    assert not (api_impl / "source_plan/preparation").exists()
    assert len(output_owner.read_text(encoding="utf-8").splitlines()) <= 500
    assert len(preparation_owner.read_text(encoding="utf-8").splitlines()) <= 500


def test_registry_provider_helpers_live_in_real_compilation_units() -> None:
    """Registry providers and public methods use explicit compilation units."""
    registry = ROOT / "cpp/src/api/python_abi3/registry"
    runtime_owners = (
        registry / "arrow_source_sinks.cc",
        registry / "path_source_sinks.cc",
    )
    method_owners = (
        registry / "arrow_source_registry_methods.cc",
        registry / "arrow_source_provider_methods.cc",
        registry / "arrow_source_probe_methods.cc",
        registry / "path_source_input_methods.cc",
        registry / "path_source_registry_methods.cc",
        registry / "path_source_auto_methods.cc",
    )
    assert all(owner.is_file() for owner in (*runtime_owners, *method_owners))
    assert all(
        len(owner.read_text(encoding="utf-8").splitlines()) <= 1000 for owner in runtime_owners
    )
    assert all(
        len(owner.read_text(encoding="utf-8").splitlines()) <= 500 for owner in method_owners
    )
    assert not (registry / "arrow_source_methods.cc").exists()
    assert not (registry / "path_source_methods.cc").exists()
    assert not (registry / "arrow_source_sinks").exists()
    assert not (registry / "path_source_sinks").exists()


def test_registry_state_result_is_packed_without_an_intermediate_tuple() -> None:
    """The state-aware packer must delegate directly to the six-slot owner."""
    metadata = (ROOT / "cpp/src/api/python_abi3/registry/registry_stream_metadata.cc").read_text(
        encoding="utf-8"
    )
    sink_packing = (
        ROOT / "cpp/src/api/python_abi3/sinks/_core_abi3_sink_result_packing.cc"
    ).read_text(encoding="utf-8")

    assert "PyObject *base = pack_registry_stream_result" not in metadata
    assert "conversion_timestamp, state" in metadata
    assert "PyTuple_New(6)" in sink_packing
    assert "native_registry_state ? native_registry_state : Py_None" in sink_packing


def test_registry_json_array_append_has_one_owner() -> None:
    """Arrow and path providers must share the same drift-array appender."""
    registry = ROOT / "cpp/src/api/python_abi3/registry"
    definitions = []
    for path in registry.rglob("*"):
        if path.is_file() and path.suffix in {".cc", ".inc"}:
            text = path.read_text(encoding="utf-8")
            if "void append_json_array_items" in text:
                definitions.append(path)
    assert definitions == [registry / "registry_stream_metadata.cc"]
    text = definitions[0].read_text(encoding="utf-8")
    assert "out->reserve(out->size() + delimiter_size + array_json.size())" in text


def test_prepared_manifest_sources_are_not_round_tripped_through_list() -> None:
    """Existing immutable descriptors must stay immutable through planning."""
    owner = (ROOT / "src/schema_sanitizer/api_impl/source_plan/preparation.py").read_text(
        encoding="utf-8"
    )
    assert "return manifest.source_batch.sources" in owner
    assert "list(manifest.source_batch.sources)" not in owner
