"""Protect the registry ownership and chunk-reuse boundaries of layout 77."""

from __future__ import annotations

from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def test_core_registry_methods_are_direct_owner_modules() -> None:
    """Registry methods must not regress into a forwarding package."""
    core = ROOT / "src/schema_sanitizer/core_impl"
    assert not (core / "registry").exists()
    assert not (core / "registry_sources.py").exists()
    owner = core / "registry_sinks.py"
    source = owner.read_text(encoding="utf-8")
    for owner_type in (
        "_RegistryArrowSinkMethods",
        "_RegistryPathProviderSinkMethods",
        "_RegistryPathSourceSinkMethods",
    ):
        assert f"class {owner_type}" in source
    assert "_registry_sink_output" in source
    assert len(source.splitlines()) <= 500
    assert not (core / "registry_arrow.py").exists()
    assert not (core / "registry_paths.py").exists()
    execution = (core / "execution.py").read_text(encoding="utf-8")
    assert "def _call_native_registry_sink_from_source" in execution
    assert "_registry_sink_output" in execution
    assert len(execution.splitlines()) <= 500


def test_registry_provider_code_uses_real_compilation_units() -> None:
    """Registry providers must not return to textual or oversized public units."""
    registry = ROOT / "cpp/src/api/python_abi3/registry"
    runtime_owners = ("arrow_source_sinks.cc", "path_source_sinks.cc")
    method_owners = (
        "arrow_source_registry_methods.cc",
        "arrow_source_provider_methods.cc",
        "arrow_source_probe_methods.cc",
        "path_source_input_methods.cc",
        "path_source_registry_methods.cc",
        "path_source_auto_methods.cc",
    )
    for name in runtime_owners:
        source = registry / name
        assert source.is_file()
        assert len(source.read_text(encoding="utf-8").splitlines()) <= 1000
    for name in method_owners:
        source = registry / name
        assert source.is_file()
        assert len(source.read_text(encoding="utf-8").splitlines()) <= 500
    assert not (registry / "arrow_source_methods.cc").exists()
    assert not (registry / "path_source_methods.cc").exists()
    assert not list(registry.rglob("*.cc.inc"))


def test_provider_schema_merge_reuses_chunk_vectors() -> None:
    """Chunked registry probes should retain vector capacity across chunks."""
    registry = ROOT / "cpp/src/api/python_abi3/registry"
    arrow = (registry / "arrow_source_provider.cc").read_text(encoding="utf-8")
    paths = (registry / "path_source_provider.cc").read_text(encoding="utf-8")

    assert arrow.count("std::vector<ArrowSourceSpec> sources;") == 1
    assert paths.count("std::vector<PathSourceSpec> sources;") == 1
    assert "std::vector<ArrowSourceSpec> next_sources;" not in arrow
    assert "decref_arrow_sources(&state->sources);" in arrow


def test_registry_method_units_are_cmake_sources() -> None:
    """Every public registry method unit must compile independently."""
    cmake = (ROOT / "cmake/SchemaSanitizerSources.cmake").read_text(encoding="utf-8")
    for owner in (
        "registry/arrow_source_registry_methods.cc",
        "registry/arrow_source_provider_methods.cc",
        "registry/arrow_source_probe_methods.cc",
        "registry/path_source_input_methods.cc",
        "registry/path_source_registry_methods.cc",
        "registry/path_source_auto_methods.cc",
    ):
        assert owner in cmake
    assert "registry/arrow_source_methods.cc" not in cmake
    assert "registry/path_source_methods.cc" not in cmake
