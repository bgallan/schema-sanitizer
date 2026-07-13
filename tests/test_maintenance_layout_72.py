"""Protect direct native-result, registry-sink, and coalescing owners from layout 72."""

from __future__ import annotations

from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def test_native_results_remain_one_cohesive_module() -> None:
    """Typed ABI results must not regress into a package of forwarding modules."""
    owner = ROOT / "src/schema_sanitizer/core_impl/native_results.py"
    assert owner.is_file()
    assert not owner.with_suffix("").exists()
    source = owner.read_text(encoding="utf-8")
    for result_type in (
        "IngestDiagnostics",
        "SchemaProbeResult",
        "RegistryProbeResult",
        "SinkOutput",
    ):
        assert f"class {result_type}" in source
    assert len(source.splitlines()) <= 500


def test_registry_sink_routes_call_abi_directly() -> None:
    """Registry methods must not regain parallel call-wrapper packages."""
    core = ROOT / "src/schema_sanitizer/core_impl"
    owner = core / "registry_sinks.py"
    source = owner.read_text(encoding="utf-8")
    for owner_type in (
        "_RegistryArrowSinkMethods",
        "_RegistryPathProviderSinkMethods",
        "_RegistryPathSourceSinkMethods",
    ):
        assert f"class {owner_type}" in source
    assert "native_core as _native" in source
    assert len(source.splitlines()) <= 500
    assert not (core / "registry_arrow.py").exists()
    assert not (core / "registry_paths.py").exists()
    execution = (core / "execution.py").read_text(encoding="utf-8")
    assert "def to_registry_sink_from_source" in execution
    assert "context_to_registry_sink_from_source" in execution
    assert not (core / "registry_sources.py").exists()
    assert not (core / "registry").exists()


def test_coalescing_stream_releases_each_input_batch_after_copy() -> None:
    """Native coalescing should not retain or heap-box every source batch."""
    streaming = ROOT / "cpp/src/api/python_abi3/streaming"
    runtime = (streaming / "coalesce_stream.cc").read_text(encoding="utf-8")
    append = (streaming / "coalesce_append.cc").read_text(encoding="utf-8")
    schema = (streaming / "coalesce_schema.cc").read_text(encoding="utf-8")
    source = "\n".join((runtime, append, schema))
    assert all(len(part.splitlines()) <= 500 for part in (runtime, append, schema))
    assert "sanitize::CArrayGuard batch;" in runtime
    assert "std::vector<std::unique_ptr<sanitize::CArrayGuard>>" not in source
    assert "build_coalesced_array_state" not in source
    assert "integer_width_for_format" in schema
    assert "std::find(kInteger8Formats.cbegin(), kInteger8Formats.cend(), format)" in schema
    assert "std::ranges::contains" not in schema
    assert not (streaming / "coalesce_stream").exists()
