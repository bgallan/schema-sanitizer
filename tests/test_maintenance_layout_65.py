"""Protect maintenance layout revision 65."""

from __future__ import annotations

from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def test_remote_chunk_provider_has_one_cohesive_owner() -> None:
    """The small provider and staging gateway should not be a micro-package."""
    owner = ROOT / "src/schema_sanitizer/api_impl/source_plan/remote.py"
    assert owner.is_file()
    assert not owner.with_suffix("").exists()
    text = owner.read_text(encoding="utf-8")
    assert "class RemotePathSourceChunkProvider" in text
    assert "def open_staged_remote_chunks" in text
    assert "deque(" in text and ".popleft()" in text
    assert "remaining_remote_manifest" not in text
    assert len(text.splitlines()) <= 750


def test_bigquery_registry_has_bounded_direct_owners() -> None:
    """Registry and sidecar workflows should not be fragmented into micro-packages."""
    package = ROOT / "src/schema_sanitizer/integrations/bigquery"
    registry = package / "registry.py"
    sidecar = package / "sidecar.py"
    assert registry.is_file() and sidecar.is_file()
    assert not registry.with_suffix("").exists()
    assert not sidecar.with_suffix("").exists()
    assert len(registry.read_text(encoding="utf-8").splitlines()) <= 500
    assert len(sidecar.read_text(encoding="utf-8").splitlines()) <= 500


def test_delta_binary_packed_decode_has_one_bounded_owner() -> None:
    """Stream decode, preview validation, and page updates stay cohesive."""
    pages = ROOT / "cpp/src/internal/parquet/footer_reader/pages"
    owner = pages / "footer_reader_delta_binary.cc.inc"
    assert owner.is_file()
    assert not (pages / "delta_binary").exists()
    text = owner.read_text(encoding="utf-8")
    for symbol in (
        "decode_delta_binary_packed_stream",
        "decode_delta_binary_packed_values",
        "decode_delta_binary_packed_page",
    ):
        assert symbol in text
    assert len(text.splitlines()) <= 500


def test_registry_plan_has_one_bounded_native_owner() -> None:
    """Plan construction, capsule ownership, and parsing stay in one cohesive unit."""
    package = ROOT / "cpp/src/api/python_abi3/registry/plan"
    assert {path.name for path in package.iterdir()} == {"plan.cc", "plan.hh"}
    source = (package / "plan.cc").read_text(encoding="utf-8")
    header = (package / "plan.hh").read_text(encoding="utf-8")
    assert len(source.splitlines()) <= 500
    assert "make_native_registry_plan" in source
    assert "wrap_native_registry_state" in source
    assert "py_registry_state_from_json" in source
    assert "struct NativeRegistryPlan" in header
    manifest = (ROOT / "cmake/SchemaSanitizerSources.cmake").read_text(encoding="utf-8")
    assert "registry/plan/plan.cc" in manifest
    for retired in ("model.cc", "capsule.cc", "python_method.cc"):
        assert f"registry/plan/{retired}" not in manifest
