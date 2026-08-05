"""Protect cohesive probe, input-selection, and remote-provider owners."""

from __future__ import annotations

from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]


def test_python_micro_packages_remain_direct_modules() -> None:
    """Small cohesive domains must not regress into pass-through packages."""
    package = ROOT / "src/schema_sanitizer"
    owners = (
        package / "core_impl/probes.py",
        package / "input_impl/selection.py",
        package / "api_impl/file_conversion/converters.py",
        package / "api_impl/source_plan/remote.py",
    )
    for owner in owners:
        assert owner.is_file()
        assert not owner.with_suffix("").exists()
        assert len(owner.read_text(encoding="utf-8").splitlines()) <= 750


def test_schema_probe_matches_its_real_translation_unit() -> None:
    """The ABI3 probe unit must not hide its implementation in include fragments."""
    package = ROOT / "cpp/src/api/python_abi3/probes"
    owners = tuple(
        package / name
        for name in ("schema_probe.cc", "schema_probe_methods.cc", "schema_probe_internal.hh")
    )
    assert {path.name for path in package.iterdir() if path.is_file()} == {
        owner.name for owner in owners
    }
    assert all(len(owner.read_text(encoding="utf-8").splitlines()) <= 500 for owner in owners)
    assert not list(package.rglob("*.inc"))


def test_remote_provider_and_native_probe_avoid_linear_chunk_copies() -> None:
    """Remote chunk iteration and capsule parsing should use constant-time ownership paths."""
    provider = (
        ROOT / "src/schema_sanitizer/api_impl/source_plan/remote_runtime/provider.py"
    ).read_text(encoding="utf-8")
    probe = (ROOT / "cpp/src/api/python_abi3/probes/schema_probe.cc").read_text(encoding="utf-8")
    assert "deque(retained_chunks)" in provider
    assert "self._retained_chunks.popleft()" in provider
    assert "remaining_remote_manifest" not in provider
    assert "parse_path_sources_view(chunk_sources, &parsed_sources)" in probe
    assert "merge_path_source_schemas(\n        ctx, parsed_sources.get()" in probe
