"""Protect maintenance layout revision 93."""

from __future__ import annotations

from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def test_cloud_provider_packages_stay_flat_and_facade_free() -> None:
    """Cloud backends remain direct modules without retired package surfaces."""
    providers = ROOT / "src/schema_sanitizer/remote_impl/providers"
    for name in ("gcs", "s3", "azure"):
        owner = providers / f"{name}.py"
        assert owner.is_file()
        assert not (providers / name).exists()
        assert len(owner.read_text(encoding="utf-8").splitlines()) <= 500

    production = "\n".join(
        path.read_text(encoding="utf-8") for path in (ROOT / "src/schema_sanitizer").rglob("*.py")
    )
    for retired in (
        "providers.gcs.client",
        "providers.gcs.objects",
        "providers.gcs.direct_listing",
        "providers.gcs.bulk_discovery",
        "providers.s3.client",
        "providers.s3.discovery",
        "providers.s3.objects",
        "providers.azure.client",
        "providers.azure.discovery",
        "providers.azure.objects",
    ):
        assert retired not in production


def test_source_discovery_classifies_each_unique_uri_once() -> None:
    """Discovery carries location kinds instead of reparsing during grouping."""
    owner = (ROOT / "src/schema_sanitizer/pipeline/source_discovery.py").read_text(encoding="utf-8")
    unique = owner[
        owner.index("def _unique_source_locations") : owner.index("\ndef _partition_plans")
    ]
    grouped = owner[
        owner.index("async def _discover_directories") : owner.index("\nasync def _discover_source")
    ]
    assert unique.count("location_kind(source_uri)") == 1
    assert "dict[str, LocationKind]" in unique
    assert "remote_provider(uri)" not in grouped
    assert "for uri, kind in source_locations.items()" in grouped


def test_registry_plan_stays_consolidated_without_forwarding_headers() -> None:
    """The registry plan has one implementation and one direct contract."""
    package = ROOT / "cpp/src/api/python_abi3/registry/plan"
    assert {path.name for path in package.iterdir()} == {"plan.cc", "plan.hh"}
    source = (package / "plan.cc").read_text(encoding="utf-8")
    assert len(source.splitlines()) <= 500
    assert "std::ranges::find_if" in source
    for retired in ("capsule.hh", "model.hh", "capsule.cc", "model.cc", "python_method.cc"):
        assert not (package / retired).exists()
