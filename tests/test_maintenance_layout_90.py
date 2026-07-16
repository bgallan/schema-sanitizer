"""Protect consolidation and hot-path ownership from maintenance layout 90."""

from __future__ import annotations

from datetime import date
from pathlib import Path

from schema_sanitizer.core_impl import hive_uris

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src/schema_sanitizer"
CPP = ROOT / "cpp/src"


def test_hive_uri_helpers_have_one_cached_owner() -> None:
    """Hive URI values and rendering must not return to a micro-package."""
    owner = SRC / "core_impl/hive_uris.py"
    assert owner.is_file()
    assert not owner.with_suffix("").exists()
    text = owner.read_text(encoding="utf-8")
    assert "@lru_cache(maxsize=4096)" in text
    assert "def _partition_directory_uri" in text
    assert len(text.splitlines()) <= 500

    hive_uris._uri_template_values.cache_clear()
    hive_uris.build_partitioned_file_uri(
        "gs://bucket/table",
        date(2026, 7, 12),
        logical_hour=3,
        file_name_prefix="part",
        extension="parquet",
    )
    hive_uris.build_partition_directory_uri(
        "gs://bucket/table",
        date(2026, 7, 12),
        logical_hour=3,
    )
    assert hive_uris._uri_template_values.cache_info().hits >= 1


def test_projection_audit_modes_have_one_owner_each() -> None:
    """Closely coupled audit phases stay in bounded mode-specific modules."""
    audits = SRC / "adapters/parquet/projection/audits"
    assert {path.name for path in audits.glob("*.py")} == {
        "__init__.py",
        "composition.py",
        "coverage.py",
        "partitions.py",
        "subset.py",
        "summary.py",
    }
    assert not (audits / "subset").exists()
    for retired in (
        "coverage_inputs.py",
        "coverage_consistency.py",
        "partition_inputs.py",
        "partition_recomposition.py",
    ):
        assert not (audits / retired).exists()
    assert (
        max(len(path.read_text(encoding="utf-8").splitlines()) for path in audits.glob("*.py"))
        <= 500
    )


def test_python_reader_adapter_has_one_translation_unit() -> None:
    """The small Python reader class must compile from one cohesive owner."""
    sources = CPP / "api/python_abi3/sources"
    owner = sources / "python_reader.cc"
    assert owner.is_file()
    assert len(owner.read_text(encoding="utf-8").splitlines()) <= 500
    assert not (sources / "python_reader").exists()
    manifest = (ROOT / "cmake/SchemaSanitizerSources.cmake").read_text(encoding="utf-8")
    assert manifest.count("sources/python_reader.cc") == 1
    assert "sources/python_reader/" not in manifest


def test_versioned_field_names_have_one_exact_renderer() -> None:
    """Registry generation and numeric canonicalization share one renderer."""
    helper = (CPP / "internal/planning/variant_field_names.cc").read_text(encoding="utf-8")
    header = (CPP / "internal/planning/variant_field_names.hh").read_text(encoding="utf-8")
    registry = (CPP / "schema_registry/schema_registry.cc").read_text(encoding="utf-8")
    numeric = (CPP / "schema_registry/schema_registry_numeric.cc").read_text(encoding="utf-8")
    assert "make_versioned_field_name" in helper
    assert "make_versioned_field_name" in header
    assert "std::unreachable()" in helper
    assert "std::to_string(version)" not in registry
    assert 'canonical_name.append("_v")' not in numeric
    assert registry.count("make_versioned_field_name") == 1
    assert numeric.count("make_versioned_field_name") == 1


def test_numeric_registry_planning_borrows_names_before_moving_fields() -> None:
    """Numeric normalization avoids an owned string and a second parse per field."""
    source = (CPP / "schema_registry/schema_registry_numeric.cc").read_text(encoding="utf-8")
    assert "BorrowedStringLookupMap<std::size_t> plan_index_by_family" in source
    assert "std::vector<std::size_t> plan_by_field" in source
    assert "StringLookupMap<NumericFamilyPlan>" not in source
    second_pass = source[source.index("std::vector<LogicalField> out") :]
    assert "family_base_name(field)" not in second_pass
