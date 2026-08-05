"""Protect ownership and buffer-transfer changes from maintenance layout 75."""

from __future__ import annotations

from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]


def test_registry_output_is_one_direct_owner() -> None:
    """Registry file routing must not return to format-specific pass-through modules."""
    owner = ROOT / "src/schema_sanitizer/api_impl/registry_output.py"
    assert owner.is_file()
    assert not owner.with_suffix("").exists()
    text = owner.read_text(encoding="utf-8")
    assert "def write_registry_raw_stream_to_file" in text
    assert "def write_parquet_registry_file" in text
    assert "def write_jsonl_registry_file" in text
    assert "def write_csv_registry_file" in text
    assert len(text.splitlines()) <= 500


def test_source_plan_registry_and_probing_are_direct_owners() -> None:
    """Source-plan lifecycle and probing must not regain router packages."""
    package = ROOT / "src/schema_sanitizer/api_impl/source_plan"
    expected = {
        "registry.py": (
            "class OpenedSourcePlanRegistryStream",
            "def open_source_plan_registry_stream",
            "def materialize_opened_registry_stream",
            "def write_source_plan_registry_to_file",
        ),
        "probing.py": (
            "def probe_source_plan_registry",
            "def _probe_sequence_registry",
            "def probe_prepared_source_plan_registry",
        ),
    }
    for filename, symbols in expected.items():
        owner = package / filename
        assert owner.is_file()
        assert not owner.with_suffix("").exists()
        text = owner.read_text(encoding="utf-8")
        for symbol in symbols:
            assert symbol in text
        assert len(text.splitlines()) <= 500


def test_scalar_materialization_matches_real_translation_units() -> None:
    """Scalar builders and conversion remain cohesive, visible compilation units."""
    materialization = ROOT / "cpp/src/internal/materialization"
    owners = (
        materialization / "builders/scalar.cc",
        materialization / "conversion/scalar.cc",
    )
    for owner in owners:
        assert owner.is_file()
        assert not owner.with_suffix("").exists()
        assert len(owner.read_text(encoding="utf-8").splitlines()) <= 500
    assert not list(materialization.rglob("*.cc.inc"))


def test_fixed_width_finish_transfers_column_buffers() -> None:
    """Finishing fixed-width columns must transfer, not duplicate, their vectors."""
    owner = ROOT / "cpp/src/internal/materialization/builders/scalar.cc"
    text = owner.read_text(encoding="utf-8")
    for member in ("f64", "i64", "i32"):
        assert f"payload->{member} = std::move(values_)" in text
    assert ".assign(values_.begin(), values_.end())" not in text
