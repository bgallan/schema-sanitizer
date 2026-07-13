"""Maintenance contracts introduced by layout 78."""

from __future__ import annotations

from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def test_stream_output_has_one_direct_owner_and_one_normalization() -> None:
    """File stream output must not return to per-phase gateway modules."""
    api = ROOT / "src/schema_sanitizer/api_impl"
    owner = api / "stream_output.py"
    source = owner.read_text(encoding="utf-8")

    assert owner.is_file()
    assert len(source.splitlines()) <= 500
    assert not (api / "stream_output").exists()
    assert source.count("normalize_options(") == 1
    assert "call_options: Options | None" in source
    assert "options=call_options" in source


def test_registry_warmup_has_one_direct_owner() -> None:
    """Warm-up input preparation and inference remain one cohesive workflow."""
    pipeline = ROOT / "src/schema_sanitizer/pipeline"
    owner = pipeline / "registry_warmup.py"
    source = owner.read_text(encoding="utf-8")

    assert owner.is_file()
    assert len(source.splitlines()) <= 500
    assert not (pipeline / "registry_warmup").exists()
    assert "def prepare_schema_warm_up_input(" in source
    assert "def infer_warm_up_schema_registry_state(" in source


def test_later_registry_chunks_do_not_copy_first_row_values() -> None:
    """Null metadata on later chunks must not first copy large registry JSON strings."""
    source = (ROOT / "cpp/src/api/python_abi3/registry/registry_stream_metadata.cc").read_text(
        encoding="utf-8"
    )

    assert "for (const auto &source : first_row_columns)" in source
    assert "if (first_row_pending) {\n      column.value = source.value;" in source
    assert "for (auto column : first_row_columns)" not in source
    assert "column.value.clear()" not in source


def test_registry_public_methods_are_small_real_units() -> None:
    """Public ABI methods remain split by operation rather than textual fragments."""
    registry = ROOT / "cpp/src/api/python_abi3/registry"
    owners = {
        "arrow_source_registry_methods.cc",
        "arrow_source_provider_methods.cc",
        "arrow_source_probe_methods.cc",
        "path_source_input_methods.cc",
        "path_source_registry_methods.cc",
        "path_source_auto_methods.cc",
    }
    for name in owners:
        owner = registry / name
        assert owner.is_file()
        assert len(owner.read_text(encoding="utf-8").splitlines()) <= 500

    assert not (registry / "arrow_source_methods.cc").exists()
    assert not (registry / "path_source_methods.cc").exists()
