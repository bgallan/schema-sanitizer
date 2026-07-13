"""Protect ownership boundaries introduced by maintenance layout 53."""

from __future__ import annotations

import importlib.util
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def test_output_diagnostics_are_not_owned_by_analytical_backends() -> None:
    """File and table diagnostics must remain an output concern."""
    analytical = ROOT / "src/schema_sanitizer/api_impl/analytical.py"
    assert analytical.is_file()
    assert not (ROOT / "src/schema_sanitizer/api_impl/analytical").exists()
    assert "patch_table_diagnostics" not in analytical.read_text(encoding="utf-8")
    owner = ROOT / "src/schema_sanitizer/api_impl/output_diagnostics.py"
    assert owner.is_file()
    assert not (ROOT / "src/schema_sanitizer/api_impl/output_diagnostics").exists()
    text = owner.read_text(encoding="utf-8")
    assert "def patch_file_output_diagnostics" in text
    assert "def patch_table_diagnostics" in text


def test_layout_finalization_has_one_direct_owner() -> None:
    """Collision detection, contract maps, and summary assembly stay cohesive."""
    assert (
        importlib.util.find_spec("schema_sanitizer.adapters.parquet.layout.reducer_finalize")
        is None
    )
    layout = ROOT / "src/schema_sanitizer/adapters/parquet/layout"
    owner = layout / "finalization.py"
    assert owner.is_file()
    assert not (layout / "finalization").exists()
    text = owner.read_text(encoding="utf-8")
    for symbol in (
        "def collect_layout_path_collisions",
        "def build_layout_contract_maps",
        "def finalize_recursive_layout_summary",
    ):
        assert symbol in text
    assert len(text.splitlines()) <= 500


def test_path_source_probes_use_the_execution_probe_owner() -> None:
    """Path collections, best-effort, and providers share one direct ABI owner."""
    owner = ROOT / "src/schema_sanitizer/core_impl/probes.py"
    text = owner.read_text(encoding="utf-8")
    assert not (ROOT / "src/schema_sanitizer/core_impl/probes").exists()
    assert "registry_probe_path_sources" in text
    assert "registry_probe_path_sources_best_effort" in text
    assert "registry_probe_path_source_chunk_provider" in text


def test_nested_validity_has_one_bounded_materialization_owner() -> None:
    """Container validity remains cohesive without a directory of tiny fragments."""
    materialization = ROOT / "cpp/src/internal/parquet/footer_reader/native_stream/materialization"
    owner = materialization / "native_stream_validity.cc.inc"
    assert owner.is_file()
    assert len(owner.read_text(encoding="utf-8").splitlines()) <= 500
    assert not (materialization / "validity").exists()
    assert not (materialization / "native_stream_nested_validity.cc.inc").exists()
    assert not (materialization / "native_stream_nested_validity.cc.inc").exists()


def test_footer_reader_contract_has_api_and_model_owners() -> None:
    """Footer declarations and metadata models must not reconverge in one header."""
    parquet = ROOT / "cpp/src/internal/parquet"
    reader = parquet / "footer_reader"
    assert (reader / "api.hh").is_file()
    assert {path.name for path in (reader / "model").iterdir() if path.is_file()} == {
        "column.hh",
        "footer.hh",
        "pages.hh",
        "schema.hh",
    }
    assert not (parquet / "parquet_footer_reader.hh").exists()
