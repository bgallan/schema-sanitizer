"""Maintenance contracts for layout revision 113."""

from __future__ import annotations

from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
SRC = ROOT / "src/schema_sanitizer"
FOOTER = ROOT / "cpp/src/internal/parquet/footer_reader"
DIAGNOSTICS = FOOTER / "native_stream/diagnostics"
SCHEMA = FOOTER / "native_stream/schema"


def test_runtime_parquet_gates_share_one_owner() -> None:
    """Preflight and certification compose in one bounded runtime owner."""
    owner = SRC / "adapters/parquet/status.py"
    source = owner.read_text(encoding="utf-8")
    assert "def _parquet_contract_runtime_readiness_status_from_capabilities" in source
    assert "def _parquet_preflight_contract_status_from_writer_status" in source
    assert "def _parquet_contract_certification_status_from_parts" in source
    assert "copy.deepcopy" not in source
    assert not (SRC / "adapters/parquet/contract_gates/readiness.py").exists()
    assert not (SRC / "adapters/parquet/contract_gates/certification.py").exists()
    assert not (SRC / "adapters/parquet/contract_gates/runtime.py").exists()
    assert len(source.splitlines()) <= 500


def test_recursive_diagnostics_use_one_iterative_snapshot() -> None:
    """Recursive Parquet diagnostics traverse each field tree only once."""
    owner = DIAGNOSTICS / "native_stream_recursive_diagnostics.cc.inc"
    source = owner.read_text(encoding="utf-8")
    output = (DIAGNOSTICS / "native_stream_output_layout.cc.inc").read_text(encoding="utf-8")
    assert "native_recursive_materialization_diagnostics(" in source
    assert "std::vector<NativeRecursiveDiagnosticsFrame> pending" in source
    assert "std::vector<bool> visited" in source
    assert "native_recursive_materialization_diagnostics(field.recursive_tree)" in output
    assert output.count("native_recursive_materialization_diagnostics(") == 1
    assert "native_recursive_materialization_shape_signature(" not in output
    assert "native_recursive_leaf_paths(" not in output
    assert not (DIAGNOSTICS / "native_stream_structural_paths.cc.inc").exists()
    assert not (SCHEMA / "native_stream_schema_diagnostics.cc.inc").exists()
    assert len(source.splitlines()) <= 500


def test_runtime_parquet_gate_snapshots_keep_inputs_defensive() -> None:
    """Contract reports copy the mutable lists they expose to callers."""
    from schema_sanitizer.adapters.parquet.status import (
        _parquet_contract_certification_status_from_parts,
        _parquet_preflight_contract_status_from_writer_status,
    )

    writer = {
        "applicable": False,
        "satisfied": False,
        "issues": ["external writer"],
        "nested_contract_issues": ["not applicable"],
    }
    preflight = _parquet_preflight_contract_status_from_writer_status(
        writer,
        pyarrow_available=True,
    )
    projection = {"stable": False, "mismatches": ["drift"]}
    certificate = _parquet_contract_certification_status_from_parts(
        preflight_status=preflight,
        writer_status=writer,
        projection_audit=projection,
    )

    certificate["preflight_status"]["issues"].append("caller mutation")
    certificate["native_writer_status"]["issues"].append("caller mutation")
    certificate["projection_audit"]["mismatches"].append("caller mutation")
    assert preflight["issues"] == []
    assert writer["issues"] == ["external writer"]
    assert projection["mismatches"] == ["drift"]
