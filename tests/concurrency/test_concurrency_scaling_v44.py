"""Regression coverage for v44 stable high-core output admission."""

from __future__ import annotations

from pathlib import Path

from conftest import require_native

from schema_sanitizer.core_impl.native_runtime import native_core


def test_v44_full_admission_avoids_high_core_executor_reconfiguration() -> None:
    """Fixed-wide 16-CPU output starts once at its bounded eight-worker lane."""
    require_native()
    adaptive = native_core.output_worker_admission_probe(False)
    stable = native_core.output_worker_admission_probe(True)

    assert adaptive == (4, 8, 2, True, 8, 8, 6)
    assert stable == (8, 8, 1, False, 8, 8, 0)


def test_v44_sources_preserve_the_scaled_fixed_wide_admission_gate() -> None:
    """Later admission paths must preserve the v43 high-core fixed gate."""
    root = Path(__file__).resolve().parents[2]
    writer = (root / "cpp/src/internal/json_output/jsonl_stream_writer.cc").read_text()
    ordered = (root / "cpp/src/internal/output/ordered_text_output.hh").read_text()
    admission = (root / "cpp/src/internal/output/output_worker_admission.hh").read_text()

    assert "scale_wide_fixed_output || admit_full_wide_variable_output," in writer
    assert "reclaim_wide_variable_packet_window);" in writer
    assert "bool full_worker_admission = false" in ordered
    assert "output_admission_requires_sampling(full_worker_admission)" in ordered
    assert "select_output_admission" in ordered
    assert "if (full_worker_admission)" in admission
    assert "return base_policy;" in admission
