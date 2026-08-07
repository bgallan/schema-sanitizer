"""Tests for the multidimensional threading benchmark orchestrator."""

from __future__ import annotations

import importlib.util
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
SCRIPT = ROOT / "benchmarks" / "bench_threading_matrix.py"


def _module():
    """Load the benchmark orchestrator as a testable module."""
    spec = importlib.util.spec_from_file_location("bench_threading_matrix", SCRIPT)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def test_standard_matrix_varies_every_required_local_dimension() -> None:
    """The standard profile must vary width, depth, sources, memory, and compression."""
    module = _module()
    cases = module._cases("standard")

    assert {case.wide_columns for case in cases} >= {4, 16, 64}
    assert {case.nested_depth for case in cases} >= {1, 2, 4}
    assert {case.source_count for case in cases} >= {1, 8}
    assert {case.memory_mib for case in cases} >= {64, 128, 512}
    assert {case.compression for case in cases} == {"uncompressed", "snappy", "gzip"}


def test_full_matrix_adds_cpu_quota_cases_where_supported(monkeypatch) -> None:
    """Linux and Windows full profiles must include explicit CPU-capacity runs."""
    module = _module()
    monkeypatch.setattr(module.sys, "platform", "linux")

    quotas = {case.cpu_quota for case in module._cases("full") if case.cpu_quota is not None}
    assert quotas == {1, 2, 4}


def test_matrix_rejects_child_cross_mode_mismatch(monkeypatch, tmp_path: Path) -> None:
    """A child benchmark mismatch must fail the complete matrix."""
    module = _module()
    case = module._cases("ci")[0]

    def fake_run(command: list[str], *, check: bool, stdout: object) -> None:
        """Write one deliberately non-equivalent child report."""
        assert check is True
        assert stdout is not None
        output = Path(command[command.index("--output") + 1])
        output.write_text(
            json.dumps({"cases": {"bad": {"equivalent": False}}}),
            encoding="utf-8",
        )

    monkeypatch.setattr(module.subprocess, "run", fake_run)
    try:
        module._run_case(
            case,
            rows=8,
            warmups=0,
            repeats=1,
            selection="parquet",
            directory=tmp_path,
        )
    except RuntimeError as exc:
        assert "cross-mode mismatch" in str(exc)
    else:
        raise AssertionError("matrix accepted a non-equivalent child report")


def test_ci_profile_is_small_and_cross_platform() -> None:
    """The CI profile must avoid affinity controls unavailable on macOS."""
    module = _module()
    cases = module._cases("ci")

    assert len(cases) == 2
    assert all(case.cpu_quota is None for case in cases)
    assert any(case.source_count > 1 for case in cases)
