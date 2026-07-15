"""Tests for deterministic execution of promoted fuzz crashes."""

from __future__ import annotations

import importlib.util
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
SCRIPT = ROOT / "meta" / "ci" / "run_fuzz_regressions.py"


def _module():
    """Load the CI script as a testable module."""
    spec = importlib.util.spec_from_file_location("run_fuzz_regressions", SCRIPT)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_regression_runner_discovers_stable_cases_and_commands(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """Every parser directory is executed against its matching fuzzer binary."""
    module = _module()
    build_root = tmp_path / "build"
    regression_root = tmp_path / "regressions"
    calls: list[list[str]] = []

    for target in module.TARGETS:
        build_root.mkdir(exist_ok=True)
        (build_root / f"schema_sanitizer_fuzz_{target}").write_bytes(b"binary")
        target_root = regression_root / target
        target_root.mkdir(parents=True)
        (target_root / "case.bin").write_bytes(target.encode())
        (target_root / ".ignored").write_bytes(b"ignored")

    def fake_run(command: list[str], *, check: bool) -> None:
        """Capture one deterministic fuzzer invocation."""
        assert check is True
        calls.append(command)

    monkeypatch.setattr(module.subprocess, "run", fake_run)
    assert module.run_regressions(build_root, regression_root) == len(module.TARGETS)
    assert len(calls) == len(module.TARGETS)
    for target, command in zip(module.TARGETS, calls, strict=True):
        assert command[0].endswith(f"schema_sanitizer_fuzz_{target}")
        assert command[1] == "-runs=1"
        assert command[2].endswith(f"regressions/{target}/case.bin")


def test_regression_runner_rejects_empty_regression_set(tmp_path: Path) -> None:
    """CI fails rather than silently claiming regression coverage with no inputs."""
    module = _module()
    build_root = tmp_path / "build"
    regression_root = tmp_path / "regressions"
    build_root.mkdir()
    for target in module.TARGETS:
        (build_root / f"schema_sanitizer_fuzz_{target}").write_bytes(b"binary")
        (regression_root / target).mkdir(parents=True)

    with pytest.raises(RuntimeError, match="no fuzz regression inputs"):
        module.run_regressions(build_root, regression_root)
