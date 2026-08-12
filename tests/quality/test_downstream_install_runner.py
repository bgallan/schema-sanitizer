"""Tests for compact isolated downstream installation checks."""

from __future__ import annotations

import importlib.util
import os
import tomllib
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]
SCRIPT = ROOT / "meta" / "ci" / "release" / "check_downstream_install.py"


def _module():
    """Load the CI helper as a testable module."""
    spec = importlib.util.spec_from_file_location("check_downstream_install", SCRIPT)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_every_extra_is_installed_and_imported_in_isolation(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """The compact job preserves the former one-environment-per-extra guarantee."""
    module = _module()
    wheel = tmp_path / "schema_sanitizer-0.4.0-cp311-abi3-linux.whl"
    wheel.write_bytes(b"wheel")
    calls: list[tuple[list[str], Path | None]] = []

    def fake_environment(root: Path, name: str) -> Path:
        """Return a distinct fake interpreter without creating a venv."""
        assert root == tmp_path
        return Path("/isolated") / name / "python"

    def fake_run(
        command: list[str],
        *,
        check: bool,
        cwd: Path | None = None,
    ) -> None:
        """Capture a checked subprocess call."""
        assert check is True
        calls.append((command, cwd))

    monkeypatch.setattr(module, "create_environment", fake_environment)
    monkeypatch.setattr(module.subprocess, "run", fake_run)

    module.validate_extras(wheel, tmp_path)

    installs = [command for command, _cwd in calls if command[1:4] == ["-m", "pip", "install"]]
    imports = [command for command, _cwd in calls if command[1:3] == ["-I", "-c"]]
    assert len(installs) == len(module.EXTRA_IMPORTS)
    assert len(imports) == len(module.EXTRA_IMPORTS)
    for extra, command in zip(module.EXTRA_IMPORTS, installs, strict=True):
        requirement = command[-1]
        if extra == "core":
            assert requirement == os.fspath(wheel)
        else:
            assert requirement == f"{wheel}[{extra}]"


def test_downstream_profiles_cover_every_published_runtime_extra() -> None:
    """Adding an optional runtime dependency requires an isolated install gate."""
    module = _module()
    pyproject = tomllib.loads((ROOT / "pyproject.toml").read_text(encoding="utf-8"))
    published = set(pyproject["project"]["optional-dependencies"]) - {"dev"}

    assert set(module.EXTRA_IMPORTS) == {"core", *published}
