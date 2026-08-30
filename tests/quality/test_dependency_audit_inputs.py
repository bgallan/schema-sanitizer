"""Dependency-audit inventory tests.

These contracts keep vulnerability scans independent of the quality runner's host
markers while retaining exact, independently installable owner-lock versions.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from meta.ci.quality.build_dependency_audit_inputs import build_audit_inputs


def _write_project(path: Path, dependency: str = "example>=1") -> None:
    """Write the smallest project declaration accepted by the audit builder."""
    path.write_text(
        "[build-system]\nrequires = []\nbuild-backend = 'unused'\n\n"
        f"[project]\nname = 'fixture'\nversion = '1.0'\ndependencies = [{dependency!r}]\n",
        encoding="utf-8",
    )


def test_audit_inputs_strip_environment_markers_without_dropping_pins(tmp_path: Path) -> None:
    """Windows and older-Python packages remain audited from any runner host."""
    project = tmp_path / "pyproject.toml"
    locks = tmp_path / "locks"
    output = tmp_path / "audit"
    locks.mkdir()
    _write_project(project)
    (locks / "owner.txt").write_text(
        "example==1.2\n"
        "colorama==0.4.6; sys_platform == 'win32'\n"
        "backports.tarfile==1.2.0; python_version < '3.12'\n",
        encoding="utf-8",
    )

    (audit_input,) = build_audit_inputs(project, locks, output, ci_tools=())

    assert audit_input.read_text(encoding="utf-8").splitlines() == [
        "backports-tarfile==1.2.0",
        "colorama==0.4.6",
        "example==1.2",
    ]


def test_audit_inputs_reject_conflicting_marker_variants(tmp_path: Path) -> None:
    """One owner lock cannot conceal incompatible versions behind host markers."""
    project = tmp_path / "pyproject.toml"
    locks = tmp_path / "locks"
    locks.mkdir()
    _write_project(project)
    (locks / "owner.txt").write_text(
        "example==1.2; sys_platform == 'linux'\nexample==1.3; sys_platform == 'win32'\n",
        encoding="utf-8",
    )

    with pytest.raises(ValueError, match="conflicting versions for example"):
        build_audit_inputs(project, locks, tmp_path / "audit", ci_tools=())


def test_repository_audit_inventory_contains_platform_only_dependencies(tmp_path: Path) -> None:
    """The checked-in locks audit all known platform-only packages on Linux CI."""
    root = Path(__file__).parents[2]
    outputs = build_audit_inputs(
        root / "pyproject.toml",
        root / "meta/ci/requirements",
        tmp_path / "audit",
    )
    pins = {line for output in outputs for line in output.read_text(encoding="utf-8").splitlines()}

    assert {"colorama==0.4.6", "pywin32-ctypes==0.2.3", "tzdata==2026.3"} <= pins
    assert all(";" not in pin for pin in pins)
