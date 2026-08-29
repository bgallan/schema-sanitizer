"""Tests for compact isolated downstream installation checks.

It validates isolated base, typed, and optional-extra consumer profiles plus complete
constraints for every published dependency group.
"""

from __future__ import annotations

import importlib.util
import shlex
import tomllib
from pathlib import Path

from packaging.requirements import Requirement
from packaging.utils import canonicalize_name

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
    tmp_path: Path,
) -> None:
    """The compact job gives every extra its own isolated environment."""
    module = _module()
    wheel = tmp_path / "schema_sanitizer-0.4.0-cp311-abi3-linux.whl"
    wheel.write_bytes(b"wheel")
    constraints = ROOT / "meta/ci/requirements/downstream.txt"
    plan = module.shell_plan(wheel, tmp_path, SCRIPT.parent, constraints)

    assert plan.count(" -m venv ") == len(module.EXTRA_IMPORTS) + 1
    assert plan.count("pip==26.1.2") == len(module.EXTRA_IMPORTS) + 1
    assert plan.count("[downstream-extra]") == len(module.EXTRA_IMPORTS)
    for extra, imports in module.EXTRA_IMPORTS.items():
        assert f"[downstream-extra] {extra}" in plan
        requirement = wheel.as_posix() if extra == "core" else f"{wheel.as_posix()}[{extra}]"
        assert requirement in plan
        for imported in imports:
            assert f"import {imported}" in plan
    assert plan.index("downstream_typecheck.py") < plan.index("[downstream-extra] core")


def test_downstream_plan_quotes_paths_and_owns_cleanup(tmp_path: Path) -> None:
    """Hostile path characters stay inside argv boundaries and owned cleanup."""
    module = _module()
    wheel_root = tmp_path / "wheel dir; touch injected"
    wheel_root.mkdir()
    wheel = wheel_root / "schema sanitizer.whl"
    wheel.write_bytes(b"wheel")
    constraints = ROOT / "meta/ci/requirements/downstream.txt"

    plan = module.shell_plan(wheel, tmp_path / "work root", SCRIPT.parent, constraints)

    assert shlex.quote(wheel.as_posix()) in plan
    assert "trap cleanup_downstream EXIT" in plan
    assert 'rm -rf -- "${isolated_root}"' in plan


def test_downstream_profiles_cover_every_published_runtime_extra() -> None:
    """Adding an optional runtime dependency requires an isolated install gate."""
    module = _module()
    pyproject = tomllib.loads((ROOT / "pyproject.toml").read_text(encoding="utf-8"))
    published = set(pyproject["project"]["optional-dependencies"]) - {"dev"}

    assert set(module.EXTRA_IMPORTS) == {"core", *published}


def test_downstream_constraints_pin_every_direct_extra_dependency() -> None:
    """Resolver drift cannot silently change the canonical downstream environment."""
    pyproject = tomllib.loads((ROOT / "pyproject.toml").read_text(encoding="utf-8"))
    constraints = (ROOT / "meta/ci/requirements/downstream.txt").read_text(encoding="utf-8")
    normalized_constraints = {
        canonicalize_name(Requirement(line).name)
        for line in constraints.splitlines()
        if line and not line.startswith("#")
    }
    direct = {
        canonicalize_name(Requirement(requirement).name)
        for name, values in pyproject["project"]["optional-dependencies"].items()
        if name != "dev"
        for requirement in values
    }

    assert direct <= normalized_constraints
    assert all("==" in line for line in constraints.splitlines() if line)
