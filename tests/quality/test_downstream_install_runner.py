"""Tests for compact isolated downstream installation checks.

It validates isolated base, typed, and optional-extra consumer profiles plus complete
constraints for every published dependency group.
"""

from __future__ import annotations

import importlib.util
import os
import shlex
import shutil
import subprocess
import tomllib
from pathlib import Path

import pytest
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
    assert plan.count("pip==26.2.1") == len(module.EXTRA_IMPORTS) + 1
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
    assert plan == module.shell_plan(wheel, tmp_path / "work root", SCRIPT.parent, constraints)


def test_downstream_command_output_is_atomic_and_rejects_symlinks(tmp_path: Path) -> None:
    """Repeated writes preserve identical output while symlink targets fail closed."""
    module = _module()
    output = tmp_path / "commands.sh"
    module._write_text_atomically(output, "set -e\n")
    first_mtime = output.stat().st_mtime_ns
    module._write_text_atomically(output, "set -e\n")

    assert output.read_text(encoding="utf-8") == "set -e\n"
    assert output.stat().st_mtime_ns == first_mtime

    target = tmp_path / "target.sh"
    target.write_text("preserve\n", encoding="utf-8")
    output.unlink()
    try:
        output.symlink_to(target)
    except OSError:
        pytest.skip("symlinks are unavailable")
    with pytest.raises(ValueError, match="symlinked command output"):
        module._write_text_atomically(output, "replace\n")
    assert target.read_text(encoding="utf-8") == "preserve\n"


def test_downstream_plan_rejects_overlapping_owned_inputs_and_outputs(tmp_path: Path) -> None:
    """Owned cleanup and plan publication cannot consume or replace plan inputs."""
    module = _module()
    work_root = tmp_path / "work"
    isolated_root = work_root / "downstream"
    isolated_root.mkdir(parents=True)
    owned_wheel = isolated_root / "owned.whl"
    owned_wheel.write_bytes(b"preserve")
    outside_wheel = tmp_path / "outside.whl"
    outside_wheel.write_bytes(b"wheel")
    constraints = ROOT / "meta/ci/requirements/downstream.txt"
    outside_output = tmp_path / "commands.sh"
    invalid = (
        (owned_wheel, SCRIPT.parent, constraints, outside_output, "owned downstream root"),
        (outside_wheel, SCRIPT.parent, constraints, outside_wheel, "command output"),
        (
            outside_wheel,
            SCRIPT.parent,
            constraints,
            SCRIPT.parent / "commands.sh",
            "scripts root",
        ),
    )

    for wheel, scripts, constraint, command_output, message in invalid:
        with pytest.raises(ValueError, match=message):
            module.shell_plan(
                wheel,
                work_root,
                scripts,
                constraint,
                command_output=command_output,
            )
    assert owned_wheel.read_bytes() == b"preserve"
    assert outside_wheel.read_bytes() == b"wheel"


def test_downstream_plan_allows_read_only_inputs_to_share_a_tree(tmp_path: Path) -> None:
    """A constraints file may live beneath the read-only scripts directory."""
    module = _module()
    scripts = tmp_path / "support"
    scripts.mkdir()
    constraints = scripts / "constraints.txt"
    constraints.write_text("dependency==1\n", encoding="utf-8")
    wheel = tmp_path / "package.whl"
    wheel.write_bytes(b"wheel")

    plan = module.shell_plan(
        wheel,
        tmp_path / "work",
        scripts,
        constraints,
        command_output=tmp_path / "commands.sh",
    )

    assert constraints.as_posix() in plan


@pytest.mark.parametrize(("primary_status", "expected_status"), ((0, 23), (7, 7)))
def test_downstream_cleanup_traps_report_cleanup_only_failures(
    tmp_path: Path,
    primary_status: int,
    expected_status: int,
) -> None:
    """Generated and outer traps fail success but preserve an existing failure."""
    module = _module()
    wheel = tmp_path / "package.whl"
    wheel.write_bytes(b"wheel")
    constraints = ROOT / "meta/ci/requirements/downstream.txt"
    generated = module.shell_plan(
        wheel,
        tmp_path / f"work-{primary_status}",
        SCRIPT.parent,
        constraints,
        command_output=tmp_path / f"commands-{primary_status}.sh",
    )
    wrapper = SCRIPT.with_suffix(".sh").read_text(encoding="utf-8")
    scripts = (
        (generated, "trap cleanup_downstream EXIT"),
        (wrapper, "trap cleanup_plan EXIT"),
    )
    fake_bin = tmp_path / "fake-bin"
    fake_bin.mkdir(exist_ok=True)
    fake_rm = fake_bin / "rm"
    required_commands = {
        command: shutil.which(command) for command in ("bash", "dirname", "mktemp")
    }
    assert all(required_commands.values())
    bash = required_commands["bash"]
    assert bash is not None
    fake_rm.write_text("#!/usr/bin/env bash\nexit 23\n", encoding="utf-8")
    fake_rm.chmod(0o755)
    temporary_root = tmp_path / "temporary"
    temporary_root.mkdir(exist_ok=True)
    tool_directories = sorted(
        {str(Path(command).parent) for command in required_commands.values() if command is not None}
    )
    environment = {
        "LC_ALL": "C",
        "PATH": os.pathsep.join((str(fake_bin), *tool_directories)),
        "TMPDIR": str(temporary_root),
        "TZ": "UTC",
    }

    for content, marker in scripts:
        end = content.index(marker) + len(marker)
        preamble = content[:end] + f"\nexit {primary_status}\n"
        completed = subprocess.run(
            [bash, "-c", preamble],
            check=False,
            cwd=ROOT,
            env=environment,
        )
        assert completed.returncode == expected_status


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
