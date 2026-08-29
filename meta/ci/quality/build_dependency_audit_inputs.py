#!/usr/bin/env python3
"""Build dependency-audit inputs from exact executable-environment locks.

Each owner lock remains an independent audit target. A static coverage check first
proves that project declarations, build requirements, and directly executed CI tools
have a compatible exact pin in at least one lock, avoiding any live-index resolution
of a synthetic union that CI never installs.
"""

from __future__ import annotations

import argparse
import tomllib
from pathlib import Path
from typing import Sequence

from packaging.requirements import Requirement
from packaging.utils import canonicalize_name
from packaging.version import Version

CI_TOOLS = (
    "abi3audit==0.0.26",
    "actionlint-py==1.7.12.24",
    "bandit==1.9.4",
    "build==1.5.0",
    "cibuildwheel==4.2.0",
    "clang-format==22.1.8",
    "cmake==4.3.4",
    "cmakelang==0.6.13",
    "coverage==7.15.4",
    "detect-secrets==1.5.0",
    "mdformat==1.0.0",
    "mypy==1.19.1",
    "ninja==1.13.0",
    "packaging==26.3",
    "pip==26.2.1",
    "pip-audit==2.10.1",
    "polars==1.43.2",
    "pre-commit==4.6.2",
    "pre-commit-hooks==6.0.0",
    "pyarrow==25.0.1",
    "pypi-attestations==0.0.30",
    "pytest==9.1.1",
    "ruff==0.16.2",
    "scikit-build-core==0.11.6",
    "shellcheck-py==0.11.0.1",
    "shfmt-py==4.0.0",
    "toml-sort==0.24.3",
    "twine==7.0.0",
    "yamlfix==1.18.0",
    "zizmor==1.29.0",
)


def _lock_requirements(path: Path) -> tuple[str, ...]:
    """Read the effective requirement lines from one pinned environment lock."""
    return tuple(
        line.strip()
        for line in path.read_text(encoding="utf-8").splitlines()
        if line.strip() and not line.lstrip().startswith("#")
    )


def _declared_requirements(pyproject_path: Path, ci_tools: Sequence[str]) -> tuple[str, ...]:
    """Return every project, build, and executed-tool dependency declaration."""
    pyproject = tomllib.loads(pyproject_path.read_text(encoding="utf-8"))
    project = pyproject.get("project", {})
    return tuple(
        dict.fromkeys(
            (
                *project.get("dependencies", ()),
                *pyproject.get("build-system", {}).get("requires", ()),
                *(
                    requirement
                    for values in project.get("optional-dependencies", {}).values()
                    for requirement in values
                ),
                *ci_tools,
            )
        )
    )


def validate_owner_lock_coverage(
    pyproject_path: Path, requirements_dir: Path, ci_tools: Sequence[str] = CI_TOOLS
) -> None:
    """Require a compatible exact owner-lock pin for every declared dependency."""
    locked_versions: dict[str, set[Version]] = {}
    for path in sorted(requirements_dir.glob("*.txt")):
        for line in _lock_requirements(path):
            requirement = Requirement(line)
            specifiers = tuple(requirement.specifier)
            if (
                requirement.url is not None
                or requirement.extras
                or len(specifiers) != 1
                or specifiers[0].operator != "=="
                or "*" in specifiers[0].version
            ):
                raise ValueError(f"owner lock entry must be one exact pin: {path}: {line}")
            locked_versions.setdefault(canonicalize_name(requirement.name), set()).add(
                Version(specifiers[0].version)
            )

    uncovered: list[str] = []
    for declaration in _declared_requirements(pyproject_path, ci_tools):
        requirement = Requirement(declaration)
        candidates = locked_versions.get(canonicalize_name(requirement.name), set())
        if not candidates or not any(
            not requirement.specifier or candidate in requirement.specifier
            for candidate in candidates
        ):
            uncovered.append(declaration)
    if uncovered:
        raise ValueError(
            "dependencies without a compatible owner-lock pin: " + ", ".join(uncovered)
        )


def build_audit_inputs(
    pyproject_path: Path,
    requirements_dir: Path,
    output_dir: Path,
    *,
    ci_tools: Sequence[str] = CI_TOOLS,
) -> tuple[Path, ...]:
    """Materialize one stable audit input per independently resolvable environment."""
    if output_dir.resolve() == requirements_dir.resolve():
        raise ValueError("audit output directory cannot overwrite requirement locks")

    validate_owner_lock_coverage(pyproject_path, requirements_dir, ci_tools)
    documents = {
        f"locked-{path.stem}.txt": _lock_requirements(path)
        for path in sorted(requirements_dir.glob("*.txt"))
    }

    output_dir.mkdir(parents=True, exist_ok=True)
    expected = {output_dir / name for name in documents}
    for stale in sorted(output_dir.glob("*.txt")):
        if stale not in expected:
            if stale.is_dir() and not stale.is_symlink():
                raise ValueError(f"stale audit input is not a file: {stale}")
            stale.unlink()

    outputs: list[Path] = []
    for name, requirements in sorted(documents.items()):
        if not requirements:
            raise ValueError(f"dependency audit input cannot be empty: {name}")
        output = output_dir / name
        if output.is_symlink() or (output.exists() and not output.is_file()):
            raise ValueError(f"dependency audit output must be a regular file: {output}")
        output.write_text("\n".join(requirements) + "\n", encoding="utf-8")
        outputs.append(output)
    return tuple(outputs)


def main(argv: Sequence[str] | None = None) -> int:
    """Parse paths, build every audit input, and print them in execution order."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--pyproject", type=Path, default=Path("pyproject.toml"))
    parser.add_argument("--requirements-dir", type=Path, default=Path("meta/ci/requirements"))
    parser.add_argument("--output-dir", type=Path, default=Path(".work/audit"))
    args = parser.parse_args(argv)
    for path in build_audit_inputs(args.pyproject, args.requirements_dir, args.output_dir):
        print(path.as_posix())
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
