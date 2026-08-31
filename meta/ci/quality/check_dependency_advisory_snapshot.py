#!/usr/bin/env python3
"""Verify or refresh the reviewed vulnerability snapshot for exact owner locks.

Pull-request CI performs only deterministic digest and policy verification.  The
separate scheduled advisory workflow runs pip-audit against live advisory data,
uploads a candidate report, and requires a reviewed commit whenever that report or
the dependency inventory changes.
"""

from __future__ import annotations

import argparse
import hashlib
import importlib.metadata
import json
import os
import sys
import tempfile
import time
from pathlib import Path
from typing import Any, Sequence

from packaging.requirements import Requirement
from packaging.utils import canonicalize_name
from packaging.version import Version

from meta.ci.quality.build_dependency_audit_inputs import build_audit_inputs
from meta.ci.quality.run_bounded_command import BoundedCommandError, run_bounded

SCHEMA_VERSION = 1
AUDITOR = "pip-audit==2.10.1"


def _sha256(path: Path) -> str:
    """Return the lowercase SHA-256 digest of one regular repository input."""
    if path.is_symlink() or not path.is_file():
        raise ValueError(f"advisory input must be a regular file: {path}")
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _write_atomically(destination: Path, content: str) -> None:
    """Replace one regular snapshot without following a pre-created path alias."""
    if destination.is_symlink() or (destination.exists() and not destination.is_file()):
        raise ValueError(f"advisory snapshot output is unsafe: {destination}")
    destination.parent.mkdir(parents=True, exist_ok=True)
    descriptor, temporary_name = tempfile.mkstemp(
        prefix=f".{destination.name}.", suffix=".tmp", dir=destination.parent
    )
    temporary = Path(temporary_name)
    try:
        with os.fdopen(descriptor, "w", encoding="utf-8", newline="\n") as handle:
            handle.write(content)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, destination)
    finally:
        temporary.unlink(missing_ok=True)


def dependency_inventory(
    pyproject: Path, requirements_dir: Path, artifact_lock: Path
) -> dict[str, str]:
    """Return stable content digests for every advisory and artifact-lock input."""
    locks = sorted(requirements_dir.glob("*.txt"))
    if not locks:
        raise ValueError(f"no owner locks found below {requirements_dir}")
    inventory = {f"requirements/{path.name}": _sha256(path) for path in locks}
    inventory["pyproject.toml"] = _sha256(pyproject)
    inventory[artifact_lock.name] = _sha256(artifact_lock)
    return inventory


def _expected_inventory(requirements: Path) -> dict[str, str]:
    """Return the exact canonical package/version inventory in one audit input."""
    expected: dict[str, str] = {}
    for line in requirements.read_text(encoding="utf-8").splitlines():
        requirement = Requirement(line)
        specifiers = tuple(requirement.specifier)
        if len(specifiers) != 1 or specifiers[0].operator != "==":
            raise ValueError(f"advisory input is not exactly pinned: {requirements}: {line}")
        name = canonicalize_name(requirement.name)
        if name in expected:
            raise ValueError(f"advisory input contains a duplicate package: {name}")
        expected[name] = str(Version(specifiers[0].version))
    if not expected:
        raise ValueError(f"advisory input cannot be empty: {requirements}")
    return expected


def _normalized_vulnerabilities(
    report: dict[str, Any], owner: str, expected: dict[str, str]
) -> list[dict[str, Any]]:
    """Extract stable vulnerability identity and remediation fields from pip-audit."""
    findings: list[dict[str, Any]] = []
    dependencies = report.get("dependencies")
    if not isinstance(dependencies, list):
        raise ValueError(f"pip-audit report has no dependency list for {owner}")
    observed: dict[str, str] = {}
    for dependency in dependencies:
        if not isinstance(dependency, dict):
            raise ValueError(f"pip-audit dependency is not an object for {owner}")
        if "skip_reason" in dependency:
            raise ValueError(f"pip-audit skipped a dependency for {owner}")
        try:
            name = canonicalize_name(dependency["name"])
            version = str(Version(dependency["version"]))
        except (KeyError, TypeError, ValueError) as error:
            raise ValueError(f"pip-audit dependency identity is malformed for {owner}") from error
        if name in observed:
            raise ValueError(f"pip-audit duplicated dependency {name} for {owner}")
        observed[name] = version
        for vulnerability in dependency.get("vulns", ()):
            findings.append(
                {
                    "aliases": sorted(vulnerability.get("aliases", ())),
                    "fix_versions": sorted(vulnerability.get("fix_versions", ())),
                    "id": vulnerability["id"],
                    "owner": owner,
                    "package": name,
                    "version": version,
                }
            )
    if observed != expected:
        raise ValueError(
            f"pip-audit dependency inventory mismatch for {owner}: "
            f"expected={sorted(expected.items())}, observed={sorted(observed.items())}"
        )
    return findings


def refresh_snapshot(
    pyproject: Path,
    requirements_dir: Path,
    artifact_lock: Path,
    output: Path,
) -> dict[str, Any]:
    """Query current advisory data and write one canonical candidate snapshot."""
    installed_auditor = f"pip-audit=={importlib.metadata.version('pip-audit')}"
    if installed_auditor != AUDITOR:
        raise RuntimeError(f"advisory refresh requires {AUDITOR}, found {installed_auditor}")
    findings: list[dict[str, Any]] = []
    with tempfile.TemporaryDirectory(prefix="schema-sanitizer-audit-") as directory:
        audit_inputs = build_audit_inputs(
            pyproject, requirements_dir, Path(directory) / "requirements"
        )
        for requirements in audit_inputs:
            report_path = Path(directory) / f"{requirements.stem}.json"
            command = [
                sys.executable,
                "-m",
                "pip_audit",
                "--requirement",
                str(requirements),
                "--no-deps",
                "--strict",
                "--disable-pip",
                "--format",
                "json",
                "--output",
                str(report_path),
                "--progress-spinner",
                "off",
                "--timeout",
                "15",
            ]
            for attempt in range(3):
                report_path.unlink(missing_ok=True)
                try:
                    run_bounded(
                        command,
                        timeout_seconds=180,
                        label=f"dependency-audit-{requirements.stem}",
                    )
                except (BoundedCommandError, TimeoutError) as error:
                    # pip-audit documents status 1 for findings only when it also
                    # produced the requested report. A status-one exit without a
                    # report can still be transport failure, so retry it normally.
                    status_one = isinstance(error, BoundedCommandError) and error.status == 1
                    if status_one and report_path.is_file():
                        break
                    if attempt == 2:
                        if status_one:
                            raise RuntimeError(
                                "pip-audit repeatedly exited with status one without a JSON report"
                            ) from error
                        raise
                    time.sleep(attempt + 1)
                else:
                    break
            findings.extend(
                _normalized_vulnerabilities(
                    json.loads(report_path.read_text(encoding="utf-8")),
                    requirements.name,
                    _expected_inventory(requirements),
                )
            )
    payload = {
        "artifact_lock": artifact_lock.name,
        "auditor": AUDITOR,
        "inputs": dependency_inventory(pyproject, requirements_dir, artifact_lock),
        "schema": SCHEMA_VERSION,
        "vulnerabilities": sorted(
            findings,
            key=lambda item: (item["owner"], item["package"], item["version"], item["id"]),
        ),
    }
    _write_atomically(output, json.dumps(payload, indent=2, sort_keys=True) + "\n")
    return payload


def verify_snapshot(
    snapshot: Path,
    pyproject: Path,
    requirements_dir: Path,
    artifact_lock: Path,
    *,
    allow_vulnerabilities: bool = False,
) -> None:
    """Require a canonical, current, vulnerability-free reviewed snapshot."""
    if snapshot.is_symlink() or not snapshot.is_file():
        raise ValueError(f"advisory snapshot must be a regular file: {snapshot}")
    payload = json.loads(snapshot.read_text(encoding="utf-8"))
    expected_inventory = dependency_inventory(pyproject, requirements_dir, artifact_lock)
    if payload.get("schema") != SCHEMA_VERSION:
        raise ValueError(f"unsupported advisory snapshot schema: {payload.get('schema')}")
    if payload.get("auditor") != AUDITOR:
        raise ValueError(f"unexpected advisory snapshot auditor: {payload.get('auditor')}")
    if payload.get("artifact_lock") != artifact_lock.name:
        raise ValueError("advisory snapshot names the wrong artifact lock")
    if payload.get("inputs") != expected_inventory:
        raise ValueError("advisory snapshot is stale for the current dependency locks")
    vulnerabilities = payload.get("vulnerabilities")
    if not isinstance(vulnerabilities, list):
        raise ValueError("advisory snapshot vulnerability inventory must be a list")
    if vulnerabilities and not allow_vulnerabilities:
        identifiers = sorted({finding.get("id", "unknown") for finding in vulnerabilities})
        raise ValueError(
            "reviewed dependency snapshot contains vulnerabilities: " + ", ".join(identifiers)
        )
    canonical = json.dumps(payload, indent=2, sort_keys=True) + "\n"
    if snapshot.read_text(encoding="utf-8") != canonical:
        raise ValueError("advisory snapshot is not in canonical JSON form")


def main(argv: Sequence[str] | None = None) -> int:
    """Run deterministic verification or a live maintenance refresh."""
    root = Path(__file__).resolve().parents[3]
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("mode", choices=("verify", "verify-candidate", "refresh"))
    parser.add_argument(
        "--snapshot",
        type=Path,
        default=root / "meta/ci/requirements/dependency-advisories.json",
    )
    parser.add_argument("--pyproject", type=Path, default=root / "pyproject.toml")
    parser.add_argument("--requirements-dir", type=Path, default=root / "meta/ci/requirements")
    parser.add_argument(
        "--artifact-lock",
        type=Path,
        default=root / "meta/ci/requirements/python-artifact-sha256.lock",
    )
    args = parser.parse_args(argv)
    try:
        if args.mode == "refresh":
            refresh_snapshot(
                args.pyproject, args.requirements_dir, args.artifact_lock, args.snapshot
            )
        else:
            verify_snapshot(
                args.snapshot,
                args.pyproject,
                args.requirements_dir,
                args.artifact_lock,
                allow_vulnerabilities=args.mode == "verify-candidate",
            )
    except (OSError, RuntimeError, ValueError, json.JSONDecodeError) as error:
        parser.error(str(error))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
