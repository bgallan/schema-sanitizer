"""Dependency-audit inventory tests.

These contracts keep vulnerability scans independent of the quality runner's host
markers while retaining exact, independently installable owner-lock versions.
"""

from __future__ import annotations

import hashlib
import json
import os
import subprocess
import venv
import zipfile
from pathlib import Path

import pytest

from meta.ci.quality import check_dependency_advisory_snapshot as advisories
from meta.ci.quality.build_dependency_audit_inputs import build_audit_inputs
from meta.ci.quality.check_action_pins import action_pin_violations
from meta.ci.quality.locked_requirements import (
    read_artifact_lock,
    read_owner_lock,
    render_hashed_requirements,
)
from meta.ci.quality.run_bounded_command import BoundedCommandError, run_bounded

ROOT = Path(__file__).resolve().parents[2]


def _fixture_wheel(directory: Path, name: str, dependency: str | None = None) -> Path:
    """Build a minimal valid pure-Python wheel for an offline hash-mode test."""
    distribution = name.replace("-", "_")
    wheel = directory / f"{distribution}-1.0-py3-none-any.whl"
    metadata = f"Metadata-Version: 2.1\nName: {name}\nVersion: 1.0\n"
    if dependency is not None:
        metadata += f"Requires-Dist: {dependency}==1.0\n"
    dist_info = f"{distribution}-1.0.dist-info"
    with zipfile.ZipFile(wheel, "w") as archive:
        archive.writestr(f"{distribution}/__init__.py", "__version__ = '1.0'\n")
        archive.writestr(f"{dist_info}/METADATA", metadata)
        archive.writestr(
            f"{dist_info}/WHEEL",
            "Wheel-Version: 1.0\nGenerator: schema-sanitizer-test\n"
            "Root-Is-Purelib: true\nTag: py3-none-any\n",
        )
        archive.writestr(f"{dist_info}/RECORD", "")
    return wheel


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


def test_artifact_lock_covers_every_exact_owner_pin() -> None:
    """Every package/version selected by an owner has reviewed artifact digests."""
    requirements_dir = ROOT / "meta/ci/requirements"
    artifact_lock = read_artifact_lock(requirements_dir / "python-artifact-sha256.lock")
    required = {
        requirement.key
        for path in requirements_dir.glob("*.txt")
        for requirement in read_owner_lock(path)
    }

    assert required == artifact_lock.keys()
    assert all(hashes for hashes in artifact_lock.values())


@pytest.mark.parametrize("install_all", (False, True))
def test_hash_locked_installer_works_offline_in_a_clean_environment(
    tmp_path: Path, install_all: bool
) -> None:
    """A clean bundled pip accepts complete hashed selections without an index."""
    wheelhouse = tmp_path / "wheelhouse"
    wheelhouse.mkdir()
    child = _fixture_wheel(wheelhouse, "fixture-child")
    root = _fixture_wheel(wheelhouse, "fixture-root", "fixture-child")
    owner_lock = tmp_path / "owner.txt"
    owner_lock.write_text("fixture-child==1.0\nfixture-root==1.0\n", encoding="utf-8")
    artifact_lock = tmp_path / "artifacts.lock"
    artifact_lock.write_text(
        "fixture-child==1.0 sha256:"
        + hashlib.sha256(child.read_bytes()).hexdigest()
        + "\nfixture-root==1.0 sha256:"
        + hashlib.sha256(root.read_bytes()).hexdigest()
        + "\n",
        encoding="utf-8",
    )
    environment = tmp_path / "environment"
    venv.EnvBuilder(with_pip=True).create(environment)
    python = environment / ("Scripts/python.exe" if os.name == "nt" else "bin/python")
    command = [
        python,
        ROOT / "meta/ci/quality/install_locked_requirements.py",
        "--lock",
        owner_lock,
        "--artifact-lock",
        artifact_lock,
        *(["--all"] if install_all else ["--packages", "fixture-root", "fixture-child"]),
    ]
    process_environment = {
        **os.environ,
        "PIP_DISABLE_PIP_VERSION_CHECK": "1",
        "PIP_FIND_LINKS": os.fspath(wheelhouse),
        "PIP_NO_INDEX": "1",
    }
    installed = subprocess.run(
        command,
        check=False,
        capture_output=True,
        text=True,
        env=process_environment,
    )

    assert installed.returncode == 0, installed.stderr
    versions = subprocess.run(
        [
            python,
            "-c",
            "import importlib.metadata as m; print("
            "m.version('fixture-root'), m.version('fixture-child'))",
        ],
        check=True,
        capture_output=True,
        text=True,
    )
    assert versions.stdout == "1.0 1.0\n"


def test_lock_parsers_reject_malformed_pins_and_hashes(tmp_path: Path) -> None:
    """Owner and artifact locks fail closed on floating or unauthenticated input."""
    owner = tmp_path / "owner.txt"
    owner.write_text("example>=1\n", encoding="utf-8")
    with pytest.raises(ValueError, match="exact pin"):
        read_owner_lock(owner)
    artifact = tmp_path / "artifacts.lock"
    artifact.write_text("example==1 sha256:not-a-digest\n", encoding="utf-8")
    with pytest.raises(ValueError, match="SHA-256"):
        read_artifact_lock(artifact)


def test_hashed_requirement_rendering_preserves_owner_markers(tmp_path: Path) -> None:
    """Platform markers survive while every selected artifact remains hashed."""
    owner = tmp_path / "owner.txt"
    owner.write_text('example==1; sys_platform == "win32"\n', encoding="utf-8")
    rendered = render_hashed_requirements(
        read_owner_lock(owner), {"example==1": (f"sha256:{'a' * 64}",)}
    )

    assert rendered == (f'example==1; sys_platform == "win32" \\\n    --hash=sha256:{"a" * 64}\n')


def _advisory_fixture(tmp_path: Path) -> tuple[Path, Path, Path, Path]:
    """Create one canonical advisory inventory and empty reviewed snapshot."""
    project = tmp_path / "pyproject.toml"
    requirements = tmp_path / "requirements"
    requirements.mkdir()
    _write_project(project)
    (requirements / "owner.txt").write_text("example==1\n", encoding="utf-8")
    artifact = tmp_path / "python-artifact-sha256.lock"
    artifact.write_text(f"example==1 sha256:{'a' * 64}\n", encoding="utf-8")
    snapshot = tmp_path / "dependency-advisories.json"
    payload = {
        "artifact_lock": artifact.name,
        "auditor": advisories.AUDITOR,
        "inputs": advisories.dependency_inventory(project, requirements, artifact),
        "schema": advisories.SCHEMA_VERSION,
        "vulnerabilities": [],
    }
    snapshot.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return project, requirements, artifact, snapshot


def test_reviewed_advisory_snapshot_is_bound_to_every_input(tmp_path: Path) -> None:
    """Canonical snapshot verification binds pyproject, owner locks, and hashes."""
    project, requirements, artifact, snapshot = _advisory_fixture(tmp_path)
    advisories.verify_snapshot(snapshot, project, requirements, artifact)

    project.write_text(project.read_text(encoding="utf-8") + "# changed\n", encoding="utf-8")
    with pytest.raises(ValueError, match="stale"):
        advisories.verify_snapshot(snapshot, project, requirements, artifact)


@pytest.mark.parametrize("field", ("schema", "auditor", "inputs", "vulnerabilities"))
def test_reviewed_advisory_snapshot_rejects_policy_tampering(tmp_path: Path, field: str) -> None:
    """Schema, tool identity, inventory, and vulnerability policy are fail closed."""
    project, requirements, artifact, snapshot = _advisory_fixture(tmp_path)
    payload = json.loads(snapshot.read_text(encoding="utf-8"))
    replacements = {
        "schema": 2,
        "auditor": "pip-audit==0",
        "inputs": {},
        "vulnerabilities": [{"id": "TEST-1"}],
    }
    payload[field] = replacements[field]
    snapshot.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")

    with pytest.raises(ValueError):
        advisories.verify_snapshot(snapshot, project, requirements, artifact)


def test_advisory_candidate_preserves_findings_without_turning_drift_red(
    tmp_path: Path,
) -> None:
    """Maintenance validation accepts canonical findings for human review only."""
    project, requirements, artifact, snapshot = _advisory_fixture(tmp_path)
    payload = json.loads(snapshot.read_text(encoding="utf-8"))
    payload["vulnerabilities"] = [{"id": "TEST-1"}]
    snapshot.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")

    advisories.verify_snapshot(
        snapshot,
        project,
        requirements,
        artifact,
        allow_vulnerabilities=True,
    )


def test_advisory_refresh_accepts_only_status_one_as_findings(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """pip-audit status one is parsed once rather than retried as transport noise."""
    project, requirements, artifact, _snapshot = _advisory_fixture(tmp_path)
    audit_input = tmp_path / "locked-owner.txt"
    audit_input.write_text("example==1\n", encoding="utf-8")
    monkeypatch.setattr(advisories, "build_audit_inputs", lambda *_args: (audit_input,))
    monkeypatch.setattr(advisories.importlib.metadata, "version", lambda _name: "2.10.1")
    calls = 0

    def findings(command: list[str], *, timeout_seconds: int, label: str) -> None:
        """Write a valid findings report and expose pip-audit's status one."""
        nonlocal calls
        calls += 1
        assert timeout_seconds == 180 and label == "dependency-audit-locked-owner"
        assert "--strict" in command
        report = Path(command[command.index("--output") + 1])
        report.write_text(
            json.dumps(
                {
                    "dependencies": [
                        {
                            "name": "example",
                            "version": "1",
                            "vulns": [{"id": "TEST-1", "aliases": [], "fix_versions": ["2"]}],
                        }
                    ]
                }
            ),
            encoding="utf-8",
        )
        raise BoundedCommandError(1, label)

    monkeypatch.setattr(advisories, "run_bounded", findings)
    payload = advisories.refresh_snapshot(
        project, requirements, artifact, tmp_path / "candidate.json"
    )

    assert calls == 1
    assert payload["vulnerabilities"][0]["id"] == "TEST-1"


def test_advisory_normalization_rejects_skipped_dependencies() -> None:
    """A dependency omitted by the advisory backend cannot look vulnerability-free."""
    report = {
        "dependencies": [
            {"name": "example", "version": "1", "vulns": [], "skip_reason": "unsupported"}
        ]
    }

    with pytest.raises(ValueError, match="skipped"):
        advisories._normalized_vulnerabilities(report, "owner.txt", {"example": "1"})


@pytest.mark.parametrize(
    "dependencies",
    (
        [],
        [
            {"name": "example", "version": "1", "vulns": []},
            {"name": "example", "version": "1", "vulns": []},
        ],
        [{"name": "example", "version": "2", "vulns": []}],
        [{"name": "extra", "version": "1", "vulns": []}],
    ),
)
def test_advisory_normalization_requires_the_exact_audited_inventory(
    dependencies: list[dict[str, object]],
) -> None:
    """Omitted, duplicate, wrong-version, and extra report entries are rejected."""
    with pytest.raises(ValueError):
        advisories._normalized_vulnerabilities(
            {"dependencies": dependencies}, "owner.txt", {"example": "1"}
        )


def test_advisory_refresh_retries_only_nonfinding_failures(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Transient audit statuses get exactly two bounded retries with clean reports."""
    project, requirements, artifact, _snapshot = _advisory_fixture(tmp_path)
    audit_input = tmp_path / "locked-owner.txt"
    audit_input.write_text("example==1\n", encoding="utf-8")
    monkeypatch.setattr(advisories, "build_audit_inputs", lambda *_args: (audit_input,))
    monkeypatch.setattr(advisories.importlib.metadata, "version", lambda _name: "2.10.1")
    sleeps: list[int] = []
    monkeypatch.setattr(advisories.time, "sleep", sleeps.append)
    calls = 0

    def transient(command: list[str], *, timeout_seconds: int, label: str) -> None:
        """Leave partial reports twice, then return one valid clean report."""
        nonlocal calls
        calls += 1
        assert timeout_seconds == 180
        report = Path(command[command.index("--output") + 1])
        if calls < 3:
            report.write_text("partial", encoding="utf-8")
            raise BoundedCommandError(2, label)
        assert not report.exists()
        report.write_text(
            '{"dependencies": [{"name": "example", "version": "1", "vulns": []}]}',
            encoding="utf-8",
        )

    monkeypatch.setattr(advisories, "run_bounded", transient)
    payload = advisories.refresh_snapshot(
        project, requirements, artifact, tmp_path / "candidate.json"
    )

    assert calls == 3
    assert sleeps == [1, 2]
    assert payload["vulnerabilities"] == []


def test_advisory_refresh_retries_status_one_without_a_report(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Status one is a finding only with a fresh report; otherwise it is retried."""
    project, requirements, artifact, _snapshot = _advisory_fixture(tmp_path)
    audit_input = tmp_path / "locked-owner.txt"
    audit_input.write_text("example==1\n", encoding="utf-8")
    monkeypatch.setattr(advisories, "build_audit_inputs", lambda *_args: (audit_input,))
    monkeypatch.setattr(advisories.importlib.metadata, "version", lambda _name: "2.10.1")
    sleeps: list[int] = []
    monkeypatch.setattr(advisories.time, "sleep", sleeps.append)
    calls = 0

    def missing_report(command: list[str], *, timeout_seconds: int, label: str) -> None:
        """Return two ambiguous status-one failures, then one clean report."""
        nonlocal calls
        calls += 1
        assert timeout_seconds == 180
        report = Path(command[command.index("--output") + 1])
        assert not report.exists()
        if calls < 3:
            raise BoundedCommandError(1, label)
        report.write_text(
            '{"dependencies": [{"name": "example", "version": "1", "vulns": []}]}',
            encoding="utf-8",
        )

    monkeypatch.setattr(advisories, "run_bounded", missing_report)
    payload = advisories.refresh_snapshot(
        project, requirements, artifact, tmp_path / "candidate.json"
    )

    assert calls == 3
    assert sleeps == [1, 2]
    assert payload["vulnerabilities"] == []


def test_bounded_runner_converts_timeout_and_preserves_exit_status(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Watchdog timeout and child failure remain distinguishable to callers."""
    from meta.ci.quality import run_bounded_command

    monkeypatch.setattr(run_bounded_command.os, "spawnv", lambda *_args: 124)
    with pytest.raises(TimeoutError, match="timed out"):
        run_bounded(["python", "-V"], timeout_seconds=1, label="fixture")
    monkeypatch.setattr(run_bounded_command.os, "spawnv", lambda *_args: 7)
    with pytest.raises(BoundedCommandError) as raised:
        run_bounded(["python", "-V"], timeout_seconds=1, label="fixture")
    assert raised.value.status == 7


def test_action_pin_scanner_covers_nested_composites_and_reusable_workflows(
    tmp_path: Path,
) -> None:
    """Only local references or lowercase full commit IDs pass recursively."""
    workflows = tmp_path / ".github/workflows"
    nested = tmp_path / ".github/actions/nested/example"
    workflows.mkdir(parents=True)
    nested.mkdir(parents=True)
    workflows.joinpath("valid.yml").write_text(
        f"steps:\n  - uses : 'owner/action@{'a' * 40}'\n  - uses: ./local\n",
        encoding="utf-8",
    )
    nested.joinpath("action.yml").write_text(
        "runs:\n  using: composite\n  steps:\n"
        "    - uses: owner/tag@v1\n"
        "    - uses: owner/short@abcdef0\n"
        f"    - uses: owner/upper@{'A' * 40}\n"
        "    - uses: owner/repository/.github/workflows/ci.yml@main\n"
        f"    - uses: owner/action@{'b' * 40}\n"
        "      uses: owner/duplicate@main\n",
        encoding="utf-8",
    )

    violations = action_pin_violations(tmp_path)

    assert len(violations) == 5
    assert all("nested/example/action.yml" in violation for violation in violations)
