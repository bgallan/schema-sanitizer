#!/usr/bin/env python3
"""Fail the CI quality gate on secrets outside narrow public-data exclusions.

It recognizes narrowly approved public digests and revisions while retaining every
actionable scanner finding.
"""

from __future__ import annotations

import argparse
import ast
import hashlib
import json
import re
import stat
from pathlib import Path
from typing import Any

_PUBLIC_SHA256_LINE = re.compile(r'^\s*"[^"]+_sha256"\s*:\s*"[0-9a-fA-F]{64}"\s*,?\s*$')
_FUZZ_INTEGRITY_SHA256_LINE = re.compile(
    r'^\s*(?:EXPECTED_TREE_SHA256\s*=|"[^"]+"\s*:)\s*"[0-9a-fA-F]{64}"\s*,?\s*$'
)
_PUBLIC_READER_REFERENCE_COMMIT_LINE = re.compile(
    r'^\s*"commit_sha"\s*:\s*"[0-9a-fA-F]{40}"\s*,?\s*$'
)
_PINNED_PRE_COMMIT_REVISION = re.compile(r"^\s+rev:\s+[0-9a-f]{40}\s+#\s+[^\s]+\s*$")
_WINDOWS_RUNTIME_POLICY = "meta/ci/native/windows-release-toolchain.json"
_WINDOWS_RUNTIME_DIGEST_LINE = re.compile(
    r'^\s*"(?P<member>schema_sanitizer\.libs/[a-z0-9][a-z0-9_.-]*-[0-9a-f]{32}\.dll)"'
    r'\s*:\s*"(?P<digest>[0-9a-f]{64})"\s*,?\s*$'
)
_PINNED_PIP_HELPER = "meta/ci/quality/ensure_pinned_pip.py"
_PINNED_PIP_DIGEST_LINE = re.compile(r'^PIP_SHA256\s*=\s*"(?P<digest>[0-9a-f]{64})"\s*$')
_PYTHON_ARTIFACT_LOCK = "meta/ci/requirements/python-artifact-sha256.lock"
_PLATFORM_EVIDENCE_HELPER = "meta/ci/quality/platform_test_evidence.py"
_PLATFORM_EVIDENCE_DIGEST_LINE = re.compile(
    r'^\s*(?:"sha256"|\))\s*:\s*"(?P<digest>[0-9a-f]{64})"\s*,?\s*$'
)
_ADVISORY_SNAPSHOT = "meta/ci/requirements/dependency-advisories.json"
_ADVISORY_INPUT_DIGEST_LINE = re.compile(
    r'^\s*"(?P<member>[A-Za-z0-9./-]+)"\s*:\s*"(?P<digest>[0-9a-f]{64})"\s*,?\s*$'
)
_ADVISORY_KEYS = {"artifact_lock", "auditor", "inputs", "schema", "vulnerabilities"}


def _literal_assignments(root: Path, filename: str, names: set[str]) -> dict[str, Any]:
    """Load unique top-level literal assignments without executing repository code."""
    path = root / filename
    try:
        mode = path.lstat().st_mode
        if path.is_symlink() or not stat.S_ISREG(mode):
            return {}
        tree = ast.parse(path.read_text(encoding="utf-8"), filename=filename)
    except (OSError, UnicodeError, SyntaxError):
        return {}

    assignments: dict[str, Any] = {}
    for node in tree.body:
        targets: list[ast.expr]
        if isinstance(node, ast.Assign):
            targets = node.targets
            value = node.value
        elif isinstance(node, ast.AnnAssign):
            targets = [node.target]
            value = node.value
        else:
            continue
        for target in targets:
            if not isinstance(target, ast.Name) or target.id not in names:
                continue
            if target.id in assignments or value is None:
                return {}
            try:
                assignments[target.id] = ast.literal_eval(value)
            except (ValueError, TypeError, MemoryError, RecursionError):
                return {}
    return assignments if set(assignments) == names else {}


def _locked_pip_hashes(root: Path, version: str) -> set[str]:
    """Return the exact SHA-256 set locked for one pip version, or an empty set."""
    lock = root / _PYTHON_ARTIFACT_LOCK
    try:
        mode = lock.lstat().st_mode
        if lock.is_symlink() or not stat.S_ISREG(mode):
            return set()
        lines = lock.read_text(encoding="utf-8").splitlines()
    except (OSError, UnicodeError):
        return set()
    entries = [line.split() for line in lines if line.startswith("pip==")]
    if len(entries) != 1 or not entries[0] or entries[0][0] != f"pip=={version}":
        return set()
    hashes = {
        token.removeprefix("sha256:")
        for token in entries[0][1:]
        if re.fullmatch(r"sha256:[0-9a-f]{64}", token) is not None
    }
    return hashes if len(hashes) == len(entries[0]) - 1 and hashes else set()


def _platform_inventory_hashes(root: Path) -> set[str]:
    """Return every structurally valid reviewed platform inventory fingerprint."""
    assignments = _literal_assignments(
        root,
        _PLATFORM_EVIDENCE_HELPER,
        {"EXPECTED_EXACT_SKIP_INVENTORY_SHA256", "EXPECTED_TEST_INVENTORY"},
    )
    test_inventory = assignments.get("EXPECTED_TEST_INVENTORY")
    skip_inventory = assignments.get("EXPECTED_EXACT_SKIP_INVENTORY_SHA256")
    if not isinstance(test_inventory, dict) or not isinstance(skip_inventory, dict):
        return set()

    hashes: set[str] = set()
    for component, policy in test_inventory.items():
        if (
            not isinstance(component, str)
            or not isinstance(policy, dict)
            or set(policy) != {"count", "sha256"}
            or type(policy.get("count")) is not int
            or policy["count"] < 1
            or not isinstance(policy.get("sha256"), str)
            or re.fullmatch(r"[0-9a-f]{64}", policy["sha256"]) is None
        ):
            return set()
        hashes.add(policy["sha256"])
    for identity, digest in skip_inventory.items():
        if (
            not isinstance(identity, tuple)
            or len(identity) != 2
            or any(not isinstance(part, str) or not part for part in identity)
            or not isinstance(digest, str)
            or re.fullmatch(r"[0-9a-f]{64}", digest) is None
        ):
            return set()
        hashes.add(digest)
    return hashes


def _regular_file_sha256(path: Path) -> str | None:
    """Return one regular non-symlinked file digest, or ``None`` when unsafe."""
    try:
        mode = path.lstat().st_mode
        if path.is_symlink() or not stat.S_ISREG(mode):
            return None
        return hashlib.sha256(path.read_bytes()).hexdigest()
    except OSError:
        return None


def _advisory_input_path(root: Path, member: str) -> Path | None:
    """Resolve one recognized advisory input identity without path traversal."""
    if member == "pyproject.toml":
        return root / member
    if member == "python-artifact-sha256.lock":
        return root / "meta/ci/requirements" / member
    if re.fullmatch(r"requirements/[A-Za-z0-9._-]+\.txt", member):
        return root / "meta/ci/requirements" / Path(member).name
    return None


def _source_line(root: Path, filename: str, line_number: int | None) -> str:
    """Return one reported source line, or an empty string when unavailable."""
    if line_number is None or line_number < 1:
        return ""
    try:
        return (root / filename).read_text(encoding="utf-8").splitlines()[line_number - 1]
    except (OSError, IndexError):
        return ""


def _is_notebook_cell_id(root: Path, filename: str, line_number: int | None) -> bool:
    """Recognize random Jupyter cell identifiers, which are not credentials."""
    if not filename.endswith(".ipynb"):
        return False
    stripped = _source_line(root, filename, line_number).strip()
    return stripped.startswith('"id":') and (stripped.endswith('"') or stripped.endswith('",'))


def _is_public_benchmark_digest(root: Path, filename: str, item: dict[str, Any]) -> bool:
    """Recognize checked-in SHA-256 artifact evidence under benchmarks/."""
    if item.get("type") != "Hex High Entropy String":
        return False
    path = Path(filename)
    if not path.parts or path.parts[0] != "benchmarks":
        return False
    line = _source_line(root, filename, item.get("line_number"))
    return _PUBLIC_SHA256_LINE.fullmatch(line) is not None


def _is_public_fuzz_integrity_digest(root: Path, filename: str, item: dict[str, Any]) -> bool:
    """Recognize the byte-integrity manifest for checked-in fuzz inputs."""
    if item.get("type") != "Hex High Entropy String":
        return False
    if filename != "meta/ci/fuzz/check_fuzz_corpus.py":
        return False
    line = _source_line(root, filename, item.get("line_number"))
    return _FUZZ_INTEGRITY_SHA256_LINE.fullmatch(line) is not None


def _is_public_reader_reference_commit(root: Path, filename: str, item: dict[str, Any]) -> bool:
    """Recognize the public Git commit anchoring the reader latency policy."""
    if item.get("type") != "Hex High Entropy String":
        return False
    if filename != "benchmarks/readers/linear_scaling_budget.json":
        return False
    line = _source_line(root, filename, item.get("line_number"))
    return _PUBLIC_READER_REFERENCE_COMMIT_LINE.fullmatch(line) is not None


def _is_pinned_pre_commit_revision(root: Path, filename: str, item: dict[str, Any]) -> bool:
    """Recognize immutable public Git commit pins for remote hooks."""
    if item.get("type") != "Hex High Entropy String" or filename != ".pre-commit-config.yaml":
        return False
    line = _source_line(root, filename, item.get("line_number"))
    return _PINNED_PRE_COMMIT_REVISION.fullmatch(line) is not None


def _is_public_windows_runtime_digest(root: Path, filename: str, item: dict[str, Any]) -> bool:
    """Recognize canonical SHA-256 evidence for bundled Windows runtime DLLs."""
    if item.get("type") != "Hex High Entropy String" or filename != _WINDOWS_RUNTIME_POLICY:
        return False
    match = _WINDOWS_RUNTIME_DIGEST_LINE.fullmatch(
        _source_line(root, filename, item.get("line_number"))
    )
    if match is None:
        return False
    try:
        serialized = (root / filename).read_text(encoding="utf-8")
        payload = json.loads(serialized)
    except (OSError, json.JSONDecodeError):
        return False
    if not isinstance(payload, dict):
        return False
    if serialized != json.dumps(payload, indent=2, sort_keys=True, ensure_ascii=False) + "\n":
        return False
    runtimes = payload.get("wheel_runtime_dlls")
    return (
        payload.get("format") == "schema-sanitizer-windows-toolchain-v1"
        and isinstance(runtimes, dict)
        and runtimes.get(match.group("member")) == match.group("digest")
    )


def _is_public_pip_artifact_digest(root: Path, filename: str, item: dict[str, Any]) -> bool:
    """Recognize the public wheel digest used to bootstrap the pinned pip release."""
    if item.get("type") != "Hex High Entropy String" or filename != _PINNED_PIP_HELPER:
        return False
    match = _PINNED_PIP_DIGEST_LINE.fullmatch(_source_line(root, filename, item.get("line_number")))
    if match is None:
        return False
    assignments = _literal_assignments(root, filename, {"PIP_SHA256", "PIP_VERSION"})
    version = assignments.get("PIP_VERSION")
    digest = assignments.get("PIP_SHA256")
    return (
        isinstance(version, str)
        and digest == match.group("digest")
        and digest in _locked_pip_hashes(root, version)
    )


def _is_public_platform_inventory_digest(root: Path, filename: str, item: dict[str, Any]) -> bool:
    """Recognize reviewed pytest collection and skip-inventory digests."""
    if item.get("type") != "Hex High Entropy String" or filename != _PLATFORM_EVIDENCE_HELPER:
        return False
    match = _PLATFORM_EVIDENCE_DIGEST_LINE.fullmatch(
        _source_line(root, filename, item.get("line_number"))
    )
    return match is not None and match.group("digest") in _platform_inventory_hashes(root)


def _is_public_advisory_input_digest(root: Path, filename: str, item: dict[str, Any]) -> bool:
    """Recognize canonical dependency-advisory input file fingerprints."""
    if item.get("type") != "Hex High Entropy String" or filename != _ADVISORY_SNAPSHOT:
        return False
    match = _ADVISORY_INPUT_DIGEST_LINE.fullmatch(
        _source_line(root, filename, item.get("line_number"))
    )
    if match is None:
        return False
    try:
        serialized = (root / filename).read_text(encoding="utf-8")
        payload = json.loads(serialized)
    except (OSError, json.JSONDecodeError):
        return False
    if not isinstance(payload, dict) or set(payload) != _ADVISORY_KEYS:
        return False
    if serialized != json.dumps(payload, indent=2, sort_keys=True, ensure_ascii=False) + "\n":
        return False
    inputs = payload.get("inputs")
    member = match.group("member")
    digest = match.group("digest")
    input_path = _advisory_input_path(root, member)
    return (
        payload.get("schema") == 1
        and payload.get("artifact_lock") == "python-artifact-sha256.lock"
        and isinstance(payload.get("auditor"), str)
        and isinstance(payload.get("vulnerabilities"), list)
        and isinstance(inputs, dict)
        and inputs.get(member) == digest
        and input_path is not None
        and _regular_file_sha256(input_path) == digest
    )


def filter_findings(report: dict[str, Any], root: Path) -> dict[str, list[dict[str, Any]]]:
    """Return actionable findings from one detect-secrets JSON report."""
    findings: dict[str, list[dict[str, Any]]] = {}
    for filename, raw_items in report.get("results", {}).items():
        kept = [
            item
            for item in raw_items
            if not _is_notebook_cell_id(root, filename, item.get("line_number"))
            and not _is_public_benchmark_digest(root, filename, item)
            and not _is_public_fuzz_integrity_digest(root, filename, item)
            and not _is_public_reader_reference_commit(root, filename, item)
            and not _is_pinned_pre_commit_revision(root, filename, item)
            and not _is_public_windows_runtime_digest(root, filename, item)
            and not _is_public_pip_artifact_digest(root, filename, item)
            and not _is_public_platform_inventory_digest(root, filename, item)
            and not _is_public_advisory_input_digest(root, filename, item)
        ]
        if kept:
            findings[filename] = kept
    return findings


def check_report(report_path: Path, root: Path) -> None:
    """Raise SystemExit when the report contains actionable findings."""
    report = json.loads(report_path.read_text(encoding="utf-8"))
    findings = filter_findings(report, root)
    if not findings:
        print("detect-secrets: no findings")
        return

    total = sum(len(items) for items in findings.values())
    print(f"detect-secrets found {total} potential secret(s) in {len(findings)} file(s):")
    for filename, items in sorted(findings.items()):
        for item in items:
            kind = item.get("type", "unknown")
            line = item.get("line_number", "?")
            verified = item.get("is_verified", False)
            print(f"- {filename}:{line}: {kind} (verified={verified})")
    raise SystemExit(f"detect-secrets found {total} potential secret(s) in {len(findings)} file(s)")


def main() -> None:
    """Validate one report from the repository root."""
    parser = argparse.ArgumentParser()
    parser.add_argument("report", type=Path)
    parser.add_argument("--root", type=Path, default=Path("."))
    args = parser.parse_args()
    check_report(args.report, args.root)


if __name__ == "__main__":
    main()
