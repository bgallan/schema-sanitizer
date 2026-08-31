#!/usr/bin/env python3
"""Fail the CI quality gate on secrets outside narrow public-data exclusions.

It recognizes narrowly approved public digests and revisions while retaining every
actionable scanner finding.
"""

from __future__ import annotations

import argparse
import json
import re
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
