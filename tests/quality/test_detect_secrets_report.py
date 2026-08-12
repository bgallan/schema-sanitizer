"""Tests for narrow detect-secrets false-positive exclusions."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from meta.ci.quality.check_detect_secrets_report import check_report, filter_findings

PUBLIC_DIGEST = "ab" * 32


def _finding(line_number: int, kind: str = "Hex High Entropy String") -> dict[str, object]:
    """Build one synthetic detect-secrets result."""
    return {
        "type": kind,
        "line_number": line_number,
        "is_verified": False,
    }


def test_public_benchmark_sha256_is_not_treated_as_a_secret(tmp_path: Path) -> None:
    """Published artifact digests are evidence, not authentication material."""
    benchmark = tmp_path / "benchmarks/evidence.json"
    benchmark.parent.mkdir()
    benchmark.write_text(
        json.dumps({"artifact_sha256": PUBLIC_DIGEST}, indent=2) + "\n",
        encoding="utf-8",
    )
    report = {
        "results": {
            "benchmarks/evidence.json": [_finding(2)],
        }
    }

    assert filter_findings(report, tmp_path) == {}


@pytest.mark.parametrize(
    "line",
    [
        f'EXPECTED_TREE_SHA256 = "{PUBLIC_DIGEST}"',
        f'    "csv/unterminated.csv": "{PUBLIC_DIGEST}",',
    ],
)
def test_fuzz_manifest_sha256_is_not_treated_as_a_secret(tmp_path: Path, line: str) -> None:
    """Fuzz-tree integrity digests remain scanned but are not credentials."""
    manifest = tmp_path / "meta/ci/fuzz/check_fuzz_corpus.py"
    manifest.parent.mkdir(parents=True)
    manifest.write_text(line + "\n", encoding="utf-8")
    report = {"results": {"meta/ci/fuzz/check_fuzz_corpus.py": [_finding(1)]}}

    assert filter_findings(report, tmp_path) == {}


def test_pinned_pre_commit_revision_is_not_treated_as_a_secret(tmp_path: Path) -> None:
    """A commented immutable public Git revision is supply-chain metadata."""
    config = tmp_path / ".pre-commit-config.yaml"
    config.write_text(
        f"    rev: {'ab' * 20}  # v1.2.3\n",
        encoding="utf-8",
    )
    report = {"results": {config.name: [_finding(1)]}}

    assert filter_findings(report, tmp_path) == {}


@pytest.mark.parametrize(
    ("filename", "line", "kind"),
    [
        (
            "src/config.json",
            f'"artifact_sha256": "{PUBLIC_DIGEST}"',
            "Hex High Entropy String",
        ),
        (
            "benchmarks/evidence.json",
            f'"token": "{PUBLIC_DIGEST}"',
            "Hex High Entropy String",
        ),
        (
            "benchmarks/evidence.json",
            f'"artifact_sha256": "{PUBLIC_DIGEST}"',
            "Secret Keyword",
        ),
        (
            "src/config.py",
            f'EXPECTED_TREE_SHA256 = "{PUBLIC_DIGEST}"',
            "Hex High Entropy String",
        ),
        (
            ".pre-commit-config.yaml",
            f"    rev: {PUBLIC_DIGEST[:40]}",
            "Hex High Entropy String",
        ),
        (
            ".pre-commit-config.yaml",
            f"    token: {PUBLIC_DIGEST[:40]}  # v1.2.3",
            "Hex High Entropy String",
        ),
    ],
)
def test_secret_exclusion_does_not_hide_broader_findings(
    tmp_path: Path, filename: str, line: str, kind: str
) -> None:
    """Only typed SHA-256 evidence inside benchmarks is allowlisted."""
    source = tmp_path / filename
    source.parent.mkdir(parents=True, exist_ok=True)
    source.write_text(line + "\n", encoding="utf-8")
    report = {"results": {filename: [_finding(1, kind)]}}

    assert filter_findings(report, tmp_path) == report["results"]


def test_check_report_retains_actionable_failure_output(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    """The CI checker still fails and identifies genuine findings."""
    source = tmp_path / "src/config.py"
    source.parent.mkdir()
    source.write_text('api_token = "candidate"\n', encoding="utf-8")
    report_path = tmp_path / "report.json"
    report_path.write_text(
        json.dumps(
            {"results": {"src/config.py": [_finding(1, "Secret Keyword")]}},
        ),
        encoding="utf-8",
    )

    with pytest.raises(SystemExit, match="1 potential secret"):
        check_report(report_path, tmp_path)

    assert "src/config.py:1: Secret Keyword" in capsys.readouterr().out
