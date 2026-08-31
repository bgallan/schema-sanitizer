"""Tests for narrow detect-secrets false-positive exclusions.

It permits only reviewed public digests and revisions while preserving every actionable
secret-scanner finding and failure message.
"""

from __future__ import annotations

import json
import re
from pathlib import Path

import pytest

from meta.ci.quality.check_detect_secrets_report import check_report, filter_findings

PUBLIC_DIGEST = "ab" * 32
WINDOWS_RUNTIME_POLICY = Path("meta/ci/native/windows-release-toolchain.json")


def _finding(line_number: int, kind: str = "Hex High Entropy String") -> dict[str, object]:
    """Build one synthetic detect-secrets result."""
    return {
        "type": kind,
        "line_number": line_number,
        "is_verified": False,
    }


def _line_number_containing(path: Path, needle: str) -> int:
    """Return the unique one-based line containing a test value."""
    matches = [
        number
        for number, line in enumerate(path.read_text(encoding="utf-8").splitlines(), start=1)
        if needle in line
    ]
    assert len(matches) == 1
    return matches[0]


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


def test_public_reader_reference_commit_is_not_treated_as_a_secret(tmp_path: Path) -> None:
    """The reviewed Git commit anchoring the latency policy is public metadata."""
    budget = tmp_path / "benchmarks/readers/linear_scaling_budget.json"
    budget.parent.mkdir(parents=True)
    budget.write_text(
        json.dumps({"commit_sha": "ab" * 20}, indent=2) + "\n",
        encoding="utf-8",
    )
    report = {"results": {"benchmarks/readers/linear_scaling_budget.json": [_finding(2)]}}

    assert filter_findings(report, tmp_path) == {}


def test_checked_in_windows_runtime_sha256_values_are_public_evidence() -> None:
    """Every reviewed runtime fingerprint in the real policy is non-secret evidence."""
    root = Path(__file__).parents[2]
    policy = root / WINDOWS_RUNTIME_POLICY
    payload = json.loads(policy.read_text(encoding="utf-8"))
    runtimes = payload["wheel_runtime_dlls"]
    assert runtimes
    findings = [_finding(_line_number_containing(policy, digest)) for digest in runtimes.values()]
    report = {"results": {WINDOWS_RUNTIME_POLICY.as_posix(): findings}}

    assert len(findings) == len(runtimes)
    assert filter_findings(report, root) == {}


@pytest.mark.parametrize(
    ("filename", "member", "digest", "kind", "policy_format"),
    [
        (
            "meta/ci/native/other-toolchain.json",
            "schema_sanitizer.libs/msvcp140-" + "12" * 16 + ".dll",
            PUBLIC_DIGEST,
            "Hex High Entropy String",
            "schema-sanitizer-windows-toolchain-v1",
        ),
        (
            WINDOWS_RUNTIME_POLICY.as_posix(),
            "other.libs/msvcp140-" + "12" * 16 + ".dll",
            PUBLIC_DIGEST,
            "Hex High Entropy String",
            "schema-sanitizer-windows-toolchain-v1",
        ),
        (
            WINDOWS_RUNTIME_POLICY.as_posix(),
            "schema_sanitizer.libs/msvcp140-" + "12" * 16 + ".dll",
            PUBLIC_DIGEST.upper(),
            "Hex High Entropy String",
            "schema-sanitizer-windows-toolchain-v1",
        ),
        (
            WINDOWS_RUNTIME_POLICY.as_posix(),
            "schema_sanitizer.libs/msvcp140-" + "12" * 16 + ".dll",
            PUBLIC_DIGEST[:-1],
            "Hex High Entropy String",
            "schema-sanitizer-windows-toolchain-v1",
        ),
        (
            WINDOWS_RUNTIME_POLICY.as_posix(),
            "schema_sanitizer.libs/msvcp140-" + "12" * 16 + ".dll",
            PUBLIC_DIGEST,
            "Secret Keyword",
            "schema-sanitizer-windows-toolchain-v1",
        ),
        (
            WINDOWS_RUNTIME_POLICY.as_posix(),
            "schema_sanitizer.libs/msvcp140-" + "12" * 16 + ".dll",
            PUBLIC_DIGEST,
            "Hex High Entropy String",
            "schema-sanitizer-windows-toolchain-v2",
        ),
    ],
)
def test_windows_runtime_digest_exclusion_rejects_policy_lookalikes(
    tmp_path: Path,
    filename: str,
    member: str,
    digest: str,
    kind: str,
    policy_format: str,
) -> None:
    """Wrong locations, schemas, detector types, and digest forms stay actionable."""
    policy = tmp_path / filename
    policy.parent.mkdir(parents=True)
    policy.write_text(
        json.dumps(
            {
                "format": policy_format,
                "wheel_runtime_dlls": {member: digest},
            },
            indent=2,
            sort_keys=True,
        )
        + "\n",
        encoding="utf-8",
    )
    report = {"results": {filename: [_finding(_line_number_containing(policy, digest), kind)]}}

    assert filter_findings(report, tmp_path) == report["results"]


def test_windows_runtime_policy_secret_sibling_stays_actionable(tmp_path: Path) -> None:
    """A secret-looking sibling field cannot inherit the runtime digest exemption."""
    policy = tmp_path / WINDOWS_RUNTIME_POLICY
    policy.parent.mkdir(parents=True)
    policy.write_text(
        json.dumps(
            {
                "api_token": PUBLIC_DIGEST,
                "format": "schema-sanitizer-windows-toolchain-v1",
                "wheel_runtime_dlls": {
                    "schema_sanitizer.libs/msvcp140-" + "12" * 16 + ".dll": "cd" * 32,
                },
            },
            indent=2,
            sort_keys=True,
        )
        + "\n",
        encoding="utf-8",
    )
    report = {
        "results": {
            WINDOWS_RUNTIME_POLICY.as_posix(): [
                _finding(_line_number_containing(policy, PUBLIC_DIGEST))
            ]
        }
    }

    assert filter_findings(report, tmp_path) == report["results"]


def test_windows_runtime_digest_requires_canonical_policy_json(tmp_path: Path) -> None:
    """A correctly shaped digest in noncanonical policy JSON stays actionable."""
    policy = tmp_path / WINDOWS_RUNTIME_POLICY
    policy.parent.mkdir(parents=True)
    policy.write_text(
        json.dumps(
            {
                "format": "schema-sanitizer-windows-toolchain-v1",
                "wheel_runtime_dlls": {
                    "schema_sanitizer.libs/msvcp140-" + "12" * 16 + ".dll": PUBLIC_DIGEST,
                },
            },
            indent=4,
            sort_keys=True,
        )
        + "\n",
        encoding="utf-8",
    )
    report = {
        "results": {
            WINDOWS_RUNTIME_POLICY.as_posix(): [
                _finding(_line_number_containing(policy, PUBLIC_DIGEST))
            ]
        }
    }

    assert filter_findings(report, tmp_path) == report["results"]


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
        (
            "benchmarks/readers/other.json",
            f'"commit_sha": "{PUBLIC_DIGEST[:40]}"',
            "Hex High Entropy String",
        ),
        (
            "benchmarks/readers/linear_scaling_budget.json",
            f'"token": "{PUBLIC_DIGEST[:40]}"',
            "Hex High Entropy String",
        ),
    ],
)
def test_secret_exclusion_does_not_hide_broader_findings(
    tmp_path: Path, filename: str, line: str, kind: str
) -> None:
    """Only narrowly typed and located integrity evidence is allowlisted."""
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


def test_ci_scans_textual_fuzz_assets_and_excludes_only_binary_inputs() -> None:
    """CSV, JSON, and XML regressions stay visible to the secret scanner."""
    action = (
        Path(__file__).parents[2] / ".github/actions/quality-validation/action.yml"
    ).read_text(encoding="utf-8")
    match = re.search(r"--exclude-files '([^']+)'", action)
    assert match is not None
    excluded = re.compile(match.group(1))

    for path in (
        "fuzz/corpus/csv/basic.csv",
        "fuzz/corpus/json/object.json",
        "fuzz/corpus/xml/basic.xml",
        "fuzz/regressions/csv/unterminated.csv",
        "fuzz/regressions/json/truncated.json",
        "fuzz/regressions/xml/mismatched.xml",
    ):
        assert excluded.search(path) is None
    for path in (
        "fuzz/corpus/parquet/minimal.parquet",
        "fuzz/regressions/parquet/truncated.parquet",
        "fuzz/regressions/json.sha1.zip",
        "fuzz/corpus/json/invalid-utf8.json",
    ):
        assert excluded.search(path) is not None
