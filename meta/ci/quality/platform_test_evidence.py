"""Create and verify content-addressed evidence for one platform-test job.

Each job keeps its timing, JUnit, runner, benchmark, and in-process integrity records in
one artifact. This tool checks the exact shard-specific inventory, binds it to immutable
GitHub run coordinates, and emits one compact certificate for the validation gate.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import stat
from pathlib import Path
from typing import Any, Sequence

_FORMAT = "schema-sanitizer-platform-job-evidence-v1"
_INTEGRITY_FORMAT = "schema-sanitizer-platform-integrity-v2"
_SHARDS = frozenset({"concurrency", "memory-parquet", "io-pipeline"})
_PLATFORMS = frozenset({"linux", "macos-arm64", "macos-x86_64", "windows"})

# The gate owns the reviewed collection identities independently from the pytest
# process that reports them. Canonical node IDs are sorted and newline-terminated.
EXPECTED_TEST_INVENTORY = {
    "concurrency": {
        "count": 511,
        "sha256": "22730c892d95e474773bbbc9b8c28afc46cff05348795bbe3a5485ffc50484a5",
    },
    "memory-parquet": {
        "count": 1704,
        "sha256": "f05c4444a34906fd021d401e8850e06ccc261a2a14d5c14e9da8683b118b1f2c",
    },
    "io-pipeline": {
        "count": 1025,
        "sha256": "d4f5d7d9f8b149f1436f02ea2b35502f2ab1d9632ed6270071c2fee22cfbd79d",
    },
    "native-stress": {
        "count": 1,
        "sha256": "1d9f359012a9a69d9b9918e5b6d211258951ca5980f2c9fb06f22eab9e68b812",
    },
    "release-matrix": {
        "count": 3,
        "sha256": "8ecfcc11d5478d971ea5b5766ca91b37d70ae2ff1ba3446dcf575797c2da3a5a",
    },
}

# Other platform skips are exact node/reason allowlists and may shrink when a
# runner gains capabilities. Windows' module-level POSIX policy is instead bound
# to this complete reviewed set.
EXPECTED_EXACT_SKIP_INVENTORY_SHA256 = {
    (
        "windows",
        "memory-parquet",
    ): "a184dfafb6894585a7eaf20b6e99bf1b9ddeefc82df0c886a43bba554246e973",
}


def _github_binding(sha: str, run_id: str | int, run_attempt: str | int) -> dict[str, int | str]:
    """Normalize and validate immutable GitHub run coordinates."""
    normalized_sha = str(sha).lower()
    normalized_run_id = str(run_id)
    normalized_attempt = str(run_attempt)
    if re.fullmatch(r"[0-9a-f]{40}", normalized_sha) is None:
        raise ValueError("GitHub SHA must be a full 40-character commit digest")
    if not normalized_run_id.isdecimal() or int(normalized_run_id) <= 0:
        raise ValueError("GitHub run ID must be a positive integer")
    if not normalized_attempt.isdecimal() or int(normalized_attempt) <= 0:
        raise ValueError("GitHub run attempt must be a positive integer")
    return {
        "sha": normalized_sha,
        "run_id": int(normalized_run_id),
        "run_attempt": int(normalized_attempt),
    }


def _regular_file(path: Path) -> None:
    """Reject missing, linked, and non-regular evidence paths."""
    try:
        mode = path.lstat().st_mode
    except FileNotFoundError as exc:
        raise ValueError(f"missing evidence file: {path}") from exc
    if path.is_symlink() or not stat.S_ISREG(mode):
        raise ValueError(f"evidence path must be a regular file: {path}")


def _sha256(path: Path) -> str:
    """Return the SHA-256 digest of one previously validated evidence file."""
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _skip_inventory_sha256(skips: list[dict[str, str]]) -> str:
    """Hash validated skip identities and reasons using canonical compact JSON."""
    canonical = json.dumps(
        sorted(skips, key=lambda item: (item["nodeid"], item["reason"])),
        ensure_ascii=True,
        separators=(",", ":"),
        sort_keys=True,
    )
    return hashlib.sha256(f"{canonical}\n".encode()).hexdigest()


def _load_json(path: Path) -> dict[str, Any]:
    """Load a bounded JSON object from a regular evidence file."""
    _regular_file(path)
    if path.stat().st_size > 4 * 1024 * 1024:
        raise ValueError(f"evidence JSON exceeds the 4 MiB limit: {path}")
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeError, json.JSONDecodeError) as exc:
        raise ValueError(f"invalid evidence JSON: {path}") from exc
    if not isinstance(payload, dict):
        raise ValueError(f"evidence JSON must contain an object: {path}")
    return payload


def _expected_files(platform: str, shard: str) -> tuple[str, ...]:
    """Return the exact raw evidence inventory owned by one platform job."""
    common = {
        f"integrity-{platform}-{shard}.json",
        f"pytest-{platform}-{shard}.xml",
        f"pytest-durations-{platform}-{shard}.log",
        f"runner-cpu-{platform}-{shard}.json",
    }
    if shard == "concurrency":
        common.update(
            {
                f"integrity-native-stress-{platform}.json",
                f"integrity-release-matrix-{platform}.json",
                f"pytest-native-stress-{platform}.xml",
                f"pytest-native-stress-durations-{platform}.log",
                f"pytest-release-matrix-{platform}.xml",
                f"pytest-release-matrix-durations-{platform}.log",
                f"threading-matrix-{platform}.json",
            }
        )
    elif shard == "memory-parquet":
        common.update(
            {
                f"parquet-contract-runtime-certificate-{platform}.json",
                f"reader-linear-scaling-{platform}.json",
            }
        )
    return tuple(sorted(common))


def _expected_integrity_shards(shard: str) -> tuple[str, ...]:
    """Return the pytest-process certificates required by one job shard."""
    if shard == "concurrency":
        return ("concurrency", "native-stress", "release-matrix")
    return (shard,)


def _integrity_filename(platform: str, component: str) -> str:
    """Return the stable filename for one in-process integrity certificate."""
    if component in {"native-stress", "release-matrix"}:
        return f"integrity-{component}-{platform}.json"
    return f"integrity-{platform}-{component}.json"


def _validate_integrity(
    path: Path,
    *,
    platform: str,
    component: str,
    github: dict[str, int | str],
) -> dict[str, Any]:
    """Validate one in-process certificate and return its bounded summary."""
    payload = _load_json(path)
    if payload.get("format") != _INTEGRITY_FORMAT:
        raise ValueError(f"unexpected platform integrity format: {path}")
    if payload.get("github") != github:
        raise ValueError(f"platform integrity GitHub binding mismatch: {path}")
    if payload.get("platform") != platform or payload.get("shard") != component:
        raise ValueError(f"platform integrity identity mismatch: {path}")
    if payload.get("satisfied") is not True or payload.get("issues") != []:
        raise ValueError(f"platform integrity reports an unsatisfied contract: {path}")
    if payload.get("pytest_exitstatus") != 0:
        raise ValueError(f"platform integrity reports a failed pytest process: {path}")
    skips = payload.get("skips")
    maximum_skips = payload.get("maximum_skip_count")
    selected_tests = payload.get("selected_test_count")
    expected_tests = payload.get("expected_test_count")
    selected_inventory = payload.get("selected_test_inventory_sha256")
    expected_inventory = payload.get("expected_test_inventory_sha256")
    skip_inventory = payload.get("skip_inventory_sha256")
    expected_skip_inventory = payload.get("expected_skip_inventory_sha256")
    if not isinstance(skips, list) or type(maximum_skips) is not int:
        raise ValueError(f"platform integrity skip evidence is malformed: {path}")
    if any(
        not isinstance(skip, dict)
        or set(skip) != {"nodeid", "reason"}
        or not isinstance(skip.get("nodeid"), str)
        or not skip["nodeid"]
        or not isinstance(skip.get("reason"), str)
        or not skip["reason"]
        for skip in skips
    ):
        raise ValueError(f"platform integrity skip entries are malformed: {path}")
    if maximum_skips < 0 or len(skips) > maximum_skips:
        raise ValueError(f"platform integrity skip ceiling is inconsistent: {path}")
    policy = EXPECTED_TEST_INVENTORY[component]
    if (
        type(selected_tests) is not int
        or type(expected_tests) is not int
        or selected_tests != expected_tests
        or selected_tests != policy["count"]
        or selected_tests < 1
    ):
        raise ValueError(f"platform integrity test inventory is inconsistent: {path}")
    if (
        not isinstance(selected_inventory, str)
        or re.fullmatch(r"[0-9a-f]{64}", selected_inventory) is None
        or selected_inventory != expected_inventory
        or selected_inventory != policy["sha256"]
    ):
        raise ValueError(f"platform integrity test identities are inconsistent: {path}")
    if (
        not isinstance(skip_inventory, str)
        or re.fullmatch(r"[0-9a-f]{64}", skip_inventory) is None
        or skip_inventory != _skip_inventory_sha256(skips)
    ):
        raise ValueError(f"platform integrity skip identities are malformed: {path}")
    reviewed_skip_inventory = EXPECTED_EXACT_SKIP_INVENTORY_SHA256.get((platform, component))
    if expected_skip_inventory != reviewed_skip_inventory or (
        reviewed_skip_inventory is not None and skip_inventory != reviewed_skip_inventory
    ):
        raise ValueError(f"platform integrity skip identities are inconsistent: {path}")
    return {
        "filename": path.name,
        "sha256": _sha256(path),
        "skip_count": len(skips),
        "skip_inventory_sha256": skip_inventory,
        "test_count": selected_tests,
        "test_inventory_sha256": selected_inventory,
    }


def _atomic_write_json(path: Path, payload: dict[str, object]) -> None:
    """Atomically publish canonical JSON without following a symlink."""
    path.parent.mkdir(parents=True, exist_ok=True)
    if path.is_symlink() or (path.exists() and not path.is_file()):
        raise ValueError(f"certificate target must be a regular file: {path}")
    temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    if temporary.exists() or temporary.is_symlink():
        raise ValueError(f"certificate temporary path already exists: {temporary}")
    try:
        with temporary.open("x", encoding="utf-8", newline="\n") as handle:
            json.dump(payload, handle, indent=2, sort_keys=True)
            handle.write("\n")
            handle.flush()
            os.fsync(handle.fileno())
        if path.is_symlink():
            raise ValueError(f"certificate target became a symlink: {path}")
        os.replace(temporary, path)
    finally:
        try:
            temporary.unlink()
        except FileNotFoundError:
            pass


def create_certificate(
    evidence_dir: Path,
    certificate: Path,
    *,
    platform: str,
    shard: str,
    github_sha: str,
    github_run_id: str | int,
    github_run_attempt: str | int,
) -> dict[str, object]:
    """Validate a job inventory and create its content-addressed certificate."""
    if platform not in _PLATFORMS or shard not in _SHARDS:
        raise ValueError(f"unknown platform-test identity: {platform}:{shard}")
    if evidence_dir.is_symlink() or not evidence_dir.is_dir():
        raise ValueError(f"evidence directory must be a real directory: {evidence_dir}")
    github = _github_binding(github_sha, github_run_id, github_run_attempt)
    expected = _expected_files(platform, shard)
    actual = tuple(sorted(path.name for path in evidence_dir.iterdir() if path != certificate))
    if actual != expected:
        raise ValueError(
            f"platform evidence inventory mismatch: expected={expected}, actual={actual}"
        )
    files = []
    for filename in expected:
        path = evidence_dir / filename
        _regular_file(path)
        files.append({"filename": filename, "sha256": _sha256(path), "size": path.stat().st_size})
    components = [
        _validate_integrity(
            evidence_dir / _integrity_filename(platform, component),
            platform=platform,
            component=component,
            github=github,
        )
        for component in _expected_integrity_shards(shard)
    ]
    payload: dict[str, object] = {
        "format": _FORMAT,
        "github": github,
        "platform": platform,
        "shard": shard,
        "satisfied": True,
        "integrity": components,
        "files": files,
    }
    _atomic_write_json(certificate, payload)
    return payload


def verify_certificate(
    evidence_dir: Path,
    certificate: Path,
    *,
    platform: str,
    shard: str,
    github_sha: str,
    github_run_id: str | int,
    github_run_attempt: str | int,
) -> dict[str, Any]:
    """Verify a downloaded certificate and every content-addressed evidence file."""
    payload = _load_json(certificate)
    github = _github_binding(github_sha, github_run_id, github_run_attempt)
    if certificate.parent.resolve() != evidence_dir.resolve():
        raise ValueError("platform job certificate must reside in its evidence directory")
    actual = tuple(sorted(path.name for path in evidence_dir.iterdir()))
    expected_inventory = tuple(sorted((*_expected_files(platform, shard), certificate.name)))
    if actual != expected_inventory:
        raise ValueError(
            "downloaded platform evidence inventory mismatch: "
            f"expected={expected_inventory}, actual={actual}"
        )
    if payload.get("format") != _FORMAT or payload.get("satisfied") is not True:
        raise ValueError(f"platform job certificate is not satisfied: {certificate}")
    if payload.get("github") != github:
        raise ValueError(f"platform job certificate GitHub binding mismatch: {certificate}")
    if payload.get("platform") != platform or payload.get("shard") != shard:
        raise ValueError(f"platform job certificate identity mismatch: {certificate}")
    expected = _expected_files(platform, shard)
    records = payload.get("files")
    if not isinstance(records, list) or len(records) != len(expected):
        raise ValueError(f"platform job certificate file inventory is malformed: {certificate}")
    by_name: dict[str, dict[str, Any]] = {}
    for record in records:
        if not isinstance(record, dict) or set(record) != {"filename", "sha256", "size"}:
            raise ValueError(f"platform job certificate file entry is malformed: {certificate}")
        filename = record.get("filename")
        if not isinstance(filename, str) or filename in by_name:
            raise ValueError(f"platform job certificate has duplicate file entries: {certificate}")
        by_name[filename] = record
    if tuple(sorted(by_name)) != expected:
        raise ValueError(f"platform job certificate file names changed: {certificate}")
    for filename in expected:
        path = evidence_dir / filename
        _regular_file(path)
        record = by_name[filename]
        if record["size"] != path.stat().st_size or record["sha256"] != _sha256(path):
            raise ValueError(f"platform job evidence digest mismatch: {path}")
    expected_components = tuple(_expected_integrity_shards(shard))
    components = payload.get("integrity")
    if not isinstance(components, list) or len(components) != len(expected_components):
        raise ValueError(f"platform job integrity inventory is malformed: {certificate}")
    for component, record in zip(expected_components, components, strict=True):
        if not isinstance(record, dict):
            raise ValueError(f"platform job integrity entry is malformed: {certificate}")
        filename = _integrity_filename(platform, component)
        if record.get("filename") != filename or record.get("sha256") != _sha256(
            evidence_dir / filename
        ):
            raise ValueError(f"platform job integrity digest mismatch: {filename}")
    return payload


def _parser() -> argparse.ArgumentParser:
    """Build the command-line parser for certificate creation and verification."""
    parser = argparse.ArgumentParser(description=__doc__)
    subparsers = parser.add_subparsers(dest="command", required=True)
    for command in ("create", "verify"):
        child = subparsers.add_parser(command)
        child.add_argument("--evidence-dir", required=True, type=Path)
        child.add_argument("--certificate", required=True, type=Path)
        child.add_argument("--platform", required=True, choices=sorted(_PLATFORMS))
        child.add_argument("--shard", required=True, choices=sorted(_SHARDS))
        child.add_argument("--github-sha", required=True)
        child.add_argument("--github-run-id", required=True)
        child.add_argument("--github-run-attempt", required=True)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    """Create or verify one fail-closed platform job evidence certificate."""
    args = _parser().parse_args(argv)
    options = {
        "platform": args.platform,
        "shard": args.shard,
        "github_sha": args.github_sha,
        "github_run_id": args.github_run_id,
        "github_run_attempt": args.github_run_attempt,
    }
    if args.command == "create":
        create_certificate(args.evidence_dir, args.certificate, **options)
    else:
        verify_certificate(args.evidence_dir, args.certificate, **options)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
