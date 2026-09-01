"""Create and verify content-addressed evidence for one platform-test job.

Each job keeps its timing, JUnit, runner, benchmark, and in-process integrity records in
one artifact whose exact shard-specific inventory this tool binds to immutable GitHub
run coordinates before emitting one compact certificate for the validation gate.
Same-run evidence from an earlier successful attempt remains valid during a partial rerun
without weakening the immutable run and commit identity.
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
_INTEGRITY_KEYS = frozenset(
    {
        "expected_skip_inventory_sha256",
        "expected_test_count",
        "expected_test_inventory_sha256",
        "final_native_anomalies",
        "final_process_anomalies",
        "format",
        "github",
        "initial_native_anomalies",
        "initial_process_anomalies",
        "issues",
        "maximum_skip_count",
        "platform",
        "provenance",
        "pytest_exitstatus",
        "satisfied",
        "selected_test_count",
        "selected_test_inventory_sha256",
        "shard",
        "skip_inventory_sha256",
        "skips",
    }
)
_NATIVE_ANOMALY_KEYS = frozenset(
    {
        "native.completion_memory_protocol_violations",
        "native.counter_underflows",
        "native.external_runtime_resident_protocol_violations",
        "native_fd.protocol_violations",
        "native_fd.uncertain_close_debts",
    }
)
_PROCESS_ANOMALY_KEYS = frozenset(
    {
        "async.corrupted",
        "async.protocol_violations",
        "cleanup.protocol_violations",
        "fds.over_release_count",
        "fds.unknown_lease_releases",
        "guardian.protocol_violations",
        "janitor.protocol_violations",
        "remote.protocol_violations",
        "retry.protocol_violations",
        "temporary.authoritative_protocol_violations",
        "temporary.over_release_count",
        "temporary.protocol_violations",
        "threads.over_release_count",
        "threads.unknown_lease_releases",
    }
)

# The gate owns the reviewed collection identities independently from the pytest
# process that reports them. Canonical node IDs are sorted and newline-terminated.
EXPECTED_TEST_INVENTORY = {
    "concurrency": {
        "count": 502,
        "sha256": "faa41818148030a36eb83bca69a95845ebbbb198198f410434183f3aec60bc2c",
    },
    "memory-parquet": {
        "count": 1665,
        "sha256": "5817118d2063cada52dbfe9dce35978850661a52eeafae7ef1c6aa2d664d771d",
    },
    "io-pipeline": {
        "count": 1074,
        "sha256": "8d4dd18c7a1f473df236be39154af58c448f2e6011e44faadbef7c22c48c4310",
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
    ): "52f3111af895aed3517c771b7a323c1e4de96d750eefbabb1aa651084b5f80b2",
    (
        "windows",
        "io-pipeline",
    ): "04083befaabc216f25d572c65a544f305de597f7fc3c6203961df1f4d898cbe0",
}


def _nodeid_inventory_sha256(nodeids: Sequence[str]) -> str:
    """Hash sorted pytest node IDs with one unambiguous trailing newline."""
    canonical = "".join(f"{nodeid}\n" for nodeid in sorted(nodeids))
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def _github_binding(sha: str, run_id: int, run_attempt: int) -> dict[str, int | str]:
    """Validate one strict immutable GitHub run identity."""
    if type(sha) is not str or re.fullmatch(r"[0-9a-f]{40}", sha) is None:
        raise ValueError("GitHub SHA must be a full 40-character commit digest")
    if type(run_id) is not int or run_id <= 0:
        raise ValueError("GitHub run ID must be a positive integer")
    if type(run_attempt) is not int or run_attempt <= 0:
        raise ValueError("GitHub run attempt must be a positive integer")
    return {
        "sha": sha,
        "run_id": run_id,
        "run_attempt": run_attempt,
    }


def _producer_github_binding(raw: object, current: dict[str, int | str]) -> dict[str, int | str]:
    """Validate a producer binding reusable by the current workflow attempt."""
    if not isinstance(raw, dict) or set(raw) != {"sha", "run_id", "run_attempt"}:
        raise ValueError("platform job certificate GitHub binding mismatch")
    producer_run_id = raw.get("run_id")
    producer_attempt = raw.get("run_attempt")
    current_attempt = current["run_attempt"]
    if (
        raw.get("sha") != current["sha"]
        or type(producer_run_id) is not int
        or producer_run_id != current["run_id"]
        or type(producer_attempt) is not int
        or producer_attempt < 1
        or type(current_attempt) is not int
        or producer_attempt > current_attempt
    ):
        raise ValueError("platform job certificate GitHub binding mismatch")
    return {"sha": raw["sha"], "run_id": producer_run_id, "run_attempt": producer_attempt}


def _regular_file(path: Path) -> None:
    """Reject missing, linked, and non-regular evidence paths."""
    try:
        mode = path.lstat().st_mode
    except FileNotFoundError as exc:
        raise ValueError(f"missing evidence file: {path}") from exc
    if path.is_symlink() or not stat.S_ISREG(mode):
        raise ValueError(f"evidence path must be a regular file: {path}")


def _canonical_json(payload: dict[str, Any]) -> str:
    """Serialize one certificate using the repository's canonical JSON form."""
    return json.dumps(payload, indent=2, sort_keys=True, ensure_ascii=False) + "\n"


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


def _load_json(path: Path, *, canonical: bool = False) -> dict[str, Any]:
    """Load one bounded JSON object and optionally require canonical bytes."""
    _regular_file(path)
    if path.stat().st_size > 4 * 1024 * 1024:
        raise ValueError(f"evidence JSON exceeds the 4 MiB limit: {path}")
    try:
        serialized = path.read_text(encoding="utf-8")
        payload = json.loads(serialized)
    except (OSError, UnicodeError, json.JSONDecodeError) as exc:
        raise ValueError(f"invalid evidence JSON: {path}") from exc
    if not isinstance(payload, dict):
        raise ValueError(f"evidence JSON must contain an object: {path}")
    if canonical and serialized != _canonical_json(payload):
        raise ValueError(f"platform job certificate is not canonical JSON: {path}")
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
            }
        )
    elif shard == "io-pipeline":
        common.add(f"reader-linear-scaling-{platform}.json")
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


def _validate_zero_anomalies(raw: object, expected: frozenset[str], *, label: str) -> None:
    """Require one exact zero-valued runtime-anomaly snapshot."""
    if (
        not isinstance(raw, dict)
        or set(raw) != expected
        or any(type(value) is not int or value != 0 for value in raw.values())
    ):
        raise ValueError(f"platform integrity {label} anomalies are malformed")


def _validate_provenance(raw: object, *, path: Path) -> None:
    """Require bounded installed-wheel module provenance for one process."""
    if (
        not isinstance(raw, dict)
        or not raw
        or any(
            not isinstance(name, str)
            or (name != "schema_sanitizer" and not name.startswith("schema_sanitizer."))
            or not isinstance(location, str)
            or not location
            or len(location) > 4096
            or not (location.startswith("/") or re.match(r"^[A-Za-z]:[\\/]", location))
            for name, location in raw.items()
        )
        or not {
            "schema_sanitizer",
            "schema_sanitizer._core_abi3",
        }.issubset(raw)
    ):
        raise ValueError(f"platform integrity wheel provenance is malformed: {path}")


def _validate_integrity(
    path: Path,
    *,
    platform: str,
    component: str,
    github: dict[str, int | str],
) -> dict[str, Any]:
    """Validate one in-process certificate and return its bounded summary."""
    payload = _load_json(path)
    if set(payload) != _INTEGRITY_KEYS or payload.get("format") != _INTEGRITY_FORMAT:
        raise ValueError(f"unexpected platform integrity format: {path}")
    if payload.get("github") != github:
        raise ValueError(f"platform integrity GitHub binding mismatch: {path}")
    if payload.get("platform") != platform or payload.get("shard") != component:
        raise ValueError(f"platform integrity identity mismatch: {path}")
    if payload.get("satisfied") is not True or payload.get("issues") != []:
        raise ValueError(f"platform integrity reports an unsatisfied contract: {path}")
    if payload.get("pytest_exitstatus") != 0:
        raise ValueError(f"platform integrity reports a failed pytest process: {path}")
    _validate_zero_anomalies(
        payload.get("initial_native_anomalies"),
        _NATIVE_ANOMALY_KEYS,
        label="initial native",
    )
    _validate_zero_anomalies(
        payload.get("final_native_anomalies"),
        _NATIVE_ANOMALY_KEYS,
        label="final native",
    )
    _validate_zero_anomalies(
        payload.get("initial_process_anomalies"),
        _PROCESS_ANOMALY_KEYS,
        label="initial process",
    )
    _validate_zero_anomalies(
        payload.get("final_process_anomalies"),
        _PROCESS_ANOMALY_KEYS,
        label="final process",
    )
    _validate_provenance(payload.get("provenance"), path=path)
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
            handle.write(_canonical_json(payload))
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
    github_run_id: int,
    github_run_attempt: int,
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
    github_run_id: int,
    github_run_attempt: int,
) -> dict[str, Any]:
    """Verify downloaded same-run evidence from the current or a prior attempt."""
    if platform not in _PLATFORMS or shard not in _SHARDS:
        raise ValueError(f"unknown platform-test identity: {platform}:{shard}")
    if evidence_dir.is_symlink() or not evidence_dir.is_dir():
        raise ValueError(f"evidence directory must be a real directory: {evidence_dir}")
    payload = _load_json(certificate, canonical=True)
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
    if (
        set(payload)
        != {
            "files",
            "format",
            "github",
            "integrity",
            "platform",
            "satisfied",
            "shard",
        }
        or payload.get("format") != _FORMAT
        or payload.get("satisfied") is not True
    ):
        raise ValueError(f"platform job certificate is not satisfied: {certificate}")
    producer_github = _producer_github_binding(payload.get("github"), github)
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
    rebuilt_components = [
        _validate_integrity(
            evidence_dir / _integrity_filename(platform, component),
            platform=platform,
            component=component,
            github=producer_github,
        )
        for component in expected_components
    ]
    if components != rebuilt_components:
        raise ValueError(f"platform job integrity summaries changed: {certificate}")
    return payload


def _positive_integer(raw: str) -> int:
    """Parse one canonical positive decimal for an argparse option."""
    if re.fullmatch(r"[1-9][0-9]*", raw) is None:
        raise argparse.ArgumentTypeError("value must be a positive decimal integer")
    return int(raw, 10)


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
        child.add_argument("--github-run-id", required=True, type=_positive_integer)
        child.add_argument("--github-run-attempt", required=True, type=_positive_integer)
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
