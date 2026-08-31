#!/usr/bin/env python3
"""Turn LLVM's native coverage export into an enforceable CI certificate.

The checker proves that every code-mapped production translation unit is represented,
authenticates the exact zero-region wrapper policy and complete native source tree,
applies aggregate and high-risk floors, and emits provenance-bound canonical JSON.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import tempfile
from fractions import Fraction
from pathlib import Path
from typing import Any, Iterable, Mapping

CERTIFICATE_FORMAT = "schema-sanitizer-native-coverage-v2"
METRICS = ("regions", "functions", "lines", "branches")
TRANSLATION_UNIT_SUFFIXES = frozenset({".c", ".cc", ".cpp", ".cxx"})
NATIVE_SOURCE_SUFFIXES = frozenset(
    {".c", ".cc", ".cpp", ".cxx", ".def", ".h", ".hh", ".hpp", ".hxx", ".inc"}
)
ZERO_REGION_TRANSLATION_UNITS = frozenset(
    {
        "cpp/src/internal/parquet/footer_reader/footer_reader.cc",
        "cpp/src/internal/parquet/stream_writer/stream_writer.cc",
    }
)
TOTAL_FLOORS = {
    "regions": 40.0,
    "functions": 60.0,
    "lines": 39.0,
    "branches": 28.0,
}
CRITICAL_FLOORS = {
    "cpp/src/frontends/json/text_frontend.cc": {
        "regions": 40.0,
        "functions": 47.0,
        "lines": 40.0,
        "branches": 27.0,
    },
    "cpp/src/ingest/secure_read_only_file.cc": {
        "regions": 77.0,
        "functions": 99.0,
        "lines": 66.0,
        "branches": 49.0,
    },
    "cpp/src/internal/memory/memory_budget.cc": {
        "regions": 60.0,
        "functions": 99.0,
        "lines": 71.0,
        "branches": 44.0,
    },
    "cpp/src/internal/memory/memory_pool.cc": {
        "regions": 69.0,
        "functions": 71.0,
        "lines": 64.0,
        "branches": 49.0,
    },
    "cpp/src/internal/runtime/operation_task_arena.cc": {
        "regions": 39.0,
        "functions": 53.0,
        "lines": 42.0,
        "branches": 24.0,
    },
}
_GIT_SHA = re.compile(r"[0-9a-f]{40}(?:[0-9a-f]{24})?")


def _regular_file(path: Path, label: str) -> None:
    """Require one input to be a non-symlinked regular file."""
    if path.is_symlink() or not path.is_file():
        raise AssertionError(f"{label} must be a regular file: {path}")


def _percentage(metric: Mapping[str, Any]) -> float:
    """Calculate a stable coverage percentage from integer LLVM counters."""
    count = metric.get("count")
    covered = metric.get("covered")
    if not isinstance(count, int) or isinstance(count, bool) or count < 0:
        raise AssertionError(f"invalid LLVM coverage count: {count!r}")
    if not isinstance(covered, int) or isinstance(covered, bool) or not 0 <= covered <= count:
        raise AssertionError(f"invalid LLVM covered count: {covered!r}/{count!r}")
    return 100.0 if count == 0 else round(100.0 * covered / count, 2)


def _metric_summary(summary: Mapping[str, Any]) -> dict[str, dict[str, int | float]]:
    """Normalize the four LLVM counters used by the release floor."""
    normalized: dict[str, dict[str, int | float]] = {}
    for name in METRICS:
        raw = summary.get(name)
        if not isinstance(raw, Mapping):
            raise AssertionError(f"LLVM coverage summary omits {name}")
        normalized[name] = {
            "count": int(raw["count"]),
            "covered": int(raw["covered"]),
            "percent": _percentage(raw),
        }
    return normalized


def _repository_filename(raw_filename: str, repository: Path) -> str | None:
    """Return a canonical repository path for one LLVM filename when owned."""
    owned_root = repository.resolve()
    raw = Path(raw_filename.replace("\\", "/"))
    candidate = raw if raw.is_absolute() else owned_root / raw
    try:
        return candidate.resolve().relative_to(owned_root).as_posix()
    except (OSError, ValueError):
        return None


def _owned_source_files(source_root: Path) -> list[Path]:
    """List regular source-tree files while rejecting every symlink or special entry."""
    if source_root.is_symlink() or not source_root.is_dir():
        raise AssertionError(f"native source root must be a directory: {source_root}")
    files: list[Path] = []
    pending = [source_root]
    while pending:
        directory = pending.pop()
        try:
            with os.scandir(directory) as iterator:
                entries = sorted(iterator, key=lambda entry: entry.name)
            for entry in entries:
                path = Path(entry.path)
                if entry.is_symlink():
                    raise AssertionError(f"native source tree contains a symlink: {path}")
                if entry.is_dir(follow_symlinks=False):
                    pending.append(path)
                elif entry.is_file(follow_symlinks=False):
                    files.append(path)
                else:
                    raise AssertionError(f"native source tree contains a special entry: {path}")
        except OSError as error:
            raise AssertionError(f"native source tree is unreadable: {directory}") from error
    return sorted(files)


def _expected_sources(source_root: Path, repository: Path) -> list[str]:
    """Discover every production translation unit owned by the native build."""
    sources = sorted(
        path.relative_to(repository).as_posix()
        for path in _owned_source_files(source_root)
        if path.suffix.lower() in TRANSLATION_UNIT_SUFFIXES
    )
    if not sources:
        raise AssertionError(f"no native translation units found below {source_root}")
    return sources


def _native_source_files(source_root: Path, repository: Path) -> list[str]:
    """Discover every maintained native source, header, and implementation fragment."""
    sources = sorted(
        path.relative_to(repository).as_posix()
        for path in _owned_source_files(source_root)
        if path.suffix.lower() in NATIVE_SOURCE_SUFFIXES
    )
    if not sources:
        raise AssertionError(f"no native source files found below {source_root}")
    return sources


def _source_tree_digest(repository: Path, sources: Iterable[str]) -> str:
    """Hash canonical paths and bytes for all maintained native source files."""
    digest = hashlib.sha256()
    for name in sources:
        encoded_name = name.encode("utf-8")
        payload = (repository / name).read_bytes()
        digest.update(len(encoded_name).to_bytes(8, "big"))
        digest.update(encoded_name)
        digest.update(len(payload).to_bytes(8, "big"))
        digest.update(payload)
    return digest.hexdigest()


def _require_floor(
    label: str, metrics: Mapping[str, Mapping[str, int | float]], floors: Mapping[str, float]
) -> None:
    """Reject any coverage metric below its checked-in floor."""
    failures: list[str] = []
    for metric, floor in floors.items():
        count = int(metrics[metric]["count"])
        covered = int(metrics[metric]["covered"])
        below_floor = count > 0 and Fraction(covered * 100, count) < Fraction(str(floor))
        if below_floor:
            failures.append(f"{metric}={metrics[metric]['percent']:.2f}% < {floor:.2f}%")
    if failures:
        raise AssertionError(f"native coverage floor failed for {label}: {', '.join(failures)}")


def _canonical_json(payload: Mapping[str, Any]) -> str:
    """Serialize one certificate with stable ordering and whitespace."""
    return json.dumps(payload, indent=2, sort_keys=True, ensure_ascii=False) + "\n"


def _write_atomically(destination: Path, content: str) -> None:
    """Replace a regular certificate atomically without following symlinks."""
    if destination.is_symlink() or (destination.exists() and not destination.is_file()):
        raise AssertionError(f"coverage certificate output is unsafe: {destination}")
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


def build_certificate(
    report: Path,
    *,
    repository: Path,
    source_root: Path,
    github_sha: str,
    github_run_id: int,
    github_run_attempt: int,
) -> dict[str, Any]:
    """Validate an LLVM JSON export and return its deterministic certificate."""
    _regular_file(report, "LLVM coverage export")
    if _GIT_SHA.fullmatch(github_sha) is None:
        raise ValueError("github_sha must be a lowercase 40- or 64-character Git object ID")
    if github_run_id < 1 or github_run_attempt < 1:
        raise ValueError("GitHub run identity values must be positive integers")
    payload = json.loads(report.read_text(encoding="utf-8"))
    if payload.get("type") != "llvm.coverage.json.export":
        raise AssertionError(f"unexpected LLVM export type: {payload.get('type')!r}")
    data = payload.get("data")
    if not isinstance(data, list) or len(data) != 1 or not isinstance(data[0], Mapping):
        raise AssertionError("LLVM coverage export must contain exactly one data object")
    document = data[0]
    files = document.get("files")
    if not isinstance(files, list):
        raise AssertionError("LLVM coverage export omits its file inventory")

    observed: dict[str, dict[str, dict[str, int | float]]] = {}
    for entry in files:
        if not isinstance(entry, Mapping) or not isinstance(entry.get("filename"), str):
            raise AssertionError("LLVM coverage export contains a malformed file entry")
        filename = _repository_filename(entry["filename"], repository)
        if filename is None or not filename.startswith("cpp/src/"):
            continue
        if filename in observed:
            raise AssertionError(f"LLVM coverage export repeats source file: {filename}")
        summary = entry.get("summary")
        if not isinstance(summary, Mapping):
            raise AssertionError(f"LLVM coverage export omits summary for {filename}")
        observed[filename] = _metric_summary(summary)

    expected = _expected_sources(source_root, repository)
    expected_set = set(expected)
    if not ZERO_REGION_TRANSLATION_UNITS <= expected_set:
        raise AssertionError("zero-region translation-unit policy references missing sources")
    missing = expected_set - set(observed)
    if missing != ZERO_REGION_TRANSLATION_UNITS:
        raise AssertionError(
            "LLVM coverage translation-unit inventory mismatch: "
            f"expected zero-region omissions={sorted(ZERO_REGION_TRANSLATION_UNITS)!r}; "
            f"actual omissions={sorted(missing)[:20]!r}"
        )
    native_sources = _native_source_files(source_root, repository)
    totals_raw = document.get("totals")
    if not isinstance(totals_raw, Mapping):
        raise AssertionError("LLVM coverage export omits aggregate totals")
    totals = _metric_summary(totals_raw)
    _require_floor("TOTAL", totals, TOTAL_FLOORS)

    critical: dict[str, dict[str, dict[str, int | float]]] = {}
    for filename, floors in CRITICAL_FLOORS.items():
        metrics = observed.get(filename)
        if metrics is None:
            raise AssertionError(f"LLVM coverage omits critical source: {filename}")
        _require_floor(filename, metrics, floors)
        critical[filename] = metrics

    return {
        "critical_floors": CRITICAL_FLOORS,
        "critical_sources": critical,
        "expected_source_files": len(native_sources),
        "expected_translation_units": len(expected),
        "format": CERTIFICATE_FORMAT,
        "observed_production_files": len(observed),
        "observed_translation_units": len(expected_set & set(observed)),
        "provenance": {
            "git_sha": github_sha,
            "github_run_attempt": github_run_attempt,
            "github_run_id": github_run_id,
        },
        "report_sha256": hashlib.sha256(report.read_bytes()).hexdigest(),
        "source_tree_sha256": _source_tree_digest(repository, native_sources),
        "total_floors": TOTAL_FLOORS,
        "totals": totals,
        "zero_region_translation_units": sorted(ZERO_REGION_TRANSLATION_UNITS),
    }


def create_certificate(report: Path, certificate: Path, **options: Any) -> None:
    """Build, atomically write, and read back one coverage certificate."""
    payload = build_certificate(report, **options)
    _write_atomically(certificate, _canonical_json(payload))
    verify_certificate(
        certificate,
        repository=options["repository"],
        source_root=options["source_root"],
        github_sha=options["github_sha"],
        github_run_id=options["github_run_id"],
        github_run_attempt=options["github_run_attempt"],
    )


def verify_certificate(
    certificate: Path,
    *,
    repository: Path,
    source_root: Path,
    github_sha: str,
    github_run_id: int,
    github_run_attempt: int,
) -> None:
    """Verify canonical serialization, format, floors, and workflow provenance."""
    _regular_file(certificate, "native coverage certificate")
    serialized = certificate.read_text(encoding="utf-8")
    payload = json.loads(serialized)
    if not isinstance(payload, Mapping) or serialized != _canonical_json(payload):
        raise AssertionError("native coverage certificate is not canonical JSON")
    if payload.get("format") != CERTIFICATE_FORMAT:
        raise AssertionError(
            f"unexpected native coverage certificate format: {payload.get('format')!r}"
        )
    expected_provenance = {
        "git_sha": github_sha,
        "github_run_attempt": github_run_attempt,
        "github_run_id": github_run_id,
    }
    if payload.get("provenance") != expected_provenance:
        raise AssertionError("native coverage certificate provenance mismatch")
    if set(payload) != {
        "critical_floors",
        "critical_sources",
        "expected_source_files",
        "expected_translation_units",
        "format",
        "observed_production_files",
        "observed_translation_units",
        "provenance",
        "report_sha256",
        "source_tree_sha256",
        "total_floors",
        "totals",
        "zero_region_translation_units",
    }:
        raise AssertionError("native coverage certificate schema mismatch")
    if (
        payload.get("total_floors") != TOTAL_FLOORS
        or payload.get("critical_floors") != CRITICAL_FLOORS
    ):
        raise AssertionError("native coverage certificate floor policy mismatch")
    expected = _expected_sources(source_root, repository)
    expected_set = set(expected)
    if not ZERO_REGION_TRANSLATION_UNITS <= expected_set:
        raise AssertionError("zero-region translation-unit policy references missing sources")
    native_sources = _native_source_files(source_root, repository)
    if payload.get("expected_source_files") != len(native_sources):
        raise AssertionError("native coverage source-file count mismatch")
    if payload.get("expected_translation_units") != len(expected):
        raise AssertionError("native coverage translation-unit count mismatch")
    if payload.get("zero_region_translation_units") != sorted(ZERO_REGION_TRANSLATION_UNITS):
        raise AssertionError("native coverage zero-region policy mismatch")
    expected_observed_translation_units = len(expected) - len(ZERO_REGION_TRANSLATION_UNITS)
    if payload.get("observed_translation_units") != expected_observed_translation_units:
        raise AssertionError("native coverage observed translation-unit count is invalid")
    observed_count = payload.get("observed_production_files")
    if (
        not isinstance(observed_count, int)
        or isinstance(observed_count, bool)
        or observed_count < expected_observed_translation_units
    ):
        raise AssertionError("native coverage observed-file count is invalid")
    if payload.get("source_tree_sha256") != _source_tree_digest(repository, native_sources):
        raise AssertionError("native coverage source-tree digest mismatch")
    if (
        not isinstance(payload.get("report_sha256"), str)
        or re.fullmatch(r"[0-9a-f]{64}", str(payload["report_sha256"])) is None
    ):
        raise AssertionError("native coverage report digest is invalid")
    totals_payload = payload.get("totals")
    if not isinstance(totals_payload, Mapping):
        raise AssertionError("native coverage certificate omits totals")
    totals = _metric_summary(totals_payload)
    if totals != totals_payload:
        raise AssertionError("native coverage total percentages are not canonical")
    _require_floor("TOTAL", totals, TOTAL_FLOORS)
    critical_payload = payload.get("critical_sources")
    if not isinstance(critical_payload, Mapping) or set(critical_payload) != set(CRITICAL_FLOORS):
        raise AssertionError("native coverage critical-source inventory mismatch")
    for filename, floors in CRITICAL_FLOORS.items():
        raw_metrics = critical_payload[filename]
        if not isinstance(raw_metrics, Mapping):
            raise AssertionError(f"native coverage critical metrics are malformed: {filename}")
        metrics = _metric_summary(raw_metrics)
        if metrics != raw_metrics:
            raise AssertionError(
                f"native coverage critical percentages are not canonical: {filename}"
            )
        _require_floor(filename, metrics, floors)


def _positive_integer(raw: str) -> int:
    """Parse one strictly positive command-line integer."""
    value = int(raw)
    if value < 1:
        raise argparse.ArgumentTypeError("value must be a positive integer")
    return value


def main() -> None:
    """Run the native coverage create-or-verify command-line interface."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("operation", choices=("create", "verify"))
    parser.add_argument("--certificate", required=True, type=Path)
    parser.add_argument("--report", type=Path)
    parser.add_argument("--repository", type=Path, default=Path.cwd())
    parser.add_argument("--source-root", type=Path, default=Path("cpp/src"))
    parser.add_argument("--github-sha", required=True)
    parser.add_argument("--github-run-id", required=True, type=_positive_integer)
    parser.add_argument("--github-run-attempt", required=True, type=_positive_integer)
    args = parser.parse_args()
    try:
        if args.operation == "create":
            if args.report is None:
                parser.error("create requires --report")
            repository = args.repository.resolve(strict=True)
            source_root = args.source_root
            if not source_root.is_absolute():
                source_root = repository / source_root
            create_certificate(
                args.report,
                args.certificate,
                repository=repository,
                source_root=source_root,
                github_sha=args.github_sha,
                github_run_id=args.github_run_id,
                github_run_attempt=args.github_run_attempt,
            )
        else:
            repository = args.repository.resolve(strict=True)
            source_root = args.source_root
            if not source_root.is_absolute():
                source_root = repository / source_root
            verify_certificate(
                args.certificate,
                repository=repository,
                source_root=source_root,
                github_sha=args.github_sha,
                github_run_id=args.github_run_id,
                github_run_attempt=args.github_run_attempt,
            )
        print(f"native coverage certificate {args.operation} passed: {args.certificate}")
    except (
        AssertionError,
        json.JSONDecodeError,
        KeyError,
        OSError,
        TypeError,
        ValueError,
    ) as error:
        raise SystemExit(str(error)) from error


if __name__ == "__main__":
    main()
