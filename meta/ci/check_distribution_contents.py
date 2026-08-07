#!/usr/bin/env python3
"""Validate release archive contents from a downstream-consumer perspective."""

from __future__ import annotations

import argparse
import tarfile
import zipfile
from pathlib import Path, PurePosixPath
from typing import Iterable

_SCRATCH_PARTS = {
    ".mypy_cache",
    ".pytest_cache",
    ".ruff_cache",
    "__pycache__",
    "build",
    "dist",
    "wheelhouse",
}
_SCRATCH_FILES = {".coverage", "coverage.xml"}
_SCRATCH_SUFFIXES = {
    ".gcda",
    ".gcno",
    ".o",
    ".obj",
    ".profdata",
    ".profraw",
    ".pyc",
    ".pyo",
}
_SDIST_REQUIRED = {
    "CMakeLists.txt",
    "LICENSE",
    "README.md",
    "cmake/SchemaSanitizerCompression.cmake",
    "cmake/SchemaSanitizerSources.cmake",
    "cmake/SchemaSanitizerTargetOptions.cmake",
    "docs/README.md",
    "docs/ci-cd.md",
    "docs/compatibility.md",
    "docs/heuristics.md",
    "docs/python-api.md",
    "fuzz/regressions/README.md",
    "fuzz/regressions/csv/unterminated.csv",
    "fuzz/regressions/json/truncated.json",
    "fuzz/regressions/parquet/truncated.parquet",
    "fuzz/regressions/xml/mismatched.xml",
    "meta/VERSION",
    "meta/ci/check_downstream_install.py",
    "meta/ci/check_parquet_compression_matrix.py",
    "meta/ci/report_risk_coverage.py",
    "meta/ci/run_fuzz_regressions.py",
    "pyproject.toml",
    "src/schema_sanitizer/py.typed",
}


def _members(path: Path) -> list[str]:
    """Return file members from a supported distribution archive."""
    if path.name.endswith((".tar.gz", ".tgz")):
        with tarfile.open(path, "r:gz") as archive:
            return [member.name for member in archive.getmembers() if member.isfile()]
    if path.suffix == ".whl" or path.suffix == ".zip":
        with zipfile.ZipFile(path) as archive:
            return [item.filename for item in archive.infolist() if not item.is_dir()]
    raise ValueError(f"unsupported distribution type: {path}")


def _strip_sdist_root(names: Iterable[str]) -> set[str]:
    """Remove the single top-level directory from sdist member names."""
    normalized = [PurePosixPath(name) for name in names]
    roots = {name.parts[0] for name in normalized if name.parts}
    if len(roots) != 1:
        raise AssertionError(f"sdist must have one archive root, got {sorted(roots)}")
    return {PurePosixPath(*name.parts[1:]).as_posix() for name in normalized}


def _scratch_entries(names: Iterable[str]) -> list[str]:
    """Return archive entries that look like local build scratch files."""
    rejected: list[str] = []
    for raw_name in names:
        path = PurePosixPath(raw_name)
        root = path.parts[0] if path.parts else ""
        if root.startswith(("build-", "cmake-build-", ".build")):
            rejected.append(raw_name)
            continue
        if any(part in _SCRATCH_PARTS for part in path.parts):
            rejected.append(raw_name)
            continue
        if path.name in _SCRATCH_FILES:
            rejected.append(raw_name)
            continue
        if path.suffix.lower() in _SCRATCH_SUFFIXES:
            rejected.append(raw_name)
    return sorted(rejected)


def _validate_sdist(path: Path, names: list[str]) -> None:
    """Validate source-distribution completeness and cleanliness."""
    relative = _strip_sdist_root(names)
    missing = sorted(_SDIST_REQUIRED - relative)
    if missing:
        raise AssertionError(f"{path.name}: missing required sdist files: {missing}")

    scratch = _scratch_entries(relative)
    if scratch:
        preview = scratch[:20]
        raise AssertionError(f"{path.name}: contains scratch/build files: {preview}")

    if not any(name.startswith("cpp/src/") for name in relative):
        raise AssertionError(f"{path.name}: native sources are missing")
    if not any(name.startswith("tests/") for name in relative):
        raise AssertionError(f"{path.name}: test inputs are missing")


def _validate_source_zip(path: Path, names: list[str]) -> None:
    """Validate a repository source ZIP with no enclosing root directory."""
    relative = {PurePosixPath(name).as_posix() for name in names}
    missing = sorted(_SDIST_REQUIRED - relative)
    if missing:
        raise AssertionError(f"{path.name}: missing required source files: {missing}")

    scratch = _scratch_entries(relative)
    if scratch:
        raise AssertionError(f"{path.name}: contains scratch/build files: {scratch[:20]}")

    if not any(name.startswith("cpp/src/") for name in relative):
        raise AssertionError(f"{path.name}: native sources are missing")
    if not any(name.startswith("tests/") for name in relative):
        raise AssertionError(f"{path.name}: tests are missing")


def _validate_wheel(path: Path, names: list[str]) -> None:
    """Validate installed-wheel metadata and cleanliness."""
    wheel_names = set(names)
    if "schema_sanitizer/py.typed" not in wheel_names:
        raise AssertionError(f"{path.name}: schema_sanitizer/py.typed is missing")
    if not any(name.endswith(".dist-info/licenses/LICENSE") for name in wheel_names):
        raise AssertionError(f"{path.name}: packaged LICENSE is missing")

    scratch = _scratch_entries(wheel_names)
    if scratch:
        raise AssertionError(f"{path.name}: contains scratch/build files: {scratch[:20]}")


def validate(path: Path) -> None:
    """Validate one release artifact according to its archive type."""
    names = _members(path)
    if path.name.endswith((".tar.gz", ".tgz")):
        _validate_sdist(path, names)
    elif path.suffix == ".whl":
        _validate_wheel(path, names)
    elif path.suffix == ".zip":
        _validate_source_zip(path, names)
    else:
        raise ValueError(f"unsupported release artifact: {path}")
    print(f"validated {path}: {len(names)} files")


def validate_release_filenames(filenames: Iterable[str]) -> None:
    """Validate the completeness and version consistency of one release set."""
    from packaging.utils import parse_sdist_filename, parse_wheel_filename

    names = sorted(filenames)
    sdists = [name for name in names if name.endswith(".tar.gz")]
    wheels = [name for name in names if name.endswith(".whl")]
    if len(sdists) != 1:
        raise AssertionError(f"expected exactly 1 sdist, found {len(sdists)}")
    if len(wheels) != 4:
        raise AssertionError(f"expected exactly 4 wheels, found {len(wheels)}")

    sdist_name, sdist_version = parse_sdist_filename(sdists[0])
    wheel_versions = set()
    wheel_platforms = set()
    for wheel in wheels:
        name, version, _build, tags = parse_wheel_filename(wheel)
        if name != sdist_name:
            raise AssertionError(f"wheel project name {name!s} != sdist name {sdist_name!s}")
        wheel_versions.add(version)
        wheel_platforms.update(tag.platform for tag in tags)

    versions = {sdist_version, *wheel_versions}
    if len(versions) != 1:
        raise AssertionError(f"mismatched distribution versions: {sorted(map(str, versions))}")

    required_platforms = (
        ("Linux x86_64", lambda platform: "manylinux_2_28_x86_64" in platform),
        ("Windows AMD64", lambda platform: platform == "win_amd64"),
        (
            "macOS x86_64",
            lambda platform: platform.startswith("macosx") and platform.endswith("_x86_64"),
        ),
        (
            "macOS arm64",
            lambda platform: platform.startswith("macosx") and platform.endswith("_arm64"),
        ),
    )
    for label, predicate in required_platforms:
        if not any(predicate(platform) for platform in wheel_platforms):
            raise AssertionError(f"missing {label} wheel: {' '.join(sorted(wheel_platforms))}")


def validate_release_set(paths: Iterable[Path]) -> None:
    """Validate every archive and the release set as a whole."""
    artifacts = sorted(paths)
    for artifact in artifacts:
        validate(artifact)
    validate_release_filenames(path.name for path in artifacts)


def main() -> None:
    """Parse command-line artifacts and validate each one."""
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--release-set",
        action="store_true",
        help="also require one sdist and the four supported, version-consistent wheels",
    )
    parser.add_argument("artifacts", nargs="+", type=Path)
    args = parser.parse_args()
    if args.release_set:
        validate_release_set(args.artifacts)
    else:
        for artifact in args.artifacts:
            validate(artifact)


if __name__ == "__main__":
    main()
