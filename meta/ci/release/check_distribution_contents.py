#!/usr/bin/env python3
"""Validate release archives from a downstream-consumer perspective."""

from __future__ import annotations

import argparse
import tarfile
import zipfile
from email.message import Message
from email.parser import BytesParser
from email.policy import compat32
from pathlib import Path, PurePosixPath
from typing import Iterable

RELEASE_PROJECT = "schema-sanitizer"
RELEASE_WHEEL_PLATFORM_TAGS = {
    "Linux x86_64": frozenset({"manylinux_2_27_x86_64", "manylinux_2_28_x86_64"}),
    "Windows AMD64": frozenset({"win_amd64"}),
    "macOS x86_64": frozenset({"macosx_11_0_x86_64"}),
    "macOS arm64": frozenset({"macosx_11_0_arm64"}),
}

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
    "docs/guides/flat-prefix-modified-time-csv.md",
    "docs/guides/getting-started.md",
    "docs/guides/partitioned-pipelines.md",
    "docs/internals/concurrency-lifecycle.md",
    "docs/internals/execution-heuristics.md",
    "docs/operations/reader-complexity.md",
    "docs/operations/reader-security-limits.md",
    "docs/operations/resources-and-concurrency.md",
    "docs/project/ci-cd.md",
    "docs/project/development.md",
    "docs/reference/bigquery.md",
    "docs/reference/compatibility.md",
    "docs/reference/inputs-and-filesystems.md",
    "docs/reference/options.md",
    "docs/reference/python-api.md",
    "docs/reference/schema-and-registry.md",
    "fuzz/regressions/README.md",
    "fuzz/regressions/csv/unterminated.csv",
    "fuzz/regressions/json/truncated.json",
    "fuzz/regressions/parquet/truncated.parquet",
    "fuzz/regressions/xml/mismatched.xml",
    "meta/VERSION",
    "meta/ci/README.md",
    "meta/ci/fuzz/check_fuzz_corpus.py",
    "meta/ci/fuzz/run_fuzz_regressions.py",
    "meta/ci/native/check_cpp_documentation.py",
    "meta/ci/native/check_no_arrow_cpp.sh",
    "meta/ci/native/check_no_libarrow_linkage.sh",
    "meta/ci/parquet/check_parquet_compression_matrix.py",
    "meta/ci/parquet/check_parquet_contract_runtime.py",
    "meta/ci/parquet/check_parquet_contract_runtime_suite.py",
    "meta/ci/quality/check_detect_secrets_report.py",
    "meta/ci/quality/check_primary_cleanup.py",
    "meta/ci/quality/report_risk_coverage.py",
    "meta/ci/release/check_distribution_contents.py",
    "meta/ci/release/check_downstream_install.py",
    "meta/ci/release/check_github_release_environment.py",
    "meta/ci/release/check_pypi_version.py",
    "meta/ci/release/downstream_smoke.py",
    "meta/ci/release/downstream_typecheck.py",
    "meta/ci/release/release_manifest.py",
    "meta/ci/release/validate_release_version.py",
    "meta/ci/sanitizers/asan_python_launcher.cc",
    "meta/ci/sanitizers/run_tsan_extension_suite.sh",
    "meta/ci/sanitizers/tsan_python_launcher.cc",
    "pyproject.toml",
    "src/schema_sanitizer/py.typed",
}


def _members(path: Path) -> list[str]:
    """Return file members from a supported distribution archive."""
    if path.name.endswith((".tar.gz", ".tgz")):
        with tarfile.open(path, "r:gz") as archive:
            return [member.name for member in archive.getmembers() if member.isfile()]
    if path.suffix == ".whl":
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


def _metadata_member(path: Path) -> str:
    """Return the only valid core-metadata member name for an archive."""
    if path.name.endswith((".tar.gz", ".tgz")):
        suffix = ".tar.gz" if path.name.endswith(".tar.gz") else ".tgz"
        return f"{path.name.removesuffix(suffix)}/PKG-INFO"
    wheel_parts = path.name.removesuffix(".whl").split("-")
    if len(wheel_parts) < 5:
        raise AssertionError(f"{path.name}: malformed wheel filename")
    return f"{wheel_parts[0]}-{wheel_parts[1]}.dist-info/METADATA"


def _metadata_payload(path: Path) -> bytes:
    """Read the single core-metadata member from its canonical archive path."""
    expected_member = _metadata_member(path)
    if path.name.endswith((".tar.gz", ".tgz")):
        with tarfile.open(path, "r:gz") as archive:
            members = [
                member
                for member in archive.getmembers()
                if member.isfile() and member.name == expected_member
            ]
            if len(members) != 1:
                raise AssertionError(
                    f"{path.name}: expected one {expected_member}, found {len(members)}"
                )
            handle = archive.extractfile(members[0])
            if handle is None:
                raise AssertionError(f"{path.name}: could not read PKG-INFO")
            return handle.read()
    with zipfile.ZipFile(path) as archive:
        members = [name for name in archive.namelist() if name == expected_member]
        if len(members) != 1:
            raise AssertionError(
                f"{path.name}: expected one {expected_member}, found {len(members)}"
            )
        return archive.read(members[0])


def _unique_metadata_value(message: Message, field: str, path: Path) -> str:
    """Return one required, non-empty core-metadata field."""
    values = message.get_all(field, [])
    if len(values) != 1 or not values[0]:
        raise AssertionError(
            f"{path.name}: expected exactly one non-empty {field}, found {values!r}"
        )
    return values[0]


def _validate_archive_metadata(path: Path) -> None:
    """Cross-check archive metadata against its immutable distribution filename."""
    from packaging.utils import canonicalize_name, parse_sdist_filename, parse_wheel_filename

    message = BytesParser(policy=compat32).parsebytes(_metadata_payload(path))
    if message.defects:
        raise AssertionError(f"{path.name}: malformed core metadata: {message.defects!r}")
    _unique_metadata_value(message, "Metadata-Version", path)
    metadata_name = canonicalize_name(_unique_metadata_value(message, "Name", path))
    metadata_version = _unique_metadata_value(message, "Version", path)
    requires_python = _unique_metadata_value(message, "Requires-Python", path)
    if path.name.endswith((".tar.gz", ".tgz")):
        sdist_name = (
            path.name
            if path.name.endswith(".tar.gz")
            else f"{path.name.removesuffix('.tgz')}.tar.gz"
        )
        filename_name, filename_version = parse_sdist_filename(sdist_name)
    else:
        filename_name, filename_version, _build, _tags = parse_wheel_filename(path.name)
    if metadata_name != canonicalize_name(RELEASE_PROJECT) or metadata_name != filename_name:
        raise AssertionError(
            f"{path.name}: metadata project {metadata_name!s} != filename project {filename_name!s}"
        )
    if metadata_version != str(filename_version):
        raise AssertionError(
            f"{path.name}: metadata version {metadata_version!r} != filename {filename_version!s}"
        )
    if requires_python != ">=3.11":
        raise AssertionError(
            f"{path.name}: Requires-Python must be exactly >=3.11, got {requires_python!r}"
        )


def validate(path: Path) -> None:
    """Validate one release artifact according to its archive type."""
    names = _members(path)
    if path.name.endswith((".tar.gz", ".tgz")):
        _validate_sdist(path, names)
        _validate_archive_metadata(path)
    elif path.suffix == ".whl":
        _validate_wheel(path, names)
        _validate_archive_metadata(path)
    else:
        raise ValueError(f"unsupported release artifact: {path}")
    print(f"validated {path}: {len(names)} files")


def validate_release_filenames(
    filenames: Iterable[str],
    *,
    expected_version: str | None = None,
) -> str:
    """Validate and return the version of the canonical five-file release set."""
    from packaging.utils import canonicalize_name, parse_sdist_filename, parse_wheel_filename
    from packaging.version import InvalidVersion, Version

    names = sorted(filenames)
    if len(names) != 5:
        raise AssertionError(f"expected exactly 5 release files, found {len(names)}")

    unsupported = [name for name in names if not name.endswith((".tar.gz", ".whl"))]
    if unsupported:
        raise AssertionError(f"unexpected release files: {unsupported}")

    sdists = [name for name in names if name.endswith(".tar.gz")]
    wheels = [name for name in names if name.endswith(".whl")]
    if len(sdists) != 1:
        raise AssertionError(f"expected exactly 1 sdist, found {len(sdists)}")
    if len(wheels) != 4:
        raise AssertionError(f"expected exactly 4 wheels, found {len(wheels)}")

    sdist_name, sdist_version = parse_sdist_filename(sdists[0])
    expected_project = canonicalize_name(RELEASE_PROJECT)
    if sdist_name != expected_project:
        raise AssertionError(f"sdist project name {sdist_name!s} != expected {expected_project!s}")

    wheel_versions = set()
    wheel_platforms: dict[str, str] = {}
    for wheel in wheels:
        name, version, build, tags = parse_wheel_filename(wheel)
        if name != expected_project:
            raise AssertionError(f"wheel project name {name!s} != expected {expected_project!s}")
        if build:
            raise AssertionError(f"{wheel}: release wheels must not carry a build tag")
        wheel_versions.add(version)

        interpreter_abis = {(tag.interpreter, tag.abi) for tag in tags}
        if interpreter_abis != {("cp311", "abi3")}:
            raise AssertionError(
                f"{wheel}: expected only cp311-abi3 tags, got {sorted(interpreter_abis)}"
            )
        platforms = frozenset(tag.platform for tag in tags)
        matching_platforms = [
            label
            for label, expected_tags in RELEASE_WHEEL_PLATFORM_TAGS.items()
            if platforms == expected_tags
        ]
        if len(matching_platforms) != 1:
            raise AssertionError(f"{wheel}: unexpected release platform tags {sorted(platforms)}")
        platform = matching_platforms[0]
        if platform in wheel_platforms:
            raise AssertionError(
                f"duplicate {platform} wheels: {wheel_platforms[platform]} and {wheel}"
            )
        wheel_platforms[platform] = wheel

    versions = {sdist_version, *wheel_versions}
    if len(versions) != 1:
        raise AssertionError(f"mismatched distribution versions: {sorted(map(str, versions))}")

    missing_platforms = sorted(set(RELEASE_WHEEL_PLATFORM_TAGS) - set(wheel_platforms))
    if missing_platforms:
        raise AssertionError(f"missing release wheels: {missing_platforms}")

    release_version = next(iter(versions))
    if expected_version is not None:
        try:
            wanted_version = Version(expected_version)
        except InvalidVersion as exc:
            raise AssertionError(f"invalid expected version: {expected_version!r}") from exc
        if release_version != wanted_version:
            raise AssertionError(
                f"release version {release_version!s} != expected {wanted_version!s}"
            )
    return str(release_version)


def validate_release_set(
    paths: Iterable[Path],
    *,
    expected_version: str | None = None,
) -> str:
    """Validate every archive and return the release-set version."""
    artifacts = sorted(paths)
    for artifact in artifacts:
        validate(artifact)
    return validate_release_filenames(
        (path.name for path in artifacts),
        expected_version=expected_version,
    )


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
