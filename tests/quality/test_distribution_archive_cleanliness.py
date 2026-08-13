"""Tests for release archive identity, completeness, and cleanliness."""

from __future__ import annotations

import gzip
import io
import tarfile
from importlib.util import module_from_spec, spec_from_file_location
from pathlib import Path
from types import ModuleType

import pytest


def _load_validator() -> ModuleType:
    """Load the distribution validator without packaging ``meta``."""
    path = Path(__file__).parents[2] / "meta" / "ci" / "release" / "check_distribution_contents.py"
    spec = spec_from_file_location("check_distribution_contents", path)
    assert spec is not None and spec.loader is not None
    module = module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _release_filenames(version: str = "0.4.0") -> list[str]:
    """Return the one supported sdist plus the exact stable-ABI wheel matrix."""
    prefix = f"schema_sanitizer-{version}"
    return [
        f"{prefix}.tar.gz",
        f"{prefix}-cp311-abi3-manylinux_2_27_x86_64.manylinux_2_28_x86_64.whl",
        f"{prefix}-cp311-abi3-win_amd64.whl",
        f"{prefix}-cp311-abi3-macosx_11_0_x86_64.whl",
        f"{prefix}-cp311-abi3-macosx_11_0_arm64.whl",
    ]


def test_release_filename_validator_requires_all_supported_wheels() -> None:
    """One consistent sdist and the four release platforms form a valid set."""
    validator = _load_validator()
    assert (
        validator.validate_release_filenames(
            _release_filenames(),
            expected_version="0.4.0",
        )
        == "0.4.0"
    )


def test_release_filename_validator_rejects_version_drift() -> None:
    """A stale platform wheel cannot enter a release set."""
    validator = _load_validator()

    with pytest.raises(AssertionError, match="mismatched distribution versions"):
        validator.validate_release_filenames(
            [
                "schema_sanitizer-0.4.0.tar.gz",
                "schema_sanitizer-0.4.0-cp311-abi3-manylinux_2_27_x86_64.manylinux_2_28_x86_64.whl",
                "schema_sanitizer-0.4.0-cp311-abi3-win_amd64.whl",
                "schema_sanitizer-0.4.0-cp311-abi3-macosx_11_0_x86_64.whl",
                "schema_sanitizer-0.3.9-cp311-abi3-macosx_11_0_arm64.whl",
            ]
        )


def test_release_filename_validator_requires_expected_project_and_version() -> None:
    """Neither another distribution nor a version outside meta/VERSION can pass."""
    validator = _load_validator()
    wrong_project = _release_filenames()
    wrong_project[0] = "other_project-0.4.0.tar.gz"

    with pytest.raises(AssertionError, match="sdist project name"):
        validator.validate_release_filenames(wrong_project)
    with pytest.raises(AssertionError, match="release version .* != expected"):
        validator.validate_release_filenames(
            _release_filenames(),
            expected_version="0.4.1",
        )


@pytest.mark.parametrize(
    ("old", "new", "message"),
    [
        ("cp311-abi3-win_amd64", "cp312-abi3-win_amd64", "only cp311-abi3"),
        ("cp311-abi3-win_amd64", "cp311-cp311-win_amd64", "only cp311-abi3"),
        (
            "manylinux_2_27_x86_64.manylinux_2_28_x86_64",
            "manylinux_2_28_x86_64",
            "unexpected release platform tags",
        ),
        (
            "cp311-abi3-win_amd64",
            "cp311.cp312-abi3-win_amd64",
            "only cp311-abi3",
        ),
    ],
)
def test_release_filename_validator_rejects_noncanonical_wheel_tags(
    old: str,
    new: str,
    message: str,
) -> None:
    """Release wheels use only the audited cp311-abi3 platform tags."""
    validator = _load_validator()
    filenames = [name.replace(old, new) for name in _release_filenames()]

    with pytest.raises(AssertionError, match=message):
        validator.validate_release_filenames(filenames)


def test_release_filename_validator_rejects_duplicate_or_extra_artifacts() -> None:
    """The release set is exactly one artifact per owned platform and nothing else."""
    validator = _load_validator()
    duplicate_platform = _release_filenames()
    duplicate_platform[-1] = duplicate_platform[-2]

    with pytest.raises(AssertionError, match="duplicate macOS x86_64 wheels"):
        validator.validate_release_filenames(duplicate_platform)
    with pytest.raises(AssertionError, match="exactly 5 release files"):
        validator.validate_release_filenames([*_release_filenames(), "checksums.txt"])


def test_release_filename_validator_rejects_wheel_build_tags() -> None:
    """A noncanonical rebuild cannot masquerade as one of the four release wheels."""
    validator = _load_validator()
    filenames = [
        name.replace("-cp311-abi3-win_amd64", "-1-cp311-abi3-win_amd64")
        for name in _release_filenames()
    ]

    with pytest.raises(AssertionError, match="must not carry a build tag"):
        validator.validate_release_filenames(filenames)


def test_archive_timestamp_check_reads_gzip_header(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """Archive clocks must encode the configured source epoch, not runner time."""
    validator = _load_validator()
    epoch = 946_684_800
    monkeypatch.setenv("SOURCE_DATE_EPOCH", str(epoch))
    sdist = tmp_path / "fixture.tar.gz"
    with sdist.open("wb") as handle:
        with gzip.GzipFile(fileobj=handle, mode="wb", mtime=epoch) as compressed:
            with tarfile.open(fileobj=compressed, mode="w") as archive:
                member = tarfile.TarInfo("fixture/value.txt")
                member.mtime = epoch
                member.size = 1
                archive.addfile(member, io.BytesIO(b"x"))
    validator._validate_archive_timestamps(sdist)


def test_archive_timestamp_check_rejects_runner_clock_drift(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """A wheel carrying the runner wall clock cannot pass deterministic validation."""
    validator = _load_validator()
    monkeypatch.setenv("SOURCE_DATE_EPOCH", "946684800")
    sdist = tmp_path / "fixture.tar.gz"
    with sdist.open("wb") as handle:
        with gzip.GzipFile(fileobj=handle, mode="wb", mtime=946_684_802) as compressed:
            with tarfile.open(fileobj=compressed, mode="w") as archive:
                member = tarfile.TarInfo("fixture/value.txt")
                member.mtime = 946_684_802
                member.size = 1
                archive.addfile(member, io.BytesIO(b"x"))

    with pytest.raises(AssertionError, match="SOURCE_DATE_EPOCH"):
        validator._validate_archive_timestamps(sdist)
