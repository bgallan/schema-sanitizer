"""Tests for release archive identity, completeness, and cleanliness.

It checks release filenames, wheel and source contents, deterministic timestamps,
metadata identity, scratch-file rejection, and manifest verification.
"""

from __future__ import annotations

import gzip
import hashlib
import importlib.util
import io
import json
import subprocess
import sys
import tarfile
import zipfile
from pathlib import Path
from types import ModuleType

import pytest

_GITHUB_SHA = "0123456789abcdef" * 2 + "01234567"


def _load_validator(name: str = "check_distribution_contents") -> ModuleType:
    """Load one standalone release helper with sibling imports available."""
    directory = Path(__file__).parents[2] / "meta/ci/release"
    path = directory / f"{name}.py"
    spec = importlib.util.spec_from_file_location(name, path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.path.insert(0, str(directory))
    try:
        spec.loader.exec_module(module)
    finally:
        sys.path.pop(0)
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
    ("canonical", "replacement", "message"),
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
    canonical: str,
    replacement: str,
    message: str,
) -> None:
    """Release wheels use only the audited cp311-abi3 platform tags."""
    validator = _load_validator()
    filenames = [name.replace(canonical, replacement) for name in _release_filenames()]

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


@pytest.mark.parametrize(
    "member_name",
    ("../escape.py", "/absolute.py", "C:/windows.py", "package\\windows.py"),
)
def test_distribution_validator_rejects_unsafe_member_names(
    member_name: str,
    tmp_path: Path,
) -> None:
    """Release archives cannot encode traversal or platform-dependent paths."""
    wheel = tmp_path / "unsafe.whl"
    with zipfile.ZipFile(wheel, "w") as archive:
        member = zipfile.ZipInfo("placeholder")
        member.filename = member.orig_filename = member_name
        archive.writestr(member, b"unsafe")

    with pytest.raises(AssertionError, match="unsafe archive member names"):
        _load_validator().validate(wheel)


def test_distribution_validator_rejects_duplicate_members_and_symlinks(tmp_path: Path) -> None:
    """Ambiguous member lookup and symlinked artifact aliases fail closed."""
    wheel = tmp_path / "duplicates.whl"
    with zipfile.ZipFile(wheel, "w") as archive:
        archive.writestr("duplicate", b"first")
        with pytest.warns(UserWarning, match="Duplicate name"):
            archive.writestr("duplicate", b"second")
    with pytest.raises(AssertionError, match="must be unique"):
        _load_validator().validate(wheel)

    linked = tmp_path / "linked.whl"
    try:
        linked.symlink_to(wheel)
    except OSError:
        pytest.skip("symlinks are unavailable")
    with pytest.raises(AssertionError, match="regular file"):
        _load_validator().validate(linked)


def _add_tar_file(archive: tarfile.TarFile, name: str, content: bytes = b"fixture\n") -> None:
    """Add a deterministic regular file member to the test archive."""
    member = tarfile.TarInfo(name)
    member.size = len(content)
    member.mtime = 0
    member.mode = 0o644
    archive.addfile(member, io.BytesIO(content))


def _release_artifacts(
    tmp_path: Path,
    version: str = "0.4.0",
    *,
    metadata_version: str | None = None,
    metadata_name: str = "schema-sanitizer",
    requires_python: str = ">=3.11",
    extra_metadata_headers: str = "",
    sdist_root: str | None = None,
    wheel_metadata_project: str = "schema_sanitizer",
) -> list[Path]:
    """Build a minimal content-valid five-file release set."""
    tmp_path.mkdir(parents=True, exist_ok=True)
    metadata_version = metadata_version or version
    required = _load_validator()._SDIST_REQUIRED
    sdist = tmp_path / f"schema_sanitizer-{version}.tar.gz"
    root = sdist_root or f"schema_sanitizer-{version}"
    metadata = (
        "Metadata-Version: 2.4\n"
        f"Name: {metadata_name}\nVersion: {metadata_version}\n"
        f"Requires-Python: {requires_python}\n{extra_metadata_headers}\n"
    ).encode()
    with tarfile.open(sdist, "w:gz") as archive:
        for name in sorted({*required, "cpp/src/minimal.cpp", "tests/test_minimal.py"}):
            _add_tar_file(archive, f"{root}/{name}")
        _add_tar_file(archive, f"{root}/PKG-INFO", metadata)

    wheels = []
    for platform in (
        "manylinux_2_27_x86_64.manylinux_2_28_x86_64",
        "win_amd64",
        "macosx_11_0_x86_64",
        "macosx_11_0_arm64",
    ):
        wheel = tmp_path / f"schema_sanitizer-{version}-cp311-abi3-{platform}.whl"
        with zipfile.ZipFile(wheel, "w", compression=zipfile.ZIP_DEFLATED) as archive:
            archive.writestr("schema_sanitizer/py.typed", b"")
            archive.writestr(
                f"schema_sanitizer-{version}.dist-info/licenses/LICENSE", b"Apache-2.0\n"
            )
            archive.writestr(f"{wheel_metadata_project}-{version}.dist-info/METADATA", metadata)
        wheels.append(wheel)
    return [wheels[2], sdist, wheels[0], wheels[3], wheels[1]]


def test_release_manifest_is_canonical_complete_and_verifiable(tmp_path: Path) -> None:
    """Verify release manifest is canonical complete and verifiable."""
    helper = _load_validator("release_manifest")
    artifacts = _release_artifacts(tmp_path)
    version = tmp_path / "VERSION"
    version.write_text("0.4.0\n", encoding="utf-8")
    manifest = tmp_path / "audit/release-manifest.json"
    options = {
        "version_file": version,
        "github_sha": _GITHUB_SHA,
        "github_run_id": 123456,
        "github_run_attempt": 2,
    }
    helper.write_release_manifest(manifest, artifacts, **options)
    payload = json.loads(manifest.read_text(encoding="utf-8"))
    assert payload["format"] == "schema-sanitizer-release-manifest-v1"
    assert payload["project"] == "schema-sanitizer"
    assert payload["version"] == "0.4.0"
    assert payload["provenance"] == {
        "git_sha": _GITHUB_SHA,
        "github_run_attempt": 2,
        "github_run_id": 123456,
    }
    assert [entry["filename"] for entry in payload["artifacts"]] == sorted(
        path.name for path in artifacts
    )
    for entry in payload["artifacts"]:
        artifact = next(path for path in artifacts if path.name == entry["filename"])
        assert entry["size"] == artifact.stat().st_size
        assert entry["sha256"] == hashlib.sha256(artifact.read_bytes()).hexdigest()
    assert manifest.read_text(encoding="utf-8") == helper.canonical_json(payload)
    helper.verify_release_manifest(manifest, reversed(artifacts), **options)
    first_mtime = manifest.stat().st_mtime_ns
    helper.write_release_manifest(manifest, reversed(artifacts), **options)
    assert manifest.stat().st_mtime_ns == first_mtime


def test_release_manifest_verification_rejects_tampering(tmp_path: Path) -> None:
    """Verify release manifest verification rejects tampering."""
    helper = _load_validator("release_manifest")
    artifacts = _release_artifacts(tmp_path)
    version = tmp_path / "VERSION"
    version.write_text("0.4.0\n", encoding="utf-8")
    manifest = tmp_path / "release-manifest.json"
    options = {
        "version_file": version,
        "github_sha": _GITHUB_SHA,
        "github_run_id": 42,
        "github_run_attempt": 1,
    }
    helper.write_release_manifest(manifest, artifacts, **options)
    with zipfile.ZipFile(artifacts[0], "a") as archive:
        archive.writestr("schema_sanitizer/tampered.txt", b"changed")
    with pytest.raises(AssertionError, match="digest mismatch"):
        helper.verify_release_manifest(manifest, artifacts, **options)

    artifacts = _release_artifacts(tmp_path / "fresh")
    helper.write_release_manifest(manifest, artifacts, **options)
    with pytest.raises(AssertionError, match="digest mismatch"):
        helper.verify_release_manifest(manifest, artifacts, **{**options, "github_run_attempt": 2})
    manifest.write_text("\n" + manifest.read_text(encoding="utf-8"), encoding="utf-8")
    with pytest.raises(AssertionError, match="not canonical"):
        helper.verify_release_manifest(manifest, artifacts, **options)


def test_release_manifest_rejects_symlinked_inputs_and_outputs(tmp_path: Path) -> None:
    """Release provenance cannot be read or written through mutable symlink aliases."""
    helper = _load_validator("release_manifest")
    artifacts = _release_artifacts(tmp_path / "artifacts")
    version = tmp_path / "VERSION"
    version.write_text("0.4.0\n", encoding="utf-8")
    options = {
        "version_file": version,
        "github_sha": _GITHUB_SHA,
        "github_run_id": 42,
        "github_run_attempt": 1,
    }
    manifest = tmp_path / "release-manifest.json"
    target = tmp_path / "manifest-target.json"
    target.write_text("preserve\n", encoding="utf-8")
    try:
        manifest.symlink_to(target)
    except OSError:
        pytest.skip("symlinks are unavailable")

    with pytest.raises(AssertionError, match="must not be a symlink"):
        helper.write_release_manifest(manifest, artifacts, **options)
    assert target.read_text(encoding="utf-8") == "preserve\n"

    manifest.unlink()
    linked_version = tmp_path / "VERSION.link"
    linked_version.symlink_to(version)
    with pytest.raises(AssertionError, match="version source must be a regular file"):
        helper.write_release_manifest(
            manifest,
            artifacts,
            **{**options, "version_file": linked_version},
        )


def test_release_validation_rejects_filename_metadata_drift(tmp_path: Path) -> None:
    """Verify release validation rejects filename metadata drift."""
    artifacts = _release_artifacts(tmp_path, metadata_version="0.3.9")
    with pytest.raises(AssertionError, match="metadata version"):
        _load_validator().validate_release_set(artifacts, expected_version="0.4.0")


@pytest.mark.parametrize(
    ("options", "message"),
    (
        ({"metadata_name": "other-project"}, "metadata project"),
        ({"requires_python": ">=3.12"}, "Requires-Python must be exactly"),
        ({"extra_metadata_headers": "Version: 0.4.0\n"}, "exactly one non-empty Version"),
        ({"sdist_root": "renamed-0.4.0"}, "expected one schema_sanitizer-0.4.0/PKG-INFO"),
        (
            {"wheel_metadata_project": "renamed"},
            "expected one schema_sanitizer-0.4.0.dist-info/METADATA",
        ),
    ),
)
def test_release_validation_rejects_core_metadata_drift(
    tmp_path: Path, options: dict[str, str], message: str
) -> None:
    """Verify release validation rejects core metadata drift."""
    artifacts = _release_artifacts(tmp_path, **options)
    with pytest.raises(AssertionError, match=message):
        _load_validator().validate_release_set(artifacts, expected_version="0.4.0")


def test_release_manifest_cli_creates_and_rechecks_the_same_contract(tmp_path: Path) -> None:
    """Verify release manifest CLI creates and rechecks the same contract."""
    artifacts = _release_artifacts(tmp_path)
    version = tmp_path / "VERSION"
    version.write_text("0.4.0\n", encoding="utf-8")
    manifest = tmp_path / "release-manifest.json"
    command = [
        sys.executable,
        str(Path(__file__).parents[2] / "meta/ci/release/release_manifest.py"),
        "--manifest",
        str(manifest),
        "--version-file",
        str(version),
        "--github-sha",
        _GITHUB_SHA,
        "--github-run-id",
        "123456",
        "--github-run-attempt",
        "1",
        *(str(path) for path in artifacts),
    ]
    for action in ("create", "verify"):
        result = subprocess.run(
            [command[0], command[1], action, *command[2:]],
            check=False,
            capture_output=True,
            text=True,
        )
        assert result.returncode == 0, result.stderr
