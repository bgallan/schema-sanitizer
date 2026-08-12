"""Tests for deterministic and independently verifiable release manifests."""

from __future__ import annotations

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


def _load_ci_module(name: str) -> ModuleType:
    """Load one standalone CI helper with its sibling imports available."""
    ci_dir = Path(__file__).resolve().parents[2] / "meta/ci/release"
    path = ci_dir / f"{name}.py"
    spec = importlib.util.spec_from_file_location(name, path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.path.insert(0, str(ci_dir))
    try:
        spec.loader.exec_module(module)
    finally:
        sys.path.pop(0)
    return module


def _add_tar_file(archive: tarfile.TarFile, name: str, content: bytes = b"fixture\n") -> None:
    """Add one deterministic regular file to a test tarball."""
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
    """Build a minimal, content-valid five-file release set."""
    tmp_path.mkdir(parents=True, exist_ok=True)
    metadata_version = metadata_version or version
    validator = _load_ci_module("check_distribution_contents")
    sdist = tmp_path / f"schema_sanitizer-{version}.tar.gz"
    root = sdist_root or f"schema_sanitizer-{version}"
    metadata = (
        "Metadata-Version: 2.4\n"
        f"Name: {metadata_name}\n"
        f"Version: {metadata_version}\n"
        f"Requires-Python: {requires_python}\n"
        f"{extra_metadata_headers}\n"
    ).encode()
    source_names = {
        *validator._SDIST_REQUIRED,
        "cpp/src/minimal.cpp",
        "tests/test_minimal.py",
    }
    with tarfile.open(sdist, "w:gz") as archive:
        for name in sorted(source_names):
            _add_tar_file(archive, f"{root}/{name}")
        _add_tar_file(
            archive,
            f"{root}/PKG-INFO",
            metadata,
        )

    wheel_platforms = (
        "manylinux_2_27_x86_64.manylinux_2_28_x86_64",
        "win_amd64",
        "macosx_11_0_x86_64",
        "macosx_11_0_arm64",
    )
    wheels: list[Path] = []
    for platform in wheel_platforms:
        wheel = tmp_path / f"schema_sanitizer-{version}-cp311-abi3-{platform}.whl"
        with zipfile.ZipFile(wheel, "w", compression=zipfile.ZIP_DEFLATED) as archive:
            archive.writestr("schema_sanitizer/py.typed", b"")
            archive.writestr(
                f"schema_sanitizer-{version}.dist-info/licenses/LICENSE",
                b"Apache-2.0\n",
            )
            archive.writestr(
                f"{wheel_metadata_project}-{version}.dist-info/METADATA",
                metadata,
            )
        wheels.append(wheel)
    return [wheels[2], sdist, wheels[0], wheels[3], wheels[1]]


def test_release_manifest_is_canonical_complete_and_verifiable(tmp_path: Path) -> None:
    """The manifest binds sorted artifact hashes to immutable run provenance."""
    helper = _load_ci_module("release_manifest")
    artifacts = _release_artifacts(tmp_path)
    version_file = tmp_path / "VERSION"
    version_file.write_text("0.4.0\n", encoding="utf-8")
    manifest_file = tmp_path / "audit/release-manifest.json"

    helper.write_release_manifest(
        manifest_file,
        artifacts,
        version_file=version_file,
        github_sha=_GITHUB_SHA,
        github_run_id=123456,
        github_run_attempt=2,
    )
    payload = json.loads(manifest_file.read_text(encoding="utf-8"))

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
    assert manifest_file.read_text(encoding="utf-8") == helper.canonical_json(payload)

    helper.verify_release_manifest(
        manifest_file,
        reversed(artifacts),
        version_file=version_file,
        github_sha=_GITHUB_SHA,
        github_run_id=123456,
        github_run_attempt=2,
    )


def test_release_manifest_verification_rejects_tampering(tmp_path: Path) -> None:
    """Artifact, provenance, and serialization changes all fail verification."""
    helper = _load_ci_module("release_manifest")
    artifacts = _release_artifacts(tmp_path)
    version_file = tmp_path / "VERSION"
    version_file.write_text("0.4.0\n", encoding="utf-8")
    manifest_file = tmp_path / "release-manifest.json"
    options = {
        "version_file": version_file,
        "github_sha": _GITHUB_SHA,
        "github_run_id": 42,
        "github_run_attempt": 1,
    }
    helper.write_release_manifest(manifest_file, artifacts, **options)

    with zipfile.ZipFile(artifacts[0], "a") as archive:
        archive.writestr("schema_sanitizer/tampered.txt", b"changed")
    with pytest.raises(AssertionError, match="digest mismatch"):
        helper.verify_release_manifest(manifest_file, artifacts, **options)

    artifacts = _release_artifacts(tmp_path / "fresh")
    helper.write_release_manifest(manifest_file, artifacts, **options)
    with pytest.raises(AssertionError, match="digest mismatch"):
        helper.verify_release_manifest(
            manifest_file,
            artifacts,
            **{**options, "github_run_attempt": 2},
        )

    manifest_file.write_text("\n" + manifest_file.read_text(encoding="utf-8"), encoding="utf-8")
    with pytest.raises(AssertionError, match="not canonical"):
        helper.verify_release_manifest(manifest_file, artifacts, **options)


def test_release_validation_rejects_filename_metadata_drift(tmp_path: Path) -> None:
    """A renamed or stale archive cannot pass on filename checks alone."""
    validator = _load_ci_module("check_distribution_contents")
    artifacts = _release_artifacts(tmp_path, metadata_version="0.3.9")

    with pytest.raises(AssertionError, match="metadata version"):
        validator.validate_release_set(artifacts, expected_version="0.4.0")


@pytest.mark.parametrize(
    ("options", "message"),
    [
        ({"metadata_name": "other-project"}, "metadata project"),
        ({"requires_python": ">=3.12"}, "Requires-Python must be exactly"),
        (
            {"extra_metadata_headers": "Version: 0.4.0\n"},
            "exactly one non-empty Version",
        ),
        ({"sdist_root": "renamed-0.4.0"}, "expected one schema_sanitizer-0.4.0/PKG-INFO"),
        (
            {"wheel_metadata_project": "renamed"},
            "expected one schema_sanitizer-0.4.0.dist-info/METADATA",
        ),
    ],
)
def test_release_validation_rejects_core_metadata_drift(
    tmp_path: Path,
    options: dict[str, str],
    message: str,
) -> None:
    """Core identity, compatibility, uniqueness, and member paths are immutable."""
    validator = _load_ci_module("check_distribution_contents")
    artifacts = _release_artifacts(tmp_path, **options)

    with pytest.raises(AssertionError, match=message):
        validator.validate_release_set(artifacts, expected_version="0.4.0")


def test_release_manifest_cli_creates_and_rechecks_the_same_contract(tmp_path: Path) -> None:
    """The command used by CI supports a separate verification pass."""
    artifacts = _release_artifacts(tmp_path)
    version_file = tmp_path / "VERSION"
    version_file.write_text("0.4.0\n", encoding="utf-8")
    manifest_file = tmp_path / "release-manifest.json"
    script = Path(__file__).resolve().parents[2] / "meta/ci/release/release_manifest.py"
    base_command = [
        sys.executable,
        str(script),
        "--manifest",
        str(manifest_file),
        "--version-file",
        str(version_file),
        "--github-sha",
        _GITHUB_SHA,
        "--github-run-id",
        "123456",
        "--github-run-attempt",
        "1",
        *(str(path) for path in artifacts),
    ]

    created = subprocess.run(
        [base_command[0], base_command[1], "create", *base_command[2:]],
        check=False,
        capture_output=True,
        text=True,
    )
    assert created.returncode == 0, created.stderr
    verified = subprocess.run(
        [base_command[0], base_command[1], "verify", *base_command[2:]],
        check=False,
        capture_output=True,
        text=True,
    )
    assert verified.returncode == 0, verified.stderr
