#!/usr/bin/env python3
"""Build and verify the four durable wheel bundles retained after CI.

The validation gate places every compact certificate and release input inside the
four platform wheel artifacts.  Later job reruns can therefore reconstruct their
validated inputs after transient artifacts have been pruned, without trusting an
unhashed cache or increasing the final artifact count.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import shutil
from collections.abc import Iterable, Mapping
from pathlib import Path
from typing import Any

BUNDLE_FORMAT = "schema-sanitizer-certified-wheel-bundle-v1"
PLATFORMS = ("linux-x86_64", "macos-arm64", "macos-x86_64", "windows-amd64")
_GLOBAL_ARTIFACTS = ("native-coverage-certificate", "source-distribution")
_SANITIZER_ARTIFACTS = {
    "linux-x86_64": (
        "sanitizer-certificate-Linux-X64-asan-ubsan",
        "sanitizer-certificate-Linux-X64-tsan",
    ),
    "macos-arm64": ("sanitizer-certificate-macOS-ARM64-asan-ubsan",),
    "macos-x86_64": ("sanitizer-certificate-macOS-X64-asan-ubsan",),
    "windows-amd64": ("sanitizer-certificate-Windows-X64-asan",),
}
_GIT_SHA_LENGTHS = {40, 64}


def _canonical_json(payload: Mapping[str, Any]) -> str:
    """Serialize one bundle manifest using the repository's canonical JSON form."""
    return json.dumps(payload, indent=2, sort_keys=True, ensure_ascii=False) + "\n"


def _validate_identity(github_sha: str, github_run_id: int, github_run_attempt: int) -> None:
    """Require an exact positive workflow identity and lowercase hexadecimal SHA."""
    if (
        type(github_sha) is not str
        or len(github_sha) not in _GIT_SHA_LENGTHS
        or github_sha.lower() != github_sha
        or any(character not in "0123456789abcdef" for character in github_sha)
    ):
        raise ValueError("github_sha must be a lowercase 40- or 64-character Git object ID")
    if (
        type(github_run_id) is not int
        or type(github_run_attempt) is not int
        or github_run_id < 1
        or github_run_attempt < 1
    ):
        raise ValueError("GitHub run identity values must be positive exact integers")


def _require_directory(path: Path, label: str) -> Path:
    """Resolve and return one non-symlinked directory."""
    if path.is_symlink() or not path.is_dir():
        raise AssertionError(f"{label} must be a regular directory: {path}")
    return path.resolve()


def _regular_files(root: Path) -> tuple[Path, ...]:
    """Return every regular descendant in stable order and reject unsafe entries."""
    resolved = _require_directory(root, "bundle input")
    files: list[Path] = []
    for entry in sorted(root.rglob("*"), key=lambda candidate: candidate.as_posix()):
        if entry.is_symlink() or (not entry.is_file() and not entry.is_dir()):
            raise AssertionError(f"bundle tree contains an unsafe entry: {entry}")
        if entry.is_file():
            if not entry.resolve().is_relative_to(resolved):
                raise AssertionError(f"bundle file escapes its root: {entry}")
            files.append(entry)
    return tuple(files)


def _require_new_directory(path: Path, label: str) -> None:
    """Create one output directory only when no prior entry occupies the path."""
    if path.is_symlink() or path.exists():
        raise AssertionError(f"{label} must not already exist: {path}")
    path.mkdir(parents=True)


def _copy_tree(source: Path, destination: Path) -> None:
    """Copy one validated regular tree without following symbolic links."""
    files = _regular_files(source)
    if not files:
        raise AssertionError(f"bundle source tree must not be empty: {source}")
    _require_new_directory(destination, "bundle destination")
    source_root = source.resolve()
    for file in files:
        relative = file.resolve().relative_to(source_root)
        target = destination / relative
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(file, target)


def _copy_file(source: Path, destination: Path) -> None:
    """Copy one regular file to a previously absent destination."""
    if source.is_symlink() or not source.is_file():
        raise AssertionError(f"bundle source must be a regular file: {source}")
    if destination.is_symlink() or destination.exists():
        raise AssertionError(f"bundle destination already exists: {destination}")
    destination.parent.mkdir(parents=True, exist_ok=True)
    shutil.copyfile(source, destination)


def _file_metadata(root: Path, files: Iterable[Path]) -> list[dict[str, object]]:
    """Return stable path, size, and SHA-256 metadata for bundle files."""
    resolved = root.resolve()
    metadata = []
    for file in sorted(files, key=lambda candidate: candidate.as_posix()):
        relative = file.resolve().relative_to(resolved).as_posix()
        payload = file.read_bytes()
        metadata.append(
            {
                "path": relative,
                "sha256": hashlib.sha256(payload).hexdigest(),
                "size": len(payload),
            }
        )
    return metadata


def _wheel_from_certificate(wheels_root: Path, platform: str) -> tuple[Path, Path]:
    """Resolve the exact wheel named by one already-validated platform certificate."""
    certificate = wheels_root / f"platform-wheel-certificate-{platform}.json"
    if certificate.is_symlink() or not certificate.is_file():
        raise AssertionError(f"platform certificate is missing: {certificate}")
    payload = json.loads(certificate.read_text(encoding="utf-8"))
    wheel = payload.get("wheel") if isinstance(payload, dict) else None
    filename = wheel.get("filename") if isinstance(wheel, dict) else None
    if (
        not isinstance(filename, str)
        or filename in {"", ".", ".."}
        or Path(filename).name != filename
        or "/" in filename
        or "\\" in filename
    ):
        raise AssertionError(f"platform certificate has an unsafe wheel name: {certificate}")
    candidate = wheels_root / filename
    if candidate.is_symlink() or not candidate.is_file():
        raise AssertionError(f"certified platform wheel is missing: {candidate}")
    return certificate, candidate


def _platform_validation_artifacts(validation_root: Path, platform: str) -> tuple[Path, ...]:
    """Return the exact transient validation directories assigned to one platform."""
    artifacts: list[Path] = []
    if platform == "linux-x86_64":
        artifacts.extend(validation_root / name for name in _GLOBAL_ARTIFACTS)
    artifacts.extend(validation_root / name for name in _SANITIZER_ARTIFACTS[platform])
    artifacts.extend(
        validation_root / f"platform-test-evidence-{platform}-{shard}"
        for shard in ("concurrency", "io-pipeline", "memory-parquet")
    )
    return tuple(artifacts)


def _validation_artifact_names(platform: str) -> tuple[str, ...]:
    """Return the exact validation artifact names assigned to one platform."""
    return tuple(artifact.name for artifact in _platform_validation_artifacts(Path(), platform))


def _all_validation_artifact_names() -> set[str]:
    """Return the complete non-overlapping validation artifact inventory."""
    return {name for platform in PLATFORMS for name in _validation_artifact_names(platform)}


def create_bundles(
    *,
    wheels_root: Path,
    validation_root: Path,
    release_root: Path,
    output_root: Path,
    github_sha: str,
    github_run_id: int,
    github_run_attempt: int,
) -> None:
    """Create four manifest-bound wheel bundles from already-validated gate inputs."""
    _validate_identity(github_sha, github_run_id, github_run_attempt)
    wheels_root = _require_directory(wheels_root, "validated wheels root")
    validation_root = _require_directory(validation_root, "validation-artifact root")
    release_root = _require_directory(release_root, "release-distribution root")
    _require_new_directory(output_root, "certified bundle output root")
    assigned: set[str] = set()
    for platform in PLATFORMS:
        bundle = output_root / platform
        _require_new_directory(bundle, "platform bundle")
        certificate, wheel = _wheel_from_certificate(wheels_root, platform)
        _copy_file(certificate, bundle / certificate.name)
        _copy_file(wheel, bundle / wheel.name)
        for artifact in _platform_validation_artifacts(validation_root, platform):
            if artifact.name in assigned:
                raise AssertionError(f"validation artifact assigned twice: {artifact.name}")
            _copy_tree(artifact, bundle / "rerun-state" / "validation" / artifact.name)
            assigned.add(artifact.name)
        if platform == "linux-x86_64":
            _copy_tree(release_root, bundle / "rerun-state" / "release-distributions")
        files = _regular_files(bundle)
        manifest = {
            "files": _file_metadata(bundle, files),
            "format": BUNDLE_FORMAT,
            "platform": platform,
            "provenance": {
                "git_sha": github_sha,
                "github_run_attempt": github_run_attempt,
                "github_run_id": github_run_id,
            },
        }
        (bundle / "certified-wheel-bundle.json").write_text(
            _canonical_json(manifest), encoding="utf-8", newline="\n"
        )
    expected = _all_validation_artifact_names()
    if assigned != expected:
        raise AssertionError(
            f"bundle validation assignment mismatch: missing={sorted(expected - assigned)}, "
            f"unknown={sorted(assigned - expected)}"
        )


def verify_bundle(
    bundle: Path,
    *,
    platform: str,
    github_sha: str,
    github_run_id: int,
    github_run_attempt: int,
) -> dict[str, object]:
    """Verify one complete bundle and return its canonical manifest."""
    _validate_identity(github_sha, github_run_id, github_run_attempt)
    if platform not in PLATFORMS:
        raise ValueError(f"unsupported bundle platform: {platform}")
    bundle = _require_directory(bundle, "certified wheel bundle")
    manifest_file = bundle / "certified-wheel-bundle.json"
    if manifest_file.is_symlink() or not manifest_file.is_file():
        raise AssertionError(f"certified bundle manifest is missing: {manifest_file}")
    serialized = manifest_file.read_text(encoding="utf-8")
    manifest = json.loads(serialized)
    if (
        not isinstance(manifest, dict)
        or serialized != _canonical_json(manifest)
        or set(manifest) != {"files", "format", "platform", "provenance"}
        or manifest.get("format") != BUNDLE_FORMAT
        or manifest.get("platform") != platform
    ):
        raise AssertionError("certified wheel bundle manifest identity is invalid")
    provenance = manifest.get("provenance")
    if not isinstance(provenance, dict):
        raise AssertionError("certified wheel bundle provenance is malformed")
    producer_run_id = provenance.get("github_run_id")
    producer_attempt = provenance.get("github_run_attempt")
    if (
        set(provenance) != {"git_sha", "github_run_attempt", "github_run_id"}
        or provenance.get("git_sha") != github_sha
        or type(producer_run_id) is not int
        or producer_run_id != github_run_id
        or type(producer_attempt) is not int
        or producer_attempt < 1
        or producer_attempt > github_run_attempt
    ):
        raise AssertionError("certified wheel bundle provenance does not match this workflow")
    declared_files = manifest.get("files")
    if not isinstance(declared_files, list) or any(
        type(entry) is not dict
        or set(entry) != {"path", "sha256", "size"}
        or type(entry.get("path")) is not str
        or type(entry.get("sha256")) is not str
        or len(entry["sha256"]) != 64
        or entry["sha256"].lower() != entry["sha256"]
        or any(character not in "0123456789abcdef" for character in entry["sha256"])
        or type(entry.get("size")) is not int
        or entry["size"] < 0
        for entry in declared_files
    ):
        raise AssertionError("certified wheel bundle file manifest is malformed")
    observed = _file_metadata(
        bundle,
        (file for file in _regular_files(bundle) if file != manifest_file),
    )
    if declared_files != observed:
        raise AssertionError("certified wheel bundle files do not match the manifest")
    root_names = sorted(entry.name for entry in bundle.iterdir())
    certificate_name = f"platform-wheel-certificate-{platform}.json"
    certificate = bundle / certificate_name
    root_wheel_paths = sorted(bundle.glob("*.whl"), key=lambda path: path.name)
    root_wheels = [entry.name for entry in root_wheel_paths]
    if (
        len(root_wheels) != 1
        or root_wheel_paths[0].is_symlink()
        or not root_wheel_paths[0].is_file()
        or certificate.is_symlink()
        or not certificate.is_file()
        or root_names
        != sorted(
            [
                "certified-wheel-bundle.json",
                certificate_name,
                "rerun-state",
                root_wheels[0],
            ]
        )
    ):
        raise AssertionError("certified wheel bundle root inventory is invalid")
    rerun_state = bundle / "rerun-state"
    if rerun_state.is_symlink() or not rerun_state.is_dir():
        raise AssertionError("certified wheel bundle rerun-state must be a regular directory")
    expected_rerun_names = {"validation"}
    if platform == "linux-x86_64":
        expected_rerun_names.add("release-distributions")
    rerun_entries = tuple(rerun_state.iterdir())
    if {entry.name for entry in rerun_entries} != expected_rerun_names or any(
        entry.is_symlink() or not entry.is_dir() for entry in rerun_entries
    ):
        raise AssertionError("certified wheel bundle rerun-state inventory is invalid")
    validation = rerun_state / "validation"
    validation_entries = tuple(validation.iterdir())
    if (
        {entry.name for entry in validation_entries} != set(_validation_artifact_names(platform))
        or any(entry.is_symlink() or not entry.is_dir() for entry in validation_entries)
        or any(not _regular_files(entry) for entry in validation_entries)
    ):
        raise AssertionError("certified wheel bundle validation inventory is invalid")
    if platform == "linux-x86_64" and not _regular_files(rerun_state / "release-distributions"):
        raise AssertionError("certified wheel bundle release inventory is empty")
    return manifest


def restore_validation(
    *,
    bundles_root: Path,
    output_root: Path,
    overlays_root: Path | None = None,
    github_sha: str,
    github_run_id: int,
    github_run_attempt: int,
) -> bool:
    """Restore gate inputs, apply recognized rerun overlays, and report pure reuse."""
    bundles_root = _require_directory(bundles_root, "downloaded bundle root")
    expected_bundle_names = {f"dist-wheels-{platform}" for platform in PLATFORMS}
    bundle_entries = tuple(bundles_root.iterdir())
    if {entry.name for entry in bundle_entries} != expected_bundle_names or any(
        entry.is_symlink() or not entry.is_dir() for entry in bundle_entries
    ):
        raise AssertionError("downloaded bundle root inventory is invalid")
    candidate_names = {f"candidate-wheels-{platform}" for platform in PLATFORMS}
    validation_names = _all_validation_artifact_names()
    overlay_sources: dict[str, Path] = {}
    if overlays_root is not None:
        overlays_root = _require_directory(overlays_root, "rerun overlay root")
        allowed_overlay_names = (
            candidate_names
            | validation_names
            | {
                "pypi-publish-distributions",
                "release-distributions",
            }
        )
        overlay_entries = tuple(overlays_root.iterdir())
        if any(
            entry.name not in allowed_overlay_names or entry.is_symlink() or not entry.is_dir()
            for entry in overlay_entries
        ):
            raise AssertionError("rerun overlay root inventory is invalid")
        overlay_sources = {
            entry.name: entry
            for entry in overlay_entries
            if entry.name in candidate_names or entry.name in validation_names
        }
    _require_new_directory(output_root, "restored validation root")
    restored: set[str] = set()
    for platform in PLATFORMS:
        bundle = bundles_root / f"dist-wheels-{platform}"
        verify_bundle(
            bundle,
            platform=platform,
            github_sha=github_sha,
            github_run_id=github_run_id,
            github_run_attempt=github_run_attempt,
        )
        candidate_name = f"candidate-wheels-{platform}"
        candidate = output_root / candidate_name
        candidate_overlay = overlay_sources.get(candidate_name)
        if candidate_overlay is None:
            _require_new_directory(candidate, "restored candidate wheel")
            for source in sorted(bundle.glob("*.whl")):
                _copy_file(source, candidate / source.name)
            certificate = bundle / f"platform-wheel-certificate-{platform}.json"
            _copy_file(certificate, candidate / certificate.name)
        else:
            certificate, wheel = _wheel_from_certificate(candidate_overlay, platform)
            candidate_entries = tuple(candidate_overlay.iterdir())
            if {entry.resolve() for entry in candidate_entries} != {
                certificate.resolve(),
                wheel.resolve(),
            } or any(entry.is_symlink() or not entry.is_file() for entry in candidate_entries):
                raise AssertionError(f"candidate overlay inventory is invalid: {candidate_name}")
            _copy_tree(candidate_overlay, candidate)
        validation = bundle / "rerun-state" / "validation"
        for artifact in sorted(validation.iterdir(), key=lambda path: path.name):
            if artifact.name in restored:
                raise AssertionError(f"restored validation artifact is duplicated: {artifact.name}")
            source = overlay_sources.get(artifact.name, artifact)
            _copy_tree(source, output_root / artifact.name)
            restored.add(artifact.name)
    expected = _all_validation_artifact_names()
    if restored != expected:
        raise AssertionError("restored validation artifacts are incomplete")
    return not overlay_sources


def _append_reuse_output(path: Path, reused: bool) -> None:
    """Append the validated restoration mode to one GitHub output file."""
    if path.is_symlink() or (path.exists() and not path.is_file()):
        raise AssertionError(f"GitHub output must be a regular file: {path}")
    with path.open("a", encoding="utf-8", newline="\n") as output:
        output.write(f"reused={'true' if reused else 'false'}\n")


def restore_release(
    *,
    bundle: Path,
    output_root: Path,
    github_sha: str,
    github_run_id: int,
    github_run_attempt: int,
) -> None:
    """Restore the release distribution from the retained Linux wheel bundle."""
    verify_bundle(
        bundle,
        platform="linux-x86_64",
        github_sha=github_sha,
        github_run_id=github_run_id,
        github_run_attempt=github_run_attempt,
    )
    source = bundle / "rerun-state" / "release-distributions"
    _copy_tree(source, output_root)


def _positive_integer(raw: str) -> int:
    """Parse one CLI workflow identity as a positive exact integer."""
    if not raw.isascii() or not raw.isdecimal() or raw.startswith("0"):
        raise argparse.ArgumentTypeError("workflow identity must be a positive decimal integer")
    value = int(raw)
    if value < 1:
        raise argparse.ArgumentTypeError("workflow identity must be positive")
    return value


def main() -> None:
    """Run the certified-wheel bundle create, verify, or restore operation."""
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "operation", choices=("create", "verify", "restore-validation", "restore-release")
    )
    parser.add_argument("--bundle", type=Path)
    parser.add_argument("--bundles-root", type=Path)
    parser.add_argument("--github-run-attempt", required=True, type=_positive_integer)
    parser.add_argument("--github-run-id", required=True, type=_positive_integer)
    parser.add_argument("--github-sha", required=True)
    parser.add_argument("--github-output", type=Path)
    parser.add_argument("--output-root", type=Path)
    parser.add_argument("--overlays-root", type=Path)
    parser.add_argument("--platform", choices=PLATFORMS)
    parser.add_argument("--release-root", type=Path)
    parser.add_argument("--validation-root", type=Path)
    parser.add_argument("--wheels-root", type=Path)
    args = parser.parse_args()
    if args.operation != "restore-validation" and (
        args.github_output is not None or args.overlays_root is not None
    ):
        parser.error("github-output and overlays-root require restore-validation")
    identity = {
        "github_sha": args.github_sha,
        "github_run_id": args.github_run_id,
        "github_run_attempt": args.github_run_attempt,
    }
    if args.operation == "create":
        if not all(
            path is not None
            for path in (
                args.wheels_root,
                args.validation_root,
                args.release_root,
                args.output_root,
            )
        ):
            parser.error(
                "create requires wheels-root, validation-root, release-root, and output-root"
            )
        create_bundles(
            wheels_root=args.wheels_root,
            validation_root=args.validation_root,
            release_root=args.release_root,
            output_root=args.output_root,
            **identity,
        )
    elif args.operation == "verify":
        if args.bundle is None or args.platform is None:
            parser.error("verify requires bundle and platform")
        verify_bundle(args.bundle, platform=args.platform, **identity)
    elif args.operation == "restore-validation":
        if args.bundles_root is None or args.output_root is None:
            parser.error("restore-validation requires bundles-root and output-root")
        reused = restore_validation(
            bundles_root=args.bundles_root,
            output_root=args.output_root,
            overlays_root=args.overlays_root,
            **identity,
        )
        if args.github_output is not None:
            _append_reuse_output(args.github_output, reused)
    else:
        if args.bundle is None or args.output_root is None:
            parser.error("restore-release requires bundle and output-root")
        restore_release(bundle=args.bundle, output_root=args.output_root, **identity)


if __name__ == "__main__":
    main()
