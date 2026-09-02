"""Exercise durable wheel-bundle creation, verification, and restoration.

The tests protect the four-artifact rerun contract, canonical manifests, exact
platform ownership, same-run provenance, content hashes, and symlink-safe copies.
"""

from __future__ import annotations

import hashlib
import json
import shutil
from pathlib import Path

import pytest

from meta.ci.release import certified_wheel_bundle as bundles

GITHUB_SHA = "0123456789abcdef" * 2 + "01234567"
GITHUB_RUN_ID = 73
GITHUB_RUN_ATTEMPT = 2
SHARDS = ("concurrency", "io-pipeline", "memory-parquet")
VALIDATION_BY_PLATFORM = {
    "linux-x86_64": {
        "native-coverage-certificate",
        "sanitizer-certificate-Linux-X64-asan-ubsan",
        "sanitizer-certificate-Linux-X64-tsan",
        "source-distribution",
        *(f"platform-test-evidence-linux-x86_64-{shard}" for shard in SHARDS),
    },
    "macos-arm64": {
        "sanitizer-certificate-macOS-ARM64-asan-ubsan",
        *(f"platform-test-evidence-macos-arm64-{shard}" for shard in SHARDS),
    },
    "macos-x86_64": {
        "sanitizer-certificate-macOS-X64-asan-ubsan",
        *(f"platform-test-evidence-macos-x86_64-{shard}" for shard in SHARDS),
    },
    "windows-amd64": {
        "sanitizer-certificate-Windows-X64-asan",
        *(f"platform-test-evidence-windows-amd64-{shard}" for shard in SHARDS),
    },
}
ALL_VALIDATION = set().union(*VALIDATION_BY_PLATFORM.values())


def _build_inputs(root: Path) -> tuple[Path, Path, Path]:
    """Create the complete validated input topology consumed by bundle creation."""
    wheels = root / "wheels"
    validation = root / "validation"
    release = root / "release"
    wheels.mkdir(parents=True)
    validation.mkdir()
    release.mkdir()
    for platform in bundles.PLATFORMS:
        wheel_name = f"schema_sanitizer-0.4.3-cp311-abi3-{platform}.whl"
        (wheels / wheel_name).write_bytes(b"x")
        certificate = {"wheel": {"filename": wheel_name}}
        (wheels / f"platform-wheel-certificate-{platform}.json").write_text(
            json.dumps(certificate, sort_keys=True) + "\n", encoding="utf-8"
        )
    for artifact in sorted(ALL_VALIDATION):
        directory = validation / artifact
        directory.mkdir()
        (directory / "evidence.json").write_text(
            json.dumps({"artifact": artifact}, sort_keys=True) + "\n",
            encoding="utf-8",
        )
    (release / "release-manifest.json").write_text('{"release":true}\n', encoding="utf-8")
    packages = release / "packages"
    packages.mkdir()
    (packages / "schema_sanitizer-0.4.3.tar.gz").write_bytes(b"source distribution")
    return wheels, validation, release


def _create_fixture(root: Path) -> tuple[Path, Path, Path, Path]:
    """Build four valid bundles and return their inputs and output root."""
    wheels, validation, release = _build_inputs(root / "inputs")
    output = root / "bundles"
    bundles.create_bundles(
        wheels_root=wheels,
        validation_root=validation,
        release_root=release,
        output_root=output,
        github_sha=GITHUB_SHA,
        github_run_id=GITHUB_RUN_ID,
        github_run_attempt=GITHUB_RUN_ATTEMPT,
    )
    return wheels, validation, release, output


@pytest.fixture
def certified_bundles(tmp_path: Path) -> tuple[Path, Path, Path, Path]:
    """Provide one isolated complete set of certified wheel bundles."""
    return _create_fixture(tmp_path)


def _download_layout(output: Path, destination: Path) -> Path:
    """Copy created bundles into the artifact downloader's directory layout."""
    destination.mkdir()
    for platform in bundles.PLATFORMS:
        shutil.copytree(output / platform, destination / f"dist-wheels-{platform}")
    return destination


def _manifest_path(bundle: Path) -> Path:
    """Return one bundle's canonical manifest path."""
    return bundle / "certified-wheel-bundle.json"


def _read_manifest(bundle: Path) -> dict[str, object]:
    """Read one test bundle manifest as a JSON object."""
    payload = json.loads(_manifest_path(bundle).read_text(encoding="utf-8"))
    assert isinstance(payload, dict)
    return payload


def _write_manifest(bundle: Path, manifest: dict[str, object]) -> None:
    """Write one mutated manifest in the required canonical representation."""
    _manifest_path(bundle).write_text(
        json.dumps(manifest, indent=2, sort_keys=True, ensure_ascii=False) + "\n",
        encoding="utf-8",
        newline="\n",
    )


def _refresh_file_manifest(bundle: Path) -> None:
    """Rebind a manifest to current bytes for topology-only rejection tests."""
    manifest_path = _manifest_path(bundle)
    records: list[dict[str, object]] = []
    for path in sorted(bundle.rglob("*"), key=lambda candidate: candidate.as_posix()):
        if not path.is_file() or path == manifest_path:
            continue
        payload = path.read_bytes()
        records.append(
            {
                "path": path.relative_to(bundle).as_posix(),
                "sha256": hashlib.sha256(payload).hexdigest(),
                "size": len(payload),
            }
        )
    manifest = _read_manifest(bundle)
    manifest["files"] = records
    _write_manifest(bundle, manifest)


def _verify(bundle: Path, platform: str, *, attempt: int = GITHUB_RUN_ATTEMPT) -> None:
    """Verify a bundle against the shared test workflow identity."""
    bundles.verify_bundle(
        bundle,
        platform=platform,
        github_sha=GITHUB_SHA,
        github_run_id=GITHUB_RUN_ID,
        github_run_attempt=attempt,
    )


def test_create_and_verify_bind_four_exact_platform_bundles(
    certified_bundles: tuple[Path, Path, Path, Path],
) -> None:
    """Creation assigns all evidence once and verification accepts later reruns."""
    _wheels, _validation, _release, output = certified_bundles

    assert {entry.name for entry in output.iterdir()} == set(bundles.PLATFORMS)
    for platform in bundles.PLATFORMS:
        bundle = output / platform
        manifest = bundles.verify_bundle(
            bundle,
            platform=platform,
            github_sha=GITHUB_SHA,
            github_run_id=GITHUB_RUN_ID,
            github_run_attempt=GITHUB_RUN_ATTEMPT + 1,
        )
        wheel_names = {path.name for path in bundle.glob("*.whl")}
        assert len(wheel_names) == 1
        assert {entry.name for entry in bundle.iterdir()} == {
            "certified-wheel-bundle.json",
            f"platform-wheel-certificate-{platform}.json",
            "rerun-state",
            *wheel_names,
        }
        validation_names = {
            entry.name for entry in (bundle / "rerun-state" / "validation").iterdir()
        }
        assert validation_names == VALIDATION_BY_PLATFORM[platform]
        rerun_names = {entry.name for entry in (bundle / "rerun-state").iterdir()}
        expected_rerun = {"validation", "release-distributions"}
        if platform != "linux-x86_64":
            expected_rerun.remove("release-distributions")
        assert rerun_names == expected_rerun
        assert manifest["provenance"] == {
            "git_sha": GITHUB_SHA,
            "github_run_attempt": GITHUB_RUN_ATTEMPT,
            "github_run_id": GITHUB_RUN_ID,
        }


def test_restore_validation_reconstructs_every_gate_input_exactly(
    tmp_path: Path,
    certified_bundles: tuple[Path, Path, Path, Path],
) -> None:
    """Four durable artifacts restore all wheels, certificates, and evidence."""
    wheels, validation, _release, output = certified_bundles
    downloads = _download_layout(output, tmp_path / "downloads")
    overlays = tmp_path / "overlays"
    overlays.mkdir()
    for trusted_later_output in ("release-distributions", "pypi-publish-distributions"):
        directory = overlays / trusted_later_output
        directory.mkdir()
        (directory / "ignored").write_text("later output\n", encoding="utf-8")
    restored = tmp_path / "restored"

    reused = bundles.restore_validation(
        bundles_root=downloads,
        output_root=restored,
        overlays_root=overlays,
        github_sha=GITHUB_SHA,
        github_run_id=GITHUB_RUN_ID,
        github_run_attempt=GITHUB_RUN_ATTEMPT,
    )

    assert reused is True
    assert {entry.name for entry in restored.iterdir()} == {
        *ALL_VALIDATION,
        *(f"candidate-wheels-{platform}" for platform in bundles.PLATFORMS),
    }
    for platform in bundles.PLATFORMS:
        candidate = restored / f"candidate-wheels-{platform}"
        expected = {
            f"platform-wheel-certificate-{platform}.json",
            next(path.name for path in wheels.glob(f"*{platform}.whl")),
        }
        assert {entry.name for entry in candidate.iterdir()} == expected
    for artifact in ALL_VALIDATION:
        assert (restored / artifact / "evidence.json").read_bytes() == (
            validation / artifact / "evidence.json"
        ).read_bytes()


def test_restore_validation_applies_only_recognized_partial_rerun_overlays(
    tmp_path: Path,
    certified_bundles: tuple[Path, Path, Path, Path],
) -> None:
    """A selective rerun replaces its exact candidate and evidence inputs."""
    wheels, validation, _release, output = certified_bundles
    downloads = _download_layout(output, tmp_path / "downloads")
    overlays = tmp_path / "overlays"
    candidate_name = "candidate-wheels-macos-arm64"
    candidate = overlays / candidate_name
    candidate.mkdir(parents=True)
    certificate = wheels / "platform-wheel-certificate-macos-arm64.json"
    wheel_name = json.loads(certificate.read_text(encoding="utf-8"))["wheel"]["filename"]
    shutil.copyfile(certificate, candidate / certificate.name)
    shutil.copyfile(wheels / wheel_name, candidate / wheel_name)
    (candidate / wheel_name).write_bytes(b"rerun wheel")
    evidence_name = "platform-test-evidence-windows-amd64-io-pipeline"
    shutil.copytree(validation / evidence_name, overlays / evidence_name)
    (overlays / evidence_name / "evidence.json").write_text(
        '{"artifact":"rerun evidence"}\n', encoding="utf-8"
    )
    restored = tmp_path / "restored"

    reused = bundles.restore_validation(
        bundles_root=downloads,
        output_root=restored,
        overlays_root=overlays,
        github_sha=GITHUB_SHA,
        github_run_id=GITHUB_RUN_ID,
        github_run_attempt=GITHUB_RUN_ATTEMPT,
    )

    assert reused is False
    assert (restored / candidate_name / wheel_name).read_bytes() == b"rerun wheel"
    assert (restored / evidence_name / "evidence.json").read_text(encoding="utf-8") == (
        '{"artifact":"rerun evidence"}\n'
    )
    baseline_name = "platform-test-evidence-linux-x86_64-memory-parquet"
    assert (restored / baseline_name / "evidence.json").read_bytes() == (
        validation / baseline_name / "evidence.json"
    ).read_bytes()


@pytest.mark.parametrize(
    "mutation", (".hidden-unknown", "malformed-candidate", "extra-candidate-directory")
)
def test_restore_validation_rejects_unknown_and_malformed_overlays(
    mutation: str,
    tmp_path: Path,
    certified_bundles: tuple[Path, Path, Path, Path],
) -> None:
    """Hidden names and incomplete candidate artifacts cannot enter restored state."""
    wheels, _validation, _release, output = certified_bundles
    downloads = _download_layout(output, tmp_path / "downloads")
    overlays = tmp_path / "overlays"
    overlays.mkdir()
    if mutation == ".hidden-unknown":
        (overlays / mutation).mkdir()
        message = "overlay root inventory"
    elif mutation == "malformed-candidate":
        candidate = overlays / "candidate-wheels-linux-x86_64"
        candidate.mkdir()
        (candidate / "unexpected.txt").write_text("incomplete\n", encoding="utf-8")
        message = "platform certificate is missing"
    else:
        candidate = overlays / "candidate-wheels-linux-x86_64"
        candidate.mkdir()
        certificate = wheels / "platform-wheel-certificate-linux-x86_64.json"
        wheel_name = json.loads(certificate.read_text(encoding="utf-8"))["wheel"]["filename"]
        shutil.copyfile(certificate, candidate / certificate.name)
        shutil.copyfile(wheels / wheel_name, candidate / wheel_name)
        (candidate / "empty").mkdir()
        message = "candidate overlay inventory"

    with pytest.raises(AssertionError, match=message):
        bundles.restore_validation(
            bundles_root=downloads,
            output_root=tmp_path / "restored",
            overlays_root=overlays,
            github_sha=GITHUB_SHA,
            github_run_id=GITHUB_RUN_ID,
            github_run_attempt=GITHUB_RUN_ATTEMPT,
        )


def test_restore_reuse_output_appends_one_unambiguous_boolean(tmp_path: Path) -> None:
    """The gate receives an exact lowercase reuse result without replacing outputs."""
    output = tmp_path / "github-output"
    output.write_text("prior=value\n", encoding="utf-8")

    bundles._append_reuse_output(output, True)
    bundles._append_reuse_output(output, False)

    assert output.read_text(encoding="utf-8") == ("prior=value\nreused=true\nreused=false\n")


def test_restore_release_reconstructs_only_the_manifest_bound_release_tree(
    tmp_path: Path,
    certified_bundles: tuple[Path, Path, Path, Path],
) -> None:
    """The retained Linux bundle restores the complete release distribution."""
    _wheels, _validation, release, output = certified_bundles
    restored = tmp_path / "release-restored"

    bundles.restore_release(
        bundle=output / "linux-x86_64",
        output_root=restored,
        github_sha=GITHUB_SHA,
        github_run_id=GITHUB_RUN_ID,
        github_run_attempt=GITHUB_RUN_ATTEMPT,
    )

    expected_files = {
        path.relative_to(release).as_posix(): path.read_bytes()
        for path in release.rglob("*")
        if path.is_file()
    }
    restored_files = {
        path.relative_to(restored).as_posix(): path.read_bytes()
        for path in restored.rglob("*")
        if path.is_file()
    }
    assert restored_files == expected_files


@pytest.mark.parametrize("tamper", ("wheel", "missing", "extra", "nested-manifest"))
def test_verify_rejects_content_tampering_and_unmanifested_files(
    tamper: str,
    certified_bundles: tuple[Path, Path, Path, Path],
) -> None:
    """Any byte change or exact-file inventory drift invalidates a bundle."""
    _wheels, _validation, _release, output = certified_bundles
    bundle = output / "macos-arm64"
    evidence = (
        bundle
        / "rerun-state"
        / "validation"
        / next(iter(sorted(VALIDATION_BY_PLATFORM["macos-arm64"])))
    )
    if tamper == "wheel":
        next(bundle.glob("*.whl")).write_bytes(b"tampered")
    elif tamper == "missing":
        (evidence / "evidence.json").unlink()
    elif tamper == "extra":
        (evidence / "unexpected.txt").write_text("unexpected\n", encoding="utf-8")
    else:
        (evidence / "certified-wheel-bundle.json").write_text("nested\n", encoding="utf-8")

    with pytest.raises(AssertionError, match="files do not match"):
        _verify(bundle, "macos-arm64")


def test_verify_rejects_boolean_file_sizes_even_when_python_equality_matches(
    certified_bundles: tuple[Path, Path, Path, Path],
) -> None:
    """JSON booleans cannot impersonate integer sizes in canonical metadata."""
    _wheels, _validation, _release, output = certified_bundles
    bundle = output / "windows-amd64"
    manifest = _read_manifest(bundle)
    files = manifest["files"]
    assert isinstance(files, list)
    wheel = next(entry for entry in files if str(entry["path"]).endswith(".whl"))
    assert wheel["size"] == 1
    wheel["size"] = True
    _write_manifest(bundle, manifest)

    with pytest.raises(AssertionError, match="file manifest is malformed"):
        _verify(bundle, "windows-amd64")


@pytest.mark.parametrize(
    ("field", "value"),
    (
        ("git_sha", "f" * 40),
        ("github_run_id", GITHUB_RUN_ID + 1),
        ("github_run_attempt", GITHUB_RUN_ATTEMPT + 1),
        ("github_run_id", True),
        ("github_run_attempt", True),
    ),
)
def test_verify_rejects_foreign_future_and_boolean_provenance(
    field: str,
    value: object,
    certified_bundles: tuple[Path, Path, Path, Path],
) -> None:
    """Only exact same-run provenance from this or an earlier attempt is reusable."""
    _wheels, _validation, _release, output = certified_bundles
    bundle = output / "macos-x86_64"
    manifest = _read_manifest(bundle)
    provenance = manifest["provenance"]
    assert isinstance(provenance, dict)
    provenance[field] = value
    _write_manifest(bundle, manifest)

    with pytest.raises(AssertionError, match="provenance does not match"):
        _verify(bundle, "macos-x86_64")


def test_verify_rejects_a_future_producer_attempt_without_manifest_mutation(
    certified_bundles: tuple[Path, Path, Path, Path],
) -> None:
    """An earlier rerun cannot consume an artifact claiming a later attempt."""
    _wheels, _validation, _release, output = certified_bundles

    with pytest.raises(AssertionError, match="provenance does not match"):
        _verify(output / "linux-x86_64", "linux-x86_64", attempt=1)


@pytest.mark.parametrize(
    "mutation",
    ("extra-root", "extra-rerun", "extra-validation", "empty-validation", "wheel-directory"),
)
def test_verify_rejects_self_consistent_but_inexact_bundle_topologies(
    mutation: str,
    certified_bundles: tuple[Path, Path, Path, Path],
) -> None:
    """Rehashing cannot authorize extra or empty rerun-state components."""
    _wheels, _validation, _release, output = certified_bundles
    platform = "windows-amd64"
    bundle = output / platform
    if mutation == "extra-root":
        (bundle / "extra.txt").write_text("extra\n", encoding="utf-8")
    elif mutation == "extra-rerun":
        extra = bundle / "rerun-state" / "logs"
        extra.mkdir()
        (extra / "log.txt").write_text("extra\n", encoding="utf-8")
    elif mutation == "extra-validation":
        extra = bundle / "rerun-state" / "validation" / "foreign-evidence"
        extra.mkdir()
        (extra / "evidence.json").write_text("{}\n", encoding="utf-8")
    elif mutation == "empty-validation":
        evidence = (
            bundle
            / "rerun-state"
            / "validation"
            / next(iter(sorted(VALIDATION_BY_PLATFORM[platform])))
        )
        (evidence / "evidence.json").unlink()
    else:
        wheel = next(bundle.glob("*.whl"))
        wheel.unlink()
        wheel.mkdir()
    _refresh_file_manifest(bundle)

    with pytest.raises(AssertionError, match="inventory"):
        _verify(bundle, platform)


def test_bundle_operations_reject_symlinked_roots_and_descendants(
    tmp_path: Path,
    certified_bundles: tuple[Path, Path, Path, Path],
) -> None:
    """Neither input aliases nor descendants can escape their certified roots."""
    _wheels, _validation, _release, output = certified_bundles
    bundle = output / "linux-x86_64"
    bundle_alias = tmp_path / "bundle-alias"
    try:
        bundle_alias.symlink_to(bundle, target_is_directory=True)
    except OSError:
        pytest.skip("symlinks are unavailable")
    with pytest.raises(AssertionError, match="regular directory"):
        _verify(bundle_alias, "linux-x86_64")

    evidence = bundle / "rerun-state" / "validation" / "source-distribution"
    (evidence / "alias").symlink_to(next(bundle.glob("*.whl")))
    with pytest.raises(AssertionError, match="unsafe entry"):
        _verify(bundle, "linux-x86_64")

    wheels, validation, release = _build_inputs(tmp_path / "symlinked-inputs")
    source = validation / "source-distribution"
    external = tmp_path / "external-evidence"
    source.rename(external)
    source.symlink_to(external, target_is_directory=True)
    with pytest.raises(AssertionError, match="regular directory"):
        bundles.create_bundles(
            wheels_root=wheels,
            validation_root=validation,
            release_root=release,
            output_root=tmp_path / "unsafe-output",
            github_sha=GITHUB_SHA,
            github_run_id=GITHUB_RUN_ID,
            github_run_attempt=GITHUB_RUN_ATTEMPT,
        )


@pytest.mark.parametrize("unsafe_name", ("../escape.whl", "/absolute.whl", "dir\\wheel.whl"))
def test_create_rejects_unsafe_certificate_wheel_names(
    unsafe_name: str,
    tmp_path: Path,
) -> None:
    """A platform certificate cannot redirect wheel copying outside its root."""
    wheels, validation, release = _build_inputs(tmp_path / "inputs")
    certificate = wheels / "platform-wheel-certificate-linux-x86_64.json"
    certificate.write_text(
        json.dumps({"wheel": {"filename": unsafe_name}}, sort_keys=True) + "\n",
        encoding="utf-8",
    )

    with pytest.raises(AssertionError, match="unsafe wheel name"):
        bundles.create_bundles(
            wheels_root=wheels,
            validation_root=validation,
            release_root=release,
            output_root=tmp_path / "output",
            github_sha=GITHUB_SHA,
            github_run_id=GITHUB_RUN_ID,
            github_run_attempt=GITHUB_RUN_ATTEMPT,
        )


@pytest.mark.parametrize("mutation", ("missing", "extra"))
def test_restore_validation_requires_exactly_four_downloaded_artifacts(
    mutation: str,
    tmp_path: Path,
    certified_bundles: tuple[Path, Path, Path, Path],
) -> None:
    """Restoration rejects incomplete or contaminated download inventories."""
    _wheels, _validation, _release, output = certified_bundles
    downloads = _download_layout(output, tmp_path / "downloads")
    if mutation == "missing":
        shutil.rmtree(downloads / "dist-wheels-macos-arm64")
    else:
        (downloads / "unexpected").mkdir()
    restored = tmp_path / "restored"

    with pytest.raises(AssertionError, match="downloaded bundle root inventory"):
        bundles.restore_validation(
            bundles_root=downloads,
            output_root=restored,
            github_sha=GITHUB_SHA,
            github_run_id=GITHUB_RUN_ID,
            github_run_attempt=GITHUB_RUN_ATTEMPT,
        )
    assert not restored.exists()


def test_restore_operations_refuse_preexisting_destinations(
    tmp_path: Path,
    certified_bundles: tuple[Path, Path, Path, Path],
) -> None:
    """Reruns cannot merge certified state into stale output directories."""
    _wheels, _validation, _release, output = certified_bundles
    downloads = _download_layout(output, tmp_path / "downloads")
    validation_output = tmp_path / "validation-output"
    release_output = tmp_path / "release-output"
    validation_output.mkdir()
    release_output.mkdir()

    with pytest.raises(AssertionError, match="must not already exist"):
        bundles.restore_validation(
            bundles_root=downloads,
            output_root=validation_output,
            github_sha=GITHUB_SHA,
            github_run_id=GITHUB_RUN_ID,
            github_run_attempt=GITHUB_RUN_ATTEMPT,
        )
    with pytest.raises(AssertionError, match="must not already exist"):
        bundles.restore_release(
            bundle=output / "linux-x86_64",
            output_root=release_output,
            github_sha=GITHUB_SHA,
            github_run_id=GITHUB_RUN_ID,
            github_run_attempt=GITHUB_RUN_ATTEMPT,
        )
