"""Exercise platform-test evidence creation, verification, and tamper rejection.

The focused fixtures model one complete job artifact and prove that run identity, exact
file inventory, in-process health certificates, byte digests, and safe publication all
remain mandatory at the validation boundary.
"""

from __future__ import annotations

import json
from pathlib import Path
from types import SimpleNamespace

import pytest
from _support.ci_integrity import StrictPlatformIntegrity, _skip_is_allowed, _SkipRecord

from meta.ci.quality import platform_test_evidence as evidence

_SHA = "a" * 40
_RUN_ID = 123
_RUN_ATTEMPT = 2
_TEST_COUNTS = {
    "concurrency": 511,
    "memory-parquet": 1704,
    "io-pipeline": 1025,
    "native-stress": 1,
    "release-matrix": 3,
}


def _integrity_payload(platform: str, shard: str) -> dict[str, object]:
    """Return one minimal satisfied in-process integrity certificate."""
    return {
        "format": "schema-sanitizer-platform-integrity-v1",
        "github": {"sha": _SHA, "run_id": _RUN_ID, "run_attempt": _RUN_ATTEMPT},
        "platform": platform,
        "shard": shard,
        "satisfied": True,
        "issues": [],
        "pytest_exitstatus": 0,
        "maximum_skip_count": 1,
        "skips": [],
        "expected_test_count": _TEST_COUNTS[shard],
        "selected_test_count": _TEST_COUNTS[shard],
    }


def _complete_evidence(root: Path, platform: str, shard: str) -> None:
    """Populate the exact raw inventory expected for one platform job."""
    root.mkdir()
    for filename in evidence._expected_files(platform, shard):
        path = root / filename
        component = next(
            (
                candidate
                for candidate in evidence._expected_integrity_shards(shard)
                if filename == evidence._integrity_filename(platform, candidate)
            ),
            None,
        )
        if component is None:
            path.write_text(f"evidence:{filename}\n", encoding="utf-8")
        else:
            path.write_text(
                json.dumps(_integrity_payload(platform, component), sort_keys=True) + "\n",
                encoding="utf-8",
            )


def _options(platform: str, shard: str) -> dict[str, object]:
    """Return common immutable command options for one fixture run."""
    return {
        "platform": platform,
        "shard": shard,
        "github_sha": _SHA,
        "github_run_id": _RUN_ID,
        "github_run_attempt": _RUN_ATTEMPT,
    }


def test_platform_job_certificate_round_trips_exact_concurrency_evidence(tmp_path: Path) -> None:
    """All three concurrency pytest processes and their evidence round-trip."""
    root = tmp_path / "artifacts"
    certificate = root / "platform-test-certificate-linux-concurrency.json"
    _complete_evidence(root, "linux", "concurrency")

    created = evidence.create_certificate(root, certificate, **_options("linux", "concurrency"))
    verified = evidence.verify_certificate(root, certificate, **_options("linux", "concurrency"))

    assert created == verified
    assert len(created["integrity"]) == 3


def test_platform_job_certificate_rejects_tampered_junit_evidence(tmp_path: Path) -> None:
    """A post-certification byte change invalidates the downloaded job artifact."""
    root = tmp_path / "artifacts"
    certificate = root / "platform-test-certificate-windows-io-pipeline.json"
    _complete_evidence(root, "windows", "io-pipeline")
    options = _options("windows", "io-pipeline")
    evidence.create_certificate(root, certificate, **options)
    (root / "pytest-windows-io-pipeline.xml").write_text("tampered\n", encoding="utf-8")

    with pytest.raises(ValueError, match="digest mismatch"):
        evidence.verify_certificate(root, certificate, **options)


def test_platform_job_certificate_requires_satisfied_process_integrity(tmp_path: Path) -> None:
    """A green job certificate cannot hide a failed in-process health guard."""
    root = tmp_path / "artifacts"
    certificate = root / "platform-test-certificate-macos-arm64-memory-parquet.json"
    _complete_evidence(root, "macos-arm64", "memory-parquet")
    integrity = root / "integrity-macos-arm64-memory-parquet.json"
    payload = _integrity_payload("macos-arm64", "memory-parquet")
    payload["satisfied"] = False
    payload["issues"] = ["counter underflow"]
    integrity.write_text(json.dumps(payload) + "\n", encoding="utf-8")

    with pytest.raises(ValueError, match="unsatisfied contract"):
        evidence.create_certificate(
            root,
            certificate,
            **_options("macos-arm64", "memory-parquet"),
        )

    payload["satisfied"] = True
    payload["issues"] = []
    payload["maximum_skip_count"] = 0
    payload["skips"] = [{"nodeid": "tests/example.py::test_case", "reason": "reviewed"}]
    integrity.write_text(json.dumps(payload) + "\n", encoding="utf-8")
    with pytest.raises(ValueError, match="skip ceiling"):
        evidence.create_certificate(
            root,
            certificate,
            **_options("macos-arm64", "memory-parquet"),
        )


def test_platform_job_certificate_rejects_extra_inventory(tmp_path: Path) -> None:
    """Unowned files cannot enter an otherwise valid platform evidence artifact."""
    root = tmp_path / "artifacts"
    certificate = root / "platform-test-certificate-linux-io-pipeline.json"
    _complete_evidence(root, "linux", "io-pipeline")
    (root / "unexpected.txt").write_text("unexpected\n", encoding="utf-8")

    with pytest.raises(ValueError, match="inventory mismatch"):
        evidence.create_certificate(root, certificate, **_options("linux", "io-pipeline"))


def test_platform_job_verifier_rejects_run_binding_tamper(tmp_path: Path) -> None:
    """A certificate from another workflow attempt cannot satisfy this gate."""
    root = tmp_path / "artifacts"
    certificate = root / "platform-test-certificate-linux-io-pipeline.json"
    _complete_evidence(root, "linux", "io-pipeline")
    options = _options("linux", "io-pipeline")
    evidence.create_certificate(root, certificate, **options)

    with pytest.raises(ValueError, match="GitHub binding mismatch"):
        evidence.verify_certificate(root, certificate, **{**options, "github_run_attempt": 3})


def test_platform_job_verifier_rejects_missing_or_extra_downloaded_files(tmp_path: Path) -> None:
    """The gate accepts exactly the certified raw files plus their certificate."""
    root = tmp_path / "artifacts"
    certificate = root / "platform-test-certificate-linux-io-pipeline.json"
    _complete_evidence(root, "linux", "io-pipeline")
    options = _options("linux", "io-pipeline")
    evidence.create_certificate(root, certificate, **options)
    missing = root / "runner-cpu-linux-io-pipeline.json"
    missing.unlink()
    with pytest.raises(ValueError, match="inventory mismatch"):
        evidence.verify_certificate(root, certificate, **options)

    missing.write_text("restored\n", encoding="utf-8")
    (root / "extra").write_text("extra\n", encoding="utf-8")
    with pytest.raises(ValueError, match="inventory mismatch"):
        evidence.verify_certificate(root, certificate, **options)


def test_platform_job_verifier_rejects_symlinked_evidence(tmp_path: Path) -> None:
    """A symlink cannot substitute bytes after artifact extraction."""
    root = tmp_path / "artifacts"
    certificate = root / "platform-test-certificate-linux-io-pipeline.json"
    _complete_evidence(root, "linux", "io-pipeline")
    options = _options("linux", "io-pipeline")
    evidence.create_certificate(root, certificate, **options)
    target = root / "pytest-linux-io-pipeline.xml"
    replacement = tmp_path / "replacement.xml"
    replacement.write_bytes(target.read_bytes())
    target.unlink()
    try:
        target.symlink_to(replacement)
    except OSError as exc:
        pytest.skip(f"symlinks are unavailable: {exc}")

    with pytest.raises(ValueError, match="regular file"):
        evidence.verify_certificate(root, certificate, **options)


@pytest.mark.parametrize(
    ("platform", "shard", "nodeid", "reason"),
    [
        (
            "windows",
            "concurrency",
            "tests/concurrency/test_concurrency_cross_process_telemetry_tuning.py::"
            "test_cross_process_registry_reclaims_dead_owner",
            "optional cross-process coordination requires POSIX advisory locks",
        ),
        (
            "macos-arm64",
            "concurrency",
            "tests/concurrency/test_concurrency_every_format_declares_an_eligible_multi_benefit_proof.py::"
            "test_parquet_output_parallelizes_and_remains_logically_exact",
            "every-format-declares-an-eligible-multi-benefit row-group overlap requires "
            "at least four effective workers",
        ),
        (
            "windows",
            "memory-parquet",
            "tests/memory/test_memory_live_owner_prepared_storage_transaction_rolls_back_before_retry.py::"
            "test_empty_or_canonical_prefix_never_discards_prepared_journal[b'{}']",
            "optional cross-process coordination requires POSIX advisory locks",
        ),
        (
            "windows",
            "memory-parquet",
            "tests/memory/test_memory_cancel_linearizes_before_claimed_callback_starts.py::"
            "test_after_fork_path_reset_never_acquires_inherited_owner_lock",
            "requires POSIX fork",
        ),
        (
            "windows",
            "memory-parquet",
            "tests/memory/test_memory_limit_enforcement.py::"
            "test_large_file_does_not_cause_file_sized_resident_memory_growth",
            "ru_maxrss byte conversion and allocator baseline are Linux-specific",
        ),
    ],
)
def test_platform_skip_allowlist_binds_exact_node_and_reason(
    platform: str,
    shard: str,
    nodeid: str,
    reason: str,
) -> None:
    """Reviewed skips accept only their exact test identity and rationale."""
    assert _skip_is_allowed(platform, shard, _SkipRecord(nodeid, reason))
    assert not _skip_is_allowed(platform, shard, _SkipRecord(f"{nodeid}-adjacent", reason))
    assert not _skip_is_allowed(platform, shard, _SkipRecord(nodeid, f"{reason} changed"))


def test_windows_module_skip_is_limited_to_its_reviewed_file() -> None:
    """A module-wide POSIX skip cannot authorize another memory module."""
    reason = "POSIX descriptor-relative filesystem hardening suite"
    reviewed = "tests/memory/test_memory_process_identity_includes_linux_boot_id.py::test_one"
    adjacent = "tests/memory/test_unreviewed.py::test_one"

    assert _skip_is_allowed("windows", "memory-parquet", _SkipRecord(reviewed, reason))
    assert not _skip_is_allowed("windows", "memory-parquet", _SkipRecord(adjacent, reason))


def test_concurrency_skips_are_bound_to_the_reviewed_platforms() -> None:
    """A valid node/reason pair cannot consume another platform's skip budget."""
    windows_only = _SkipRecord(
        "tests/concurrency/test_concurrency_cross_process_telemetry_tuning.py::"
        "test_cross_process_registry_reclaims_dead_owner",
        "optional cross-process coordination requires POSIX advisory locks",
    )
    linux_only = _SkipRecord(
        "tests/concurrency/test_concurrency_python_is_a_first_class_concurrency_input.py::"
        "test_python_generator_file_outputs_do_not_deadlock_at_high_width[16]",
        "requires at least 16 visible CPUs",
    )

    assert _skip_is_allowed("windows", "concurrency", windows_only)
    assert not _skip_is_allowed("linux", "concurrency", windows_only)
    assert _skip_is_allowed("linux", "concurrency", linux_only)
    assert not _skip_is_allowed("windows", "concurrency", linux_only)


def test_windows_io_skips_require_the_exact_test_node() -> None:
    """A POSIX limitation in one I/O test cannot authorize its neighbors."""
    reason = "requires POSIX interval timers"
    reviewed = (
        "tests/io/test_input_python_and_local.py::"
        "test_native_python_rows_batch_encoder_checks_pending_signals"
    )
    adjacent = "tests/io/test_input_python_and_local.py::test_unreviewed"

    assert _skip_is_allowed("windows", "io-pipeline", _SkipRecord(reviewed, reason))
    assert not _skip_is_allowed("windows", "io-pipeline", _SkipRecord(adjacent, reason))


def test_teardown_skip_is_recorded_and_fails_integrity() -> None:
    """A fixture teardown skip can never bypass the reviewed skip policy."""
    plugin = StrictPlatformIntegrity()
    report = SimpleNamespace(
        skipped=True,
        when="teardown",
        nodeid="tests/example.py::test_case",
        longrepr=("tests/example.py", 1, "Skipped: teardown escaped"),
    )

    plugin.pytest_runtest_logreport(report)  # type: ignore[arg-type]

    assert plugin.skips == [_SkipRecord("tests/example.py::test_case", "teardown escaped")]
    assert plugin.issues == [
        "teardown skip is forbidden: tests/example.py::test_case (teardown escaped)"
    ]
