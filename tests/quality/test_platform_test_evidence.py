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
from _support.ci_integrity import (
    StrictCollectionIntegrity,
    StrictPlatformIntegrity,
    _skip_inventory_sha256,
    _skip_is_allowed,
    _SkipRecord,
)

from meta.ci.quality import platform_test_evidence as evidence

_SHA = "a" * 40
_RUN_ID = 123
_RUN_ATTEMPT = 2
_TEST_COUNTS = {
    "concurrency": 511,
    "memory-parquet": 1705,
    "io-pipeline": 1025,
    "native-stress": 1,
    "release-matrix": 3,
}


def test_source_collection_integrity_fails_closed_on_identity_drift(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A same-sized replacement cannot satisfy the authenticated collection policy."""
    reviewed = ("tests/sample.py::test_one", "tests/sample.py::test_two")
    monkeypatch.setitem(
        evidence.EXPECTED_TEST_INVENTORY,
        "sample",
        {
            "count": len(reviewed),
            "sha256": evidence._nodeid_inventory_sha256(reviewed),
        },
    )
    plugin = StrictCollectionIntegrity("sample")
    exact = SimpleNamespace(items=[SimpleNamespace(nodeid=nodeid) for nodeid in reviewed])
    plugin.pytest_collection_finish(exact)  # type: ignore[arg-type]
    replaced = SimpleNamespace(
        items=[
            SimpleNamespace(nodeid=reviewed[0]),
            SimpleNamespace(nodeid="tests/sample.py::test_replacement"),
        ]
    )

    with pytest.raises(pytest.exit.Exception, match="sample platform collection drifted"):
        plugin.pytest_collection_finish(replaced)  # type: ignore[arg-type]


def _integrity_payload(platform: str, shard: str) -> dict[str, object]:
    """Return one minimal satisfied in-process integrity certificate."""
    inventory = evidence.EXPECTED_TEST_INVENTORY[shard]["sha256"]
    skip_inventory = _skip_inventory_sha256([])
    return {
        "format": "schema-sanitizer-platform-integrity-v2",
        "github": {"sha": _SHA, "run_id": _RUN_ID, "run_attempt": _RUN_ATTEMPT},
        "platform": platform,
        "shard": shard,
        "satisfied": True,
        "issues": [],
        "pytest_exitstatus": 0,
        "maximum_skip_count": 1,
        "skips": [],
        "expected_skip_inventory_sha256": None,
        "skip_inventory_sha256": skip_inventory,
        "expected_test_count": _TEST_COUNTS[shard],
        "selected_test_count": _TEST_COUNTS[shard],
        "expected_test_inventory_sha256": inventory,
        "selected_test_inventory_sha256": inventory,
        "initial_native_anomalies": {name: 0 for name in evidence._NATIVE_ANOMALY_KEYS},
        "final_native_anomalies": {name: 0 for name in evidence._NATIVE_ANOMALY_KEYS},
        "initial_process_anomalies": {name: 0 for name in evidence._PROCESS_ANOMALY_KEYS},
        "final_process_anomalies": {name: 0 for name in evidence._PROCESS_ANOMALY_KEYS},
        "provenance": {
            "schema_sanitizer": "/installed/schema_sanitizer/__init__.py",
            "schema_sanitizer._core_abi3": "/installed/schema_sanitizer/_core_abi3.so",
        },
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
    payload["skip_inventory_sha256"] = _skip_inventory_sha256(
        [_SkipRecord("tests/example.py::test_case", "reviewed")]
    )
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


def test_platform_job_certificate_rejects_replaced_test_identity(tmp_path: Path) -> None:
    """Equal test counts cannot hide a changed selected-node inventory."""
    root = tmp_path / "artifacts"
    certificate = root / "platform-test-certificate-linux-io-pipeline.json"
    _complete_evidence(root, "linux", "io-pipeline")
    integrity = root / "integrity-linux-io-pipeline.json"
    payload = json.loads(integrity.read_text(encoding="utf-8"))
    payload["selected_test_inventory_sha256"] = "f" * 64
    integrity.write_text(json.dumps(payload) + "\n", encoding="utf-8")

    with pytest.raises(ValueError, match="test identities"):
        evidence.create_certificate(root, certificate, **_options("linux", "io-pipeline"))


def test_platform_job_certificate_rejects_changed_exact_skip_set(tmp_path: Path) -> None:
    """A reviewed exact skip digest cannot be replaced at the same count."""
    root = tmp_path / "artifacts"
    certificate = root / "platform-test-certificate-linux-memory-parquet.json"
    _complete_evidence(root, "linux", "memory-parquet")
    integrity = root / "integrity-linux-memory-parquet.json"
    payload = json.loads(integrity.read_text(encoding="utf-8"))
    payload["expected_skip_inventory_sha256"] = "a" * 64
    payload["skip_inventory_sha256"] = "b" * 64
    integrity.write_text(json.dumps(payload) + "\n", encoding="utf-8")

    with pytest.raises(ValueError, match="skip identities"):
        evidence.create_certificate(root, certificate, **_options("linux", "memory-parquet"))


def test_platform_job_verifier_accepts_only_same_run_prior_attempts(tmp_path: Path) -> None:
    """Partial reruns reuse prior evidence but reject foreign or future producers."""
    root = tmp_path / "artifacts"
    certificate = root / "platform-test-certificate-linux-io-pipeline.json"
    _complete_evidence(root, "linux", "io-pipeline")
    options = _options("linux", "io-pipeline")
    evidence.create_certificate(root, certificate, **options)

    evidence.verify_certificate(root, certificate, **{**options, "github_run_attempt": 3})
    with pytest.raises(ValueError, match="GitHub binding mismatch"):
        evidence.verify_certificate(root, certificate, **{**options, "github_run_attempt": 1})
    with pytest.raises(ValueError, match="GitHub binding mismatch"):
        evidence.verify_certificate(root, certificate, **{**options, "github_run_id": _RUN_ID + 1})
    with pytest.raises(ValueError, match="GitHub binding mismatch"):
        evidence.verify_certificate(root, certificate, **{**options, "github_sha": "b" * 40})

    payload = json.loads(certificate.read_text(encoding="utf-8"))
    payload["github"]["run_attempt"] = True
    certificate.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    with pytest.raises(ValueError, match="GitHub binding mismatch"):
        evidence.verify_certificate(root, certificate, **{**options, "github_run_attempt": 3})


def test_platform_job_verifier_revalidates_raw_process_integrity(tmp_path: Path) -> None:
    """Updated digests cannot conceal an unsatisfied raw process certificate."""
    root = tmp_path / "artifacts"
    certificate = root / "platform-test-certificate-linux-io-pipeline.json"
    options = _options("linux", "io-pipeline")
    _complete_evidence(root, "linux", "io-pipeline")
    evidence.create_certificate(root, certificate, **options)

    integrity = root / "integrity-linux-io-pipeline.json"
    raw = json.loads(integrity.read_text(encoding="utf-8"))
    raw["satisfied"] = False
    raw["issues"] = ["tampered process evidence"]
    integrity.write_text(json.dumps(raw, sort_keys=True) + "\n", encoding="utf-8")

    outer = json.loads(certificate.read_text(encoding="utf-8"))
    digest = evidence._sha256(integrity)
    for record in outer["files"]:
        if record["filename"] == integrity.name:
            record["sha256"] = digest
            record["size"] = integrity.stat().st_size
    outer["integrity"][0]["sha256"] = digest
    certificate.write_text(evidence._canonical_json(outer), encoding="utf-8")

    with pytest.raises(ValueError, match="unsatisfied contract"):
        evidence.verify_certificate(root, certificate, **options)


def test_platform_job_verifier_rejects_rehashed_runtime_anomalies(tmp_path: Path) -> None:
    """A rehashed outer record cannot conceal a nonzero final runtime anomaly."""
    root = tmp_path / "artifacts"
    certificate = root / "platform-test-certificate-linux-io-pipeline.json"
    options = _options("linux", "io-pipeline")
    _complete_evidence(root, "linux", "io-pipeline")
    evidence.create_certificate(root, certificate, **options)

    integrity = root / "integrity-linux-io-pipeline.json"
    raw = json.loads(integrity.read_text(encoding="utf-8"))
    raw["final_native_anomalies"]["native.counter_underflows"] = 1
    integrity.write_text(json.dumps(raw, sort_keys=True) + "\n", encoding="utf-8")

    outer = json.loads(certificate.read_text(encoding="utf-8"))
    digest = evidence._sha256(integrity)
    for record in outer["files"]:
        if record["filename"] == integrity.name:
            record["sha256"] = digest
            record["size"] = integrity.stat().st_size
    outer["integrity"][0]["sha256"] = digest
    certificate.write_text(evidence._canonical_json(outer), encoding="utf-8")

    with pytest.raises(ValueError, match="final native anomalies"):
        evidence.verify_certificate(root, certificate, **options)


def test_platform_job_verifier_requires_canonical_outer_certificate(tmp_path: Path) -> None:
    """Equivalent noncanonical JSON cannot replace the certified outer record."""
    root = tmp_path / "artifacts"
    certificate = root / "platform-test-certificate-linux-io-pipeline.json"
    options = _options("linux", "io-pipeline")
    _complete_evidence(root, "linux", "io-pipeline")
    payload = evidence.create_certificate(root, certificate, **options)
    certificate.write_text(json.dumps(payload) + "\n", encoding="utf-8")

    with pytest.raises(ValueError, match="not canonical JSON"):
        evidence.verify_certificate(root, certificate, **options)


@pytest.mark.parametrize("run_attempt", [True, False, 0, -1, "invalid"])
def test_platform_job_verifier_rejects_invalid_current_attempts(
    tmp_path: Path, run_attempt: object
) -> None:
    """The gate attempt is always a strictly positive non-boolean integer."""
    root = tmp_path / "artifacts"
    certificate = root / "platform-test-certificate-linux-io-pipeline.json"
    _complete_evidence(root, "linux", "io-pipeline")
    options = _options("linux", "io-pipeline")
    evidence.create_certificate(root, certificate, **options)

    with pytest.raises(ValueError, match="positive integer"):
        evidence.verify_certificate(
            root, certificate, **{**options, "github_run_attempt": run_attempt}
        )


@pytest.mark.parametrize("run_id", [True, False, 0, -1, "123"])
def test_platform_job_verifier_rejects_invalid_current_run_ids(
    tmp_path: Path, run_id: object
) -> None:
    """Library verification requires a strict positive non-boolean run ID."""
    root = tmp_path / "artifacts"
    certificate = root / "platform-test-certificate-linux-io-pipeline.json"
    _complete_evidence(root, "linux", "io-pipeline")
    options = _options("linux", "io-pipeline")
    evidence.create_certificate(root, certificate, **options)

    with pytest.raises(ValueError, match="run ID must be a positive integer"):
        evidence.verify_certificate(root, certificate, **{**options, "github_run_id": run_id})


def test_platform_job_verifier_rejects_noncanonical_current_sha(tmp_path: Path) -> None:
    """The current commit identity must remain an exact lowercase full digest."""
    root = tmp_path / "artifacts"
    certificate = root / "platform-test-certificate-linux-io-pipeline.json"
    _complete_evidence(root, "linux", "io-pipeline")
    options = _options("linux", "io-pipeline")
    evidence.create_certificate(root, certificate, **options)

    with pytest.raises(ValueError, match="full 40-character commit digest"):
        evidence.verify_certificate(root, certificate, **{**options, "github_sha": _SHA.upper()})


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
            "linux",
            "concurrency",
            "tests/concurrency/test_operation_arena_telemetry_contracts.py::"
            "test_public_four_worker_stats_cover_every_shard",
            "at least four available CPUs are required",
        ),
        (
            "linux",
            "concurrency",
            "tests/concurrency/test_concurrency_memory_governor.py::"
            "test_process_cpu_governor_observes_live_affinity_changes",
            "host exposes only one CPU",
        ),
        (
            "macos-arm64",
            "concurrency",
            "tests/concurrency/"
            "test_concurrency_output_steal_preference_is_dormant_through_eight_workers.py::"
            "test_output_steal_preference_is_dormant_through_eight_workers",
            "output-steal topology requires at least three runnable CPUs",
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
    """A POSIX module skip requires the authenticated full shard inventory."""
    reason = "POSIX descriptor-relative filesystem hardening suite"
    reviewed = (
        "tests/memory/test_memory_process_identity_includes_linux_boot_id.py::"
        "test_process_identity_includes_linux_boot_id"
    )
    adjacent = "tests/memory/test_unreviewed.py::test_one"
    inventory = str(evidence.EXPECTED_TEST_INVENTORY["memory-parquet"]["sha256"])

    assert not _skip_is_allowed("windows", "memory-parquet", _SkipRecord(reviewed, reason))
    assert _skip_is_allowed(
        "windows",
        "memory-parquet",
        _SkipRecord(reviewed, reason),
        selected_inventory_sha256=inventory,
    )
    assert not _skip_is_allowed(
        "windows",
        "memory-parquet",
        _SkipRecord(adjacent, reason),
        selected_inventory_sha256=inventory,
    )


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
