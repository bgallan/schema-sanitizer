"""Behavioral architecture contracts for the CI helper tree.

The check keeps fuzz, native, Parquet, quality, and release helpers under explicit owner
directories and rejects obsolete flat-script locations.
"""

from __future__ import annotations

import importlib.util
import json
import os
import struct
import sys
import zipfile
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]
CI_ROOT = ROOT / "meta" / "ci"

OWNER_DIRECTORIES = {
    "fuzz",
    "native",
    "parquet",
    "quality",
    "release",
    "requirements",
    "sanitizers",
}


def _load_ci_helper(relative_path: str):
    """Load one standalone CI helper without making meta a runtime package."""
    path = CI_ROOT / relative_path
    spec = importlib.util.spec_from_file_location(f"test_{path.stem}_{id(path)}", path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_ci_helpers_are_grouped_by_owner() -> None:
    """Runnable helpers belong to a thematic owner, not a filename inventory."""
    helpers = [
        path.relative_to(CI_ROOT)
        for path in CI_ROOT.rglob("*")
        if path.is_file() and path.suffix in {".cc", ".py", ".sh"}
    ]

    assert helpers
    assert all(len(path.parts) >= 2 for path in helpers)
    assert {path.parts[0] for path in helpers} <= OWNER_DIRECTORIES
    assert all(path.suffix == ".md" for path in CI_ROOT.iterdir() if path.is_file())


def test_ci_shell_entry_points_remain_executable() -> None:
    """Moved shell gates retain the executable bit expected by workflows."""
    scripts = tuple(CI_ROOT.rglob("*.sh"))

    assert scripts
    assert all(os.access(script, os.X_OK) for script in scripts)


def test_only_the_bounded_sanitizer_watchdog_imports_subprocess() -> None:
    """The sole subprocess owner applies fixed bounds and process-tree cleanup."""
    production_roots = (CI_ROOT, ROOT / "benchmarks")
    offenders = {
        path.relative_to(ROOT).as_posix()
        for root in production_roots
        for path in root.rglob("*.py")
        if "import subprocess" in path.read_text(encoding="utf-8")
        or "from subprocess import" in path.read_text(encoding="utf-8")
    }

    assert offenders == {"meta/ci/sanitizers/run_with_watchdog.py"}
    watchdog = (ROOT / next(iter(offenders))).read_text(encoding="utf-8")
    assert 'start_new_session=os.name != "nt"' in watchdog
    assert "subprocess.CREATE_NEW_PROCESS_GROUP" in watchdog
    assert '"System32" / "taskkill.exe"' in watchdog
    assert "signal.SIGTERM" in watchdog
    assert "signal.SIGKILL" in watchdog
    assert '["/bin/ps", "-e", "-o", "pgid=,stat="]' in watchdog
    assert 'not fields[1].startswith("Z")' in watchdog
    assert "process.wait(timeout=timeout_seconds)" in watchdog
    assert "TERMINATION_GRACE_SECONDS = 10" in watchdog
    assert "shell=True" not in watchdog
    assert not (CI_ROOT / "quality" / "run_process.py").exists()


def test_sanitizer_watchdog_overrides_and_records_all_runtime_options(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A hostile ambient sanitizer environment cannot reach a guarded child."""
    helper = _load_ci_helper("sanitizers/run_with_watchdog.py")
    expected = {
        "ASAN_OPTIONS": "detect_leaks=0:halt_on_error=1",
        "LSAN_OPTIONS": "",
        "TSAN_OPTIONS": "",
        "UBSAN_OPTIONS": "halt_on_error=1:print_stacktrace=1",
    }
    for name in helper.SANITIZER_ENVIRONMENT_NAMES:
        monkeypatch.setenv(name, "ambient-runner-value")
    observed_path = tmp_path / "observed.json"
    certificate = tmp_path / "watchdog.json"
    child = (
        "import json, os, pathlib, sys; "
        "names=('ASAN_OPTIONS','LSAN_OPTIONS','TSAN_OPTIONS','UBSAN_OPTIONS'); "
        "pathlib.Path(sys.argv[1]).write_text("
        "json.dumps({name: os.environ.get(name) for name in names}, sort_keys=True), "
        "encoding='utf-8')"
    )

    status = helper.run_guarded(
        [sys.executable, "-c", child, str(observed_path)],
        timeout_seconds=10,
        label="runtime-environment-test",
        certificate=certificate,
        sanitizer_environment=expected,
    )

    assert status == 0
    assert json.loads(observed_path.read_text(encoding="utf-8")) == expected
    evidence = json.loads(certificate.read_text(encoding="utf-8"))
    assert evidence["sanitizer_environment"] == expected
    with pytest.raises(ValueError, match="inventory mismatch"):
        helper.run_guarded(
            [sys.executable, "-c", "pass"],
            timeout_seconds=10,
            label="incomplete-environment-test",
            certificate=certificate,
            sanitizer_environment={"ASAN_OPTIONS": ""},
        )


def test_retired_source_zip_pipeline_stays_absent() -> None:
    """The obsolete ZIP chain must not return beside the canonical sdist flow."""
    retired = {
        "check_cmake_sources_exist.sh",
        "check_zip_contains_cmake_sources.sh",
        "create_source_zip.sh",
    }

    assert not any(path.name in retired for path in CI_ROOT.rglob("*"))


def test_native_coverage_certificate_closes_inventory_floors_and_provenance(
    tmp_path: Path,
) -> None:
    """Coverage evidence fails on omitted sources, low totals, or run rebinding."""
    helper = _load_ci_helper("native/check_llvm_coverage.py")
    summaries = {metric: {"count": 1, "covered": 1, "percent": 100.0} for metric in helper.METRICS}
    sources = sorted(
        path
        for path in (ROOT / "cpp/src").rglob("*")
        if path.is_file() and path.suffix.lower() in helper.TRANSLATION_UNIT_SUFFIXES
    )
    document = {
        "type": "llvm.coverage.json.export",
        "version": "2.0.1",
        "data": [
            {
                "files": [{"filename": str(path), "summary": summaries} for path in sources],
                "totals": summaries,
            }
        ],
    }
    report = tmp_path / "coverage.json"
    certificate = tmp_path / "certificate.json"
    report.write_text(json.dumps(document), encoding="utf-8")
    options = {
        "repository": ROOT,
        "source_root": ROOT / "cpp/src",
        "github_sha": "a" * 40,
        "github_run_id": 7,
        "github_run_attempt": 1,
    }
    helper.create_certificate(report, certificate, **options)
    helper.verify_certificate(
        certificate,
        repository=ROOT,
        source_root=ROOT / "cpp/src",
        github_sha="a" * 40,
        github_run_id=7,
        github_run_attempt=1,
    )
    with pytest.raises(AssertionError, match="provenance mismatch"):
        helper.verify_certificate(
            certificate,
            repository=ROOT,
            source_root=ROOT / "cpp/src",
            github_sha="b" * 40,
            github_run_id=7,
            github_run_attempt=1,
        )

    payload = json.loads(certificate.read_text(encoding="utf-8"))
    payload["source_tree_sha256"] = "0" * 64
    certificate.write_text(helper._canonical_json(payload), encoding="utf-8")
    with pytest.raises(AssertionError, match="source-tree digest mismatch"):
        helper.verify_certificate(certificate, **options)
    helper.create_certificate(report, certificate, **options)

    boundary = {
        metric: {"count": 20_001, "covered": 8_000, "percent": 40.0} for metric in helper.METRICS
    }
    with pytest.raises(AssertionError, match="native coverage floor failed"):
        helper._require_floor("rounding-boundary", boundary, {"regions": 40.0})

    document["data"][0]["files"].pop()
    report.write_text(json.dumps(document), encoding="utf-8")
    with pytest.raises(AssertionError, match="omits production translation units"):
        helper.build_certificate(report, **options)
    document["data"][0]["files"].append({"filename": str(sources[-1]), "summary": summaries})
    document["data"][0]["totals"] = {
        metric: {"count": 1, "covered": 0, "percent": 0.0} for metric in helper.METRICS
    }
    report.write_text(json.dumps(document), encoding="utf-8")
    with pytest.raises(AssertionError, match="native coverage floor failed"):
        helper.build_certificate(report, **options)


def test_platform_wheel_certificate_rejects_binary_and_run_tampering(tmp_path: Path) -> None:
    """Native architecture bytes and workflow identity remain authenticated."""
    helper = _load_ci_helper("native/certify_platform_wheel.py")
    wheel = tmp_path / "schema_sanitizer-0.4.3-cp311-abi3-manylinux_2_28_x86_64.whl"
    elf = bytearray(64)
    elf[:6] = b"\x7fELF\x02\x01"
    struct.pack_into("<HH", elf, 16, 3, 62)
    with zipfile.ZipFile(wheel, "w") as archive:
        archive.writestr("schema_sanitizer/_core_abi3.abi3.so", elf)
    certificate = tmp_path / "wheel-certificate.json"
    options = {
        "platform": "linux-x86_64",
        "github_sha": "a" * 40,
        "github_run_id": 9,
        "github_run_attempt": 1,
    }
    helper.create_certificate(wheel, certificate, **options)
    helper.verify_certificate(
        certificate,
        wheel_directory=tmp_path,
        platform="linux-x86_64",
        github_sha="a" * 40,
        github_run_id=9,
        github_run_attempt=1,
    )
    with pytest.raises(AssertionError, match="changed"):
        helper.verify_certificate(
            certificate,
            wheel_directory=tmp_path,
            platform="linux-x86_64",
            github_sha="a" * 40,
            github_run_id=10,
            github_run_attempt=1,
        )
    payload = json.loads(certificate.read_text(encoding="utf-8"))
    payload["platform"] = "macos-x86_64"
    certificate.write_text(helper._canonical_json(payload), encoding="utf-8")
    with pytest.raises(AssertionError, match="platform mismatch"):
        helper.verify_certificate(
            certificate,
            wheel_directory=tmp_path,
            platform="linux-x86_64",
            github_sha="a" * 40,
            github_run_id=9,
            github_run_attempt=1,
        )
    payload["platform"] = "linux-x86_64"
    payload["wheel"]["filename"] = "../" + wheel.name
    certificate.write_text(helper._canonical_json(payload), encoding="utf-8")
    with pytest.raises(AssertionError, match="unsafe wheel filename"):
        helper.verify_certificate(
            certificate,
            wheel_directory=tmp_path,
            platform="linux-x86_64",
            github_sha="a" * 40,
            github_run_id=9,
            github_run_attempt=1,
        )
    payload["wheel"]["filename"] = wheel.name
    certificate.write_text(helper._canonical_json(payload), encoding="utf-8")
    real_wheel = tmp_path / "real-wheel.whl"
    wheel.rename(real_wheel)
    try:
        wheel.symlink_to(real_wheel)
    except OSError as error:
        pytest.skip(f"symlinks are unavailable: {error}")
    with pytest.raises(AssertionError, match="must not be a symlink"):
        helper.verify_certificate(
            certificate,
            wheel_directory=tmp_path,
            platform="linux-x86_64",
            github_sha="a" * 40,
            github_run_id=9,
            github_run_attempt=1,
        )
    malformed = tmp_path / "schema_sanitizer-0.4.4-cp311-abi3-manylinux_2_28_x86_64.whl"
    with zipfile.ZipFile(malformed, "w") as archive:
        archive.writestr("schema_sanitizer/_core_abi3.abi3.so", b"not-elf")
    with pytest.raises(AssertionError, match="not an ELF"):
        helper.build_certificate(malformed, **options)


def test_sanitizer_certificates_require_exact_matrix_and_watchdog_bytes(tmp_path: Path) -> None:
    """The gate consumes five run certificates and authenticates native watchdogs."""
    helper = _load_ci_helper("sanitizers/certify_sanitizer_run.py")
    for sanitizer, mode, runner_os, runner_arch in sorted(helper.EXPECTED_RUNS):
        for filename, raw in helper._watchdog_specs(
            sanitizer, mode, runner_os, runner_arch
        ).items():
            (tmp_path / filename).write_text(helper._canonical_json(raw), encoding="utf-8")
        certificate = tmp_path / f"sanitizer-run-{runner_os}-{runner_arch}-{sanitizer}.json"
        helper.create_certificate(
            certificate,
            evidence_directory=tmp_path,
            sanitizer=sanitizer,
            mode=mode,
            runner_os=runner_os,
            runner_arch=runner_arch,
            github_sha="a" * 40,
            github_run_id=11,
            github_run_attempt=1,
        )
    helper.verify_directory(
        tmp_path,
        github_sha="a" * 40,
        github_run_id=11,
        github_run_attempt=1,
    )
    extra_evidence = tmp_path / "unreviewed-evidence.txt"
    extra_evidence.write_text("unexpected\n", encoding="utf-8")
    with pytest.raises(AssertionError, match="evidence inventory mismatch"):
        helper.verify_directory(
            tmp_path,
            github_sha="a" * 40,
            github_run_id=11,
            github_run_attempt=1,
        )
    extra_evidence.unlink()
    raw_certificate = tmp_path / "watchdog-asan-native-Windows-X64-extension.json"
    raw = json.loads(raw_certificate.read_text(encoding="utf-8"))
    raw["timeout_seconds"] += 1
    raw_certificate.write_text(helper._canonical_json(raw), encoding="utf-8")
    with pytest.raises(AssertionError, match="policy mismatch"):
        helper.verify_directory(
            tmp_path,
            github_sha="a" * 40,
            github_run_id=11,
            github_run_attempt=1,
        )
    raw["timeout_seconds"] -= 1
    raw_certificate.write_text(helper._canonical_json(raw), encoding="utf-8")
    raw["sanitizer_environment"]["ASAN_OPTIONS"] = "ambient-runner-value"
    raw_certificate.write_text(helper._canonical_json(raw), encoding="utf-8")
    with pytest.raises(AssertionError, match="policy mismatch"):
        helper.verify_directory(
            tmp_path,
            github_sha="a" * 40,
            github_run_id=11,
            github_run_attempt=1,
        )
    raw["sanitizer_environment"] = helper._runtime_environment(
        "asan", "native", "Windows", "extension"
    )
    raw_certificate.write_text(helper._canonical_json(raw), encoding="utf-8")
    raw_certificate.write_text(raw_certificate.read_text(encoding="utf-8") + "\n", encoding="utf-8")
    with pytest.raises(AssertionError, match="not canonical JSON"):
        helper.verify_directory(
            tmp_path,
            github_sha="a" * 40,
            github_run_id=11,
            github_run_attempt=1,
        )
