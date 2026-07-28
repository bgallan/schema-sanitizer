"""Protect the two-entry-point CI workflow topology."""

from __future__ import annotations

import re
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
WORKFLOWS = ROOT / ".github/workflows"


def _workflow(name: str) -> str:
    """Read one workflow definition."""
    return (WORKFLOWS / name).read_text(encoding="utf-8")


def test_only_general_sanity_and_publish_are_manually_dispatchable() -> None:
    """GitHub Actions must expose exactly two manual workflow buttons."""
    dispatched = {
        path.name
        for path in WORKFLOWS.glob("*.yml")
        if re.search(r"^  workflow_dispatch:", path.read_text(encoding="utf-8"), re.MULTILINE)
    }

    assert dispatched == {"ci.yml", "publish.yml"}


def test_actions_sidebar_has_only_general_sanity_and_publish() -> None:
    """Only the two user-facing workflow files should appear in Actions."""
    assert {path.name for path in WORKFLOWS.glob("*.yml")} == {"ci.yml", "publish.yml"}


def test_pr_main_manual_and_publish_share_general_sanity() -> None:
    """PR, post-merge, manual sanity, and publish must use one validation owner."""
    ci = _workflow("ci.yml")
    publish = _workflow("publish.yml")

    for trigger in ("workflow_call:", "push:", "pull_request:", "workflow_dispatch:"):
        assert f"  {trigger}" in ci
    assert "branches: [main]" in ci
    assert "uses: ./.github/workflows/ci.yml" in publish
    assert "python -m cibuildwheel" not in publish
    assert ci.count("python meta/ci/validate_release_version.py") == 1
    assert publish.count("python meta/ci/validate_release_version.py") == 2


def test_general_sanity_owns_validation_without_scheduled_jobs() -> None:
    """General sanity owns validation without exposing scheduled workloads."""
    ci = _workflow("ci.yml")
    validation_jobs = (
        "cloud-emulators:",
        "benchmark-matrix-smoke:",
        "coverage-python:",
        "coverage-native:",
        "downstream-wheel:",
        "downstream-extras:",
        "native-sanitizers:",
        "native-platform-sanitizers:",
        "native-thread-sanitizer:",
        "remote-http-fault-injection:",
    )

    for job in validation_jobs:
        assert f"  {job}" in ci
    assert "uses: ./.github/workflows/" not in ci

    assert "  schedule:" not in ci
    assert "github.event.schedule" not in ci
    assert "github.event_name != 'schedule'" not in ci
    assert "scheduled-benchmarks:" not in ci
    assert "scheduled-native-fuzz:" not in ci
    assert "scheduled-real-gcp:" not in ci


def test_general_sanity_owns_full_extension_tsan_gate() -> None:
    """Linux CI must build and repeatedly exercise the complete TSan extension."""
    ci = _workflow("ci.yml")

    assert "native-thread-sanitizer:" in ci
    assert "SCHEMA_SANITIZER_SANITIZER=tsan" in ci
    assert "SCHEMA_SANITIZER_ZLIB_PROVIDER=bundled" in ci
    assert "meta/ci/tsan_python_launcher.cc" in ci
    assert "meta/ci/run_tsan_extension_suite.sh" in ci
    assert "build/tsan ./python-tsan 2" in ci
    assert "--verify-only" in ci
    assert "site.getsitepackages()[0]" in ci
    for domain in (
        "test_threading_native_executor.py",
        "test_threading_inference.py",
        "test_threading_materialization.py",
        "test_threading_output.py",
        "test_threading_parquet_output.py",
        "test_threading_golden_matrix.py",
        "test_partition_lookahead.py",
    ):
        assert ci.count(domain) == 1

    runner = (ROOT / "meta/ci/run_tsan_extension_suite.sh").read_text(encoding="utf-8")
    assert "pytest_sessionfinish" in runner
    assert "domain_shutdown_grace_seconds" in runner
    assert "setsid" in runner


def test_remote_http_fault_gate_runs_on_every_supported_platform() -> None:
    """Real-socket transport faults must run against every core wheel."""
    ci = _workflow("ci.yml")

    assert "remote-http-fault-injection:" in ci
    assert "needs: [core-only]" in ci
    assert ci.count("abi3-wheel-${{ matrix.artifact }}") >= 2
    assert "tests/test_remote_http_fault_injection.py" in ci
    assert "tests/test_remote_process_lifecycle.py" in ci
    for runner in ("ubuntu-latest", "windows-latest", "macos-15-intel", "macos-14"):
        assert runner in ci


def test_native_fuzzing_and_platform_sanitizer_matrix_are_owned_by_ci() -> None:
    """Native fuzzing must run under TSan and supported platform sanitizers."""
    ci = _workflow("ci.yml")

    assert "native-platform-sanitizers:" in ci
    assert "windows-amd64-asan" in ci
    assert "macos-x86_64-asan-ubsan" in ci
    assert "macos-arm64-asan-ubsan" in ci
    assert ci.count("SCHEMA_SANITIZER_BUILD_FUZZERS=ON") >= 3
    assert ci.count("SCHEMA_SANITIZER_FUZZ_ENGINE=standalone") >= 3
    assert ci.count("meta/ci/run_fuzz_regressions.py") >= 2
    assert "--campaign-runs 1000" in ci
    assert "--campaign-runs 500" in ci
    assert "schema_sanitizer_sanitized_ordered_executor" in ci
    assert "--repeat until-fail:2" in ci

    cmake = (ROOT / "CMakeLists.txt").read_text(encoding="utf-8")
    assert "SCHEMA_SANITIZER_FUZZ_ENGINE" in cmake
    assert "cpp/fuzz/standalone_main.cc" in cmake
    assert "schema_sanitizer_sanitized_ordered_executor" in cmake


def test_benchmark_matrix_runs_on_supported_platforms_and_cloud_emulators() -> None:
    """Benchmark gates must cover OS, shape, source, and provider dimensions."""
    ci = _workflow("ci.yml")

    assert "benchmark-matrix-smoke:" in ci
    assert "benchmarks/bench_threading_matrix.py" in ci
    assert "--profile ci" in ci
    assert "benchmarks/bench_remote_providers.py" in ci
    assert "remote-provider-benchmark.json" in ci
    for artifact in ("linux", "windows", "macos-x86_64", "macos-arm64"):
        assert f"artifact: {artifact}" in ci
