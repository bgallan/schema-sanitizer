"""Protect the two-entry-point CI workflow topology."""

from __future__ import annotations

import os
import re
import stat
import tomllib
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


def test_ci_shell_entry_points_are_executable() -> None:
    """Scripts invoked directly by Actions must retain their executable bit."""
    scripts = tuple((ROOT / "meta/ci").glob("*.sh"))

    assert scripts
    for script in scripts:
        assert script.read_bytes().startswith(b"#!/")
        if os.name != "nt":
            assert script.stat().st_mode & stat.S_IXUSR, script


def test_secret_scan_uses_the_tested_report_checker() -> None:
    """Secret exclusions stay narrow and outside the workflow YAML."""
    ci = _workflow("ci.yml")

    assert "python meta/ci/check_detect_secrets_report.py .detect-secrets.ci.json" in ci
    assert "_is_notebook_cell_id" not in ci


def test_general_sanity_owns_validation_without_scheduled_jobs() -> None:
    """General sanity owns validation without exposing scheduled workloads."""
    ci = _workflow("ci.yml")
    validation_jobs = (
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
    assert "cloud-emulators:" not in ci
    assert "test_cloud_emulator_integration.py" not in ci
    assert "test_cloud_real_services.py" not in ci


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


def test_native_concurrency_gate_links_its_memory_resource_implementation() -> None:
    """The standalone sanitizer executable must own every arena dependency."""
    cmake = (ROOT / "CMakeLists.txt").read_text(encoding="utf-8")
    probe = (ROOT / "cpp/tests/ordered_executor_tsan.cc").read_text(encoding="utf-8")
    target = cmake.split("add_executable(\n    schema_sanitizer_sanitized_ordered_executor", 1)[
        1
    ].split(")", 1)[0]

    assert "cpp/src/internal/memory/memory_pool.cc" in target
    assert "cpp/src/internal/memory/pool_resource.cc" in target
    assert 'MSVC AND SCHEMA_SANITIZER_SANITIZER STREQUAL "asan"' in cmake
    assert "set(_schema_sanitizer_sanitized_executor_rounds 10)" in cmake
    assert "PROPERTIES TIMEOUT 600" in cmake
    assert "schema_sanitizer_sanitized_ordered_executor --rounds" in cmake
    assert 'std::string_view(argv[1]) != "--rounds"' in probe
    assert "shared arena startup timed out" in probe
    assert "stage cancellation startup timed out" in probe
    assert "cancellation startup timed out" in probe
    assert "sanitizer probe watchdog expired" in probe


def test_macos_native_baseline_matches_concurrency_runtime_requirements() -> None:
    """macOS wheels must not advertise a pre-atomic-wait runtime baseline."""
    cmake = (ROOT / "CMakeLists.txt").read_text(encoding="utf-8")
    pyproject = tomllib.loads((ROOT / "pyproject.toml").read_text(encoding="utf-8"))
    ci = _workflow("ci.yml")

    assert 'CMAKE_OSX_DEPLOYMENT_TARGET VERSION_LESS "11.0"' in cmake
    assert "MACOSX_DEPLOYMENT_TARGET:" not in ci
    assert (
        pyproject["tool"]["cibuildwheel"]["macos"]["environment"]["MACOSX_DEPLOYMENT_TARGET"]
        == "11.0"
    )


def test_platform_specific_standard_library_boundaries_are_explicit() -> None:
    """Intentional alignment and telemetry formatting remain portable."""
    options = (ROOT / "cmake/SchemaSanitizerTargetOptions.cmake").read_text(encoding="utf-8")
    threads = (ROOT / "cpp/src/internal/runtime/thread_compat.hh").read_text(encoding="utf-8")
    telemetry = (ROOT / "cpp/src/internal/runtime/performance_telemetry.cc").read_text(
        encoding="utf-8"
    )
    formatter = telemetry.split("void append_double_field", 1)[1].split(
        "std::int64_t nonnegative", 1
    )[0]

    assert "$<$<CXX_COMPILER_ID:MSVC>:/wd4324>" in options
    assert "defined(_MSC_VER) && defined(__SANITIZE_ADDRESS__)" in threads
    assert "SCHEMA_SANITIZER_FORCE_ATOMIC_WAIT_POLLING" in threads
    assert "SCHEMA_SANITIZER_PORTABLE_THREAD_COMPAT_ACTIVE" in threads
    assert "std::this_thread::sleep_for(std::chrono::microseconds(100))" in threads
    assert "std::to_chars" not in formatter
    assert "std::locale::classic()" in formatter


def test_benchmark_matrix_runs_on_supported_platforms() -> None:
    """Benchmark gates must cover supported OS, shape, and source dimensions."""
    ci = _workflow("ci.yml")

    assert "benchmark-matrix-smoke:" in ci
    assert "benchmarks/bench_threading_matrix.py" in ci
    assert "--profile ci" in ci
    for artifact in ("linux", "windows", "macos-x86_64", "macos-arm64"):
        assert f"artifact: {artifact}" in ci
