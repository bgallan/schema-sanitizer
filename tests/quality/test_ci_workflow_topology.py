"""Protect the compact CI/CD topology and its shared release validation."""

from __future__ import annotations

import os
import re
import stat
import tomllib
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
WORKFLOWS = ROOT / ".github/workflows"


def _workflow(name: str) -> str:
    """Read one workflow definition."""
    return (WORKFLOWS / name).read_text(encoding="utf-8")


def _job_ids(workflow: str) -> set[str]:
    """Return the top-level job identifiers from a workflow."""
    jobs = workflow.split("\njobs:\n", 1)[1]
    return set(re.findall(r"^  ([a-z0-9-]+):$", jobs, re.MULTILINE))


def _job_body(workflow: str, job_id: str) -> str:
    """Return one top-level job body."""
    body = workflow.split(f"\n  {job_id}:\n", 1)[1]
    next_job = re.search(r"^  [a-z0-9-]+:$", body, re.MULTILINE)
    return body[: next_job.start()] if next_job else body


def _workflow_preamble(workflow: str) -> str:
    """Return workflow-level configuration before the jobs."""
    return workflow.split("\njobs:\n", 1)[0]


def _step_bodies(workflow: str) -> tuple[str, ...]:
    """Return top-level step bodies without requiring a YAML dependency."""
    starts = tuple(re.finditer(r"^      - (?:name|uses):", workflow, re.MULTILINE))
    return tuple(
        workflow[match.start() : starts[index + 1].start() if index + 1 < len(starts) else None]
        for index, match in enumerate(starts)
    )


def _with_value(step: str, key: str) -> str:
    """Read a scalar from an action step's ``with`` mapping."""
    match = re.search(rf"^          {re.escape(key)}:\s*([^#\n]+)", step, re.MULTILINE)
    assert match is not None, f"missing {key!r} in action step:\n{step}"
    return match.group(1).strip().strip("'\"")


def test_only_publish_is_a_manual_entry_point() -> None:
    """PR/main validation is automatic; publishing is the sole manual action."""
    workflows = tuple(
        path for path in WORKFLOWS.iterdir() if path.is_file() and path.suffix in {".yml", ".yaml"}
    )
    contents = {path.name: path.read_text(encoding="utf-8") for path in workflows}
    dispatched = {
        name
        for name, workflow in contents.items()
        if re.search(r"^  workflow_dispatch:", _workflow_preamble(workflow), re.MULTILINE)
    }

    assert set(contents) == {"ci.yml", "publish.yml"}
    assert dispatched == {"publish.yml"}
    assert all("  schedule:" not in _workflow_preamble(workflow) for workflow in contents.values())


def test_ci_has_only_safe_pr_main_and_reusable_triggers() -> None:
    """The canonical workflow covers PRs/main and remains callable by release."""
    preamble = _workflow_preamble(_workflow("ci.yml"))

    for trigger in ("workflow_call:", "push:", "pull_request:"):
        assert f"  {trigger}" in preamble
    assert preamble.count("branches: [main]") == 2
    assert "workflow_dispatch:" not in preamble
    assert "pull_request_target:" not in preamble


def test_workflow_defaults_are_read_only() -> None:
    """Source and validation jobs inherit only repository read access."""
    for name in ("ci.yml", "publish.yml"):
        preamble = _workflow_preamble(_workflow(name))
        assert re.search(r"^permissions:\n  contents: read$", preamble, re.MULTILINE)
        assert not re.search(r"^  [a-z-]+: write$", preamble, re.MULTILINE)


def test_manual_publish_wraps_canonical_validation_once() -> None:
    """Release adds preflight and publication around the exact CI workflow."""
    ci = _workflow("ci.yml")
    publish = _workflow("publish.yml")
    preamble = _workflow_preamble(publish)

    assert _job_ids(publish) == {"preflight", "validation", "publish"}
    assert "  workflow_dispatch:" in preamble
    for forbidden_trigger in ("workflow_call:", "push:", "pull_request:"):
        assert f"  {forbidden_trigger}" not in preamble
    assert publish.count("uses: ./.github/workflows/ci.yml") == 1
    assert "needs: [preflight]" in _job_body(publish, "validation")
    assert "needs: [preflight, validation]" in _job_body(publish, "publish")
    assert "python -m cibuildwheel" not in publish
    assert "python -m build" not in publish
    assert ci.count("python meta/ci/release/validate_release_version.py") == 1


def test_publish_request_is_explicit_and_always_targets_pypi() -> None:
    """A manual release cannot silently become a dry-run or TestPyPI run."""
    publish = _workflow("publish.yml")
    preamble = _workflow_preamble(publish)
    preflight = _job_body(publish, "preflight")
    publisher = _job_body(publish, "publish")

    assert set(re.findall(r"^      ([a-z0-9_]+):$", preamble, re.MULTILINE)) == {
        "release_tag",
        "confirm_publish",
    }
    for input_name in ("release_tag", "confirm_publish"):
        input_body = preamble.split(f"      {input_name}:\n", 1)[1]
        next_input = re.search(r"^      [a-z0-9_]+:$", input_body, re.MULTILINE)
        if next_input is not None:
            input_body = input_body[: next_input.start()]
        assert "required: true" in input_body
    for obsolete_mode in ("repository:", "check-only", "testpypi", "repository-url"):
        assert obsolete_mode not in publish.lower()
    assert "--require-release-tag" in preflight
    assert "--require-publish-confirmation" in preflight
    assert "python meta/ci/release/check_pypi_version.py" in preflight
    assert "python meta/ci/release/check_github_release_environment.py" in preflight
    assert 'git cat-file -t "refs/tags/${RELEASE_TAG}"' in preflight
    assert '[[ "${TAG_TYPE}" != "tag" ]]' in preflight
    assert "refs/tags/${RELEASE_TAG}^{commit}" in preflight
    assert "git ls-remote origin refs/heads/main" in preflight
    assert publish.count("pypa/gh-action-pypi-publish@") == 1
    assert "skip-existing:" not in publisher
    assert "if:" not in publisher


def test_oidc_publisher_is_a_code_free_least_privilege_boundary() -> None:
    """Only the final artifact crosses the isolated PyPI trust boundary."""
    publisher = _job_body(_workflow("publish.yml"), "publish")

    assert re.search(r"^    environment:(?: pypi|\n      name: pypi)$", publisher, re.MULTILINE)
    assert "id-token: write" in publisher
    for unnecessary_permission in ("contents: read", "contents: write", "actions: write"):
        assert unnecessary_permission not in publisher
    assert "actions/download-artifact@" in publisher
    assert "name: release-distributions" in publisher
    assert "pattern:" not in publisher
    assert "packages-dir: release/packages/" in publisher
    assert "actions/checkout@" not in publisher
    assert "actions/setup-python@" not in publisher
    assert not re.search(r"^      - run:", publisher, re.MULTILINE)
    assert "python " not in publisher


def test_release_preflight_has_only_the_read_permissions_it_uses() -> None:
    """Environment inspection and tag validation cannot mutate repository state."""
    preflight = _job_body(_workflow("publish.yml"), "preflight")
    permissions = preflight.split("    permissions:\n", 1)[1].split("    steps:\n", 1)[0]

    assert permissions == "      actions: read\n      contents: read\n"
    assert "id-token:" not in preflight


def test_external_actions_are_pinned_to_immutable_commits() -> None:
    """Every third-party action uses a full commit SHA; local reuse is exempt."""
    workflows = (_workflow("ci.yml"), _workflow("publish.yml"))
    refs = [
        ref
        for workflow in workflows
        for ref in re.findall(r"^\s*(?:-\s+)?uses:\s*([^\s#]+)", workflow, re.MULTILINE)
    ]

    assert refs
    assert [ref for ref in refs if ref.startswith("./")] == ["./.github/workflows/ci.yml"]
    external_refs = [ref for ref in refs if not ref.startswith("./")]
    assert external_refs
    assert all(re.fullmatch(r"[^@]+@[0-9a-f]{40}", ref) for ref in external_refs)
    assert set(external_refs) == {
        "actions/checkout@3d3c42e5aac5ba805825da76410c181273ba90b1",
        "actions/setup-python@5fda3b95a4ea91299a34e894583c3862153e4b97",
        "actions/upload-artifact@043fb46d1a93c77aae656e7c1c64a875d1fc6a0a",
        "actions/download-artifact@3e5f45b2cfb9172054b4087a40e8e0b5a5461e7c",
        "pypa/gh-action-pypi-publish@dc37677b2e1c63e2034f94d8a5b11f265b73ba33",
    }

    checkout_steps = [
        step
        for workflow in workflows
        for step in _step_bodies(workflow)
        if "actions/checkout@" in step
    ]
    assert checkout_steps
    assert all(_with_value(step, "persist-credentials") == "false" for step in checkout_steps)


def test_action_pins_have_automated_review_and_semantic_security_gates() -> None:
    """Immutable Actions remain maintainable and workflow-aware tooling blocks drift."""
    dependabot = (ROOT / ".github/dependabot.yml").read_text(encoding="utf-8")
    precommit = (ROOT / ".pre-commit-config.yaml").read_text(encoding="utf-8")

    assert "package-ecosystem: github-actions" in dependabot
    assert "interval: weekly" in dependabot
    assert "id: actionlint" in precommit
    assert "actionlint-py==1.7.12.24" in precommit
    assert "id: zizmor" in precommit
    assert "zizmor==1.29.0" in precommit

    remote_hooks = re.findall(
        r"^  - repo: https://[^\n]+\n    rev: ([^\s#]+)",
        precommit,
        re.MULTILINE,
    )
    assert len(remote_hooks) == 6
    assert all(re.fullmatch(r"[0-9a-f]{40}", revision) for revision in remote_hooks)


def test_ci_shell_entry_points_are_executable() -> None:
    """Scripts invoked directly by Actions must retain their executable bit."""
    scripts = tuple((ROOT / "meta/ci").rglob("*.sh"))

    assert scripts
    for script in scripts:
        assert script.read_bytes().startswith(b"#!/")
        if os.name != "nt":
            assert script.stat().st_mode & stat.S_IXUSR, script


def test_secret_scan_uses_the_tested_report_checker() -> None:
    """Secret exclusions stay narrow and outside the workflow YAML."""
    ci = _workflow("ci.yml")

    assert "python meta/ci/quality/check_detect_secrets_report.py .detect-secrets.ci.json" in ci
    assert "_is_notebook_cell_id" not in ci


def test_static_security_scan_covers_release_automation() -> None:
    """Code with release authority receives the same Bandit gate as runtime code."""
    ci = _workflow("ci.yml")

    assert "bandit -r src meta/ci -ll" in ci


def test_dependency_audit_includes_pinned_ci_executables() -> None:
    """Security tools executed by CI are also inputs to its dependency audit."""
    ci = _workflow("ci.yml")

    for requirement in (
        "actionlint-py==1.7.12.24",
        "bandit==1.9.4",
        "build==1.5.0",
        "cibuildwheel==4.2.0",
        "cmakelang==0.6.13",
        "coverage==7.15.4",
        "detect-secrets==1.5.0",
        "mypy==1.19.1",
        "packaging==26.3",
        "pip-audit==2.10.1",
        "toml-sort==0.24.3",
        "twine==7.0.0",
        "yamlfix==1.18.0",
        "zizmor==1.29.0",
    ):
        assert f'"{requirement}"' in ci


def test_validation_has_six_job_owners_and_one_stable_gate() -> None:
    """Six domain owners feed one auditable branch-protection result."""
    ci = _workflow("ci.yml")
    owners = {
        "checks",
        "platform-wheels",
        "distribution",
        "coverage-native",
        "platform-sanitizers",
        "thread-sanitizer",
    }
    assert _job_ids(ci) == owners | {"validation-gate"}
    gate = _job_body(ci, "validation-gate")
    assert "if: always()" in gate or "if: ${{ always() }}" in gate
    assert all(owner in gate for owner in owners)

    platform_matrix = _job_body(ci, "platform-wheels").split("    steps:", 1)[0]
    sanitizer_matrix = _job_body(ci, "platform-sanitizers").split("    steps:", 1)[0]
    assert len(re.findall(r"^          - name:", platform_matrix, re.MULTILINE)) == 4
    assert len(re.findall(r"^          - name:", sanitizer_matrix, re.MULTILINE)) == 4
    assert ci.count("      matrix:") == 2
    assert "python-version: [" not in ci
    assert "uses: ./.github/workflows/" not in ci

    assert "  schedule:" not in ci
    assert "github.event_name != 'schedule'" not in ci
    assert "github.event_name == 'schedule'" not in ci
    assert "cloud-emulators:" not in ci
    assert "test_cloud_emulator_integration.py" not in ci
    assert "test_cloud_real_services.py" not in ci


def test_platform_suite_exercises_the_installed_wheel() -> None:
    """The full suite must retain the wheel's platform-specific runtime bootstrap."""
    platform_job = _job_body(_workflow("ci.yml"), "platform-wheels")

    assert "pytest -q -o pythonpath=." in platform_job


def test_validation_owns_full_extension_tsan_gate() -> None:
    """Linux CI must build and repeatedly exercise the complete TSan extension."""
    ci = _workflow("ci.yml")

    assert "thread-sanitizer:" in ci
    assert "SCHEMA_SANITIZER_SANITIZER=tsan" in ci
    assert "SCHEMA_SANITIZER_ZLIB_PROVIDER=bundled" in ci
    assert "meta/ci/sanitizers/tsan_python_launcher.cc" in ci
    assert ci.count("meta/ci/sanitizers/run_tsan_extension_suite.sh") == 1
    assert "build/tsan ./python-tsan 2" in ci
    assert "site.getsitepackages()[0]" in ci

    runner = (ROOT / "meta/ci/sanitizers/run_tsan_extension_suite.sh").read_text(encoding="utf-8")
    for domain in (
        "test_threading_native_executor.py",
        "test_threading_inference.py",
        "test_threading_materialization.py",
        "test_threading_output.py",
        "test_threading_parquet_output.py",
        "test_threading_golden_matrix.py",
        "test_partition_lookahead.py",
        "test_csv_union_projection.py",
    ):
        assert runner.count(domain) == 1

    assert "pytest_sessionfinish" in runner
    assert "domain_shutdown_grace_seconds" in runner
    assert "setsid" in runner


def test_remote_http_fault_gate_runs_on_every_supported_platform() -> None:
    """The complete suite, including real-socket faults, runs in each platform task."""
    ci = _workflow("ci.yml")

    assert "platform-wheels:" in ci
    assert "Full suite including adapters, HTTP faults, and concurrency" in ci
    assert "run: pytest -q" in ci
    assert "--ignore" not in ci
    for runner in ("ubuntu-24.04", "windows-2025", "macos-15-intel", "macos-15"):
        assert runner in ci
    for floating_or_retired in ("ubuntu-latest", "windows-latest", "macos-14"):
        assert floating_or_retired not in ci


def test_native_fuzzing_and_platform_sanitizer_matrix_are_owned_by_ci() -> None:
    """Native fuzzing must run under TSan and supported platform sanitizers."""
    ci = _workflow("ci.yml")

    assert "platform-sanitizers:" in ci
    assert "windows-amd64-asan" in ci
    assert "macos-x86_64-asan-ubsan" in ci
    assert "macos-arm64-asan-ubsan" in ci
    assert ci.count("SCHEMA_SANITIZER_BUILD_FUZZERS=ON") >= 3
    assert ci.count("SCHEMA_SANITIZER_FUZZ_ENGINE=standalone") >= 3
    assert ci.count("meta/ci/fuzz/run_fuzz_regressions.py") >= 3
    assert ci.count("--engine libfuzzer") >= 1
    assert "--campaign-runs 1000" in ci
    assert "--campaign-runs 500" in ci
    assert "schema_sanitizer_sanitized_ordered_executor" in ci
    assert "--repeat until-fail:2" in ci
    concurrency_step = ci.split("- name: Run repeated sanitized concurrency probe", 1)[1].split(
        "- name:", 1
    )[0]
    fuzz_step = (
        _job_body(ci, "platform-sanitizers")
        .split("- name: Run platform fuzz regressions and mutation campaigns", 1)[1]
        .split("- name:", 1)[0]
    )
    assert "matrix.mode == 'native'" in concurrency_step
    assert "runner.os != 'Windows'" in concurrency_step
    assert "if: matrix.mode == 'native'" in fuzz_step

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
    assert 'if(NOT (MSVC AND SCHEMA_SANITIZER_SANITIZER STREQUAL "asan"))' in cmake
    assert "set(_schema_sanitizer_sanitized_executor_rounds 100)" in cmake
    assert "schema_sanitizer_sanitized_ordered_executor --rounds" in cmake
    assert 'argc == 3 && std::string_view(argv[1]) == "--rounds"' in probe
    assert "std::from_chars" in probe
    assert 'argc == 3 && std::string_view(argv[1]) == "--case"' in probe
    assert "shared arena startup timed out" in probe
    assert "available_cpu_capacity()" in probe
    assert "const auto upstream_width = worker_count / 2U" in probe
    assert "const auto output_width = worker_count - upstream_width" in probe
    assert "arena->peak_active_tasks() == worker_count" in probe
    assert "sanitizer probe skipped: case=shared_arena reason=requires" in probe
    assert "sanitizer probe skipped: case=backlog_admission" in probe
    assert "sanitizer probe skipped: case=lane_stealing reason=requires" in probe
    assert probe.count("return true;", probe.index("run_shared_operation_arena_round")) >= 3
    assert "stage cancellation startup timed out" in probe
    assert "cancellation startup timed out" in probe
    assert "sanitizer probe watchdog expired" in probe


def test_native_launcher_arguments_preserve_shell_word_boundaries() -> None:
    """Compiler/linker flags and interpreter paths remain arrays or quoted scalars."""
    ci = _workflow("ci.yml")

    assert (
        ci.count('read -r -a python_embed_flags <<< "$(python3-config --embed --cflags --ldflags)"')
        == 2
    )
    assert ci.count('"${python_embed_flags[@]}"') == 2
    assert '-DPython3_EXECUTABLE="$(command -v python)"' in ci
    assert "$(which python)" not in ci


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
    cmake = (ROOT / "CMakeLists.txt").read_text(encoding="utf-8")
    threads = (ROOT / "cpp/src/internal/runtime/thread_compat.hh").read_text(encoding="utf-8")
    arena = (ROOT / "cpp/src/internal/runtime/operation_task_arena.hh").read_text(encoding="utf-8")
    telemetry = (ROOT / "cpp/src/internal/runtime/performance_telemetry.cc").read_text(
        encoding="utf-8"
    )
    tokens = (ROOT / "cpp/src/internal/json_encoding/token_writer.cc").read_text(encoding="utf-8")
    formatter = telemetry.split("void append_double_field", 1)[1].split(
        "std::int64_t nonnegative", 1
    )[0]
    token_formatter = tokens.split("void append_double_field", 1)[1].split(
        "} // namespace sanitize::internal::json_encoding", 1
    )[0]

    assert "$<$<CXX_COMPILER_ID:MSVC>:/wd4324>" in options
    assert "schema_sanitizer_stage_msvc_asan_runtime" in options
    assert "clang_rt.asan_dynamic-${_schema_sanitizer_asan_arch}.dll" in options
    assert "get_filename_component(_schema_sanitizer_compiler_dir" in options
    assert '"${CMAKE_CXX_COMPILER}"' in options
    assert 'schema_sanitizer_stage_msvc_asan_runtime("${CMAKE_BINARY_DIR}/fuzz")' in cmake
    assert "defined(_MSC_VER) && defined(__SANITIZE_ADDRESS__)" in threads
    assert "SCHEMA_SANITIZER_FORCE_ATOMIC_WAIT_POLLING" in threads
    assert "SCHEMA_SANITIZER_PORTABLE_THREAD_COMPAT_ACTIVE" in threads
    assert "std::this_thread::sleep_for(std::chrono::microseconds(100))" in threads
    assert "std::atomic_load_explicit" not in arena
    assert "std::atomic_exchange_explicit" not in arena
    assert "using AtomicSharedPtr = std::atomic<std::shared_ptr<T>>" in arena
    assert "mutable std::mutex mutex_" in arena
    assert "std::to_chars" not in formatter
    assert "std::locale::classic()" in formatter
    assert "#if defined(__APPLE__)" in token_formatter
    assert "std::locale::classic()" in token_formatter
    assert "std::numeric_limits<double>::max_digits10" in token_formatter


def test_benchmark_matrix_runs_on_supported_platforms() -> None:
    """Benchmark gates must cover supported OS, shape, and source dimensions."""
    ci = _workflow("ci.yml")
    platform_job = _job_body(ci, "platform-wheels")

    assert "python -m benchmarks.concurrency.threading.matrix" in platform_job
    assert "--profile ci" in platform_job
    assert "python -m benchmarks.readers.linear_scaling" in platform_job
    assert "--maximum-normalized-growth 8" in platform_job
    assert "reader-linear-scaling-${{ matrix.artifact }}.json" in platform_job
    for artifact in ("linux", "windows", "macos-x86_64", "macos-arm64"):
        assert f"artifact: {artifact}" in ci


def test_python_coverage_has_an_explicit_regression_floor() -> None:
    """Coverage collection is a gate, not merely a report artifact."""
    checks = _job_body(_workflow("ci.yml"), "checks")

    assert checks.count("coverage report --fail-under=44") == 1


def test_ci_artifact_policies_are_explicit_and_bounded() -> None:
    """Missing evidence fails and each artifact has a deliberate lifetime."""
    uploads = {
        _with_value(step, "name"): step
        for step in _step_bodies(_workflow("ci.yml"))
        if "actions/upload-artifact@" in step
    }
    retention = {
        "python-branch-coverage": "14",
        "dist-wheels-${{ matrix.name }}": "1",
        "platform-evidence-${{ matrix.artifact }}": "14",
        "release-distributions": "30",
        "native-llvm-coverage": "14",
    }

    assert set(uploads) == set(retention)
    for name, days in retention.items():
        assert _with_value(uploads[name], "retention-days") == days
        assert _with_value(uploads[name], "if-no-files-found") == "error"
    assert _with_value(uploads["release-distributions"], "path") == "release/"


def test_release_artifact_is_complete_exact_and_self_describing() -> None:
    """CI publishes one immutable package set with its audit manifest."""
    ci = _workflow("ci.yml")
    distribution = _job_body(ci, "distribution")
    publish = _job_body(_workflow("publish.yml"), "publish")
    downstream = (ROOT / "meta/ci/release/check_downstream_install.py").read_text(encoding="utf-8")

    for version in ("3.11", "3.12", "3.13", "3.14"):
        assert f"python-version: '{version}'" in ci
    assert ci.count("python -I meta/ci/release/downstream_smoke.py") == 3
    assert "needs: [platform-wheels]" in distribution
    assert "pattern: dist-wheels-*" in distribution
    assert "check_distribution_contents.py --release-set" in distribution
    assert "check_downstream_install.py" in distribution
    assert "release/packages/" in distribution
    assert "release/release-manifest.json" in distribution
    assert "name: release-distributions" in distribution
    assert "name: dist-sdist" not in ci
    assert "name: release-distributions" in publish
    assert "packages-dir: release/packages/" in publish
    assert "release/release-manifest.json" in ci
    for extra in (
        "core",
        "pyarrow",
        "pandas",
        "polars",
        "duckdb",
        "gcs",
        "s3",
        "azure",
        "bigquery",
        "cloud",
        "all",
    ):
        assert f'"{extra}"' in downstream
