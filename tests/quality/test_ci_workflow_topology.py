"""Protect the compact CI/CD topology and its shared release validation.

It protects workflow triggers, permissions, pinned dependencies, job matrices, artifact
contracts, release gates, and fail-closed publication topology.
"""

from __future__ import annotations

import json
import os
import re
import shutil
import signal
import stat
import subprocess
import sys
import textwrap
import tomllib
from pathlib import Path

import pytest
from packaging.requirements import Requirement
from packaging.utils import canonicalize_name

ROOT = Path(__file__).resolve().parents[2]
WORKFLOWS = ROOT / ".github/workflows"
ACTIONS = ROOT / ".github/actions"
VALIDATION_ACTIONS = (
    "quality-validation",
    "source-distribution",
    "native-llvm-coverage",
    "thread-sanitizer",
    "platform-sanitizer",
)


def _table(fields: tuple[str, ...], rows: str) -> tuple[dict[str, str], ...]:
    """Build compact declarative records from pipe-delimited rows."""
    return tuple(
        dict(zip(fields, (value.strip() for value in line.split("|")), strict=True))
        for line in rows.strip().splitlines()
    )


_VALIDATION_ROWS = _table(
    ("name", "task", "runner", "python", "timeout"),
    """
    quality, security, and Python coverage | quality | ubuntu-24.04 | 3.11.9 | 50
    source distribution and downstream packaging | source-distribution | ubuntu-24.04 | 3.11.9 | 90
    coverage / native LLVM | native-llvm-coverage | ubuntu-24.04 | 3.11.9 | 50
    sanitizer / Linux GCC ThreadSanitizer | thread-sanitizer | ubuntu-24.04 | 3.13.15 | 45
    """,
) + _table(
    ("name", "task", "runner", "python", "timeout", "sanitizer", "mode"),
    """
    sanitizer / linux-x86_64-asan-ubsan | platform-sanitizer | ubuntu-24.04 | 3.11.9 | 70 | asan-ubsan | linux-full
    sanitizer / windows-amd64-asan | platform-sanitizer | windows-2022 | 3.11.9 | 70 | asan | native
    sanitizer / macos-x86_64-asan-ubsan | platform-sanitizer | macos-15-intel | 3.11.9 | 70 | asan-ubsan | native
    sanitizer / macos-arm64-asan-ubsan | platform-sanitizer | macos-15 | 3.11.9 | 70 | asan-ubsan | native
    """,
)
_BUILD_PLATFORMS = _table(
    ("display-name", "runner", "platform-name", "artifact", "arch"),
    """
    Linux x86-64 | ubuntu-24.04 | linux-x86_64 | linux | x86_64
    Windows AMD64 | windows-2022 | windows-amd64 | windows | AMD64
    macOS x86-64 | macos-15-intel | macos-x86_64 | macos-x86_64 | x86_64
    macOS ARM64 | macos-15 | macos-arm64 | macos-arm64 | arm64
    """,
)
_TEST_SHARDS = ("io-pipeline", "memory-parquet", "concurrency")
_TEST_PLATFORM_JOBS = tuple(
    {
        "job-id": f"platform-tests-{platform['platform-name'].replace('_', '-')}",
        "runner": platform["runner"],
        "platform-name": platform["platform-name"],
        "artifact": platform["artifact"],
        "minimum-cpu-capacity": ("3" if platform["platform-name"] == "macos-arm64" else "4"),
    }
    for platform in _BUILD_PLATFORMS
)
_PLATFORM_LOCK_NAMES = frozenset(
    """
    aiohappyeyeballs aiohttp aiosignal attrs colorama duckdb frozenlist idna iniconfig multidict
    numpy packaging pandas pip pluggy polars polars-runtime-32 propcache pyarrow pygments pytest
    python-dateutil six typing-extensions tzdata yarl
    """.split()
)
_BUILD_TOOL_LOCK_NAMES = frozenset(
    """
    abi3audit abi3info altgraph attrs auditwheel backports-tarfile bashlex bracex build cattrs
    certifi cffi charset-normalizer cibuildwheel cmake colorama cryptography delocate delvewheel
    dependency-groups distlib docutils filelock humanize id idna importlib-metadata iniconfig
    jaraco-classes jaraco-context jaraco-functools jeepney kaitaistruct keyring macholib
    markdown-it-py mdurl more-itertools nh3 ninja packaging patchelf pathspec pefile pip pkgconf
    platformdirs pluggy polars polars-runtime-32 pyarrow pycparser pyelftools pygments
    pyproject-hooks pytest python-discovery pywin32-ctypes readme-renderer requests requests-cache
    requests-toolbelt rfc3986 rich scikit-build-core secretstorage setuptools twine
    typing-extensions url-normalize urllib3 virtualenv wheel zipp
    """.split()
)
_PRE_COMMIT_HOOK_LOCK_NAMES = frozenset(
    """
    actionlint-py annotated-doc annotated-types certifi charset-normalizer clang-format click
    cmakelang colorama distro idna librt loguru maison markdown-it-py mdformat mdurl mypy
    mypy-extensions packaging pathspec pip platformdirs pre-commit-hooks pydantic pydantic-core
    pygments requests rich ruamel-yaml ruff ruyaml semver setuptools shellcheck-py shellingham
    shfmt-py six toml-sort tomlkit typer typing-extensions typing-inspection urllib3 wheel yamlfix
    zizmor
    """.split()
)
_QUALITY_LOCK_NAMES = frozenset(
    """
    aiohappyeyeballs aiohttp aiosignal attrs bandit boolean-py build cachecontrol certifi cfgv
    charset-normalizer colorama coverage cyclonedx-python-lib defusedxml detect-secrets distlib
    duckdb filelock frozenlist identify idna iniconfig librt license-expression markdown-it-py
    mdurl msgpack multidict mypy mypy-extensions nodeenv numpy packageurl-python packaging pandas
    pathspec pip pip-api pip-audit pip-requirements-parser platformdirs pluggy polars
    polars-runtime-32 pre-commit propcache py-serializable pyarrow pygments pyparsing pyproject-hooks
    pytest pytest-asyncio python-dateutil python-discovery pyyaml requests rich ruff six
    sortedcontainers stevedore tomli tomli-w typing-extensions tzdata urllib3 virtualenv yarl
    """.split()
)
_RELEASE_VERIFICATION_LOCK_NAMES = frozenset(
    """
    annotated-types certifi cffi charset-normalizer cryptography dnspython email-validator id idna
    markdown-it-py mdurl packaging platformdirs pyasn1 pycparser pydantic pydantic-core pygments
    pip pyjwt pyopenssl pypi-attestations requests rfc3161-client rfc3986 rfc8785 rich
    securesystemslib sigstore sigstore-models sigstore-rekor-types tuf typing-extensions
    typing-inspection urllib3
    """.split()
)


def _workflow(name: str) -> str:
    """Read one workflow definition."""
    return (WORKFLOWS / name).read_text(encoding="utf-8")


def _action(name: str) -> str:
    """Read one repository-owned composite action."""
    return (ACTIONS / name / "action.yml").read_text(encoding="utf-8")


def _linux_process_group_is_live(pgid: int) -> bool:
    """Return whether a Linux process group contains any non-zombie member."""
    for stat_path in Path("/proc").glob("[0-9]*/stat"):
        try:
            stat_line = stat_path.read_text(encoding="ascii")
        except FileNotFoundError:
            continue
        _, separator, remainder = stat_line.rpartition(") ")
        fields = remainder.split()
        if separator and len(fields) >= 3 and int(fields[2]) == pgid and fields[0] != "Z":
            return True
    return False


def _ci_yaml_definitions() -> tuple[tuple[Path, str], ...]:
    """Return every workflow and composite-action definition in stable path order."""
    paths = sorted(
        (
            path
            for root in (WORKFLOWS, ACTIONS)
            for path in root.rglob("*")
            if path.is_file() and path.suffix in {".yml", ".yaml"}
        ),
        key=lambda path: path.as_posix(),
    )
    return tuple((path, path.read_text(encoding="utf-8")) for path in paths)


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


def _matrix_includes(workflow: str, job_id: str) -> tuple[dict[str, str], ...]:
    """Return the scalar rows from one job's include-only matrix."""
    body = _job_body(workflow, job_id)
    matrix = body.split("      matrix:\n        include:\n", 1)[1].split("    steps:\n", 1)[0]
    rows = re.split(r"^          - ", matrix, flags=re.MULTILINE)[1:]
    return tuple(
        {
            key: value.strip().strip("'\"")
            for key, value in re.findall(r"^\s*([a-z-]+):\s*([^#\n]+?)\s*$", row, re.MULTILINE)
        }
        for row in rows
    )


def _matrix_axis(workflow: str, job_id: str, key: str) -> tuple[str, ...]:
    """Return one inline scalar axis from a job matrix."""
    body = _job_body(workflow, job_id)
    match = re.search(rf"^        {re.escape(key)}: \[([^]]+)\]$", body, re.MULTILINE)
    assert match is not None, f"missing matrix axis {key!r} in {job_id!r}"
    return tuple(value.strip().strip("'\"") for value in match.group(1).split(","))


def _job_needs(workflow: str, job_id: str) -> tuple[str, ...]:
    """Return one job's inline or block dependency list."""
    body = _job_body(workflow, job_id)
    inline = re.search(r"^    needs: \[([^]]+)\]$", body, re.MULTILINE)
    if inline is not None:
        return tuple(value.strip() for value in inline.group(1).split(","))
    block = re.search(r"^    needs:\n((?:      - [a-z0-9-]+\n)+)", body, re.MULTILINE)
    assert block is not None, f"missing needs list in {job_id!r}"
    return tuple(re.findall(r"^      - ([a-z0-9-]+)$", block.group(1), re.MULTILINE))


def _step_bodies(workflow: str) -> tuple[str, ...]:
    """Return top-level step bodies without requiring a YAML dependency."""
    starts = tuple(re.finditer(r"^(?: {4}| {6})- (?:name|uses):", workflow, re.MULTILINE))
    return tuple(
        workflow[match.start() : starts[index + 1].start() if index + 1 < len(starts) else None]
        for index, match in enumerate(starts)
    )


def _yaml_mapping_block(source: str, key: str, indent: int) -> str:
    """Return one indentation-delimited YAML mapping body."""
    prefix = " " * indent
    match = re.search(rf"^{prefix}{re.escape(key)}:\s*(?:#.*)?$", source, re.MULTILINE)
    if match is None:
        return ""
    lines: list[str] = []
    for line in source[match.end() :].splitlines(keepends=True):
        content = line.lstrip(" ")
        if content.strip() and len(line) - len(content) <= indent:
            break
        lines.append(line)
    return "".join(lines)


def _no_isolation_constraint_violations(definitions: dict[str, str]) -> tuple[str, ...]:
    """Find every scope that can leak a build constraint into a non-isolated build."""
    constraint = "PIP_BUILD_CONSTRAINT"
    violations: set[str] = set()
    workflows = {
        path: source
        for path, source in definitions.items()
        if path.startswith(".github/workflows/")
    }
    actions = {
        path: source
        for path, source in definitions.items()
        if path.startswith(".github/actions/") and path.endswith("/action.yml")
    }

    persistent_environment_owner = ".github/actions/restore-pip-cache/action.yml"
    for path, source in definitions.items():
        if path == persistent_environment_owner:
            if constraint in source:
                violations.add(f"{path}: persistent GITHUB_ENV mutates {constraint}")
            continue
        if re.search(r"\bGITHUB_ENV\b", source, re.IGNORECASE):
            violations.add(f"{path}: persistent GITHUB_ENV mutation is forbidden")

    for action_path, action in actions.items():
        no_isolation_steps = tuple(
            step for step in _step_bodies(action) if "--no-build-isolation" in step
        )
        if not no_isolation_steps:
            continue
        for step in no_isolation_steps:
            if constraint in step:
                violations.add(f"{action_path}: non-isolated step sets {constraint}")

        action_ref = f"./{action_path.removesuffix('/action.yml')}"
        callers = 0
        for workflow_path, workflow in workflows.items():
            workflow_env = _yaml_mapping_block(_workflow_preamble(workflow), "env", 0)
            for job_id in _job_ids(workflow):
                job = _job_body(workflow, job_id)
                invocation_steps = tuple(step for step in _step_bodies(job) if action_ref in step)
                if not invocation_steps:
                    continue
                callers += len(invocation_steps)
                if constraint in workflow_env:
                    violations.add(f"{workflow_path}: workflow env leaks into {action_ref}")
                if constraint in _yaml_mapping_block(job, "env", 4):
                    violations.add(f"{workflow_path}:{job_id}: job env leaks into {action_ref}")
                for step in invocation_steps:
                    if constraint in step:
                        violations.add(
                            f"{workflow_path}:{job_id}: invocation env leaks into {action_ref}"
                        )
        if callers == 0:
            violations.add(f"{action_path}: non-isolated action has no workflow caller")

    return tuple(sorted(violations))


def _with_value(step: str, key: str) -> str:
    """Read a scalar from an action step's ``with`` mapping."""
    match = re.search(rf"^\s+{re.escape(key)}:\s*([^#\n]+)", step, re.MULTILINE)
    assert match is not None, f"missing {key!r} in action step:\n{step}"
    return match.group(1).strip().strip("'\"")


def _exact_lock_names(path: Path) -> set[str]:
    """Return canonical names after proving every nonempty lock entry is exact."""
    lines = [line for line in path.read_text(encoding="utf-8").splitlines() if line]
    requirements = [Requirement(line) for line in lines]
    assert len({canonicalize_name(requirement.name) for requirement in requirements}) == len(lines)
    for requirement in requirements:
        specifiers = tuple(requirement.specifier)
        assert requirement.url is None
        assert not requirement.extras
        assert len(specifiers) == 1
        assert specifiers[0].operator == "=="
        assert "*" not in specifiers[0].version
    return {canonicalize_name(requirement.name) for requirement in requirements}


def _assert_text_contract(
    source: str,
    *,
    required: tuple[str, ...] = (),
    forbidden: tuple[str, ...] = (),
    counts: tuple[tuple[str, int], ...] = (),
) -> None:
    """Apply one compact required/forbidden/exact-count source contract."""
    assert [value for value in required if value not in source] == []
    assert [value for value in forbidden if value in source] == []
    assert {
        value: source.count(value) for value, count in counts if source.count(value) != count
    } == {}


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
    publish = _workflow("publish.yml")
    preamble = _workflow_preamble(publish)

    assert _job_ids(publish) == {
        "preflight",
        "validation",
        "reconcile",
        "publish",
        "verify",
        "release-gate",
    }
    assert "  workflow_dispatch:" in preamble
    for forbidden_trigger in ("workflow_call:", "push:", "pull_request:"):
        assert f"  {forbidden_trigger}" not in preamble
    assert publish.count("uses: ./.github/workflows/ci.yml") == 1
    assert "needs: [preflight]" in _job_body(publish, "validation")
    assert "needs: [preflight, validation]" in _job_body(publish, "reconcile")
    assert "needs: [reconcile]" in _job_body(publish, "publish")
    assert "needs: [reconcile, publish]" in _job_body(publish, "verify")
    assert "needs: [reconcile, publish, verify]" in _job_body(publish, "release-gate")
    assert "python -m cibuildwheel" not in publish
    assert "python -m build" not in publish
    assert (
        _action("source-distribution").count("python meta/ci/release/validate_release_version.py")
        == 1
    )


def test_publish_request_is_explicit_and_always_targets_pypi() -> None:
    """A manual release cannot silently become a dry-run or TestPyPI run."""
    publish = _workflow("publish.yml")
    preamble = _workflow_preamble(publish)
    preflight = _job_body(publish, "preflight")
    publisher = _job_body(publish, "publish")

    assert set(re.findall(r"^      ([a-z0-9_]+):$", preamble, re.MULTILINE)) == {
        "release_version",
        "confirm_publish",
    }
    for input_name in ("release_version", "confirm_publish"):
        input_body = preamble.split(f"      {input_name}:\n", 1)[1]
        next_input = re.search(r"^      [a-z0-9_]+:$", input_body, re.MULTILINE)
        if next_input is not None:
            input_body = input_body[: next_input.start()]
        assert "required: true" in input_body
    assert "--require-release-version" in preflight
    assert "--require-publish-confirmation" in preflight
    assert "python meta/ci/release/check_pypi_version.py" in preflight
    assert "python meta/ci/release/check_github_release_state.py" in preflight
    assert "--expected-main-sha" in preflight
    assert '"${GITHUB_SHA}"' in preflight
    assert publish.count("pypa/gh-action-pypi-publish@") == 1
    assert "skip-existing: true" in publisher
    publish_step = next(
        step for step in _step_bodies(publisher) if "pypa/gh-action-pypi-publish@" in step
    )
    assert "if:" not in publish_step


def test_publish_serializes_every_production_version_in_one_group() -> None:
    """Differently typed release requests cannot race PyPI publication."""
    preamble = _workflow_preamble(_workflow("publish.yml"))

    concurrency = preamble.split("concurrency:\n", 1)[1]
    assert re.search(r"^  group: pypi-production$", concurrency, re.MULTILINE)
    assert re.search(r"^  cancel-in-progress: false$", concurrency, re.MULTILINE)
    assert "inputs.release_version" not in concurrency


def test_ci_cancels_only_superseded_pull_request_runs() -> None:
    """Only PRs share a key; main and reusable runs keep unique run identifiers."""
    preamble = _workflow_preamble(_workflow("ci.yml"))
    concurrency = preamble.split("concurrency:\n", 1)[1]

    assert "github.workflow" in concurrency
    assert (
        "github.event_name == 'pull_request' && github.event.pull_request.number || github.run_id"
        in " ".join(concurrency.split())
    )
    assert "github.ref" not in concurrency
    assert re.search(
        r"^  cancel-in-progress: \$\{\{ github\.event_name == 'pull_request' \}\}$",
        concurrency,
        re.MULTILINE,
    )


def test_oidc_publisher_is_a_code_free_least_privilege_boundary() -> None:
    """Only exact artifact handling crosses the isolated PyPI trust boundary."""
    publisher = _job_body(_workflow("publish.yml"), "publish")

    assert "environment:" not in publisher
    assert "id-token: write" in publisher
    for unnecessary_permission in ("contents: read", "contents: write", "actions: write"):
        assert unnecessary_permission not in publisher
    assert "actions/download-artifact@" in publisher
    assert "name: pypi-publish-distributions" in publisher
    assert "pattern:" not in publisher
    assert "packages-dir: pypi-publish/" in publisher
    assert "needs.reconcile.outputs.publish_required == 'true'" in publisher
    assert "actions/checkout@" not in publisher
    assert "actions/setup-python@" not in publisher
    run_steps = [
        step for step in _step_bodies(publisher) if re.search(r"^\s+run:", step, re.MULTILINE)
    ]
    assert len(run_steps) == 1
    assert "name: Reset a partial missing-package download" in run_steps[0]
    assert "rm -rf -- pypi-publish" in run_steps[0]
    assert "${{" not in run_steps[0].split("run:", 1)[1]
    assert "python " not in publisher


def test_manual_publisher_retries_only_after_cleaning_its_exact_download_path() -> None:
    """One transient artifact read cannot abort publication or reuse partial bytes."""
    publisher = _job_body(_workflow("publish.yml"), "publish")

    assert publisher.count("actions/download-artifact@") == 2
    assert publisher.count("name: pypi-publish-distributions") == 2
    assert publisher.count("path: pypi-publish") == 2
    first = publisher.index("id: download-pypi-publish-distributions")
    reset = publisher.index("name: Reset a partial missing-package download")
    retry = publisher.index("name: Retry the exact missing-package download")
    publish = publisher.index("name: Publish to PyPI with Trusted Publishing")
    assert "continue-on-error: true" in publisher[first:reset]
    assert "rm -rf -- pypi-publish" in publisher[reset:retry]
    assert first < reset < retry < publish


def test_download_retries_require_their_exact_destination_reset_to_succeed() -> None:
    """A failed cleanup cannot let a retry consume partial artifact bytes."""
    workflow = _workflow("publish.yml")
    owners = (
        (
            _action("test-platform-wheel"),
            "download-platform-wheel",
            "reset-platform-wheel-download",
        ),
        (
            _job_body(_workflow("ci.yml"), "validation-gate"),
            "download-source-distribution",
            "reset-source-distribution-download",
        ),
        (
            _job_body(_workflow("ci.yml"), "validation-gate"),
            "download-platform-wheels",
            "reset-platform-wheel-downloads",
        ),
        (
            _job_body(workflow, "reconcile"),
            "download-release-for-reconciliation",
            "reset-release-for-reconciliation",
        ),
        (
            _job_body(workflow, "verify"),
            "download-release-for-verification",
            "reset-release-for-verification",
        ),
    )

    for owner, download_id, reset_id in owners:
        assert f"id: {reset_id}" in owner
        retry_condition = (
            f"steps.{download_id}.outcome == 'failure' && steps.{reset_id}.outcome == 'success'"
        )
        assert retry_condition in " ".join(owner.split())


def test_non_oidc_reconciliation_stages_only_manifest_verified_missing_packages() -> None:
    """Repository code reconciles partial uploads before the isolated publisher gets OIDC."""
    workflow = _workflow("publish.yml")
    preflight = _job_body(workflow, "preflight")
    reconcile = _job_body(workflow, "reconcile")
    publisher = _job_body(workflow, "publish")

    assert "--allow-existing-for-recovery" in preflight
    assert "id-token:" not in reconcile
    assert "contents: read" in reconcile
    assert "actions/checkout@" in reconcile
    assert "persist-credentials: false" in reconcile
    assert reconcile.count("actions/download-artifact@") == 2
    assert reconcile.count("name: release-distributions") == 2
    assert "rm -rf -- release pypi-publish" in reconcile
    for argument in (
        "--manifest release/release-manifest.json",
        "--packages-dir release/packages",
        "--publish-dir pypi-publish",
        "--state-output pypi-recovery-state.json",
        '--github-output "${GITHUB_OUTPUT}"',
    ):
        assert argument in reconcile
    for output in ("missing-count", "published-count", "publish-required", "status"):
        assert output in reconcile
    assert reconcile.count("actions/upload-artifact@") == 2
    assert reconcile.count("name: pypi-publish-distributions") == 2
    assert "steps.reconcile_pypi_state.outputs['publish-required'] == 'true'" in reconcile
    assert "overwrite: true" in reconcile
    assert "check_github_release_state.py" in reconcile
    assert '--expected-main-sha "${GITHUB_SHA}"' in reconcile
    assert "meta/ci/" not in publisher
    assert "actions/checkout@" not in publisher


def test_publish_postcondition_revalidates_every_original_manifest_digest() -> None:
    """Every reconciled path reaches a no-OIDC digest and provenance postcheck."""
    workflow = _workflow("publish.yml")
    verifier = _job_body(workflow, "verify")

    assert "id-token:" not in verifier
    assert "contents: read" in verifier
    assert "always()" in verifier
    assert "needs.reconcile.result == 'success'" in verifier
    assert "needs.publish.result" not in verifier
    assert "timeout-minutes: 75" in verifier
    assert "python-version: 3.13.15" in verifier
    assert "meta/ci/requirements/release-verification.txt" in verifier
    assert "python meta/ci/quality/ensure_pinned_pip.py" in verifier
    assert "python -m pip install --no-deps 'pip==26.2.1'" not in verifier
    assert "python -m pip install --no-deps" in verifier
    assert "python -m pip check" in verifier
    assert "pypi_attestations.__version__" in verifier
    assert verifier.count("actions/download-artifact@") == 2
    assert verifier.count("name: release-distributions") == 2
    assert "rm -rf -- release" in verifier
    assert "--manifest release/release-manifest.json" in verifier
    assert "--packages-dir release/packages" in verifier
    assert "--require-complete" in verifier


def test_release_terminal_gate_requires_the_exact_publish_transition() -> None:
    """The unconditional final status rejects every ambiguous dependency outcome."""
    gate = _job_body(_workflow("publish.yml"), "release-gate")

    assert "needs: [reconcile, publish, verify]" in gate
    assert "if: ${{ always() }}" in gate
    assert "permissions: {}" in gate
    assert "needs.reconcile.outputs.publish_required" in gate
    assert "needs.reconcile.result" in gate
    assert "needs.publish.result" in gate
    assert "needs.verify.result" in gate
    assert "true) expected_publish_result=success" in gate
    assert "false) expected_publish_result=skipped" in gate
    assert '"${RECONCILE_RESULT}" != "success"' in gate
    assert '"${PUBLISH_RESULT}" != "${expected_publish_result}"' in gate
    assert '"${VERIFY_RESULT}" != "success"' in gate
    assert "actions/checkout@" not in gate
    assert "id-token:" not in gate


def test_release_preflight_has_only_the_read_permissions_it_uses() -> None:
    """Version and remote-main validation cannot mutate repository state."""
    preflight = _job_body(_workflow("publish.yml"), "preflight")
    permissions = preflight.split("    permissions:\n", 1)[1].split("    steps:\n", 1)[0]

    assert permissions == "      contents: read\n"
    assert "id-token:" not in preflight
    assert "actions/setup-python@" in preflight
    assert "python-version: 3.11.9" in preflight


def test_external_actions_are_pinned_to_immutable_commits() -> None:
    """Every third-party action uses a full commit SHA; local reuse is exempt."""
    definitions = _ci_yaml_definitions()
    refs = [
        ref
        for _path, definition in definitions
        for ref in re.findall(r"^\s*(?:-\s+)?uses:\s*([^\s#]+)", definition, re.MULTILINE)
    ]

    assert refs
    local_refs = [ref for ref in refs if ref.startswith("./")]
    assert local_refs.count("./.github/workflows/ci.yml") == 1
    assert local_refs.count("./.github/actions/build-platform-wheel") == 1
    assert local_refs.count("./.github/actions/restore-pip-cache") == 6
    assert local_refs.count("./.github/actions/test-platform-wheel") == 4
    for name in VALIDATION_ACTIONS:
        assert local_refs.count(f"./.github/actions/{name}") == 1
    assert set(local_refs) == {
        "./.github/workflows/ci.yml",
        "./.github/actions/build-platform-wheel",
        "./.github/actions/restore-pip-cache",
        "./.github/actions/test-platform-wheel",
        *(f"./.github/actions/{name}" for name in VALIDATION_ACTIONS),
    }
    external_refs = [ref for ref in refs if not ref.startswith("./")]
    assert external_refs
    assert all(re.fullmatch(r"[^@]+@[0-9a-f]{40}", ref) for ref in external_refs)
    assert set(external_refs) == {
        "actions/checkout@3d3c42e5aac5ba805825da76410c181273ba90b1",
        "actions/cache@55cc8345863c7cc4c66a329aec7e433d2d1c52a9",
        "actions/setup-python@5fda3b95a4ea91299a34e894583c3862153e4b97",
        "actions/upload-artifact@043fb46d1a93c77aae656e7c1c64a875d1fc6a0a",
        "actions/download-artifact@3e5f45b2cfb9172054b4087a40e8e0b5a5461e7c",
        "pypa/gh-action-pypi-publish@dc37677b2e1c63e2034f94d8a5b11f265b73ba33",
    }

    checkout_steps = [
        step
        for _path, definition in definitions
        for step in _step_bodies(definition)
        if "actions/checkout@" in step
    ]
    assert checkout_steps
    assert all(_with_value(step, "persist-credentials") == "false" for step in checkout_steps)


def test_ci_run_steps_use_explicit_bash_and_strict_literal_blocks() -> None:
    """Repository commands keep one shell and fail on errors, unset names, and pipelines."""
    for path, definition in _ci_yaml_definitions():
        for step in _step_bodies(definition):
            if not re.search(r"^\s+run:", step, re.MULTILINE):
                continue
            assert re.search(r"^\s+shell: bash$", step, re.MULTILINE), path
            if re.search(r"^\s+run:\s+\|[-+]?\s*$", step, re.MULTILINE):
                assert re.search(
                    r"^\s+run:\s+\|[-+]?\s*\n\s+set -euo pipefail$",
                    step,
                    re.MULTILINE,
                ), path


def test_workflows_normalize_cross_platform_python_process_state() -> None:
    """Hash iteration, text decoding, and timezone behavior stay stable across runners."""
    for name in ("ci.yml", "publish.yml"):
        preamble = _workflow_preamble(_workflow(name))
        for setting in ("PYTHONHASHSEED: '0'", "PYTHONUTF8: '1'", "TZ: UTC"):
            assert setting in preamble
        assert "LC_ALL:" not in preamble


def test_action_pins_have_automated_review_and_semantic_security_gates() -> None:
    """Immutable Actions remain maintainable and workflow-aware tooling blocks drift."""
    dependabot = (ROOT / ".github/dependabot.yml").read_text(encoding="utf-8")
    precommit = (ROOT / ".pre-commit-config.yaml").read_text(encoding="utf-8")

    assert "package-ecosystem: github-actions" in dependabot
    assert "interval: weekly" in dependabot
    assert re.search(r'^      time: "07:00"$', dependabot, re.MULTILINE)
    assert "id: actionlint" in precommit
    assert "actionlint-py==1.7.12.24" in precommit
    assert "id: zizmor" in precommit
    assert "zizmor==1.29.0" in precommit
    assert r"files: ^\.github/workflows/.*\.ya?ml$" in precommit
    assert r"files: ^\.github/(workflows/.*\.ya?ml|actions/.*/action\.ya?ml)$" in precommit
    assert r"exclude: ^\.github/dependabot\.yml$" in precommit
    assert "      - id: ruff-check\n" in precommit
    assert "      - id: ruff\n" not in precommit

    remote_hooks = dict(
        re.findall(
            r"^  - repo: (https://[^\n]+)\n    rev: ([^\s#]+)",
            precommit,
            re.MULTILINE,
        )
    )
    assert set(remote_hooks) == {
        "https://github.com/pre-commit/pre-commit-hooks",
        "https://github.com/pre-commit/mirrors-clang-format",
        "https://github.com/astral-sh/ruff-pre-commit",
        "https://github.com/executablebooks/mdformat",
    }
    assert all(re.fullmatch(r"[0-9a-f]{40}", revision) for revision in remote_hooks.values())


def test_shell_tools_use_isolated_prebuilt_wheels_without_remote_build_hooks() -> None:
    """Cold shell checks must not require system tools or release-asset downloads."""
    precommit = (ROOT / ".pre-commit-config.yaml").read_text(encoding="utf-8")
    pyproject = tomllib.loads((ROOT / "pyproject.toml").read_text(encoding="utf-8"))
    install_step = next(
        step
        for step in _step_bodies(_action("quality-validation"))
        if "name: Install development and audit tools" in step
    )

    for requirement in ("shellcheck-py==0.11.0.1", "shfmt-py==4.0.0"):
        assert requirement not in pyproject["project"]["optional-dependencies"]["dev"]
        assert f"--only-binary={requirement.split('==', 1)[0]}" not in install_step

    assert re.search(
        r"^      - id: shellcheck\n"
        r"        name: shellcheck\n"
        r"        entry: shellcheck\n"
        r"        language: python\n"
        r"        additional_dependencies: \[shellcheck-py==0\.11\.0\.1\]\n"
        r"        files: \\.sh\$$",
        precommit,
        re.MULTILINE,
    )
    assert re.search(
        r"^      - id: shfmt\n"
        r"        name: shfmt\n"
        r"        entry: shfmt\n"
        r"        language: python\n"
        r"        additional_dependencies: \[shfmt-py==4\.0\.0\]\n"
        r"        files: \\.sh\$\n"
        r"        args: \[-w, -i, '2', -ci\]$",
        precommit,
        re.MULTILINE,
    )


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
    _assert_text_contract(
        _action("quality-validation"),
        required=(
            "python meta/ci/quality/check_detect_secrets_report.py",
            ".work/audit/detect-secrets.json",
        ),
    )


def test_static_security_scan_covers_release_automation() -> None:
    """Code with release authority receives the same Bandit gate as runtime code."""
    _assert_text_contract(_action("quality-validation"), required=("bandit -r src meta/ci -ll",))


def test_dependency_audit_includes_pinned_ci_executables() -> None:
    """Every executed security tool has a compatible exact owner-lock pin."""
    from meta.ci.quality import build_dependency_audit_inputs

    for requirement in (
        "abi3audit==0.0.26",
        "actionlint-py==1.7.12.24",
        "bandit==1.9.4",
        "build==1.5.0",
        "cibuildwheel==4.2.0",
        "clang-format==22.1.8",
        "cmake==4.3.4",
        "cmakelang==0.6.13",
        "coverage==7.15.4",
        "detect-secrets==1.5.0",
        "mdformat==1.0.0",
        "mypy==1.19.1",
        "ninja==1.13.0",
        "packaging==26.3",
        "pip==26.2.1",
        "pip-audit==2.10.1",
        "polars==1.43.2",
        "pre-commit==4.6.2",
        "pre-commit-hooks==6.0.0",
        "pyarrow==25.0.1",
        "pypi-attestations==0.0.30",
        "pytest==9.1.1",
        "ruff==0.16.2",
        "scikit-build-core==0.11.6",
        "shellcheck-py==0.11.0.1",
        "shfmt-py==4.0.0",
        "toml-sort==0.24.3",
        "twine==7.0.0",
        "yamlfix==1.18.0",
        "zizmor==1.29.0",
    ):
        assert requirement in build_dependency_audit_inputs.CI_TOOLS
    build_dependency_audit_inputs.validate_owner_lock_coverage(
        ROOT / "pyproject.toml", ROOT / "meta/ci/requirements"
    )


def test_dependency_audit_keeps_conflicting_environment_locks_separate(tmp_path: Path) -> None:
    """Independent exact pins cannot be flattened into an unresolvable synthetic set."""
    from meta.ci.quality import build_dependency_audit_inputs

    pyproject = tmp_path / "pyproject.toml"
    pyproject.write_text(
        """
        [build-system]
        requires = ["builder==1"]
        [project]
        name = "audit-fixture"
        version = "1"
        dependencies = []
        """,
        encoding="utf-8",
    )
    locks = tmp_path / "locks"
    locks.mkdir()
    (locks / "first.txt").write_text("builder==1\nshared==1\n", encoding="utf-8")
    (locks / "second.txt").write_text("shared==2\n", encoding="utf-8")

    outputs = build_dependency_audit_inputs.build_audit_inputs(
        pyproject, locks, tmp_path / "audit", ci_tools=()
    )
    contents = {path.name: path.read_text(encoding="utf-8") for path in outputs}

    assert contents["locked-first.txt"] == "builder==1\nshared==1\n"
    assert contents["locked-second.txt"] == "shared==2\n"
    conflicting = {"shared==1", "shared==2"}
    assert all(not (conflicting <= set(text.splitlines())) for text in contents.values())
    quality = _action("quality-validation")
    assert 'for requirements in "${audit_inputs[@]}"' in quality
    assert 'pip-audit -r "${requirements}"' in quality
    for option in ("--no-deps", "--disable-pip", "--strict", "--progress-spinner off"):
        assert option in quality
    assert "full-requirements.txt" not in quality
    assert "declared-and-ci-tools.txt" not in contents


def test_wheel_build_runs_the_stable_abi_audit_explicitly() -> None:
    """The ABI gate stays strict without cibuildwheel's hidden venv download."""
    build_action = _action("build-platform-wheel")
    pyproject = tomllib.loads((ROOT / "pyproject.toml").read_text(encoding="utf-8"))
    cibuildwheel = pyproject["tool"]["cibuildwheel"]
    install = next(
        step for step in _step_bodies(build_action) if "name: Install wheel tooling" in step
    )
    audit = next(
        step for step in _step_bodies(build_action) if "name: Audit the CPython stable ABI" in step
    )

    assert cibuildwheel["audit-command"] == ""
    assert "abi3audit==0.0.26 cibuildwheel==4.2.0 pytest==9.1.1" in install
    assert "shell: bash" in audit
    assert "python -m abi3audit --strict --report wheelhouse/*.whl" in audit
    assert (
        build_action.index("python -m cibuildwheel")
        < build_action.index("python -m abi3audit")
        < build_action.index("name: Install the built wheel")
    )


def test_build_backend_injects_one_exact_native_toolchain() -> None:
    """Backend-managed builds pin CMake and Ninja without duplicate requirements."""
    pyproject = tomllib.loads((ROOT / "pyproject.toml").read_text(encoding="utf-8"))

    assert pyproject["build-system"]["requires"] == ["scikit-build-core==0.11.6"]
    scikit_build = pyproject["tool"]["scikit-build"]
    assert scikit_build["cmake"]["version"] == "==4.3.4"
    assert scikit_build["ninja"] == {"version": "==1.13.0", "make-fallback": False}


def test_native_linkage_certification_fails_without_an_inspection_tool() -> None:
    """Missing platform linkage tools cannot be reported as successful certification."""
    helper = (ROOT / "meta/ci/native/check_no_libarrow_linkage.sh").read_text(encoding="utf-8")
    unavailable = helper.split("else\n", 1)[1].split("fi\n", 1)[0]

    assert "ERROR: neither otool nor ldd is available; linkage cannot be certified" in unavailable
    assert "exit 2" in unavailable
    assert "OK: no libarrow/libparquet in extension linkage" not in unavailable
    assert helper.index("exit 2", helper.index("neither otool nor ldd")) < helper.index(
        "OK: no libarrow/libparquet in extension linkage"
    )


def test_validation_has_six_matrices_and_one_terminal_gate() -> None:
    """All validation work belongs to six matrices and one stable final job."""
    ci = _workflow("ci.yml")
    assert _job_ids(ci) == {
        "platform-wheel-builds",
        *(platform["job-id"] for platform in _TEST_PLATFORM_JOBS),
        "validation-matrix",
        "validation-gate",
    }
    gate = _job_body(ci, "validation-gate")
    assert "if: always()" in gate or "if: ${{ always() }}" in gate
    assert _job_needs(ci, "validation-gate") == (
        "platform-wheel-builds",
        *(platform["job-id"] for platform in _TEST_PLATFORM_JOBS),
        "validation-matrix",
    )
    assert ci.count("      matrix:") == 6
    assert "python-version: [" not in ci
    assert "uses: ./.github/workflows/" not in ci


def test_validation_matrix_has_eight_exact_workloads_and_safe_dispatch() -> None:
    """The heterogeneous matrix preserves every owner, runner, SLA, and action."""
    ci = _workflow("ci.yml")
    validation = _job_body(ci, "validation-matrix")

    assert _matrix_includes(ci, "validation-matrix") == _VALIDATION_ROWS
    assert "name: validation / ${{ matrix.name }}" in validation
    assert "runs-on: ${{ matrix.runner }}" in validation
    assert "timeout-minutes: ${{ matrix.timeout }}" in validation
    assert "fail-fast: false" in validation
    assert "python-version: ${{ matrix.python }}" in validation
    assert "VALIDATION_TASK: ${{ matrix.task }}" in validation
    assert (
        "quality|source-distribution|native-llvm-coverage|thread-sanitizer|platform-sanitizer)"
        in validation
    )
    for task in VALIDATION_ACTIONS:
        assert f"if: matrix.task == '{task.removesuffix('-validation')}'" in validation
        assert validation.count(f"uses: ./.github/actions/{task}") == 1
    assert (
        "uses: ./.github/actions/quality-validation\n"
        "        with:\n"
        "          python-version: ${{ matrix.python }}" in validation
    )
    assert "sanitizer: ${{ matrix.sanitizer }}" in validation
    assert "mode: ${{ matrix.mode }}" in validation


def test_platform_suite_exercises_the_installed_wheel() -> None:
    """The functional suite retains the wheel's platform-specific bootstrap."""
    _assert_text_contract(
        _action("test-platform-wheel"),
        required=(
            "pytest -q -o pythonpath=.",
            "wheelhouse/*.whl",
            "extension.is_relative_to(checkout)",
        ),
    )


def test_platform_matrix_uses_one_pinned_python_and_dependency_set() -> None:
    """All release wheels are compared with the same Python and adapters."""
    ci = _workflow("ci.yml")
    build_action = _action("build-platform-wheel")
    test_action = _action("test-platform-wheel")
    test_requirements_path = ROOT / "meta/ci/requirements/platform-tests.txt"
    test_requirements = test_requirements_path.read_text(encoding="utf-8").splitlines()
    cibuildwheel = tomllib.loads((ROOT / "pyproject.toml").read_text(encoding="utf-8"))["tool"][
        "cibuildwheel"
    ]
    dependencies = [
        step
        for step in _step_bodies(test_action)
        if "name: Install the wheel and functional-suite dependencies" in step
    ]

    assert build_action.count("python-version: 3.11.9") == 1
    assert test_action.count("python-version: 3.11.9") == 1
    assert build_action.count("python -m cibuildwheel") == 1
    assert build_action.count("cache: pip") == 1
    assert build_action.count("cache-dependency-path: |-") == 1
    assert build_action.count("name: dist-wheels-${{ inputs.platform-name }}") == 2
    assert test_action.count("actions/download-artifact@") == 2
    assert test_action.count("name: dist-wheels-${{ inputs.platform-name }}") == 2
    assert "id: download-platform-wheel" in test_action
    assert "continue-on-error: true" in test_action
    assert "rm -rf -- wheelhouse" in test_action
    assert test_action.count("steps.download-platform-wheel.outcome == 'failure'") == 2
    assert "python -m cibuildwheel" not in test_action
    assert len(dependencies) == 1
    assert "if:" not in dependencies[0]
    assert "--retries 10" in dependencies[0]
    assert "--timeout 60" in dependencies[0]
    assert "--no-deps" in dependencies[0]
    assert "--only-binary=:all:" in dependencies[0]
    assert "-r meta/ci/requirements/platform-tests.txt" in dependencies[0]
    assert "python -m pip check" in dependencies[0]
    assert "meta/ci/requirements/build-tools.txt" not in test_action
    assert "meta/ci/requirements/platform-tests.txt" in test_action
    assert cibuildwheel["test-requires"] == ["pyarrow==25.0.1"]
    assert cibuildwheel["dependency-versions"] == "meta/ci/requirements/build-tools.txt"
    assert cibuildwheel["build-verbosity"] == 0
    assert _exact_lock_names(test_requirements_path) == _PLATFORM_LOCK_NAMES
    assert len(test_requirements) == len(_PLATFORM_LOCK_NAMES)
    parsed_requirements = {
        canonicalize_name(requirement.name): requirement
        for requirement in map(Requirement, test_requirements)
    }
    assert (
        str(parsed_requirements[canonicalize_name("colorama")].marker) == 'sys_platform == "win32"'
    )
    assert all(
        requirement not in dependencies[0]
        for requirement in test_requirements
        if not requirement.startswith("pip==")
    )
    build = _job_body(ci, "platform-wheel-builds")

    assert _matrix_includes(ci, "platform-wheel-builds") == _BUILD_PLATFORMS
    assert "name: wheel / ${{ matrix['display-name'] }}" in build
    assert "runs-on: ${{ matrix.runner }}" in build
    assert "fail-fast: false" in build
    assert build.count("uses: ./.github/actions/build-platform-wheel") == 1
    assert "platform-name: ${{ matrix['platform-name'] }}" in build
    assert "artifact: ${{ matrix.artifact }}" in build
    assert "arch: ${{ matrix.arch }}" in build

    for platform in _TEST_PLATFORM_JOBS:
        job_id = platform["job-id"]
        tests = _job_body(ci, job_id)
        assert not re.search(r"^    name:", tests, re.MULTILINE)
        assert "if: ${{ !cancelled() }}" in tests
        assert _job_needs(ci, job_id) == ("platform-wheel-builds",)
        assert f"runs-on: {platform['runner']}" in tests
        assert "fail-fast: false" in tests
        assert _matrix_axis(ci, job_id, "shard") == _TEST_SHARDS
        assert tests.count("uses: ./.github/actions/test-platform-wheel") == 1
        assert f"platform-name: {platform['platform-name']}" in tests
        assert f"artifact: {platform['artifact']}" in tests
        assert f"minimum-cpu-capacity: {platform['minimum-cpu-capacity']}" in tests
        assert "shard: ${{ matrix.shard }}" in tests
        assert "matrix['display-name']" not in tests
    assert ci.count("uses: ./.github/actions/test-platform-wheel") == 4


def test_ci_bootstraps_pip_conditionally_from_the_exact_wheel() -> None:
    """Every runner avoids a no-op install but repairs pin drift with a binary wheel."""
    helper_path = "meta/ci/quality/ensure_pinned_pip.py"
    helper = (ROOT / helper_path).read_text(encoding="utf-8")
    callers = {
        "build-platform-wheel": (_action("build-platform-wheel"), 4),
        "test-platform-wheel": (_action("test-platform-wheel"), 1),
        "quality-validation": (_action("quality-validation"), 1),
        "source-distribution": (_action("source-distribution"), 1),
        "native-llvm-coverage": (_action("native-llvm-coverage"), 1),
        "thread-sanitizer": (_action("thread-sanitizer"), 1),
        "platform-sanitizer": (_action("platform-sanitizer"), 1),
        "validation-gate": (_job_body(_workflow("ci.yml"), "validation-gate"), 1),
        "publish-verifier": (_job_body(_workflow("publish.yml"), "verify"), 1),
    }

    for name, (definition, expected_count) in callers.items():
        assert definition.count(f"python {helper_path}") == expected_count, name
        assert "python -m pip install pip==" not in definition, name
        assert "python -m pip install --no-deps 'pip==" not in definition, name
    assert 'PIP_VERSION = "26.2.1"' in helper
    assert '"--no-deps"' in helper
    assert '"--only-binary=:all:"' in helper
    assert "os.spawnv(os.P_WAIT" in helper
    assert "pip bootstrap postcondition failed" in helper


def test_packaging_gate_uses_one_exact_runner_native_python() -> None:
    """The packaging-only tail keeps setup and cache on exact Python 3.13.15."""
    gate = _job_body(_workflow("ci.yml"), "validation-gate")

    assert gate.count("python-version: 3.13.15") == 2
    assert "python-version: 3.11.9" not in gate


def test_windows_wheel_uses_one_certified_v143_toolchain_and_runtime() -> None:
    """Windows release builds reject compiler or redistributable path drift."""
    cibuildwheel = tomllib.loads((ROOT / "pyproject.toml").read_text(encoding="utf-8"))["tool"][
        "cibuildwheel"
    ]
    windows = cibuildwheel["windows"]
    build = _action("build-platform-wheel")
    sanitizer = _action("platform-sanitizer")

    assert windows["environment"] == {
        "CMAKE_BUILD_PARALLEL_LEVEL": "1",
        "CMAKE_GENERATOR": "Visual Studio 17 2022",
        "CMAKE_GENERATOR_INSTANCE": ("C:/Program Files/Microsoft Visual Studio/2022/Enterprise"),
        "CMAKE_GENERATOR_PLATFORM": "x64",
        "CMAKE_GENERATOR_TOOLSET": "v143,host=x64",
    }
    assert windows["repair-wheel-command"] == (
        "python -W error -m delvewheel repair --extract-dir "
        "{project}/.work/delvewheel-extract --add-path "
        "{project}/.work/msvc-redist -w {dest_dir} -v {wheel}"
    )
    assert "Microsoft.VCRedistVersion.default.txt" in build
    assert "Microsoft.VC143.CRT" in build
    assert "name: Install the verified Windows CPython package" in build
    python_cache = next(
        step
        for step in _step_bodies(build)
        if "name: Restore the verified Windows CPython package" in step
    )
    assert "if: runner.os == 'Windows'" in python_cache
    assert "continue-on-error: true" in python_cache
    assert "actions/cache@55cc8345863c7cc4c66a329aec7e433d2d1c52a9" in python_cache
    assert "restore-keys:" not in python_cache
    for component in (
        "${{ runner.os }}",
        "${{ runner.arch }}",
        "python-3.11.9",
        "cibuildwheel-4.2.0",
    ):
        assert component in " ".join(python_cache.split())
    assert build.index("name: Reset the owned Windows CPython package-cache path") < build.index(
        "name: Restore the verified Windows CPython package"
    )
    assert 'target = cache_root / "python.3.11.9.nupkg"' in build
    assert "target.is_symlink() or target.is_file()" in build
    python_sha256 = "9283876d58c017e0e846f95b490da3bca0fc0a6ee1134b2870677cfb7eec3c67"  # pragma: allowlist secret
    assert python_sha256 in build
    assert 'python meta/ci/native/install_windows_cpython.py "${PYTHON_NUPKG_SHA256}"' in build
    assert "platform.machine().upper() == 'AMD64'" in build
    assert "assert struct.calcsize('P') == 8" in build
    assert "assert sys.version_info[:3] == (3, 11, 9)" in build
    installer = (ROOT / "meta/ci/native/install_windows_cpython.py").read_text(encoding="utf-8")
    assert "https://api.nuget.org/v3-flatcontainer/python/3.11.9/" in installer
    assert "from cibuildwheel.util.file import CIBW_CACHE_PATH" in installer
    assert "zipfile.ZipFile" in installer
    assert "duplicate or case-colliding archive member" in installer
    assert "archive file is also a member parent" in installer
    assert 'struct.unpack("<H", machine_bytes)[0] != 0x8664' in installer
    assert "os.replace" in installer
    assert "extractall" not in installer
    assert "nuget.exe" not in installer.casefold()
    assert "NUGET_SHA256" not in build
    assert "https://dist.nuget.org/" not in build
    assert "https://dist.nuget.org/win-x86-commandline/latest/nuget.exe" not in build
    assert "grep -Fq -- 'win-x86-commandline/latest/nuget.exe'" in build
    assert build.index("name: Restore the verified Windows CPython package") < build.index(
        "name: Install the verified Windows CPython package"
    )
    assert build.index("name: Install the verified Windows CPython package") < build.index(
        "python -m cibuildwheel"
    )
    assert "grep -Fq -- 'nuget.exe install python'" in build
    assert "grep -Fq -- '-FallbackSource'" in build
    assert "rm -rf -- .work/msvc-redist" in build
    assert 'cp -- "${redist_directory}"/*.dll .work/msvc-redist/' in build
    assert "rm -rf -- wheelhouse .work/build .work/delvewheel-extract" in build
    assert "2>&1 | tee .work/cibuildwheel.log" in build
    assert "grep -Fq -- 'Exception ignored in:' .work/cibuildwheel.log" in build
    assert (
        build.index("python -m cibuildwheel")
        < build.index("name: Certify the Windows release toolchain")
        < build.index("python -m abi3audit")
    )
    assert "shopt -s nullglob" in build
    assert "caches=(.work/build/*/CMakeCache.txt)" in build
    assert 'cache="${caches[0]}"' in build
    assert '[[ ! -f "${cache}" || -L "${cache}" ]]' in build
    assert "find .work/build -type f -name CMakeCache.txt" not in build
    for cache_entry in (
        "CMAKE_PROJECT_NAME:STATIC=schema_sanitizer",
        "CMAKE_GENERATOR:INTERNAL=Visual Studio 17 2022",
        "CMAKE_GENERATOR_INSTANCE:INTERNAL=C:/Program Files/Microsoft Visual Studio/2022/Enterprise",
        "CMAKE_GENERATOR_PLATFORM:INTERNAL=x64",
        "CMAKE_GENERATOR_TOOLSET:INTERNAL=v143,host=x64",
    ):
        assert cache_entry in build
    assert '-G "Visual Studio 17 2022" -A x64 -T v143,host=x64' in sanitizer


def test_composite_actions_reject_unknown_platform_and_sanitizer_tuples() -> None:
    """Misspelled matrix inputs cannot silently skip platform-specific validation."""
    build = _action("build-platform-wheel")
    test = _action("test-platform-wheel")
    sanitizer = _action("platform-sanitizer")

    for source, guard, tuples in (
        (
            build,
            "Require a supported build-platform tuple",
            (
                "Linux:X64:linux-x86_64:linux:x86_64",
                "Windows:X64:windows-amd64:windows:AMD64",
                "macOS:X64:macos-x86_64:macos-x86_64:x86_64",
                "macOS:ARM64:macos-arm64:macos-arm64:arm64",
            ),
        ),
        (
            test,
            "Require a supported platform-test tuple",
            (
                "Linux:X64:linux-x86_64:linux:4",
                "Windows:X64:windows-amd64:windows:4",
                "macOS:X64:macos-x86_64:macos-x86_64:4",
                "macOS:ARM64:macos-arm64:macos-arm64:3",
            ),
        ),
        (
            sanitizer,
            "Require a supported sanitizer tuple",
            (
                "Linux:X64:linux-full:asan-ubsan",
                "Windows:X64:native:asan",
                "macOS:X64:native:asan-ubsan",
                "macOS:ARM64:native:asan-ubsan",
            ),
        ),
    ):
        guard_step = next(step for step in _step_bodies(source) if f"name: {guard}" in step)
        assert "exit 2" in guard_step
        assert "RUNNER_ARCHITECTURE: ${{ runner.arch }}" in guard_step
        assert all(value in guard_step for value in tuples)
        assert guard in _step_bodies(source)[0]


def test_generated_ci_destinations_are_reset_before_their_first_write() -> None:
    """Repeated composite invocation cannot mix prior generated output into evidence."""
    build = _action("build-platform-wheel")
    test = _action("test-platform-wheel")
    source = _action("source-distribution")
    native = _action("native-llvm-coverage")
    sanitizer = _action("platform-sanitizer")
    tsan = _action("thread-sanitizer")
    gate = _job_body(_workflow("ci.yml"), "validation-gate")

    assert build.index("rm -rf -- wheelhouse .work/build") < build.index("python -m cibuildwheel")
    assert test.index("rm -rf -- wheelhouse artifacts") < test.index("actions/download-artifact@")
    assert source.index("rm -rf -- dist downstream-wheel") < source.index("python -m build --sdist")
    assert native.index("rm -rf -- coverage-native") < native.index(
        "SCHEMA_SANITIZER_COVERAGE_PROFILE_PATTERN"
    )
    assert sanitizer.index("rm -rf -- .work/build .work/bin/python-asan") < sanitizer.index(
        "cmake -S . -B .work/build/platform-sanitizer"
    )
    assert tsan.index("rm -rf -- .work/build/tsan") < tsan.index("cmake -S . -B .work/build/tsan")
    assert gate.index("rm -rf -- download dist release") < gate.index("actions/download-artifact@")


def test_native_coverage_resolves_sources_and_rejects_incomplete_html() -> None:
    """Reproducible source mappings must render completely or fail the native job."""
    render = next(
        step
        for step in _step_bodies(_action("native-llvm-coverage"))
        if "name: Render LLVM reports" in step
    )

    assert render.count('-compilation-dir="${GITHUB_WORKSPACE}"') == 3
    assert "render_diagnostics=coverage-native/llvm-cov-render.stderr" in render
    assert '2>"${render_diagnostics}"' in render
    assert 'if [[ -s "${render_diagnostics}" ]]' in render
    assert 'cat "${render_diagnostics}" >&2' in render
    assert "test -s coverage-native/html/index.html" in render


def test_quality_requirements_are_a_complete_exact_lock() -> None:
    """The quality runner cannot acquire direct or transitive dependencies implicitly."""
    quality = _action("quality-validation")

    assert _exact_lock_names(ROOT / "meta/ci/requirements/quality.txt") == _QUALITY_LOCK_NAMES
    assert "python -m pip install --no-deps --only-binary=:all:" in quality
    assert "-r meta/ci/requirements/quality.txt" in quality
    assert "python -m pip install --no-deps '.[dev]'" in quality
    assert "python -m pip check" in quality


def test_build_and_hook_requirements_are_complete_exact_owner_locks() -> None:
    """Build isolation and independent hook bootstraps cannot resolve transitive drift."""
    pyproject = tomllib.loads((ROOT / "pyproject.toml").read_text(encoding="utf-8"))
    includes = pyproject["tool"]["scikit-build"]["sdist"]["include"]
    locks = {
        "build-tools.txt": _BUILD_TOOL_LOCK_NAMES,
        "pre-commit-hooks.txt": _PRE_COMMIT_HOOK_LOCK_NAMES,
    }

    for filename, expected in locks.items():
        path = ROOT / "meta/ci/requirements" / filename
        assert _exact_lock_names(path) == expected
        names = [
            canonicalize_name(Requirement(line).name)
            for line in path.read_text(encoding="utf-8").splitlines()
        ]
        assert names == sorted(names)
        assert f"/meta/ci/requirements/{filename}" in includes


def test_every_installer_uses_its_exact_runtime_and_build_owner_lock() -> None:
    """Pip subprocesses and isolated builders inherit an absolute applicable lock."""
    build = _action("build-platform-wheel")
    source = _action("source-distribution")
    native = _action("native-llvm-coverage")
    sanitizer = _action("platform-sanitizer")
    tsan = _action("thread-sanitizer")
    quality = _action("quality-validation")
    tests = _action("test-platform-wheel")
    gate = _job_body(_workflow("ci.yml"), "validation-gate")
    host_build = "${{ github.workspace }}/meta/ci/requirements/build-tools.txt"

    for owner in (source, sanitizer, tsan, gate):
        assert f"PIP_BUILD_CONSTRAINT: {host_build}" in owner
        assert f"PIP_CONSTRAINT: {host_build}" in owner
    native_prerequisites = next(
        step for step in _step_bodies(native) if "Install native coverage prerequisites" in step
    )
    native_build = next(
        step for step in _step_bodies(native) if "Build and install instrumented package" in step
    )
    assert f"PIP_BUILD_CONSTRAINT: {host_build}" in native_prerequisites
    assert f"PIP_CONSTRAINT: {host_build}" in native_prerequisites
    assert f"PIP_BUILD_CONSTRAINT: {host_build}" not in native_build
    assert f"PIP_CONSTRAINT: {host_build}" in native_build
    assert native.index(native_prerequisites) < native.index(native_build)
    assert f"PIP_BUILD_CONSTRAINT: {host_build}" in build
    assert f"PIP_CONSTRAINT: {host_build}" in build
    wheel_build = next(step for step in _step_bodies(build) if "python -m cibuildwheel" in step)
    assert f"PIP_BUILD_CONSTRAINT: {host_build}" in wheel_build
    assert f"PIP_CONSTRAINT: {host_build}" in wheel_build
    assert "CIBW_ENVIRONMENT: >-" not in build
    linux_environment = build.split("CIBW_ENVIRONMENT_LINUX: >-", 1)[1].split(
        "        # cibuildwheel Linux", 1
    )[0]
    assert linux_environment.count("/project/meta/ci/requirements/build-tools.txt") == 2
    assert "github.workspace" not in linux_environment
    assert "PIP_CONSTRAINT: ${{ github.workspace }}/meta/ci/requirements/quality.txt" in quality
    assert (
        quality.count(
            "PIP_CONSTRAINT: ${{ github.workspace }}/meta/ci/requirements/pre-commit-hooks.txt"
        )
        == 1
    )
    assert (
        quality.count(
            "PIP_BUILD_CONSTRAINT: ${{ github.workspace }}/meta/ci/requirements/pre-commit-hooks.txt"
        )
        == 1
    )
    for setting in (
        "VIRTUALENV_NO_PERIODIC_UPDATE: '1'",
        "VIRTUALENV_PIP: 26.2.1",
        "VIRTUALENV_SETUPTOOLS: 84.0.0",
    ):
        assert setting in quality
    assert (
        "PIP_CONSTRAINT: ${{ github.workspace }}/meta/ci/requirements/platform-tests.txt" in tests
    )


def test_no_build_isolation_never_receives_a_build_constraint() -> None:
    """No direct, inherited, or persistent scope can poison non-isolated pip builds."""
    definitions = {
        path.relative_to(ROOT).as_posix(): definition for path, definition in _ci_yaml_definitions()
    }
    owners = {
        path
        for path, definition in definitions.items()
        if any("--no-build-isolation" in step for step in _step_bodies(definition))
    }

    assert owners == {
        ".github/actions/native-llvm-coverage/action.yml",
        ".github/actions/platform-sanitizer/action.yml",
    }
    assert _no_isolation_constraint_violations(definitions) == ()


def test_no_build_isolation_constraint_guard_detects_every_inherited_scope() -> None:
    """Mutation cases prove the non-isolated build guard cannot pass by lucky placement."""
    definitions = {
        path.relative_to(ROOT).as_posix(): definition for path, definition in _ci_yaml_definitions()
    }
    action_path = ".github/actions/native-llvm-coverage/action.yml"
    cache_action_path = ".github/actions/restore-pip-cache/action.yml"
    workflow_path = ".github/workflows/ci.yml"
    mutations = (
        (
            action_path,
            "    - name: Build and install instrumented package\n      env:\n",
            "    - name: Build and install instrumented package\n"
            "      env:\n"
            "        PIP_BUILD_CONSTRAINT: /tmp/build.txt\n",
        ),
        (
            action_path,
            "        set -euo pipefail\n        rm -rf -- coverage-native .work/build\n",
            "        set -euo pipefail\n"
            "        export PIP_BUILD_CONSTRAINT=/tmp/build.txt\n"
            "        rm -rf -- coverage-native .work/build\n",
        ),
        (
            workflow_path,
            "env:\n  # All Python package downloads",
            "env:\n  PIP_BUILD_CONSTRAINT: /tmp/build.txt\n  # All Python package downloads",
        ),
        (
            workflow_path,
            "  validation-matrix:\n    name:",
            "  validation-matrix:\n    env:\n      PIP_BUILD_CONSTRAINT: /tmp/build.txt\n    name:",
        ),
        (
            workflow_path,
            "      - if: matrix.task == 'native-llvm-coverage'\n"
            "        uses: ./.github/actions/native-llvm-coverage\n",
            "      - if: matrix.task == 'native-llvm-coverage'\n"
            "        uses: ./.github/actions/native-llvm-coverage\n"
            "        env:\n"
            "          PIP_BUILD_CONSTRAINT: /tmp/build.txt\n",
        ),
        (
            workflow_path,
            '          set -euo pipefail\n          case "${VALIDATION_TASK}" in\n',
            "          set -euo pipefail\n"
            "          echo 'PIP_BUILD_CONSTRAINT=/tmp/build.txt' >> \"$GITHUB_ENV\"\n"
            '          case "${VALIDATION_TASK}" in\n',
        ),
        (
            cache_action_path,
            '            stream.write(f"PIP_CACHE_DIR={cache_directory}\\n")\n',
            '            stream.write(f"PIP_CACHE_DIR={cache_directory}\\n")\n'
            '            stream.write("PIP_BUILD_CONSTRAINT=/tmp/build.txt\\n")\n',
        ),
    )
    for path, original, replacement in mutations:
        mutated = dict(definitions)
        assert mutated[path].count(original) == 1
        mutated[path] = mutated[path].replace(original, replacement, 1)
        assert _no_isolation_constraint_violations(mutated)

    unrelated = dict(definitions)
    needle = "  platform-wheel-builds:\n    name:"
    assert unrelated[workflow_path].count(needle) == 1
    unrelated[workflow_path] = unrelated[workflow_path].replace(
        needle,
        "  platform-wheel-builds:\n    env:\n      PIP_BUILD_CONSTRAINT: /tmp/build.txt\n    name:",
        1,
    )
    assert _no_isolation_constraint_violations(unrelated) == ()


def test_release_verifier_requirements_are_a_complete_exact_compatible_lock() -> None:
    """Attestation verification uses a current exact compatible cryptographic environment."""
    path = ROOT / "meta/ci/requirements/release-verification.txt"
    pyproject = tomllib.loads((ROOT / "pyproject.toml").read_text(encoding="utf-8"))

    assert _exact_lock_names(path) == _RELEASE_VERIFICATION_LOCK_NAMES
    names = [
        canonicalize_name(Requirement(line).name)
        for line in path.read_text(encoding="utf-8").splitlines()
    ]
    assert names == sorted(names)
    assert (
        "/meta/ci/requirements/release-verification.txt"
        in pyproject["tool"]["scikit-build"]["sdist"]["include"]
    )


def test_runner_network_and_toolchain_inputs_are_bounded_and_exact() -> None:
    """Cold-run variance cannot silently select tools or wait without a bound."""
    ci = _workflow("ci.yml")
    build = _action("build-platform-wheel")
    native = _action("native-llvm-coverage")
    sanitizer = _action("platform-sanitizer")
    tsan = _action("thread-sanitizer")
    quality = _action("quality-validation")

    for setting in (
        "PIP_DISABLE_PIP_VERSION_CHECK: '1'",
        "PIP_NO_INPUT: '1'",
        "PIP_RETRIES: '10'",
        "PIP_TIMEOUT: '60'",
    ):
        assert setting in _workflow_preamble(ci)
    cache = _action("restore-pip-cache")
    assert "cache-dependency-path:" not in ci
    assert "meta/ci/requirements/*.txt" not in ci
    assert ".github/actions/*/action.yml" not in ci
    assert ci.count("uses: ./.github/actions/restore-pip-cache") == 6
    assert "actions/cache@55cc8345863c7cc4c66a329aec7e433d2d1c52a9" in cache
    assert "continue-on-error: true" in cache
    assert "restore-keys:" not in cache
    assert 'cache_root = workspace / ".work" / "pip-cache"' in cache
    assert "cache_directory = cache_root / owner" in cache
    assert "python -m pip cache dir" not in cache
    assert 'stream.write(f"PIP_CACHE_DIR={cache_directory}\\n")' in cache
    assert cache.count('os.environ["GITHUB_ENV"]') == 1
    assert "PIP_BUILD_CONSTRAINT" not in cache
    assert "CACHE_RESTORE_OUTCOME" in cache
    assert "shutil.rmtree(cache_directory)" in cache
    for component in (
        "CACHE_RUNNER_SYSTEM: ${{ runner.os }}",
        "CACHE_RUNNER_ARCHITECTURE: ${{ runner.arch }}",
        "CACHE_PYTHON_VERSION: ${{ inputs.python-version }}",
        "CACHE_OWNER: ${{ inputs.owner }}",
        "dependency_digest = digest.hexdigest()",
        'f"pip-v1-{runner_system}-{runner_architecture}-python-"',
        'f"{python_version}-{owner}-{dependency_digest}"',
        "key: ${{ steps.pip-cache-identity.outputs.key }}",
    ):
        assert component in cache
    for owner in (
        "quality",
        "source-distribution",
        "native-llvm-coverage",
        "thread-sanitizer",
        "platform-sanitizer",
        "validation-gate",
    ):
        assert ci.count(f"owner: {owner}") == 1
    assert ci.count("meta/ci/requirements/quality.txt") == 1
    assert ci.count("meta/ci/requirements/downstream.txt") == 1
    assert ci.count("meta/ci/requirements/platform-tests.txt") == 1
    for action in (build, native, sanitizer, tsan, quality):
        assert "python -m pip install -U" not in action
    assert "CIBW_CONFIG_SETTINGS: >-" in build
    assert "build.verbose=false" in build
    assert "cmake.define.SCHEMA_SANITIZER_ENABLE_PCH=ON" in build
    assert native.count("SCHEMA_SANITIZER_ENABLE_PCH=OFF") == 1
    assert sanitizer.count("SCHEMA_SANITIZER_ENABLE_PCH=OFF") == 3
    assert tsan.count("SCHEMA_SANITIZER_ENABLE_PCH=OFF") == 1
    for action in (native, sanitizer, tsan):
        assert "Acquire::Retries=3" in action
        assert "Acquire::http::Timeout=30" in action
        assert "Acquire::https::Timeout=30" in action
        assert "DPkg::Lock::Timeout=60" in action
    for action in (native, sanitizer, tsan):
        assert "ninja==1.13.0" in action
        assert "cmake==4.3.4" in action
    for action in (sanitizer, tsan):
        assert "pytest==9.1.1" in action
        assert "pyarrow==25.0.1" in action
    assert "-r meta/ci/requirements/platform-tests.txt" in native
    assert "clang++-18" in sanitizer
    assert "CMAKE_C_COMPILER=clang-18" in sanitizer
    for action in (native, sanitizer):
        assert "clang-18=1:18.1.3-1ubuntu1" in action
        assert "dpkg-query -W -f='${Version}' clang-18" in action
        assert "clang-18 -dumpfullversion -dumpversion" in action
        assert "== '18.1.3'" in action
    assert "llvm-18=1:18.1.3-1ubuntu1" in native
    assert "dpkg-query -W -f='${Version}' llvm-18" in native
    assert "llvm-profdata-18 --version | grep -F 'LLVM version 18.1.3'" in native
    assert "llvm-cov-18 --version | grep -F 'LLVM version 18.1.3'" in native
    assert "clang++-18 -dumpfullversion -dumpversion" in sanitizer
    for package in ("gcc-14", "g++-14"):
        assert f"{package}=14.2.0-4ubuntu2~24.04.1" in tsan
        assert f"dpkg-query -W -f='${{Version}}' {package}" in tsan
        assert f"{package} -dumpfullversion -dumpversion" in tsan
    macos = tomllib.loads((ROOT / "pyproject.toml").read_text(encoding="utf-8"))["tool"][
        "cibuildwheel"
    ]["macos"]
    assert macos["environment"] == {
        "DEVELOPER_DIR": "/Applications/Xcode_16.4.app/Contents/Developer",
        "MACOSX_DEPLOYMENT_TARGET": "11.0",
    }
    for action in (build, sanitizer):
        assert "/Applications/Xcode_16.4.app/Contents/Developer" in action
        assert "$'Xcode 16.4\\nBuild version 16F6'" in action
        assert "xcrun --sdk macosx15.5 --show-sdk-version" in action
        assert "Apple clang version 17.0.0 (clang-1700.0.13.5)" in action
    assert "-DCMAKE_OSX_SYSROOT=macosx15.5" in sanitizer
    assert "pre-commit install-hooks" in quality
    assert "PRE_COMMIT_HOME: ${{ github.workspace }}/.work/pre-commit-cache" in quality
    assert "actions/cache@55cc8345863c7cc4c66a329aec7e433d2d1c52a9" in quality
    assert "continue-on-error: true" in quality
    assert "restore-keys:" not in quality
    assert "${{ runner.arch }}" in " ".join(quality.split())
    assert "${{ inputs.python-version }}" in " ".join(quality.split())
    assert "hashFiles('.pre-commit-config.yaml'," in quality
    assert "'meta/ci/requirements/quality.txt'," in quality
    assert "'meta/ci/requirements/pre-commit-hooks.txt')" in quality
    assert 'rm -rf -- "${PRE_COMMIT_HOME}"' in quality
    assert "for attempt in 1 2 3" in quality
    assert "timeout --signal=TERM --kill-after=15s 120s pre-commit install-hooks" in quality
    assert "sleep $((attempt * 2))" in quality


def test_release_archives_use_and_check_the_commit_timestamp() -> None:
    """The runner wall clock cannot leak into the canonical source archive."""
    build = _action("build-platform-wheel")
    source = _action("source-distribution")
    gate = _job_body(_workflow("ci.yml"), "validation-gate")
    validator = (ROOT / "meta/ci/release/check_distribution_contents.py").read_text(
        encoding="utf-8"
    )

    for owner in (build, source, gate):
        assert "git show -s --format=%ct HEAD" in owner
        assert "export SOURCE_DATE_EPOCH" in owner
        assert "GITHUB_ENV" not in owner
    assert source.count("python -m build --sdist --outdir .work/sdist-reproducibility/") == 2
    first_build = source.index("python -m build --sdist --outdir .work/sdist-reproducibility/first")
    clean_build = source.index("rm -rf -- .work/build", first_build)
    second_build = source.index(
        "python -m build --sdist --outdir .work/sdist-reproducibility/second"
    )
    compare = source.index('cmp -- "${first[0]}" "${second[0]}"')
    assert first_build < clean_build < second_build < compare
    assert "CIBW_ENVIRONMENT_PASS_LINUX: >-" in build
    container_environment = build.split("CIBW_ENVIRONMENT_PASS_LINUX: >-", 1)[1].split(
        "      shell:", 1
    )[0]
    for variable in (
        "SOURCE_DATE_EPOCH",
        "PIP_DISABLE_PIP_VERSION_CHECK",
        "PIP_NO_INPUT",
        "PIP_RETRIES",
        "PIP_TIMEOUT",
        "PYTHONHASHSEED",
        "PYTHONUTF8",
        "TZ",
    ):
        assert variable in container_environment
    assert "gzip_epoch != epoch or timestamps != {epoch}" in validator
    assert "SOURCE_DATE_EPOCH" in validator


def test_native_stress_and_functional_suites_form_an_explicit_partition() -> None:
    """Every platform runs one heavy case plus the complete functional complement."""
    test_action = _action("test-platform-wheel")
    pytest_config = tomllib.loads((ROOT / "pyproject.toml").read_text(encoding="utf-8"))["tool"][
        "pytest"
    ]["ini_options"]
    stress = next(
        step
        for step in _step_bodies(test_action)
        if "name: Cross-platform native completion stress" in step
    )
    functional = next(
        step for step in _step_bodies(test_action) if "name: Run functional test shard" in step
    )

    assert pytest_config["markers"] == [
        "native_stress: high-volume native concurrency coverage run explicitly in CI"
    ]
    assert pytest_config["addopts"] == ["--strict-config", "--strict-markers"]
    assert pytest_config["xfail_strict"] is True
    assert pytest_config["filterwarnings"] == [
        "error::pytest.PytestUnhandledThreadExceptionWarning",
        "error::pytest.PytestUnraisableExceptionWarning",
    ]
    assert "if: inputs.shard == 'concurrency'" in stress
    assert "-m native_stress" in stress
    assert "tests/concurrency/test_ordered_executor_completion_probe.py" in stress
    assert "-m 'not native_stress'" in functional
    assert "--ignore" not in stress + functional
    assert "pytest-native-stress-${PLATFORM_ARTIFACT}.xml" in stress
    assert "pytest-native-stress-durations-${PLATFORM_ARTIFACT}.log" in stress
    assert "pytest-${PLATFORM_ARTIFACT}-${TEST_SHARD}.xml" in functional
    assert test_action.index("name: Cross-platform native completion stress") < (
        test_action.index("name: Run functional test shard")
    )


def test_platform_smokes_have_stable_balanced_shard_ownership() -> None:
    """Independent smoke gates retain coverage without extending the longest shard."""
    test_action = _action("test-platform-wheel")
    ownership = {
        "Certify the Parquet runtime from functional evidence": "memory-parquet",
        "Record a cross-platform reader scaling measurement": "memory-parquet",
        "Cross-platform threading benchmark smoke": "concurrency",
        "Cross-platform native completion stress": "concurrency",
    }

    for name, shard in ownership.items():
        step = next(step for step in _step_bodies(test_action) if f"name: {name}" in step)
        assert f"if: inputs.shard == '{shard}'" in step


def test_parquet_certificate_reuses_functional_junit_and_compilation_runs_once() -> None:
    """Certification consumes full-suite evidence while quality owns syntax compilation."""
    test_action = _action("test-platform-wheel")
    quality_action = _action("quality-validation")
    functional = next(
        step for step in _step_bodies(test_action) if "name: Run functional test shard" in step
    )
    certificate = next(
        step
        for step in _step_bodies(test_action)
        if "name: Certify the Parquet runtime from functional evidence" in step
    )

    junit_path = "artifacts/pytest-${PLATFORM_ARTIFACT}-${TEST_SHARD}.xml"
    assert junit_path in functional
    assert '--junit-xml "artifacts/pytest-${PLATFORM_ARTIFACT}-memory-parquet.xml"' in certificate
    assert test_action.count("check_parquet_contract_runtime_suite.py") == 1
    assert "check_parquet_contract_runtime.py" not in test_action
    assert test_action.index(functional) < test_action.index(certificate)
    assert "python -m compileall" not in test_action
    assert quality_action.count("python -m compileall -q src") == 1


def test_source_quality_and_platform_test_ownership_is_disjoint_and_exhaustive() -> None:
    """Source contracts run once while functional domains stay identical per platform."""
    ci = _workflow("ci.yml")
    test_action = _action("test-platform-wheel")
    quality_action = _action("quality-validation")
    all_test_domains = {
        path.name
        for path in (ROOT / "tests").iterdir()
        if path.is_dir() and not path.name.startswith("_")
    }
    shard_domains = {
        "concurrency": {"concurrency"},
        "memory-parquet": {
            "memory",
            "parquet",
            "sinks",
        },
        "io-pipeline": {
            "examples",
            "io",
            "pipeline",
            "remote",
            "schema",
        },
    }
    functional = next(
        step for step in _step_bodies(test_action) if "name: Run functional test shard" in step
    )
    quality_contracts = next(
        step
        for step in _step_bodies(quality_action)
        if "name: Run source quality contracts" in step
    )

    actual_shard_domains = {}
    for shard in shard_domains:
        match = re.search(
            rf"^          {re.escape(shard)}\)\n"
            r"            test_paths=\(\n"
            r"(?P<paths>(?:              tests/[a-z0-9_-]+\n)+)"
            r"            \)\n"
            r"            ;;$",
            functional,
            re.MULTILINE,
        )
        assert match is not None, f"missing static test-path array for {shard}"
        actual_shard_domains[shard] = set(
            re.findall(r"^              tests/([a-z0-9_-]+)$", match["paths"], re.MULTILINE)
        )

    assert actual_shard_domains == shard_domains
    assert functional.count("test_paths=(") == len(shard_domains)
    assert set(re.findall(r"^              tests/([a-z0-9_-]+)$", functional, re.MULTILINE)) == (
        all_test_domains - {"quality"}
    )
    cross_toolchain_quality_test = (
        "tests/quality/test_fuzz_regression_runner.py::"
        "test_standalone_mutation_stream_matches_its_cross_library_golden"
    )
    assert quality_contracts.count("tests/quality") == 1
    assert cross_toolchain_quality_test in functional
    assert functional.count(cross_toolchain_quality_test) == 1
    assert "if [[ \"${TEST_SHARD}\" == 'concurrency' ]]" in functional
    assert "test_paths+=(" in functional

    for shard, domains in shard_domains.items():
        other_domains = set().union(
            *(candidate for name, candidate in shard_domains.items() if name != shard)
        )
        assert domains.isdisjoint(other_domains)
    assert set().union(*shard_domains.values(), {"quality"}) == all_test_domains
    for domains in shard_domains.values():
        for domain in domains:
            assert functional.count(f"tests/{domain}") == 1
    cells = {
        (platform["platform-name"], shard)
        for platform in _TEST_PLATFORM_JOBS
        for shard in _matrix_axis(ci, platform["job-id"], "shard")
    }
    assert cells == {
        (platform["platform-name"], shard)
        for platform in _TEST_PLATFORM_JOBS
        for shard in shard_domains
    }
    assert len(cells) == 12
    for platform in _TEST_PLATFORM_JOBS:
        tests = _job_body(ci, platform["job-id"])
        assert "fail-fast: false" in tests
        assert "if: ${{ !cancelled() }}" in tests


def test_linux_sanitizer_reuses_one_certified_cmake_graph(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The ASan extension, native executor, and fuzzers share one exact build tree."""
    sanitizer = _action("platform-sanitizer")
    install = next(
        step
        for step in _step_bodies(sanitizer)
        if "name: Build and install the Linux instrumented extension" in step
    )
    certification = next(
        step
        for step in _step_bodies(sanitizer)
        if "name: Certify the reused Linux sanitizer build tree" in step
    )

    assert "--config-settings=build-dir=.work/build/platform-sanitizer" in install
    assert "--config-settings=cmake.build-type=RelWithDebInfo" in install
    for setting in (
        "SCHEMA_SANITIZER_BUILD_FUZZERS=ON",
        "SCHEMA_SANITIZER_ENABLE_LTO=OFF",
        "SCHEMA_SANITIZER_ENABLE_PCH=OFF",
        "SCHEMA_SANITIZER_ENABLE_WERROR=ON",
        "SCHEMA_SANITIZER_FUZZ_ENGINE=libfuzzer",
        "SCHEMA_SANITIZER_REQUIRE_ZLIB=ON",
        "SCHEMA_SANITIZER_SANITIZER=asan-ubsan",
        "SCHEMA_SANITIZER_ZLIB_PROVIDER=bundled",
    ):
        assert setting in install
        assert (
            setting.replace("=", ":BOOL=", 1) in certification
            or setting.replace("=", ":STRING=", 1) in certification
        )
    assert "CMAKE_BUILD_TYPE:STRING=RelWithDebInfo" in certification
    assert "CMAKE_GENERATOR:INTERNAL=Ninja" in certification
    assert "CMAKE_PROJECT_NAME:STATIC=schema_sanitizer" in certification
    assert "name: Configure Linux ASan/UBSan" not in sanitizer
    assert sanitizer.count("cmake -S . -B .work/build/platform-sanitizer") == 2
    assert (
        sanitizer.index("Build and install the Linux instrumented extension")
        < sanitizer.index("Certify the reused Linux sanitizer build tree")
        < sanitizer.index("Build native concurrency and fuzz targets")
    )
    assert '("CMAKE_C_COMPILER", "clang-18")' in certification
    assert '("CMAKE_CXX_COMPILER", "clang++-18")' in certification
    assert "shutil.which(configured_value)" in certification
    assert "configured_path.resolve(strict=True)" in certification
    assert "expected_path = Path(expected_location).resolve(strict=True)" in certification
    assert "os.access(configured_path, os.X_OK)" in certification
    assert "/usr/bin/clang-18" not in certification

    invocation = "python - \"${cache}\" <<'PY'"
    certificate_source = textwrap.dedent(
        certification.split(f"{invocation}\n", 1)[1].split("\n        PY", 1)[0]
    )
    certificate = compile(certificate_source, "linux-sanitizer-cache-certificate", "exec")
    compiler_directory = tmp_path / "compilers"
    compiler_directory.mkdir()
    c_compiler = compiler_directory / "clang-18"
    cxx_compiler = compiler_directory / "clang++-18"
    wrong_compiler = compiler_directory / "wrong-clang"
    executable_mode = stat.S_IRUSR | stat.S_IWUSR | stat.S_IXUSR
    for executable in (c_compiler, cxx_compiler, wrong_compiler):
        executable.write_bytes(b"")
        executable.chmod(executable_mode)
    locations = {
        "clang-18": str(c_compiler),
        "clang++-18": str(cxx_compiler),
    }
    monkeypatch.setattr(shutil, "which", locations.get)
    cache_path = tmp_path / "CMakeCache.txt"

    def execute_certificate(
        cache_type: str,
        configured_c: str | Path = c_compiler,
        configured_cxx: str | Path = cxx_compiler,
    ) -> None:
        """Run the embedded certificate against one synthetic CMake cache."""
        cache_path.write_text(
            "\n".join(
                (
                    f"CMAKE_C_COMPILER:{cache_type}={configured_c}",
                    f"CMAKE_CXX_COMPILER:{cache_type}={configured_cxx}",
                )
            ),
            encoding="utf-8",
        )
        monkeypatch.setattr(sys, "argv", ["certificate", str(cache_path)])
        exec(certificate, {"__name__": "__main__"})

    for cache_type in ("STRING", "FILEPATH", "UNINITIALIZED"):
        execute_certificate(cache_type)
    execute_certificate("UNINITIALIZED", "clang-18", "clang++-18")
    with pytest.raises(SystemExit, match="CMAKE_C_COMPILER mismatch"):
        execute_certificate("STRING", wrong_compiler)


def test_concurrency_shards_fail_closed_at_explicit_platform_cpu_minima() -> None:
    """Three four-core platforms cover those contracts while ARM64 guarantees three cores."""
    minima = {
        platform["platform-name"]: platform["minimum-cpu-capacity"]
        for platform in _TEST_PLATFORM_JOBS
    }
    preflight = next(
        step
        for step in _step_bodies(_action("test-platform-wheel"))
        if "name: Require the declared concurrency CPU capacity" in step
    )

    assert minima == {
        "linux-x86_64": "4",
        "windows-amd64": "4",
        "macos-x86_64": "4",
        "macos-arm64": "3",
    }
    assert {platform for platform, minimum in minima.items() if minimum == "4"} == {
        "linux-x86_64",
        "windows-amd64",
        "macos-x86_64",
    }
    assert "if: inputs.shard == 'concurrency'" in preflight
    assert "native_core.execution_policy(ThreadingMode.MULTI.value, 512 << 20)[1]" in preflight
    assert "pressure_adjusted_target" not in preflight
    assert "if detected < minimum" in preflight
    assert "continue-on-error" not in preflight


def test_validation_matrix_and_terminal_gate_have_exact_dependencies() -> None:
    """Four test matrices consume builds and one final job joins every matrix."""
    ci = _workflow("ci.yml")
    gate = _job_body(ci, "validation-gate")
    for platform in _TEST_PLATFORM_JOBS:
        job_id = platform["job-id"]
        tests = _job_body(ci, job_id)
        assert _job_needs(ci, job_id) == ("platform-wheel-builds",)
        assert tests.count("needs:") == 1
        assert "if: ${{ !cancelled() }}" in tests
    assert _job_needs(ci, "validation-gate") == (
        "platform-wheel-builds",
        *(platform["job-id"] for platform in _TEST_PLATFORM_JOBS),
        "validation-matrix",
    )
    assert gate.count("needs:") == 1
    expected_results = (
        ("WHEEL_BUILDS_RESULT", "platform-wheel-builds"),
        ("PLATFORM_TESTS_LINUX_X86_64_RESULT", "platform-tests-linux-x86-64"),
        ("PLATFORM_TESTS_WINDOWS_AMD64_RESULT", "platform-tests-windows-amd64"),
        ("PLATFORM_TESTS_MACOS_X86_64_RESULT", "platform-tests-macos-x86-64"),
        ("PLATFORM_TESTS_MACOS_ARM64_RESULT", "platform-tests-macos-arm64"),
        ("VALIDATION_MATRIX_RESULT", "validation-matrix"),
    )
    for variable, job_id in expected_results:
        assert f"{variable}: ${{{{ needs.{job_id}.result }}}}" in gate
        assert f'"${{{variable}}}"' in gate
    assert gate.count(".result }}") == len(expected_results)
    require = gate.index("name: Require every validation matrix to succeed")
    downloads = gate.index("actions/download-artifact@")
    assembly = gate.index("name: Assemble the auditable release artifact")
    assert 'if [[ "${result}" != "success" ]]' in gate
    assert require < downloads < assembly


def test_validation_gate_retries_artifact_downloads_from_clean_exact_destinations() -> None:
    """Transient reads cannot reuse partial release bytes or cross artifact classes."""
    gate = _job_body(_workflow("ci.yml"), "validation-gate")
    downloads = gate[: gate.index("name: Validate the complete release artifact set")]

    assert downloads.count("actions/download-artifact@") == 4
    assert downloads.count("name: source-distribution") == 2
    assert downloads.count("path: download/source") == 2
    assert downloads.count("path: download/wheels") == 2
    assert downloads.count("pattern: dist-wheels-*") == 2
    assert downloads.count("merge-multiple: true") == 2
    assert downloads.count("continue-on-error: true") == 2
    assert downloads.count("steps.download-source-distribution.outcome == 'failure'") == 2
    assert downloads.count("steps.download-platform-wheels.outcome == 'failure'") == 2
    assert downloads.count("rm -rf -- download/source") == 1
    assert downloads.count("rm -rf -- download/wheels") == 1
    assert "cp download/source/*.tar.gz download/wheels/*.whl dist/" in gate


def test_platform_evidence_records_comparable_cpu_limits_without_job_summary() -> None:
    """Runner evidence keeps portable CPU context without rendering job summaries."""
    test_action = _action("test-platform-wheel")
    helper = (ROOT / "meta/ci/quality/record_runner_environment.py").read_text(encoding="utf-8")
    evidence = next(
        step for step in _step_bodies(test_action) if "name: Record runner CPU environment" in step
    )

    assert "if:" not in evidence
    assert "os.cpu_count()" in helper
    assert "os.sched_getaffinity(0)" in helper
    assert '"effective_count": _effective_cpu_capacity(' in helper
    assert '"installed_distributions"' in helper
    for distribution in (
        "aiohttp",
        "duckdb",
        "pandas",
        "polars",
        "pyarrow",
        "pytest",
        "schema-sanitizer",
    ):
        assert f'"{distribution}"' in helper
    assert '_optional_text("/sys/fs/cgroup/cpu.max") if is_linux else None' in helper
    assert '_optional_key_values("/sys/fs/cgroup/cpu.stat") if is_linux else None' in helper
    assert "runner-cpu-${PLATFORM_ARTIFACT}-${TEST_SHARD}.json" in evidence
    assert "os." + "environ" not in helper
    assert "arguments[0]" in helper
    assert test_action.index("name: Record runner CPU environment") < test_action.index(
        "name: Record a cross-platform reader scaling measurement"
    )
    assert "name: Summarize platform-shard evidence" not in test_action
    assert "GITHUB_STEP_SUMMARY" not in test_action
    assert "actions/upload-artifact@" not in test_action


def test_runner_environment_helper_writes_only_the_owned_schema(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Extracted runner evidence remains executable and excludes environment data."""
    from meta.ci.quality import record_runner_environment

    monkeypatch.setattr(record_runner_environment.metadata, "version", lambda _name: "1.0")

    output = tmp_path / "runner.json"
    assert record_runner_environment.main([str(output)]) == 0
    evidence = json.loads(output.read_text(encoding="utf-8"))

    assert set(evidence) == {
        "schema_version",
        "python",
        "installed_distributions",
        "platform",
        "cpu",
    }
    assert evidence["schema_version"] == 2
    assert set(evidence["cpu"]) == {
        "logical_count",
        "affinity",
        "affinity_count",
        "effective_count",
        "linux_cgroup_v2_cpu_max",
        "linux_cgroup_v2_cpu_stat",
    }
    assert "environment" not in output.read_text(encoding="utf-8").lower()


def test_build_parallelism_is_positive_bounded_and_generator_aware(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Wheel generators own fan-out while diagnostic builds keep CPU bounds."""
    from meta.ci.quality import record_runner_environment

    monkeypatch.setattr(record_runner_environment.os, "cpu_count", lambda: 12)
    monkeypatch.delattr(record_runner_environment.os, "sched_getaffinity", raising=False)
    monkeypatch.setattr(
        record_runner_environment.os,
        "sched_getaffinity",
        lambda _pid: {0, 1},
        raising=False,
    )
    monkeypatch.setattr(
        record_runner_environment,
        "_optional_text",
        lambda _path: "150000 100000",
    )

    assert record_runner_environment.effective_cpu_capacity() == 2
    assert record_runner_environment.bounded_build_parallelism() == 2
    assert record_runner_environment.bounded_build_parallelism(1) == 1
    assert record_runner_environment.bounded_build_parallelism(99) == 2
    with pytest.raises(ValueError, match="must be positive"):
        record_runner_environment.bounded_build_parallelism(0)

    build = _action("build-platform-wheel")
    platform_sanitizer = _action("platform-sanitizer")
    thread_sanitizer = _action("thread-sanitizer")

    assert "bounded_build_parallelism" not in build
    assert build.count("unset CMAKE_BUILD_PARALLEL_LEVEL") == 1
    assert "CIBW_ENVIRONMENT_WINDOWS" not in build
    assert "CIBW_CONFIG_SETTINGS_WINDOWS" not in build
    assert "SCHEMA_SANITIZER_MSVC_COMPILE_PROCESSES:STRING=auto" in build
    for action in (platform_sanitizer, thread_sanitizer):
        assert "bounded_build_parallelism" in action
        assert "CMAKE_BUILD_PARALLEL_LEVEL" in action
        assert "--parallel 4" not in action
        assert '[[ "${CMAKE_BUILD_PARALLEL_LEVEL}" =~ ^[1-4]$ ]]' in action
    assert "if [[ \"${RUNNER_SYSTEM}\" == 'Windows' ]]" in platform_sanitizer
    assert "CMAKE_BUILD_PARALLEL_LEVEL=1" in platform_sanitizer
    assert "SCHEMA_SANITIZER_MSVC_COMPILE_PROCESSES=1" not in (build + platform_sanitizer)
    assert "build.verbose=false" in build


@pytest.mark.parametrize(
    ("raw_quota", "expected"),
    (
        ("150000 100000", 2),
        ("100000 100000", 1),
        ("max 100000", None),
        ("invalid", None),
        ("0 100000", None),
    ),
)
def test_build_parallelism_parses_linux_quota_without_fractional_workers(
    raw_quota: str, expected: int | None
) -> None:
    """Cgroup quotas round up safely and malformed or unlimited values opt out."""
    from meta.ci.quality import record_runner_environment

    assert record_runner_environment._linux_cpu_quota_capacity(raw_quota) == expected


def test_build_parallelism_has_a_one_worker_fallback(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Missing hardware counts and unavailable affinity still yield one worker."""
    from meta.ci.quality import record_runner_environment

    def unavailable_affinity(_pid: int) -> set[int]:
        """Simulate a platform affinity probe that is present but unavailable."""
        raise OSError("affinity unavailable")

    monkeypatch.setattr(record_runner_environment.os, "cpu_count", lambda: None)
    monkeypatch.delattr(record_runner_environment.os, "sched_getaffinity", raising=False)
    monkeypatch.setattr(
        record_runner_environment.os,
        "sched_getaffinity",
        unavailable_affinity,
        raising=False,
    )
    monkeypatch.setattr(record_runner_environment, "_optional_text", lambda _path: None)

    assert record_runner_environment.effective_cpu_capacity() == 1
    assert record_runner_environment.bounded_build_parallelism() == 1


def test_platform_suite_reports_test_timings_without_job_summary() -> None:
    """Every wheel runner logs ranked pytest timings without rendering a summary."""
    test_action = _action("test-platform-wheel")
    full_suite = next(
        step for step in _step_bodies(test_action) if "name: Run functional test shard" in step
    )

    assert "shell: bash" in full_suite
    assert "set -euo pipefail" in full_suite
    assert "--durations=50" in full_suite
    assert "--durations-min=0.05" in full_suite
    assert '--junitxml="artifacts/pytest-${PLATFORM_ARTIFACT}-${TEST_SHARD}.xml"' in full_suite
    assert 'tee "artifacts/pytest-durations-${PLATFORM_ARTIFACT}-${TEST_SHARD}.log"' in full_suite
    assert "name: Summarize platform-shard evidence" not in test_action
    assert "GITHUB_STEP_SUMMARY" not in test_action


def test_ci_reserves_job_summaries_for_cibuildwheel() -> None:
    """Only cibuildwheel may inherit GitHub's summary destination."""
    summary_references = {
        path.relative_to(ROOT).as_posix(): [
            line.strip()
            for line in path.read_text(encoding="utf-8").splitlines()
            if "GITHUB_STEP_SUMMARY" in line
        ]
        for pattern in ("*.yml", "*.yaml")
        for path in sorted((ROOT / ".github").rglob(pattern))
        if "GITHUB_STEP_SUMMARY" in path.read_text(encoding="utf-8")
    }

    assert summary_references == {}

    build_action = _action("build-platform-wheel")
    wheel_build = next(
        step
        for step in _step_bodies(build_action)
        if "name: Build and test the CPython 3.11 ABI3 wheel" in step
    )
    assert wheel_build.count("python -m cibuildwheel") == 1
    assert "env -u GITHUB_STEP_SUMMARY" not in wheel_build


def test_validation_owns_full_extension_tsan_gate() -> None:
    """Linux CI fails closed unless complete TSan coverage has enough CPU capacity."""
    ci = _workflow("ci.yml")
    tsan = _action("thread-sanitizer")

    assert "task: thread-sanitizer" in ci
    assert "SCHEMA_SANITIZER_SANITIZER=tsan" in tsan
    assert "SCHEMA_SANITIZER_ZLIB_PROVIDER=bundled" in tsan
    assert "meta/ci/sanitizers/tsan_python_launcher.cc" in tsan
    assert tsan.count("meta/ci/sanitizers/run_tsan_extension_suite.sh") == 1
    assert ".work/build/tsan .work/bin/python-tsan 2" in tsan
    assert "site.getsitepackages()[0]" in tsan

    capacity_step = tsan.split(
        "- name: Require capacity for complete TSan concurrency coverage", 1
    )[1].split("- name:", 1)[0]
    suite_step = tsan.split("- name: Run the executor and all full-extension TSan domains", 1)[1]
    assert "--require-cpu-capacity 3" in capacity_step
    assert "continue-on-error" not in capacity_step
    assert "GITHUB_OUTPUT" not in capacity_step
    assert "::warning" not in capacity_step
    assert "if:" not in suite_step

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


@pytest.mark.skipif(sys.platform != "linux", reason="process-group semantics are Linux-specific")
def test_tsan_runner_terminates_a_live_descendant_after_its_leader_exits(
    tmp_path: Path,
) -> None:
    """A successful marker cannot hide a TERM-resistant process-group descendant."""
    build_dir = tmp_path / "build"
    site_packages = tmp_path / "site-packages"
    test_target = tmp_path / "test_domain.py"
    launcher = tmp_path / "fake-python"
    child_pid_file = tmp_path / "child.pid"
    build_dir.mkdir()
    site_packages.mkdir()
    test_target.write_text("# synthetic TSan domain\n", encoding="utf-8")
    launcher.write_text(
        """#!/usr/bin/env bash
set -euo pipefail
if (( $# != 6 )); then
  exit 0
fi
marker="$6"
(
  trap '' TERM
  child_pgid="$(ps -o pgid= -p "${BASHPID}" | tr -d '[:space:]')"
  printf '%s %s\n' "${BASHPID}" "${child_pgid}" > "${TSAN_TEST_CHILD_PID_FILE:?}.pending"
  mv -- "${TSAN_TEST_CHILD_PID_FILE}.pending" "${TSAN_TEST_CHILD_PID_FILE}"
  while :; do
    sleep 60
  done
) &
child_pid=$!
while [[ ! -s "${TSAN_TEST_CHILD_PID_FILE}" ]]; do
  kill -0 "${child_pid}"
done
printf '0' > "${marker}"
""",
        encoding="utf-8",
    )
    launcher.chmod(launcher.stat().st_mode | stat.S_IXUSR)
    required_commands = {
        command: shutil.which(command)
        for command in (
            "bash",
            "cat",
            "dirname",
            "mktemp",
            "mv",
            "ps",
            "rm",
            "setsid",
            "sleep",
            "tr",
            "uname",
        )
    }
    assert all(required_commands.values())
    bash = required_commands["bash"]
    assert bash is not None
    tool_directories = sorted(
        {str(Path(command).parent) for command in required_commands.values() if command is not None}
    )
    environment = {
        "LC_ALL": "C",
        "PATH": os.pathsep.join(tool_directories),
        "TMPDIR": str(tmp_path),
        "TSAN_TEST_CHILD_PID_FILE": str(child_pid_file),
        "TZ": "UTC",
    }
    child_identity: tuple[int, int] | None = None

    try:
        completed = subprocess.run(
            [
                bash,
                str(ROOT / "meta/ci/sanitizers/run_tsan_extension_suite.sh"),
                str(build_dir),
                str(launcher),
                "1",
                str(site_packages),
                str(test_target),
            ],
            cwd=ROOT,
            env=environment,
            check=False,
            capture_output=True,
            text=True,
            timeout=30,
        )
        child_identity = tuple(
            int(value) for value in child_pid_file.read_text(encoding="ascii").split()
        )
        assert len(child_identity) == 2
        _, child_pgid = child_identity
        assert child_pgid > 1 and child_pgid != os.getpgrp()
        assert completed.returncode == 0, completed.stdout + completed.stderr
        assert "TSan domain ignored TERM; escalating to KILL" in completed.stderr
        assert not _linux_process_group_is_live(child_pgid)
    finally:
        if child_identity is None and child_pid_file.is_file():
            values = tuple(
                int(value) for value in child_pid_file.read_text(encoding="ascii").split()
            )
            if len(values) == 2:
                child_identity = values
        if child_identity is not None:
            _, child_pgid = child_identity
            if (
                child_pgid > 1
                and child_pgid != os.getpgrp()
                and _linux_process_group_is_live(child_pgid)
            ):
                try:
                    os.killpg(child_pgid, signal.SIGKILL)
                except ProcessLookupError:
                    pass


def test_native_fuzzing_and_platform_sanitizer_matrix_are_owned_by_ci() -> None:
    """Native fuzzing must run under TSan and supported platform sanitizers."""
    ci = _workflow("ci.yml")
    sanitizer = _action("platform-sanitizer")

    assert ci.count("task: platform-sanitizer") == 4
    assert "windows-amd64-asan" in ci
    assert "macos-x86_64-asan-ubsan" in ci
    assert "macos-arm64-asan-ubsan" in ci
    assert sanitizer.count("SCHEMA_SANITIZER_BUILD_FUZZERS=ON") >= 3
    assert sanitizer.count("SCHEMA_SANITIZER_FUZZ_ENGINE=standalone") >= 2
    assert sanitizer.count("meta/ci/fuzz/run_fuzz_regressions.sh") == 2
    assert sanitizer.count("--engine libfuzzer") >= 1
    assert "--campaign-runs 1000" in sanitizer
    assert "--campaign-runs 500" in sanitizer
    assert "schema_sanitizer_sanitized_ordered_executor" in sanitizer
    assert "--repeat until-fail" not in sanitizer
    assert "-R '^schema_sanitizer_sanitized_ordered_executor$'" in sanitizer
    concurrency_step = sanitizer.split("- name: Run fixed-round sanitized concurrency probe", 1)[
        1
    ].split("- name:", 1)[0]
    capacity_step = sanitizer.split(
        "- name: Require capacity for complete native concurrency coverage", 1
    )[1].split("- name:", 1)[0]
    fuzz_step = sanitizer.split("- name: Run platform fuzz regressions and mutation campaigns", 1)[
        1
    ].split("- name:", 1)[0]
    linux_fuzz_step = sanitizer.split(
        "- name: Run Linux ASan/UBSan fuzz regressions and mutation campaigns", 1
    )[1]
    assert "--require-cpu-capacity 3" in capacity_step
    assert "inputs.mode == 'native'" in capacity_step
    assert "runner.os != 'Windows'" in capacity_step
    assert "continue-on-error" not in capacity_step
    assert "GITHUB_OUTPUT" not in capacity_step
    assert "::warning" not in capacity_step
    normalized_concurrency_step = " ".join(concurrency_step.split())
    assert "inputs.mode == 'native'" in concurrency_step
    assert "runner.os != 'Windows'" in concurrency_step
    assert "sanitizer_cpu_capacity" not in normalized_concurrency_step
    assert "if: inputs.mode == 'native'" in fuzz_step
    assert "sanitizer_cpu_capacity" not in fuzz_step
    assert "sanitizer_cpu_capacity" not in linux_fuzz_step

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
    assert 'std::string_view(argv[1]) == "--require-cpu-capacity"' in probe
    assert "at least " in probe
    assert _action("platform-sanitizer").count("--require-cpu-capacity 3") == 1
    assert _action("thread-sanitizer").count("--require-cpu-capacity 3") == 1


def test_native_concurrency_probes_wait_for_every_asserted_async_metric() -> None:
    """A completion flag cannot stand in for independently published arena state."""
    probe = (ROOT / "cpp/tests/ordered_executor_tsan.cc").read_text(encoding="utf-8")
    shared = probe.split("bool run_shared_operation_arena_round()", 1)[1].split(
        '#include "ordered_executor_tsan_completion.cc.inc"', 1
    )[0]
    backlog = probe.split("bool run_backlog_driven_admission_round()", 1)[1].split(
        "bool run_lane_work_stealing_round()", 1
    )[0]
    stealing = probe.split("bool run_lane_work_stealing_round()", 1)[1].split(
        "bool run_arena_stage_cancellation_round()", 1
    )[0]
    cancellation = probe.split("bool run_arena_stage_cancellation_round()", 1)[1].split(
        "bool run_arena_queue_capacity_round()", 1
    )[0]

    shared_startup_wait = shared.split("const auto startup_deadline", 1)[1].split(
        "release_gate(&release)", 1
    )[0]
    assert "started.load(std::memory_order_acquire) < worker_count" in shared_startup_wait
    assert "arena->peak_active_tasks() < worker_count" in shared_startup_wait

    admission_wait = backlog.split("const auto deadline", 1)[1].split(
        "const bool fully_admitted", 1
    )[0]
    assert "arena->started_workers() < worker_count" in admission_wait
    assert "entered.load(std::memory_order_acquire) < worker_count" in admission_wait
    assert "arena->peak_active_tasks() < worker_count" in admission_wait

    backlog_drain_wait = backlog.split("const auto drain_deadline", 1)[1].split(
        "const bool valid", 1
    )[0]
    assert "completed.load(std::memory_order_acquire) <" in backlog_drain_wait
    assert "arena->active_tasks() != 0U" in backlog_drain_wait
    assert "arena->queued_tasks() != 0U" in backlog_drain_wait

    stealing_drain_wait = stealing.split("const auto drain_deadline", 1)[1].split(
        "const bool valid", 1
    )[0]
    assert "completed.load(std::memory_order_acquire) != worker_count + 1U" in stealing_drain_wait
    assert "arena->stolen_tasks() == 0U" in stealing_drain_wait
    assert "arena->active_tasks() != 0U" in stealing_drain_wait
    assert "arena->queued_tasks() != 0U" in stealing_drain_wait

    reuse_wait = cancellation.split("const auto reuse_deadline", 1)[1].split("const bool valid", 1)[
        0
    ]
    assert "!arena_reused.load(std::memory_order_acquire)" in reuse_wait
    assert "arena->active_tasks() != 0U" in reuse_wait
    assert "arena->queued_tasks() != 0U" in reuse_wait
    assert "arena->retained_bytes() != 0U" in reuse_wait

    exact_peak_assertions = probe.count("arena->peak_active_tasks() == worker_count")
    peak_publication_waits = probe.count("arena->peak_active_tasks() < worker_count")
    assert exact_peak_assertions == 2
    assert peak_publication_waits >= exact_peak_assertions


def test_native_launcher_arguments_preserve_shell_word_boundaries() -> None:
    """Compiler/linker flags and interpreter paths remain arrays or quoted scalars."""
    native_actions = _action("platform-sanitizer") + _action("thread-sanitizer")
    _assert_text_contract(
        native_actions,
        required=('-DPython3_EXECUTABLE="$(command -v python)"',),
        forbidden=("python3-config --embed --cflags --ldflags", "$(which python)"),
        counts=(
            ('read -r -a python_embed_cflags <<< "$(python3-config --embed --cflags)"', 2),
            ('read -r -a python_embed_ldflags <<< "$(python3-config --embed --ldflags)"', 2),
            ('"${python_embed_cflags[@]}"', 2),
            ('"${python_embed_ldflags[@]}"', 2),
        ),
    )


def test_macos_native_baseline_matches_concurrency_runtime_requirements() -> None:
    """macOS wheels must not advertise a pre-atomic-wait runtime baseline."""
    cmake = (ROOT / "CMakeLists.txt").read_text(encoding="utf-8")
    pyproject = tomllib.loads((ROOT / "pyproject.toml").read_text(encoding="utf-8"))
    ci = _workflow("ci.yml")
    build_action = _action("build-platform-wheel")

    assert 'CMAKE_OSX_DEPLOYMENT_TARGET VERSION_LESS "11.0"' in cmake
    assert "MACOSX_DEPLOYMENT_TARGET:" not in ci
    assert "MACOSX_DEPLOYMENT_TARGET" not in build_action
    assert "CIBW_ENVIRONMENT: >-" not in build_action
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
    """Benchmark checks cover supported platforms without gating on hosted-runner timing."""
    ci = _workflow("ci.yml")
    test_action = _action("test-platform-wheel")

    assert "benchmarks/concurrency/threading/run_matrix.sh" in test_action
    assert "--profile ci" in test_action
    assert "python -I benchmarks/readers/linear_scaling.py" in test_action
    assert "--maximum-normalized-growth 8" in test_action
    assert "--failure-confirmations 0" in test_action
    assert "--latency-budget benchmarks/readers/linear_scaling_budget.json" in test_action
    assert "--wheel wheelhouse/*.whl" in test_action
    reader_step = next(
        step
        for step in _step_bodies(test_action)
        if "name: Record a cross-platform reader scaling measurement" in step
    )
    assert "shell: bash" in reader_step
    assert "if: inputs.shard == 'memory-parquet'" in reader_step
    assert "if python -I benchmarks/readers/linear_scaling.py \\" in reader_step
    assert 'report.get("failures")' in reader_step
    assert "Non-gating reader timing" in reader_step
    assert "reader-linear-scaling-${PLATFORM_ARTIFACT}.json" in test_action
    # Cheap measurements and deterministic smokes run before the full pytest suite.
    assert test_action.index("name: Record a cross-platform reader scaling measurement") < (
        test_action.index("name: Cross-platform threading benchmark smoke")
    )
    assert test_action.index("name: Cross-platform threading benchmark smoke") < (
        test_action.index("name: Run functional test shard")
    )
    assert test_action.index("name: Record a cross-platform reader scaling measurement") < (
        test_action.index("name: Run functional test shard")
    )
    for artifact in ("linux", "windows", "macos-x86_64", "macos-arm64"):
        assert f"artifact: {artifact}" in ci


def test_python_coverage_has_an_explicit_regression_floor() -> None:
    """Coverage collection is a gate, not merely a report artifact."""
    _assert_text_contract(
        _action("quality-validation"), counts=(("coverage report --fail-under=44", 1),)
    )


def test_ci_artifact_policies_are_explicit_and_bounded() -> None:
    """Only release transport and publication artifacts are uploaded."""
    sources = (
        _workflow("ci.yml"),
        _workflow("publish.yml"),
        _action("build-platform-wheel"),
        _action("test-platform-wheel"),
        _action("quality-validation"),
        _action("source-distribution"),
        _action("native-llvm-coverage"),
    )
    upload_steps = [
        step
        for source in sources
        for step in _step_bodies(source)
        if "actions/upload-artifact@" in step
    ]
    uploads: dict[str, list[str]] = {}
    for step in upload_steps:
        uploads.setdefault(_with_value(step, "name"), []).append(step)
    retention = {
        "dist-wheels-${{ inputs.platform-name }}": "7",
        "source-distribution": "7",
        "release-distributions": "7",
        "pypi-publish-distributions": "7",
    }

    assert set(uploads) == set(retention)
    for name, days in retention.items():
        assert all(_with_value(step, "retention-days") == days for step in uploads[name])
        assert all(_with_value(step, "if-no-files-found") == "error" for step in uploads[name])
        assert all(_with_value(step, "overwrite") == "true" for step in uploads[name])
    assert all(_with_value(step, "path") == "release/" for step in uploads["release-distributions"])
    assert all(
        _with_value(step, "path") == "pypi-publish/"
        for step in uploads["pypi-publish-distributions"]
    )
    assert "actions/upload-artifact@" not in _action("test-platform-wheel")
    assert "actions/upload-artifact@" not in _action("quality-validation")
    assert "actions/upload-artifact@" not in _action("native-llvm-coverage")

    retried = {
        "dist-wheels-${{ inputs.platform-name }}",
        "source-distribution",
        "release-distributions",
        "pypi-publish-distributions",
    }
    assert {name for name, steps in uploads.items() if len(steps) == 2} == retried
    for name in retried:
        first, retry = uploads[name]
        assert "continue-on-error: true" in first
        assert "outcome == 'failure'" in retry
        assert _with_value(retry, "overwrite") == "true"


def test_release_artifact_is_complete_exact_and_self_describing() -> None:
    """CI publishes one immutable package set with its audit manifest."""
    ci = _workflow("ci.yml")
    source_distribution = _action("source-distribution")
    distribution = _job_body(ci, "validation-gate")
    publish_workflow = _workflow("publish.yml")
    reconcile = _job_body(publish_workflow, "reconcile")
    publish = _job_body(publish_workflow, "publish")
    downstream = (ROOT / "meta/ci/release/check_downstream_install.py").read_text(encoding="utf-8")

    build_action = _action("build-platform-wheel")
    assert "python-version: 3.11.9" in build_action
    for version in ("3.12.11", "3.13.15", "3.14.7"):
        assert f"python-version: {version}" in build_action
    assert build_action.count("python -I meta/ci/release/downstream_smoke.py") == 3
    assert _job_needs(ci, "validation-gate") == (
        "platform-wheel-builds",
        *(platform["job-id"] for platform in _TEST_PLATFORM_JOBS),
        "validation-matrix",
    )
    assert source_distribution.count("python -m build --sdist") == 2
    assert "virtualenv==21.7.1" in source_distribution
    assert "python -m pip download --no-deps --only-binary=:all:" in source_distribution
    assert "--dest .work/virtualenv-seed pip==26.2.1" in source_distribution
    pip_seed_sha256 = "71138adf1f4ca900cdb7d289c21b7494329f2332b6d85f0e1c42108c0384ed3e"  # pragma: allowlist secret
    assert pip_seed_sha256 in source_distribution
    assert f'"{pip_seed_sha256}"  # pragma: allowlist secret' in downstream
    assert 'expected_name = "pip-26.2.1-py3-none-any.whl"' in source_distribution
    assert "len(entries) != 1" in source_distribution
    seed_invocation = "python - \"${PIP_SEED_SHA256}\" <<'PY'"
    seed_script = source_distribution.split(f"{seed_invocation}\n", 1)[1].split("\n        PY", 1)[
        0
    ]
    compile(textwrap.dedent(seed_script), "virtualenv-pip-seed", "exec")
    assert 'cmp -- "${first[0]}" "${second[0]}"' in source_distribution
    assert "check_downstream_install.sh" in source_distribution
    assert "shopt -s nullglob" in source_distribution
    assert "if (( ${#wheels[@]} != 1 ))" in source_distribution
    assert 'check_downstream_install.sh "${wheels[0]}"' in source_distribution
    assert "find downstream-wheel" not in source_distribution
    assert "name: source-distribution" in source_distribution
    assert "name: source-distribution" in distribution
    assert "pattern: dist-wheels-*" in distribution
    assert "check_distribution_contents.py --release-set" in distribution
    assert "check_downstream_install.sh" not in distribution
    assert "release/packages/" in distribution
    assert "release/release-manifest.json" in distribution
    assert "name: release-distributions" in distribution
    assert ci.index("Require every validation matrix to succeed") < ci.index(
        "Assemble the auditable release artifact"
    )
    assert "name: release-distributions" in reconcile
    assert "--manifest release/release-manifest.json" in reconcile
    assert "--packages-dir release/packages" in reconcile
    assert "name: pypi-publish-distributions" in reconcile
    assert "name: pypi-publish-distributions" in publish
    assert "packages-dir: pypi-publish/" in publish
    assert "release/release-manifest.json" in ci
    assert '_PIP_VERSION = "26.2.1"' in downstream
    assert pip_seed_sha256 in downstream
    assert "def _validated_seed_directory(" in downstream
    assert "virtualenv seed root must contain exactly" in downstream
    assert "virtualenv pip seed SHA-256 mismatch" in downstream
    for argument in (
        "--creator",
        "builtin",
        "--seeder",
        "app-data",
        "--no-download",
        "--no-periodic-update",
        "--copies",
        "--pip",
        "--no-setuptools",
        "--extra-search-dir",
    ):
        assert f'"{argument}"' in downstream
    assert '"virtualenv"' in downstream
    assert re.search(r'["\']venv["\']', downstream) is None
    assert (
        '_command([python, "-m", "pip", "install", "-c", constraints, "pip==26.2.1"])'
        not in downstream
    )
    assert "pip.__version__ ==" in downstream
    assert 'struct.calcsize("P") * 8' in downstream
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
