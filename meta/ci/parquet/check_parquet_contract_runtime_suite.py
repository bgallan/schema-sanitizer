"""Fail-closed runtime suite for the production Parquet reader contracts.

It validates the selected contract manifest, rejects skips, executes each required
group, and emits certificate-ready evidence.
"""

from __future__ import annotations

import ast
import json
import sys
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from schema_sanitizer.adapters.parquet.status import (
    parquet_contract_runtime_readiness_status,
)

# These tests are deliberately end-to-end and selected to cover the three
# product guarantees in one fail-closed CI gate:
#   1. safe PyArrow fallback when native cannot/should not serve a file;
#   2. native reader for schema-sanitizer-written files;
#   3. arbitrary nested list/map/struct grammar inside the native writer/reader
#      contract, including null/empty/full profiles, row-group segmentation,
#      projection permutation, and level/repetition topology.
#
# Keep this grouped manifest, rather than a flat list only, so CI can fail closed
# if a future refactor accidentally drops one contract family from the selected
# runtime suite.
PARQUET_CONTRACT_RUNTIME_TEST_GROUPS: dict[str, tuple[str, ...]] = {
    "schema_sanitizer_native_reader": (
        "tests/parquet/test_parquet_native_scalar_cases.py::test_native_scalar_case[native_parquet_stream_materializes_plain_fixed_width_rows]",
        "tests/parquet/test_parquet_native_scalar_cases.py::test_native_scalar_case[native_parquet_stream_materializes_plain_boolean_rows]",
    ),
    "safe_pyarrow_fallback": (
        "tests/parquet/test_parquet_native_scalar_cases.py::test_native_scalar_case[native_parquet_stream_respects_small_batch_size_with_pyarrow_fallback]",
        "tests/parquet/test_parquet_external_fallbacks.py::test_spark_flavored_nested_parquet_uses_pyarrow_fallback",
        "tests/parquet/test_parquet_external_fallbacks.py::test_pyarrow_deprecated_nested_list_map_encoding_uses_pyarrow_fallback",
    ),
    "nested_recursive_grammar": (
        "tests/parquet/test_parquet_native_recursive_cases.py::test_native_recursive_case[native_parquet_stream_materializes_cartesian_recursive_grammar_corpus]",
        "tests/parquet/test_parquet_native_recursive_cases.py::test_native_recursive_case[native_parquet_stream_materializes_seeded_recursive_fuzzer_corpus]",
    ),
    "nested_null_empty_row_group_phases": (
        "tests/parquet/test_parquet_native_recursive_cases.py::test_native_recursive_case[native_parquet_stream_materializes_recursive_null_empty_matrix_corpus]",
        "tests/parquet/test_parquet_native_recursive_cases.py::test_native_recursive_case[native_parquet_stream_materializes_recursive_row_group_phase_matrix_corpus]",
    ),
    "nested_levels_repetition_topology": (
        "tests/parquet/test_parquet_direct_directory_schema_and_batches.py::test_native_parquet_stream_materializes_deep_requiredness_level_matrix",
    ),
    "nested_projection_contracts": (
        "tests/parquet/test_parquet_native_recursive_cases.py::test_native_recursive_case[native_parquet_stream_preserves_recursive_root_fingerprints_under_projection_permutations]",
    ),
}
PARQUET_CONTRACT_RUNTIME_REQUIRED_GROUPS: tuple[str, ...] = tuple(
    PARQUET_CONTRACT_RUNTIME_TEST_GROUPS
)
PARQUET_CONTRACT_RUNTIME_TESTS: tuple[str, ...] = tuple(
    dict.fromkeys(
        test
        for group in PARQUET_CONTRACT_RUNTIME_REQUIRED_GROUPS
        for test in PARQUET_CONTRACT_RUNTIME_TEST_GROUPS[group]
    )
)
PARQUET_CONTRACT_RUNTIME_CERTIFICATE_VERSION = 1
PARQUET_CONTRACT_RUNTIME_GUARANTEE_GROUPS: dict[str, tuple[str, ...]] = {
    "pipeline_safe_fallback_with_pyarrow": ("safe_pyarrow_fallback",),
    "schema_sanitizer_native_reader": ("schema_sanitizer_native_reader",),
    "nested_arbitrary_native_grammar": (
        "nested_recursive_grammar",
        "nested_null_empty_row_group_phases",
        "nested_levels_repetition_topology",
        "nested_projection_contracts",
    ),
}


@dataclass(eq=False)
class _NoSkipPlugin:
    """Collect selected pytest outcomes and fail if any selected test skips."""

    reports: list[dict[str, Any]] = field(default_factory=list)

    def pytest_runtest_logreport(
        self, report: Any
    ) -> None:  # pragma: no cover - exercised by pytest
        """Capture each selected test's call or skip report for certification."""
        if report.when != "call" and not report.skipped:
            return
        entry: dict[str, Any] = {
            "nodeid": report.nodeid,
            "outcome": report.outcome,
            "when": report.when,
        }
        if report.skipped:
            longrepr = report.longrepr
            if isinstance(longrepr, tuple) and len(longrepr) >= 3:
                entry["reason"] = str(longrepr[2])
            else:
                entry["reason"] = str(longrepr)
        self.reports.append(entry)

    @property
    def skipped(self) -> list[dict[str, Any]]:
        """Return captured reports whose selected tests were skipped."""
        return [report for report in self.reports if report.get("outcome") == "skipped"]

    @property
    def passed(self) -> list[dict[str, Any]]:
        """Return captured reports whose selected tests passed."""
        return [report for report in self.reports if report.get("outcome") == "passed"]

    @property
    def failed(self) -> list[dict[str, Any]]:
        """Return captured reports whose selected tests failed."""
        return [report for report in self.reports if report.get("outcome") == "failed"]


def _nodeid_parts(nodeid: str) -> tuple[str, str | None]:
    """Return ``(relative_path, function_name)`` for a simple pytest nodeid."""
    path, sep, remainder = nodeid.partition("::")
    if not sep or not remainder:
        return path, None
    function = remainder.split("[", 1)[0].split("::", 1)[0]
    return path, function or None


def _test_function_names_from_file(path: Path) -> set[str]:
    """Return top-level test function names defined in ``path``."""
    try:
        module = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
    except OSError:
        return set()
    return {
        node.name
        for node in module.body
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))
    }


def _validate_runtime_suite_selection(
    *,
    base_dir: str | Path | None = None,
    groups: dict[str, tuple[str, ...]] | None = None,
    required_groups: tuple[str, ...] | None = None,
) -> dict[str, Any]:
    """Validate that the runtime contract suite is complete and current.

    This does not replace pytest execution. It catches cheaper failure modes
    before launching pytest: duplicated nodeids, empty contract groups, selected
    tests that are missing, and required guarantee families absent from the manifest.
    """
    root = Path(base_dir or Path(__file__).resolve().parents[3])
    suite_groups = dict(groups or PARQUET_CONTRACT_RUNTIME_TEST_GROUPS)
    required = tuple(required_groups or PARQUET_CONTRACT_RUNTIME_REQUIRED_GROUPS)
    issues: list[str] = []
    selected_by_group: dict[str, list[str]] = {}
    selected: list[str] = []

    for group in required:
        if group not in suite_groups:
            issues.append(f"required runtime contract group is missing: {group}")
            selected_by_group[group] = []
            continue
        tests = list(suite_groups.get(group) or ())
        selected_by_group[group] = tests
        if not tests:
            issues.append(f"required runtime contract group has no selected tests: {group}")
        selected.extend(tests)

    duplicate_tests = sorted({test for test in selected if selected.count(test) > 1})
    for test in duplicate_tests:
        issues.append(f"runtime contract test is selected more than once: {test}")

    functions_by_file: dict[str, set[str]] = {}
    for nodeid in selected:
        relative_path, function = _nodeid_parts(nodeid)
        if not relative_path or function is None:
            issues.append(f"runtime contract nodeid is not a test function: {nodeid}")
            continue
        path = root / relative_path
        if not path.is_file():
            issues.append(f"runtime contract test file does not exist: {relative_path}")
            continue
        if relative_path not in functions_by_file:
            functions_by_file[relative_path] = _test_function_names_from_file(path)
        if function not in functions_by_file[relative_path]:
            issues.append(f"runtime contract test is missing: {nodeid}")

    coverage = {group: bool(selected_by_group.get(group)) for group in required}
    return {
        "satisfied": not issues,
        "issues": list(dict.fromkeys(issues)),
        "required_groups": list(required),
        "coverage_by_group": coverage,
        "selected_tests_by_group": {
            group: list(tests) for group, tests in selected_by_group.items()
        },
        "selected_tests": list(dict.fromkeys(selected)),
        "selected_test_count": len(dict.fromkeys(selected)),
    }


def _report_matches_selected_nodeid(report_nodeid: str, selected_nodeid: str) -> bool:
    """Return whether a pytest report belongs to a selected function nodeid."""
    return report_nodeid == selected_nodeid or report_nodeid.startswith(f"{selected_nodeid}[")


def _runtime_suite_group_execution_summary(
    *,
    selected_tests_by_group: dict[str, list[str]],
    reports: list[dict[str, Any]],
) -> dict[str, Any]:
    """Summarize runtime execution by contractual family.

    The manifest-level validator proves that every contract family is selected.
    This post-pytest summary proves that every selected family actually executed
    and passed. It prevents a future pytest/plugin refactor from producing a
    misleading green result where the total pass count looks plausible but one
    contract family did not produce a passing report.
    """
    passed_reports = [report for report in reports if report.get("outcome") == "passed"]
    skipped_reports = [report for report in reports if report.get("outcome") == "skipped"]
    failed_reports = [report for report in reports if report.get("outcome") == "failed"]
    issues: list[str] = []
    passed_by_group: dict[str, list[str]] = {}
    missing_passes_by_group: dict[str, list[str]] = {}
    skipped_by_group: dict[str, list[dict[str, Any]]] = {}
    failed_by_group: dict[str, list[dict[str, Any]]] = {}

    def reports_for(selected: str, candidates: list[dict[str, Any]]) -> list[dict[str, Any]]:
        """Return reports matching one selected node ID or its parameterized cases."""
        return [
            report
            for report in candidates
            if _report_matches_selected_nodeid(str(report.get("nodeid") or ""), selected)
        ]

    for group, selected_tests in selected_tests_by_group.items():
        group_passed: list[str] = []
        group_missing: list[str] = []
        group_skipped: list[dict[str, Any]] = []
        group_failed: list[dict[str, Any]] = []
        for selected in selected_tests:
            if reports_for(selected, passed_reports):
                group_passed.append(selected)
            else:
                group_missing.append(selected)
            group_skipped.extend(reports_for(selected, skipped_reports))
            group_failed.extend(reports_for(selected, failed_reports))
        passed_by_group[group] = group_passed
        missing_passes_by_group[group] = group_missing
        skipped_by_group[group] = group_skipped
        failed_by_group[group] = group_failed
        if not group_passed:
            issues.append(f"runtime contract group produced no passing tests: {group}")
        for selected in group_missing:
            issues.append(f"runtime contract selected test did not pass: {selected}")
        for report in group_skipped:
            issues.append(f"runtime contract selected test skipped: {report.get('nodeid')}")
        for report in group_failed:
            issues.append(f"runtime contract selected test failed: {report.get('nodeid')}")

    return {
        "satisfied": not issues,
        "issues": list(dict.fromkeys(issues)),
        "passed_by_group": passed_by_group,
        "missing_passes_by_group": missing_passes_by_group,
        "skipped_by_group": skipped_by_group,
        "failed_by_group": failed_by_group,
    }


def _parse_runtime_suite_args(argv: list[str]) -> tuple[str | None, list[str]]:
    """Split suite-owned options from extra pytest arguments."""
    certificate_output: str | None = None
    pytest_args: list[str] = []
    index = 0
    while index < len(argv):
        arg = argv[index]
        if arg == "--certificate-output":
            index += 1
            if index >= len(argv):
                raise ValueError("--certificate-output requires a path")
            certificate_output = argv[index]
        elif arg.startswith("--certificate-output="):
            certificate_output = arg.split("=", 1)[1]
            if not certificate_output:
                raise ValueError("--certificate-output requires a path")
        else:
            pytest_args.append(arg)
        index += 1
    return certificate_output, pytest_args


def _selected_groups_satisfied(
    *,
    group_execution: dict[str, Any] | None,
    groups: tuple[str, ...],
) -> bool:
    """Return whether every selected group has all selected tests passed."""
    if not group_execution:
        return False
    passed_by_group = group_execution.get("passed_by_group") or {}
    missing_by_group = group_execution.get("missing_passes_by_group") or {}
    skipped_by_group = group_execution.get("skipped_by_group") or {}
    failed_by_group = group_execution.get("failed_by_group") or {}
    for group in groups:
        if not passed_by_group.get(group):
            return False
        if missing_by_group.get(group) or skipped_by_group.get(group) or failed_by_group.get(group):
            return False
    return True


def _runtime_suite_contract_certificate(
    *,
    selection: dict[str, Any] | None,
    readiness: dict[str, Any] | None = None,
    group_execution: dict[str, Any] | None = None,
    reports: list[dict[str, Any]] | None = None,
    pytest_exit_code: int | None = None,
) -> dict[str, Any]:
    """Build a durable JSON certificate for the runtime Parquet contracts.

    The CLI prints human-readable progress, but release/CI systems need one
    stable artifact they can archive and inspect later. The certificate is
    intentionally fail-closed: any missing stage, readiness failure, pytest
    failure, selected skip, or missing group pass makes the overall result
    unsatisfied even if an earlier stage succeeded.
    """
    selection = selection or {}
    readiness = readiness or {}
    group_execution = group_execution or {}
    reports = list(reports or [])
    skipped = [report for report in reports if report.get("outcome") == "skipped"]
    failed = [report for report in reports if report.get("outcome") == "failed"]
    passed = [report for report in reports if report.get("outcome") == "passed"]
    issues: list[str] = []

    for issue in list(selection.get("issues") or []):
        issues.append(f"selection: {issue}")
    for issue in list(readiness.get("issues") or []):
        issues.append(f"readiness: {issue}")
    for issue in list(group_execution.get("issues") or []):
        issues.append(f"execution: {issue}")
    if pytest_exit_code not in (None, 0):
        issues.append(f"pytest exited with status {pytest_exit_code}")
    for report in skipped:
        issues.append(f"selected test skipped: {report.get('nodeid')}")
    for report in failed:
        issues.append(f"selected test failed: {report.get('nodeid')}")

    guarantee_status: dict[str, dict[str, Any]] = {}
    for guarantee, groups in PARQUET_CONTRACT_RUNTIME_GUARANTEE_GROUPS.items():
        group_ok = _selected_groups_satisfied(group_execution=group_execution, groups=groups)
        guarantee_issues: list[str] = []
        if readiness.get("satisfied") is not True:
            guarantee_issues.append("runtime readiness did not pass")
        if not group_ok:
            guarantee_issues.append(
                "not every selected test for this guarantee produced a passing report"
            )
        guarantee_status[guarantee] = {
            "satisfied": bool(readiness.get("satisfied") is True and group_ok),
            "groups": list(groups),
            "issues": guarantee_issues,
        }

    all_guarantees_satisfied = all(
        status.get("satisfied") is True for status in guarantee_status.values()
    )
    selected_test_count = int(selection.get("selected_test_count") or 0)
    all_selected_passed = bool(
        selected_test_count and len(passed) == selected_test_count and not skipped and not failed
    )
    satisfied = bool(
        selection.get("satisfied") is True
        and readiness.get("satisfied") is True
        and group_execution.get("satisfied") is True
        and all_selected_passed
        and all_guarantees_satisfied
        and pytest_exit_code == 0
    )

    return {
        "schema_version": PARQUET_CONTRACT_RUNTIME_CERTIFICATE_VERSION,
        "certificate": "schema-sanitizer-parquet-runtime-contract-suite",
        "satisfied": satisfied,
        "issues": list(dict.fromkeys(issues)),
        "guarantees": guarantee_status,
        "selection": selection,
        "readiness": readiness,
        "execution": {
            "pytest_exit_code": pytest_exit_code,
            "selected_test_count": selected_test_count,
            "passed_count": len(passed),
            "skipped_count": len(skipped),
            "failed_count": len(failed),
            "group_execution": group_execution,
        },
    }


def _write_runtime_suite_certificate(path: str | Path, certificate: dict[str, Any]) -> None:
    """Write the runtime contract certificate as stable JSON."""
    output = Path(path)
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(certificate, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def main(argv: list[str] | None = None) -> int:
    """Run the selected runtime contract suite and fail closed on skips."""
    try:
        certificate_output, pytest_args = _parse_runtime_suite_args(list(argv or []))
    except ValueError as exc:
        print(str(exc), file=sys.stderr)
        return 2

    def emit_certificate(certificate: dict[str, Any]) -> None:
        """Print the runtime certificate and persist it when an output was requested."""
        print(json.dumps({"runtime_contract_certificate": certificate}, indent=2, sort_keys=True))
        if certificate_output:
            _write_runtime_suite_certificate(certificate_output, certificate)

    selection = _validate_runtime_suite_selection()
    print(json.dumps({"runtime_contract_suite_selection": selection}, indent=2, sort_keys=True))
    if selection.get("satisfied") is not True:
        certificate = _runtime_suite_contract_certificate(
            selection=selection,
            readiness=None,
            group_execution=None,
            reports=[],
            pytest_exit_code=None,
        )
        emit_certificate(certificate)
        print("Parquet contract runtime suite selection is invalid:", file=sys.stderr)
        for issue in list(selection.get("issues") or ["unknown selection failure"]):
            print(f"- {issue}", file=sys.stderr)
        return 1

    readiness = parquet_contract_runtime_readiness_status(
        require_pyarrow=True,
        require_native=True,
    )
    print(json.dumps({"runtime_readiness": readiness}, indent=2, sort_keys=True))
    if readiness.get("satisfied") is not True:
        certificate = _runtime_suite_contract_certificate(
            selection=selection,
            readiness=readiness,
            group_execution=None,
            reports=[],
            pytest_exit_code=None,
        )
        emit_certificate(certificate)
        print("Parquet contract runtime suite cannot run:", file=sys.stderr)
        for issue in list(readiness.get("issues") or ["unknown readiness failure"]):
            print(f"- {issue}", file=sys.stderr)
        return 1

    try:
        import pytest
    except Exception as exc:  # pragma: no cover - CI dependency guard
        certificate = _runtime_suite_contract_certificate(
            selection=selection,
            readiness=readiness,
            group_execution=None,
            reports=[],
            pytest_exit_code=None,
        )
        emit_certificate(certificate)
        print(f"pytest is required for the Parquet contract runtime suite: {exc}", file=sys.stderr)
        return 1

    plugin = _NoSkipPlugin()
    selected_tests = tuple(selection["selected_tests"])
    args = ["-q", "--tb=short", *selected_tests, *pytest_args]
    exit_code = int(pytest.main(args, plugins=[plugin]))
    group_execution = _runtime_suite_group_execution_summary(
        selected_tests_by_group=selection["selected_tests_by_group"],
        reports=plugin.reports,
    )
    summary = {
        "selected_tests": len(selected_tests),
        "selected_tests_by_group": selection["selected_tests_by_group"],
        "passed": len(plugin.passed),
        "failed": plugin.failed,
        "skipped": plugin.skipped,
        "group_execution": group_execution,
    }
    print(json.dumps({"runtime_contract_suite": summary}, indent=2, sort_keys=True))
    certificate = _runtime_suite_contract_certificate(
        selection=selection,
        readiness=readiness,
        group_execution=group_execution,
        reports=plugin.reports,
        pytest_exit_code=exit_code,
    )
    emit_certificate(certificate)
    if plugin.skipped:
        print("Parquet contract runtime suite selected tests were skipped:", file=sys.stderr)
        for report in plugin.skipped:
            print(f"- {report['nodeid']}: {report.get('reason', 'skipped')}", file=sys.stderr)
        return 1
    if exit_code != 0:
        return exit_code
    if group_execution.get("satisfied") is not True:
        print(
            "Parquet contract runtime suite did not satisfy every contract group:", file=sys.stderr
        )
        for issue in list(group_execution.get("issues") or ["unknown group execution failure"]):
            print(f"- {issue}", file=sys.stderr)
        return 1
    if len(plugin.passed) != len(selected_tests):
        print(
            "Parquet contract runtime suite did not execute every selected test; "
            f"passed {len(plugin.passed)} of {len(selected_tests)}",
            file=sys.stderr,
        )
        return 1
    if certificate.get("satisfied") is not True:
        print("Parquet contract runtime certificate is not satisfied:", file=sys.stderr)
        for issue in list(certificate.get("issues") or ["unknown certificate failure"]):
            print(f"- {issue}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
