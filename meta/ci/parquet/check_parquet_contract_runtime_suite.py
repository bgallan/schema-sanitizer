"""Certify production Parquet reader contracts from the full test suite.

The module validates the selected contract manifest and native/PyArrow readiness,
then consumes the functional suite's JUnit report.  Certification fails closed
unless every required node ID appears exactly once, passes, and is not skipped, so CI
retains durable contract evidence without executing those tests twice.
"""

from __future__ import annotations

import ast
import json
import sys
from pathlib import Path
from typing import Any

from defusedxml import ElementTree as DefusedElementTree
from defusedxml.common import DefusedXmlException

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
PARQUET_CONTRACT_RUNTIME_CERTIFICATE_VERSION = 2
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
    if report_nodeid == selected_nodeid:
        return True
    function_nodeid = selected_nodeid.partition("::")[2]
    return "[" not in function_nodeid and report_nodeid.startswith(f"{selected_nodeid}[")


def _xml_local_name(tag: str) -> str:
    """Return an XML tag name without its optional namespace."""
    return tag.rsplit("}", 1)[-1]


def _junit_testcase_report(testcase: Any) -> dict[str, Any]:
    """Convert one pytest JUnit testcase element to a normalized report."""
    classname = str(testcase.get("classname") or "").strip()
    test_name = str(testcase.get("name") or "").strip()
    module_path = classname.replace("\\", "/").replace(".", "/").strip("/")
    if module_path and not module_path.endswith(".py"):
        module_path += ".py"
    nodeid = f"{module_path}::{test_name}" if module_path and test_name else ""
    result_elements = [
        child for child in testcase if _xml_local_name(child.tag) in {"error", "failure", "skipped"}
    ]
    result_names = [_xml_local_name(child.tag) for child in result_elements]
    if not result_names:
        outcome = "passed"
    elif result_names == ["skipped"]:
        outcome = "skipped"
    else:
        outcome = "failed"
    report: dict[str, Any] = {"nodeid": nodeid, "outcome": outcome}
    if result_elements:
        result = result_elements[0]
        reason = str(result.get("message") or result.text or "").strip()
        if reason:
            report["reason"] = reason
    if len(result_names) > 1:
        report["malformed_results"] = result_names
    return report


def _junit_suite_count(
    suite: Any,
    attribute: str,
    issues: list[str],
) -> int | None:
    """Return one required nonnegative JUnit suite counter."""
    raw_count = suite.get(attribute)
    if raw_count is None:
        issues.append(f"JUnit testsuite is missing required {attribute} count")
        return None
    try:
        count = int(raw_count)
    except ValueError:
        issues.append(f"JUnit testsuite has invalid {attribute} count: {raw_count!r}")
        return None
    if count < 0:
        issues.append(f"JUnit testsuite has negative {attribute} count: {count}")
        return None
    return count


def _load_junit_evidence(path: str | Path) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    """Load complete pytest JUnit evidence and fail closed on malformed results."""
    source = Path(path)
    issues: list[str] = []
    reports: list[dict[str, Any]] = []
    if not source.is_file():
        issues.append(f"JUnit report does not exist: {source}")
    else:
        try:
            root = DefusedElementTree.parse(source).getroot()
        except (DefusedXmlException, DefusedElementTree.ParseError, OSError) as exc:
            issues.append(f"JUnit report is not valid XML: {exc}")
        else:
            root_name = _xml_local_name(root.tag)
            if root_name not in {"testsuite", "testsuites"}:
                issues.append(f"JUnit report has unsupported root element: {root.tag}")
            suites = (
                [root]
                if root_name == "testsuite"
                else [child for child in root if _xml_local_name(child.tag) == "testsuite"]
            )
            if not suites:
                issues.append("JUnit report contains no testsuites")
            processed_testcase_count = 0
            for suite in suites:
                nested_suites = [
                    element
                    for element in suite.iter()
                    if element is not suite and _xml_local_name(element.tag) == "testsuite"
                ]
                if nested_suites:
                    issues.append("JUnit report contains unsupported nested testsuites")
                testcases = [child for child in suite if _xml_local_name(child.tag) == "testcase"]
                processed_testcase_count += len(testcases)
                reports.extend(_junit_testcase_report(testcase) for testcase in testcases)

                declared = {
                    attribute: _junit_suite_count(suite, attribute, issues)
                    for attribute in ("tests", "failures", "errors", "skipped")
                }
                actual = {
                    "tests": len(testcases),
                    "failures": sum(
                        _xml_local_name(child.tag) == "failure"
                        for testcase in testcases
                        for child in testcase
                    ),
                    "errors": sum(
                        _xml_local_name(child.tag) == "error"
                        for testcase in testcases
                        for child in testcase
                    ),
                    "skipped": sum(
                        _xml_local_name(child.tag) == "skipped"
                        for testcase in testcases
                        for child in testcase
                    ),
                }
                for attribute, actual_count in actual.items():
                    declared_count = declared[attribute]
                    if declared_count is not None and declared_count != actual_count:
                        issues.append(
                            f"JUnit testsuite {attribute} count does not match its testcase "
                            f"results: declared {declared_count}, found {actual_count}"
                        )
                for attribute in ("failures", "errors"):
                    declared_count = declared[attribute]
                    if declared_count:
                        issues.append(f"JUnit testsuite declares {declared_count} {attribute}")

            all_testcase_count = sum(
                _xml_local_name(element.tag) == "testcase" for element in root.iter()
            )
            if processed_testcase_count != all_testcase_count:
                issues.append("JUnit report contains nested or ungrouped testcases")
            if not reports:
                issues.append("JUnit report contains no testcases")
            for report in reports:
                if not report.get("nodeid"):
                    issues.append("JUnit testcase is missing classname or name")
                if report.get("malformed_results"):
                    issues.append(
                        "JUnit testcase has multiple terminal results: "
                        f"{report.get('nodeid') or '<unknown>'}"
                    )

    failed_count = sum(report.get("outcome") == "failed" for report in reports)
    skipped_count = sum(report.get("outcome") == "skipped" for report in reports)
    evidence: dict[str, Any] = {
        "satisfied": not issues and failed_count == 0,
        "issues": list(dict.fromkeys(issues)),
        "format": "pytest-junit-xml",
        "testcase_count": len(reports),
        "passed_count": sum(report.get("outcome") == "passed" for report in reports),
        "skipped_count": skipped_count,
        "failed_count": failed_count,
    }
    if failed_count:
        evidence["issues"].append(f"JUnit report contains {failed_count} failed testcases")
    return evidence, reports


def _selected_junit_reports(
    *, reports: list[dict[str, Any]], selected_tests: list[str]
) -> list[dict[str, Any]]:
    """Return only JUnit reports belonging to the selected contract tests."""
    return [
        report
        for report in reports
        if any(
            _report_matches_selected_nodeid(str(report.get("nodeid") or ""), selected)
            for selected in selected_tests
        )
    ]


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
            selected_reports = reports_for(selected, reports)
            passed_reports = [
                report for report in selected_reports if report.get("outcome") == "passed"
            ]
            skipped_reports = [
                report for report in selected_reports if report.get("outcome") == "skipped"
            ]
            failed_reports = [
                report for report in selected_reports if report.get("outcome") == "failed"
            ]
            if len(selected_reports) == 1 and len(passed_reports) == 1:
                group_passed.append(selected)
            else:
                group_missing.append(selected)
            if len(selected_reports) > 1:
                issues.append(
                    f"runtime contract selected test appeared more than once in JUnit: {selected}"
                )
            group_skipped.extend(skipped_reports)
            group_failed.extend(failed_reports)
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


def _parse_runtime_suite_args(argv: list[str]) -> tuple[str | None, str | None]:
    """Parse the certificate and full-suite JUnit evidence paths."""
    certificate_output: str | None = None
    junit_xml: str | None = None
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
        elif arg == "--junit-xml":
            index += 1
            if index >= len(argv):
                raise ValueError("--junit-xml requires a path")
            junit_xml = argv[index]
        elif arg.startswith("--junit-xml="):
            junit_xml = arg.split("=", 1)[1]
            if not junit_xml:
                raise ValueError("--junit-xml requires a path")
        else:
            raise ValueError(f"unknown argument: {arg}")
        index += 1
    return certificate_output, junit_xml


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
    junit_evidence: dict[str, Any] | None = None,
    group_execution: dict[str, Any] | None = None,
    reports: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    """Build a durable JSON certificate for the runtime Parquet contracts.

    The CLI prints human-readable progress, but release/CI systems need one
    stable artifact they can archive and inspect later. The certificate is
    intentionally fail-closed: any missing stage, readiness failure, pytest
    failure, selected skip, duplicate/missing result, or missing group pass makes the result
    unsatisfied even if an earlier stage succeeded.
    """
    selection = selection or {}
    readiness = readiness or {}
    junit_evidence = junit_evidence or {}
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
    for issue in list(junit_evidence.get("issues") or []):
        issues.append(f"junit: {issue}")
    for issue in list(group_execution.get("issues") or []):
        issues.append(f"execution: {issue}")
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
        and junit_evidence.get("satisfied") is True
        and group_execution.get("satisfied") is True
        and all_selected_passed
        and all_guarantees_satisfied
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
            "evidence": junit_evidence,
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
    """Certify selected runtime contracts from a complete pytest JUnit report."""
    try:
        certificate_output, junit_xml = _parse_runtime_suite_args(list(argv or []))
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
            junit_evidence=None,
            group_execution=None,
            reports=[],
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
            junit_evidence=None,
            group_execution=None,
            reports=[],
        )
        emit_certificate(certificate)
        print("Parquet contract runtime suite cannot run:", file=sys.stderr)
        for issue in list(readiness.get("issues") or ["unknown readiness failure"]):
            print(f"- {issue}", file=sys.stderr)
        return 1

    if not junit_xml:
        junit_evidence: dict[str, Any] = {
            "satisfied": False,
            "issues": ["--junit-xml is required after readiness passes"],
            "format": "pytest-junit-xml",
            "testcase_count": 0,
            "passed_count": 0,
            "skipped_count": 0,
            "failed_count": 0,
        }
        certificate = _runtime_suite_contract_certificate(
            selection=selection,
            readiness=readiness,
            junit_evidence=junit_evidence,
            group_execution=None,
            reports=[],
        )
        emit_certificate(certificate)
        print("--junit-xml is required for runtime contract certification", file=sys.stderr)
        return 2

    junit_evidence, all_reports = _load_junit_evidence(junit_xml)
    selected_tests = list(selection["selected_tests"])
    selected_reports = _selected_junit_reports(
        reports=all_reports,
        selected_tests=selected_tests,
    )
    group_execution = _runtime_suite_group_execution_summary(
        selected_tests_by_group=selection["selected_tests_by_group"],
        reports=selected_reports,
    )
    passed = [report for report in selected_reports if report.get("outcome") == "passed"]
    failed = [report for report in selected_reports if report.get("outcome") == "failed"]
    skipped = [report for report in selected_reports if report.get("outcome") == "skipped"]
    summary = {
        "selected_tests": len(selected_tests),
        "selected_tests_by_group": selection["selected_tests_by_group"],
        "passed": len(passed),
        "failed": failed,
        "skipped": skipped,
        "group_execution": group_execution,
    }
    print(json.dumps({"runtime_contract_suite": summary}, indent=2, sort_keys=True))
    certificate = _runtime_suite_contract_certificate(
        selection=selection,
        readiness=readiness,
        junit_evidence=junit_evidence,
        group_execution=group_execution,
        reports=selected_reports,
    )
    emit_certificate(certificate)
    if skipped:
        print("Parquet contract runtime suite selected tests were skipped:", file=sys.stderr)
        for report in skipped:
            print(f"- {report['nodeid']}: {report.get('reason', 'skipped')}", file=sys.stderr)
        return 1
    if junit_evidence.get("satisfied") is not True:
        print("Parquet contract runtime JUnit evidence is invalid:", file=sys.stderr)
        for issue in list(junit_evidence.get("issues") or ["unknown JUnit failure"]):
            print(f"- {issue}", file=sys.stderr)
        return 1
    if group_execution.get("satisfied") is not True:
        print(
            "Parquet contract runtime suite did not satisfy every contract group:", file=sys.stderr
        )
        for issue in list(group_execution.get("issues") or ["unknown group execution failure"]):
            print(f"- {issue}", file=sys.stderr)
        return 1
    if len(passed) != len(selected_tests):
        print(
            "Parquet contract runtime suite did not execute every selected test; "
            f"passed {len(passed)} of {len(selected_tests)}",
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
