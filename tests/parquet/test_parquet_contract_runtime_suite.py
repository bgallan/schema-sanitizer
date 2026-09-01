"""Tests for the Parquet runtime contract CI suite.

These tests exercise the CI/certificate harness separately from the large public Parquet
API test module to keep both files easier to navigate.
"""

from __future__ import annotations

import json
from pathlib import Path
from xml.etree import ElementTree

import pytest


def _write_junit_report(
    path: Path,
    nodeids: list[str],
    *,
    outcomes: dict[str, str] | None = None,
) -> None:
    """Write a compact pytest-compatible JUnit report for the supplied node IDs."""
    outcomes = outcomes or {}
    root = ElementTree.Element("testsuites")
    selected_outcomes = [outcomes.get(nodeid, "passed") for nodeid in nodeids]
    suite = ElementTree.SubElement(
        root,
        "testsuite",
        tests=str(len(nodeids)),
        failures=str(selected_outcomes.count("failure")),
        errors=str(selected_outcomes.count("error")),
        skipped=str(selected_outcomes.count("skipped")),
    )
    for nodeid in nodeids:
        relative_path, test_name = nodeid.split("::", 1)
        classname = relative_path.removesuffix(".py").replace("/", ".")
        testcase = ElementTree.SubElement(
            suite,
            "testcase",
            classname=classname,
            name=test_name,
        )
        outcome = outcomes.get(nodeid, "passed")
        if outcome != "passed":
            ElementTree.SubElement(testcase, outcome, message=f"{outcome} by fixture")
    ElementTree.ElementTree(root).write(path, encoding="utf-8", xml_declaration=True)


def test_parquet_contract_runtime_suite_selects_no_skip_runtime_contract_tests() -> None:
    """Verify Parquet contract runtime suite selects no skip runtime contract tests."""
    from meta.ci.parquet.check_parquet_contract_runtime_suite import (
        PARQUET_CONTRACT_RUNTIME_TESTS,
    )

    selected = list(PARQUET_CONTRACT_RUNTIME_TESTS)

    assert selected
    assert len(selected) == len(set(selected))
    assert all(
        test.startswith("tests/parquet/test_parquet_") and "::test_" in test for test in selected
    )
    assert any("fallback" in test for test in selected)
    assert any("native_parquet_stream_materializes_plain" in test for test in selected)
    assert any("cartesian_recursive_grammar_corpus" in test for test in selected)
    assert any("recursive_null_empty_matrix_corpus" in test for test in selected)
    assert any("recursive_row_group_phase_matrix_corpus" in test for test in selected)
    assert any("projection_permutations" in test for test in selected)


def test_parquet_contract_runtime_suite_loads_passes_and_skips_from_junit(
    tmp_path: Path,
) -> None:
    """Verify the certificate reader normalizes pytest JUnit outcomes and node IDs."""
    from meta.ci.parquet.check_parquet_contract_runtime_suite import _load_junit_evidence

    report_path = tmp_path / "pytest.xml"
    report_path.write_text(
        """<?xml version="1.0" encoding="utf-8"?>
<testsuites><testsuite tests="2" failures="0" errors="0" skipped="1">
  <testcase classname="tests.parquet.test_parquet_native_scalar_cases"
            name="test_runtime[param]" />
  <testcase classname="tests.parquet.test_parquet_native_scalar_cases"
            name="test_other"><skipped message="pyarrow not installed" /></testcase>
</testsuite></testsuites>
""",
        encoding="utf-8",
    )

    evidence, reports = _load_junit_evidence(report_path)

    assert evidence == {
        "satisfied": True,
        "issues": [],
        "format": "pytest-junit-xml",
        "testcase_count": 2,
        "passed_count": 1,
        "skipped_count": 1,
        "failed_count": 0,
    }
    assert reports == [
        {
            "nodeid": "tests/parquet/test_parquet_native_scalar_cases.py::test_runtime[param]",
            "outcome": "passed",
        },
        {
            "nodeid": "tests/parquet/test_parquet_native_scalar_cases.py::test_other",
            "outcome": "skipped",
            "reason": "pyarrow not installed",
        },
    ]


def test_parquet_contract_runtime_suite_rejects_inconsistent_junit_summary(
    tmp_path: Path,
) -> None:
    """Verify declared failures and inconsistent counts make JUnit evidence invalid."""
    from meta.ci.parquet.check_parquet_contract_runtime_suite import _load_junit_evidence

    report_path = tmp_path / "pytest.xml"
    report_path.write_text(
        """<testsuites><testsuite tests="2" failures="1" errors="0" skipped="0">
<testcase classname="tests.parquet.test_runtime" name="test_passes" />
</testsuite></testsuites>""",
        encoding="utf-8",
    )

    evidence, reports = _load_junit_evidence(report_path)

    assert evidence["satisfied"] is False
    assert len(reports) == 1
    assert any("declares 1 failures" in issue for issue in evidence["issues"])
    assert any(
        "tests count" in issue and "declared 2, found 1" in issue for issue in evidence["issues"]
    )
    assert any(
        "failures count" in issue and "declared 1, found 0" in issue for issue in evidence["issues"]
    )


def test_parquet_contract_runtime_suite_rejects_junit_entities(tmp_path: Path) -> None:
    """JUnit evidence cannot define or expand attacker-controlled XML entities."""
    from meta.ci.parquet.check_parquet_contract_runtime_suite import _load_junit_evidence

    report_path = tmp_path / "pytest.xml"
    report_path.write_text(
        """<!DOCTYPE testsuites [<!ENTITY forged "test_forged">]>
<testsuites><testsuite tests="1" failures="0" errors="0" skipped="0">
<testcase classname="tests.parquet.test_runtime" name="&forged;" />
</testsuite></testsuites>""",
        encoding="utf-8",
    )

    evidence, reports = _load_junit_evidence(report_path)

    assert evidence["satisfied"] is False
    assert reports == []
    assert any("not valid XML" in issue for issue in evidence["issues"])


@pytest.mark.parametrize("missing_attribute", ["tests", "failures", "errors", "skipped"])
def test_parquet_contract_runtime_suite_requires_complete_junit_summary(
    tmp_path: Path,
    missing_attribute: str,
) -> None:
    """Verify every pytest JUnit suite must declare all four outcome counters."""
    from meta.ci.parquet.check_parquet_contract_runtime_suite import _load_junit_evidence

    attributes = {
        "tests": "1",
        "failures": "0",
        "errors": "0",
        "skipped": "0",
    }
    del attributes[missing_attribute]
    root = ElementTree.Element("testsuites")
    suite = ElementTree.SubElement(root, "testsuite", attributes)
    ElementTree.SubElement(
        suite,
        "testcase",
        classname="tests.parquet.test_runtime",
        name="test_passes",
    )
    report_path = tmp_path / "pytest.xml"
    ElementTree.ElementTree(root).write(report_path, encoding="utf-8", xml_declaration=True)

    evidence, reports = _load_junit_evidence(report_path)

    assert evidence["satisfied"] is False
    assert len(reports) == 1
    assert any(
        f"missing required {missing_attribute} count" in issue for issue in evidence["issues"]
    )


def test_parquet_contract_runtime_suite_rejects_inconsistent_skipped_summary(
    tmp_path: Path,
) -> None:
    """Verify a declared skip without a skipped testcase cannot certify."""
    from meta.ci.parquet.check_parquet_contract_runtime_suite import _load_junit_evidence

    report_path = tmp_path / "pytest.xml"
    report_path.write_text(
        """<testsuites><testsuite tests="1" failures="0" errors="0" skipped="1">
<testcase classname="tests.parquet.test_runtime" name="test_passes" />
</testsuite></testsuites>""",
        encoding="utf-8",
    )

    evidence, reports = _load_junit_evidence(report_path)

    assert evidence["satisfied"] is False
    assert reports == [
        {
            "nodeid": "tests/parquet/test_runtime.py::test_passes",
            "outcome": "passed",
        }
    ]
    assert any(
        "skipped count" in issue and "declared 1, found 0" in issue for issue in evidence["issues"]
    )


def test_parquet_contract_runtime_suite_fails_closed_when_readiness_fails(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Verify Parquet contract runtime suite fails closed when readiness fails."""
    from meta.ci.parquet import check_parquet_contract_runtime_suite as suite

    monkeypatch.setattr(
        suite,
        "parquet_contract_runtime_readiness_status",
        lambda **_: {
            "satisfied": False,
            "issues": ["PyArrow is required for the safe fallback contract"],
        },
    )

    assert suite.main([]) == 1


def test_parquet_contract_runtime_suite_manifest_groups_cover_every_contract_family() -> None:
    """Verify Parquet contract runtime suite manifest groups cover every contract family."""
    from meta.ci.parquet.check_parquet_contract_runtime_suite import (
        PARQUET_CONTRACT_RUNTIME_REQUIRED_GROUPS,
        PARQUET_CONTRACT_RUNTIME_TEST_GROUPS,
        PARQUET_CONTRACT_RUNTIME_TESTS,
        _validate_runtime_suite_selection,
    )

    status = _validate_runtime_suite_selection()

    assert status["satisfied"] is True
    assert status["issues"] == []
    assert set(PARQUET_CONTRACT_RUNTIME_REQUIRED_GROUPS) == {
        "schema_sanitizer_native_reader",
        "safe_pyarrow_fallback",
        "nested_recursive_grammar",
        "nested_null_empty_row_group_phases",
        "nested_levels_repetition_topology",
        "nested_projection_contracts",
    }
    assert all(
        PARQUET_CONTRACT_RUNTIME_TEST_GROUPS[group]
        for group in PARQUET_CONTRACT_RUNTIME_REQUIRED_GROUPS
    )
    assert set(status["selected_tests"]) == set(PARQUET_CONTRACT_RUNTIME_TESTS)
    assert status["coverage_by_group"] == {
        group: True for group in PARQUET_CONTRACT_RUNTIME_REQUIRED_GROUPS
    }


def test_parquet_contract_runtime_suite_selection_detects_missing_group() -> None:
    """Verify Parquet contract runtime suite selection detects missing group."""
    from meta.ci.parquet.check_parquet_contract_runtime_suite import (
        _validate_runtime_suite_selection,
    )

    status = _validate_runtime_suite_selection(
        groups={
            "safe_pyarrow_fallback": (
                "tests/parquet/test_parquet_native_scalar_cases.py::test_spark_flavored_nested_parquet_uses_pyarrow_fallback",
            )
        },
        required_groups=("safe_pyarrow_fallback", "nested_recursive_grammar"),
    )

    assert status["satisfied"] is False
    assert any("required runtime contract group is missing" in issue for issue in status["issues"])


def test_parquet_contract_runtime_suite_selection_detects_a_missing_nodeid() -> None:
    """Verify Parquet contract runtime suite selection detects a missing nodeid."""
    from meta.ci.parquet.check_parquet_contract_runtime_suite import (
        _validate_runtime_suite_selection,
    )

    status = _validate_runtime_suite_selection(
        groups={
            "safe_pyarrow_fallback": (
                "tests/parquet/test_parquet_native_scalar_cases.py::test_missing_contract",
            )
        },
        required_groups=("safe_pyarrow_fallback",),
    )

    assert status["satisfied"] is False
    assert any("runtime contract test is missing" in issue for issue in status["issues"])


def test_parquet_contract_runtime_suite_fails_closed_when_selection_is_invalid(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Verify Parquet contract runtime suite fails closed when selection is invalid."""
    from meta.ci.parquet import check_parquet_contract_runtime_suite as suite

    monkeypatch.setattr(
        suite,
        "_validate_runtime_suite_selection",
        lambda: {"satisfied": False, "issues": ["stale test nodeid"]},
    )
    monkeypatch.setattr(
        suite,
        "parquet_contract_runtime_readiness_status",
        lambda **_: (_ for _ in ()).throw(AssertionError("readiness should not run")),
    )

    assert suite.main([]) == 1


def test_parquet_contract_runtime_suite_group_execution_summary_accepts_group_passes() -> None:
    """Verify Parquet contract runtime suite group execution summary accepts group passes."""
    from meta.ci.parquet.check_parquet_contract_runtime_suite import (
        _runtime_suite_group_execution_summary,
    )

    groups = {
        "safe_pyarrow_fallback": [
            "tests/parquet/test_parquet_native_scalar_cases.py::test_fallback"
        ],
        "nested_projection_contracts": [
            "tests/parquet/test_parquet_native_scalar_cases.py::test_projection"
        ],
    }
    reports = [
        {
            "nodeid": "tests/parquet/test_parquet_native_scalar_cases.py::test_fallback",
            "outcome": "passed",
        },
        {
            "nodeid": "tests/parquet/test_parquet_native_scalar_cases.py::test_projection",
            "outcome": "passed",
        },
    ]

    summary = _runtime_suite_group_execution_summary(
        selected_tests_by_group=groups,
        reports=reports,
    )

    assert summary["satisfied"] is True
    assert summary["issues"] == []
    assert summary["passed_by_group"] == groups
    assert summary["missing_passes_by_group"] == {
        "safe_pyarrow_fallback": [],
        "nested_projection_contracts": [],
    }


def test_parquet_contract_runtime_suite_group_execution_summary_detects_missing_group_pass() -> (
    None
):
    """Verify Parquet contract runtime suite group execution summary detects missing group pass."""
    from meta.ci.parquet.check_parquet_contract_runtime_suite import (
        _runtime_suite_group_execution_summary,
    )

    groups = {
        "schema_sanitizer_native_reader": [
            "tests/parquet/test_parquet_native_scalar_cases.py::test_native"
        ],
        "nested_recursive_grammar": [
            "tests/parquet/test_parquet_native_scalar_cases.py::test_nested"
        ],
    }
    reports = [
        {
            "nodeid": "tests/parquet/test_parquet_native_scalar_cases.py::test_native",
            "outcome": "passed",
        },
        {
            "nodeid": "tests/parquet/test_parquet_native_scalar_cases.py::test_unselected",
            "outcome": "passed",
        },
    ]

    summary = _runtime_suite_group_execution_summary(
        selected_tests_by_group=groups,
        reports=reports,
    )

    assert summary["satisfied"] is False
    assert summary["passed_by_group"]["nested_recursive_grammar"] == []
    assert summary["missing_passes_by_group"]["nested_recursive_grammar"] == [
        "tests/parquet/test_parquet_native_scalar_cases.py::test_nested"
    ]
    assert any("produced no passing tests" in issue for issue in summary["issues"])
    assert any("test_nested" in issue for issue in summary["issues"])


def test_parquet_contract_runtime_suite_group_execution_summary_matches_parametrized_reports() -> (
    None
):
    """Verify Parquet contract runtime suite group execution summary matches parametrized reports."""
    from meta.ci.parquet.check_parquet_contract_runtime_suite import (
        _runtime_suite_group_execution_summary,
    )

    groups = {
        "nested_recursive_grammar": [
            "tests/parquet/test_parquet_native_scalar_cases.py::test_nested_fuzzer"
        ],
    }
    reports = [
        {
            "nodeid": "tests/parquet/test_parquet_native_scalar_cases.py::test_nested_fuzzer[seed-0]",
            "outcome": "passed",
        }
    ]

    summary = _runtime_suite_group_execution_summary(
        selected_tests_by_group=groups,
        reports=reports,
    )

    assert summary["satisfied"] is True
    assert summary["passed_by_group"] == groups


def test_parquet_contract_runtime_suite_requires_exact_selected_parameter_ids() -> None:
    """An extra parameter suffix cannot impersonate an exact selected case."""
    from meta.ci.parquet.check_parquet_contract_runtime_suite import (
        _report_matches_selected_nodeid,
    )

    selected = "tests/parquet/test_runtime.py::test_contract[required-id]"

    assert _report_matches_selected_nodeid(selected, selected) is True
    assert _report_matches_selected_nodeid(f"{selected}[extra]", selected) is False
    assert (
        _report_matches_selected_nodeid(
            "tests/parquet/test_runtime.py::test_contract[generated-id]",
            "tests/parquet/test_runtime.py::test_contract",
        )
        is True
    )


def test_parquet_contract_runtime_suite_group_execution_summary_records_skips_and_failures() -> (
    None
):
    """Verify Parquet contract runtime suite group execution summary records skips and failures."""
    from meta.ci.parquet.check_parquet_contract_runtime_suite import (
        _runtime_suite_group_execution_summary,
    )

    groups = {
        "safe_pyarrow_fallback": [
            "tests/parquet/test_parquet_native_scalar_cases.py::test_fallback"
        ],
        "schema_sanitizer_native_reader": [
            "tests/parquet/test_parquet_native_scalar_cases.py::test_native"
        ],
    }
    reports = [
        {
            "nodeid": "tests/parquet/test_parquet_native_scalar_cases.py::test_fallback",
            "outcome": "skipped",
            "reason": "pyarrow not installed",
        },
        {
            "nodeid": "tests/parquet/test_parquet_native_scalar_cases.py::test_native",
            "outcome": "failed",
        },
    ]

    summary = _runtime_suite_group_execution_summary(
        selected_tests_by_group=groups,
        reports=reports,
    )

    assert summary["satisfied"] is False
    assert (
        summary["skipped_by_group"]["safe_pyarrow_fallback"][0]["reason"] == "pyarrow not installed"
    )
    assert summary["failed_by_group"]["schema_sanitizer_native_reader"][0]["nodeid"].endswith(
        "test_native"
    )
    assert any("selected test skipped" in issue for issue in summary["issues"])
    assert any("selected test failed" in issue for issue in summary["issues"])


def test_parquet_contract_runtime_suite_group_execution_rejects_duplicate_junit_results() -> None:
    """Verify duplicated selected node IDs cannot satisfy runtime certification."""
    from meta.ci.parquet.check_parquet_contract_runtime_suite import (
        _runtime_suite_group_execution_summary,
    )

    selected = "tests/parquet/test_parquet_native_scalar_cases.py::test_native"
    report = {"nodeid": selected, "outcome": "passed"}

    summary = _runtime_suite_group_execution_summary(
        selected_tests_by_group={"schema_sanitizer_native_reader": [selected]},
        reports=[report, dict(report)],
    )

    assert summary["satisfied"] is False
    assert summary["passed_by_group"]["schema_sanitizer_native_reader"] == []
    assert any("appeared more than once" in issue for issue in summary["issues"])


def test_parquet_contract_runtime_suite_parses_certificate_output_arg() -> None:
    """Verify Parquet contract runtime suite parses certificate output arg."""
    from meta.ci.parquet.check_parquet_contract_runtime_suite import _parse_runtime_suite_args

    output, junit_xml = _parse_runtime_suite_args(
        [
            "--certificate-output",
            "artifacts/cert.json",
            "--junit-xml=artifacts/pytest.xml",
        ]
    )

    assert output == "artifacts/cert.json"
    assert junit_xml == "artifacts/pytest.xml"


def test_parquet_contract_runtime_suite_certificate_accepts_full_contract() -> None:
    """Verify Parquet contract runtime suite certificate accepts full contract."""
    from meta.ci.parquet.check_parquet_contract_runtime_suite import (
        PARQUET_CONTRACT_RUNTIME_GUARANTEE_GROUPS,
        _runtime_suite_contract_certificate,
        _runtime_suite_group_execution_summary,
    )

    selected_by_group = {
        group: [f"tests/parquet/test_parquet_native_scalar_cases.py::test_{group}"]
        for groups in PARQUET_CONTRACT_RUNTIME_GUARANTEE_GROUPS.values()
        for group in groups
    }
    selection = {
        "satisfied": True,
        "issues": [],
        "selected_tests_by_group": selected_by_group,
        "selected_tests": [test for tests in selected_by_group.values() for test in tests],
        "selected_test_count": len(selected_by_group),
    }
    readiness = {"satisfied": True, "issues": []}
    reports = [
        {"nodeid": test, "outcome": "passed"}
        for tests in selected_by_group.values()
        for test in tests
    ]
    group_execution = _runtime_suite_group_execution_summary(
        selected_tests_by_group=selected_by_group,
        reports=reports,
    )

    certificate = _runtime_suite_contract_certificate(
        selection=selection,
        readiness=readiness,
        junit_evidence={"satisfied": True, "issues": []},
        group_execution=group_execution,
        reports=reports,
    )

    assert certificate["satisfied"] is True
    assert certificate["issues"] == []
    assert set(certificate["guarantees"]) == set(PARQUET_CONTRACT_RUNTIME_GUARANTEE_GROUPS)
    assert all(status["satisfied"] is True for status in certificate["guarantees"].values())
    assert certificate["execution"]["passed_count"] == selection["selected_test_count"]


def test_parquet_contract_runtime_suite_certificate_fails_missing_nested_group() -> None:
    """Verify Parquet contract runtime suite certificate fails missing nested group."""
    from meta.ci.parquet.check_parquet_contract_runtime_suite import (
        _runtime_suite_contract_certificate,
        _runtime_suite_group_execution_summary,
    )

    selected_by_group = {
        "safe_pyarrow_fallback": [
            "tests/parquet/test_parquet_native_scalar_cases.py::test_fallback"
        ],
        "schema_sanitizer_native_reader": [
            "tests/parquet/test_parquet_native_scalar_cases.py::test_native"
        ],
        "nested_recursive_grammar": [
            "tests/parquet/test_parquet_native_scalar_cases.py::test_nested"
        ],
        "nested_null_empty_row_group_phases": [
            "tests/parquet/test_parquet_native_scalar_cases.py::test_phases"
        ],
        "nested_levels_repetition_topology": [
            "tests/parquet/test_parquet_native_scalar_cases.py::test_levels"
        ],
        "nested_projection_contracts": [
            "tests/parquet/test_parquet_native_scalar_cases.py::test_projection"
        ],
    }
    selection = {
        "satisfied": True,
        "issues": [],
        "selected_tests_by_group": selected_by_group,
        "selected_tests": [test for tests in selected_by_group.values() for test in tests],
        "selected_test_count": 6,
    }
    reports = [
        {
            "nodeid": "tests/parquet/test_parquet_native_scalar_cases.py::test_fallback",
            "outcome": "passed",
        },
        {
            "nodeid": "tests/parquet/test_parquet_native_scalar_cases.py::test_native",
            "outcome": "passed",
        },
        {
            "nodeid": "tests/parquet/test_parquet_native_scalar_cases.py::test_nested",
            "outcome": "passed",
        },
        {
            "nodeid": "tests/parquet/test_parquet_native_scalar_cases.py::test_phases",
            "outcome": "passed",
        },
        {
            "nodeid": "tests/parquet/test_parquet_native_scalar_cases.py::test_levels",
            "outcome": "passed",
        },
    ]
    group_execution = _runtime_suite_group_execution_summary(
        selected_tests_by_group=selected_by_group,
        reports=reports,
    )

    certificate = _runtime_suite_contract_certificate(
        selection=selection,
        readiness={"satisfied": True, "issues": []},
        junit_evidence={"satisfied": True, "issues": []},
        group_execution=group_execution,
        reports=reports,
    )

    assert certificate["satisfied"] is False
    assert certificate["guarantees"]["pipeline_safe_fallback_with_pyarrow"]["satisfied"] is True
    assert certificate["guarantees"]["schema_sanitizer_native_reader"]["satisfied"] is True
    assert certificate["guarantees"]["nested_arbitrary_native_grammar"]["satisfied"] is False
    assert any("test_projection" in issue for issue in certificate["issues"])


def test_parquet_contract_runtime_suite_writes_certificate_on_readiness_failure(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """Verify Parquet contract runtime suite writes certificate on readiness failure."""
    from meta.ci.parquet import check_parquet_contract_runtime_suite as suite

    output = tmp_path / "parquet-contract-runtime-certificate.json"
    monkeypatch.setattr(
        suite,
        "parquet_contract_runtime_readiness_status",
        lambda **_: {
            "satisfied": False,
            "issues": ["PyArrow is required for the safe fallback contract"],
        },
    )

    assert suite.main(["--certificate-output", str(output)]) == 1
    certificate = json.loads(output.read_text(encoding="utf-8"))

    assert certificate["satisfied"] is False
    assert certificate["selection"]["satisfied"] is True
    assert certificate["readiness"]["satisfied"] is False
    assert certificate["guarantees"]["pipeline_safe_fallback_with_pyarrow"]["satisfied"] is False
    assert any("PyArrow is required" in issue for issue in certificate["issues"])


def test_parquet_contract_runtime_suite_certifies_full_suite_junit_without_rerunning_pytest(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """Verify the CLI certifies selected contracts from the full suite's one test run."""
    from meta.ci.parquet import check_parquet_contract_runtime_suite as suite

    junit_xml = tmp_path / "pytest-memory-parquet.xml"
    certificate_output = tmp_path / "certificate.json"
    selected = list(suite.PARQUET_CONTRACT_RUNTIME_TESTS)
    _write_junit_report(
        junit_xml,
        [
            *selected,
            "tests/memory/test_unselected.py::test_full_suite_evidence_is_retained",
        ],
    )
    monkeypatch.setattr(
        suite,
        "parquet_contract_runtime_readiness_status",
        lambda **_: {"satisfied": True, "issues": []},
    )

    result = suite.main(
        [
            "--junit-xml",
            str(junit_xml),
            "--certificate-output",
            str(certificate_output),
        ]
    )

    assert result == 0
    certificate = json.loads(certificate_output.read_text(encoding="utf-8"))
    assert certificate["schema_version"] == 2
    assert certificate["satisfied"] is True
    assert certificate["execution"]["evidence"]["testcase_count"] == len(selected) + 1
    assert certificate["execution"]["selected_test_count"] == len(selected)
    assert certificate["execution"]["passed_count"] == len(selected)


def test_parquet_contract_runtime_suite_certifies_an_unrelated_junit_failure(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """A failed full suite still emits durable unsatisfied Parquet evidence."""
    from meta.ci.parquet import check_parquet_contract_runtime_suite as suite

    junit_xml = tmp_path / "pytest-memory-parquet.xml"
    certificate_output = tmp_path / "evidence/certificate.json"
    selected = list(suite.PARQUET_CONTRACT_RUNTIME_TESTS)
    unrelated = "tests/memory/test_unselected.py::test_unrelated_failure"
    _write_junit_report(
        junit_xml,
        [*selected, unrelated],
        outcomes={unrelated: "failure"},
    )
    monkeypatch.setattr(
        suite,
        "parquet_contract_runtime_readiness_status",
        lambda **_: {"satisfied": True, "issues": []},
    )

    result = suite.main(
        [
            "--junit-xml",
            str(junit_xml),
            "--certificate-output",
            str(certificate_output),
        ]
    )

    assert result == 1
    assert certificate_output.is_file()
    certificate = json.loads(certificate_output.read_text(encoding="utf-8"))
    assert certificate["satisfied"] is False
    assert certificate["execution"]["evidence"]["failed_count"] == 1
    assert certificate["execution"]["selected_test_count"] == len(selected)
    assert certificate["execution"]["passed_count"] == len(selected)
    assert any("1 failed testcases" in issue for issue in certificate["issues"])


def test_parquet_contract_runtime_suite_rejects_a_selected_skip_in_junit(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """Verify a selected skip fails certification even when the JUnit file is valid."""
    from meta.ci.parquet import check_parquet_contract_runtime_suite as suite

    junit_xml = tmp_path / "pytest-memory-parquet.xml"
    selected = list(suite.PARQUET_CONTRACT_RUNTIME_TESTS)
    _write_junit_report(
        junit_xml,
        selected,
        outcomes={selected[0]: "skipped"},
    )
    monkeypatch.setattr(
        suite,
        "parquet_contract_runtime_readiness_status",
        lambda **_: {"satisfied": True, "issues": []},
    )

    assert suite.main(["--junit-xml", str(junit_xml)]) == 1
