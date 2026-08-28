"""Tests for the Parquet runtime contract CI suite.

These tests exercise the CI/certificate harness separately from the large
public Parquet API test module to keep both files easier to navigate.
"""

from __future__ import annotations

import json
from pathlib import Path
from types import SimpleNamespace

import pytest


def test_parquet_contract_runtime_suite_selects_no_skip_runtime_contract_tests() -> None:
    """Verify the CI runtime suite covers fallback, native, and nested contracts."""
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


def test_parquet_contract_runtime_suite_plugin_detects_selected_skips() -> None:
    """Verify the CI runtime suite fails closed instead of accepting skips."""
    from meta.ci.parquet.check_parquet_contract_runtime_suite import _NoSkipPlugin

    plugin = _NoSkipPlugin()
    assert hash(plugin) == object.__hash__(plugin)
    plugin.pytest_runtest_logreport(
        SimpleNamespace(
            nodeid="tests/parquet/test_parquet_native_scalar_paths_and_staging.py::test_runtime",
            outcome="skipped",
            skipped=True,
            when="setup",
            longrepr=("file.py", 1, "pyarrow not installed"),
        )
    )
    plugin.pytest_runtest_logreport(
        SimpleNamespace(
            nodeid="tests/parquet/test_parquet_native_scalar_paths_and_staging.py::test_other",
            outcome="passed",
            skipped=False,
            when="call",
            longrepr=None,
        )
    )

    assert plugin.skipped == [
        {
            "nodeid": "tests/parquet/test_parquet_native_scalar_paths_and_staging.py::test_runtime",
            "outcome": "skipped",
            "when": "setup",
            "reason": "pyarrow not installed",
        }
    ]
    assert plugin.passed == [
        {
            "nodeid": "tests/parquet/test_parquet_native_scalar_paths_and_staging.py::test_other",
            "outcome": "passed",
            "when": "call",
        }
    ]


def test_parquet_contract_runtime_suite_fails_closed_when_readiness_fails(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Verify the CI runtime suite fails before pytest when runtime is incomplete."""
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
    """Verify the runtime suite manifest is grouped by the production guarantees."""
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
    """Verify the runtime suite fails closed if a guarantee family is dropped."""
    from meta.ci.parquet.check_parquet_contract_runtime_suite import (
        _validate_runtime_suite_selection,
    )

    status = _validate_runtime_suite_selection(
        groups={
            "safe_pyarrow_fallback": (
                "tests/parquet/test_parquet_native_scalar_paths_and_staging.py::test_spark_flavored_nested_parquet_uses_pyarrow_fallback",
            )
        },
        required_groups=("safe_pyarrow_fallback", "nested_recursive_grammar"),
    )

    assert status["satisfied"] is False
    assert any("required runtime contract group is missing" in issue for issue in status["issues"])


def test_parquet_contract_runtime_suite_selection_detects_a_missing_nodeid() -> None:
    """Verify the runtime suite fails closed if a selected test is missing."""
    from meta.ci.parquet.check_parquet_contract_runtime_suite import (
        _validate_runtime_suite_selection,
    )

    status = _validate_runtime_suite_selection(
        groups={
            "safe_pyarrow_fallback": (
                "tests/parquet/test_parquet_native_scalar_paths_and_staging.py::test_missing_contract",
            )
        },
        required_groups=("safe_pyarrow_fallback",),
    )

    assert status["satisfied"] is False
    assert any("runtime contract test is missing" in issue for issue in status["issues"])


def test_parquet_contract_runtime_suite_fails_closed_when_selection_is_invalid(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Verify the CI runtime suite fails before readiness when the manifest is stale."""
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
    """Verify runtime execution is certified per contract family, not only by totals."""
    from meta.ci.parquet.check_parquet_contract_runtime_suite import (
        _runtime_suite_group_execution_summary,
    )

    groups = {
        "safe_pyarrow_fallback": [
            "tests/parquet/test_parquet_native_scalar_paths_and_staging.py::test_fallback"
        ],
        "nested_projection_contracts": [
            "tests/parquet/test_parquet_native_scalar_paths_and_staging.py::test_projection"
        ],
    }
    reports = [
        {
            "nodeid": "tests/parquet/test_parquet_native_scalar_paths_and_staging.py::test_fallback",
            "outcome": "passed",
        },
        {
            "nodeid": "tests/parquet/test_parquet_native_scalar_paths_and_staging.py::test_projection",
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
    """Verify a green-looking total cannot hide a missing contract family."""
    from meta.ci.parquet.check_parquet_contract_runtime_suite import (
        _runtime_suite_group_execution_summary,
    )

    groups = {
        "schema_sanitizer_native_reader": [
            "tests/parquet/test_parquet_native_scalar_paths_and_staging.py::test_native"
        ],
        "nested_recursive_grammar": [
            "tests/parquet/test_parquet_native_scalar_paths_and_staging.py::test_nested"
        ],
    }
    reports = [
        {
            "nodeid": "tests/parquet/test_parquet_native_scalar_paths_and_staging.py::test_native",
            "outcome": "passed",
        },
        {
            "nodeid": "tests/parquet/test_parquet_native_scalar_paths_and_staging.py::test_unselected",
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
        "tests/parquet/test_parquet_native_scalar_paths_and_staging.py::test_nested"
    ]
    assert any("produced no passing tests" in issue for issue in summary["issues"])
    assert any("test_nested" in issue for issue in summary["issues"])


def test_parquet_contract_runtime_suite_group_execution_summary_matches_parametrized_reports() -> (
    None
):
    """Verify parametrized selected functions count as execution for their group."""
    from meta.ci.parquet.check_parquet_contract_runtime_suite import (
        _runtime_suite_group_execution_summary,
    )

    groups = {
        "nested_recursive_grammar": [
            "tests/parquet/test_parquet_native_scalar_paths_and_staging.py::test_nested_fuzzer"
        ],
    }
    reports = [
        {
            "nodeid": "tests/parquet/test_parquet_native_scalar_paths_and_staging.py::test_nested_fuzzer[seed-0]",
            "outcome": "passed",
        }
    ]

    summary = _runtime_suite_group_execution_summary(
        selected_tests_by_group=groups,
        reports=reports,
    )

    assert summary["satisfied"] is True
    assert summary["passed_by_group"] == groups


def test_parquet_contract_runtime_suite_group_execution_summary_records_skips_and_failures() -> (
    None
):
    """Verify skipped/failed selected tests are reported by contract family."""
    from meta.ci.parquet.check_parquet_contract_runtime_suite import (
        _runtime_suite_group_execution_summary,
    )

    groups = {
        "safe_pyarrow_fallback": [
            "tests/parquet/test_parquet_native_scalar_paths_and_staging.py::test_fallback"
        ],
        "schema_sanitizer_native_reader": [
            "tests/parquet/test_parquet_native_scalar_paths_and_staging.py::test_native"
        ],
    }
    reports = [
        {
            "nodeid": "tests/parquet/test_parquet_native_scalar_paths_and_staging.py::test_fallback",
            "outcome": "skipped",
            "reason": "pyarrow not installed",
        },
        {
            "nodeid": "tests/parquet/test_parquet_native_scalar_paths_and_staging.py::test_native",
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


def test_parquet_contract_runtime_suite_parses_certificate_output_arg() -> None:
    """Verify suite-owned artifact arguments are not forwarded to pytest."""
    from meta.ci.parquet.check_parquet_contract_runtime_suite import _parse_runtime_suite_args

    output, pytest_args = _parse_runtime_suite_args(
        ["--certificate-output", "artifacts/cert.json", "-k", "nested"]
    )

    assert output == "artifacts/cert.json"
    assert pytest_args == ["-k", "nested"]


def test_parquet_contract_runtime_suite_certificate_accepts_full_contract() -> None:
    """Verify the JSON certificate is satisfied only when every guarantee passes."""
    from meta.ci.parquet.check_parquet_contract_runtime_suite import (
        PARQUET_CONTRACT_RUNTIME_GUARANTEE_GROUPS,
        _runtime_suite_contract_certificate,
        _runtime_suite_group_execution_summary,
    )

    selected_by_group = {
        group: [f"tests/parquet/test_parquet_native_scalar_paths_and_staging.py::test_{group}"]
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
        group_execution=group_execution,
        reports=reports,
        pytest_exit_code=0,
    )

    assert certificate["satisfied"] is True
    assert certificate["issues"] == []
    assert set(certificate["guarantees"]) == set(PARQUET_CONTRACT_RUNTIME_GUARANTEE_GROUPS)
    assert all(status["satisfied"] is True for status in certificate["guarantees"].values())
    assert certificate["execution"]["passed_count"] == selection["selected_test_count"]


def test_parquet_contract_runtime_suite_certificate_fails_missing_nested_group() -> None:
    """Verify the certificate fails closed if one nested contract family did not pass."""
    from meta.ci.parquet.check_parquet_contract_runtime_suite import (
        _runtime_suite_contract_certificate,
        _runtime_suite_group_execution_summary,
    )

    selected_by_group = {
        "safe_pyarrow_fallback": [
            "tests/parquet/test_parquet_native_scalar_paths_and_staging.py::test_fallback"
        ],
        "schema_sanitizer_native_reader": [
            "tests/parquet/test_parquet_native_scalar_paths_and_staging.py::test_native"
        ],
        "nested_recursive_grammar": [
            "tests/parquet/test_parquet_native_scalar_paths_and_staging.py::test_nested"
        ],
        "nested_null_empty_row_group_phases": [
            "tests/parquet/test_parquet_native_scalar_paths_and_staging.py::test_phases"
        ],
        "nested_levels_repetition_topology": [
            "tests/parquet/test_parquet_native_scalar_paths_and_staging.py::test_levels"
        ],
        "nested_projection_contracts": [
            "tests/parquet/test_parquet_native_scalar_paths_and_staging.py::test_projection"
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
            "nodeid": "tests/parquet/test_parquet_native_scalar_paths_and_staging.py::test_fallback",
            "outcome": "passed",
        },
        {
            "nodeid": "tests/parquet/test_parquet_native_scalar_paths_and_staging.py::test_native",
            "outcome": "passed",
        },
        {
            "nodeid": "tests/parquet/test_parquet_native_scalar_paths_and_staging.py::test_nested",
            "outcome": "passed",
        },
        {
            "nodeid": "tests/parquet/test_parquet_native_scalar_paths_and_staging.py::test_phases",
            "outcome": "passed",
        },
        {
            "nodeid": "tests/parquet/test_parquet_native_scalar_paths_and_staging.py::test_levels",
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
        group_execution=group_execution,
        reports=reports,
        pytest_exit_code=0,
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
    """Verify CI gets an artifact even when the runtime cannot execute tests."""
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
