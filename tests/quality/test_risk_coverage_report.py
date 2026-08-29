"""Tests for the focused high-risk coverage report utility.

It requires every declared high-risk module, rejects missing coverage data, enforces
individual floors, and renders actionable gaps.
"""

from __future__ import annotations

from importlib.util import module_from_spec, spec_from_file_location
from pathlib import Path
from types import ModuleType

import pytest


def _load_reporter() -> ModuleType:
    """Load the CI helper without making ``meta`` a runtime package."""
    path = Path(__file__).parents[2] / "meta" / "ci" / "quality" / "report_risk_coverage.py"
    spec = spec_from_file_location("report_risk_coverage", path)
    assert spec is not None and spec.loader is not None
    module = module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _load_coverage_runner() -> ModuleType:
    """Load the deterministic coverage-suite command planner."""
    path = Path(__file__).parents[2] / "meta" / "ci" / "quality" / "run_coverage_suite.py"
    spec = spec_from_file_location("run_coverage_suite", path)
    assert spec is not None and spec.loader is not None
    module = module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _entry(percent: float = 75.0) -> dict[str, object]:
    """Return a minimal coverage.py JSON file entry."""
    total = 1_000
    covered = round(percent * total / 100)
    return {
        "summary": {
            "covered_lines": covered,
            "num_statements": total,
            "covered_branches": 0,
            "num_branches": 0,
            "percent_covered": percent,
        },
        "missing_lines": [3],
        "missing_branches": [[5, 7]],
    }


def _payload(files: dict[str, object], *, covered: int = 44, total: int = 100) -> dict[str, object]:
    """Return coverage JSON with one exact aggregate line/branch fraction."""
    return {
        "files": files,
        "totals": {
            "covered_lines": covered,
            "num_statements": total,
            "covered_branches": 0,
            "num_branches": 0,
        },
    }


def test_render_report_lists_every_risk_module() -> None:
    """The report keeps all configured high-risk modules visible."""
    reporter = _load_reporter()
    payload = _payload({path: _entry() for path in reporter.RISK_MODULES})

    report = reporter.render_report(payload)

    for path in reporter.RISK_MODULES:
        assert f"{path}: 75.0%" in report
        assert f"floor={reporter.MINIMUM_RISK_COVERAGE[path]:.1f}%" in report
    assert "All high-risk module floors passed" in report


def test_render_report_rejects_missing_risk_module() -> None:
    """A changed coverage source configuration cannot silently omit a risk area."""
    reporter = _load_reporter()
    files = {path: _entry() for path in reporter.RISK_MODULES}
    files.pop(reporter.RISK_MODULES[-1])

    with pytest.raises(RuntimeError, match="omitted risk modules"):
        reporter.render_report(_payload(files))


def test_render_report_enforces_each_high_risk_floor() -> None:
    """A focused risk regression fails even if aggregate coverage is unchanged."""
    reporter = _load_reporter()
    files = {path: _entry(100.0) for path in reporter.RISK_MODULES}
    target = reporter.RISK_MODULES[-1]
    files[target] = _entry(reporter.MINIMUM_RISK_COVERAGE[target] - 0.1)

    with pytest.raises(RuntimeError, match="high-risk coverage floor failed"):
        reporter.render_report(_payload(files))


def test_render_report_enforces_the_aggregate_floor_without_rounding() -> None:
    """A displayed 44% cannot hide an exact aggregate below the 44% floor."""
    reporter = _load_reporter()
    files = {path: _entry(100.0) for path in reporter.RISK_MODULES}

    with pytest.raises(RuntimeError, match=r"87/200 .* < 44\.0%"):
        reporter.render_report(_payload(files, covered=87, total=200))

    assert "All high-risk module floors passed" in reporter.render_report(
        _payload(files, covered=88, total=200)
    )


@pytest.mark.parametrize(
    "totals",
    (
        {},
        {
            "covered_lines": True,
            "num_statements": 100,
            "covered_branches": 0,
            "num_branches": 0,
        },
        {
            "covered_lines": 101,
            "num_statements": 100,
            "covered_branches": 0,
            "num_branches": 0,
        },
    ),
)
def test_render_report_rejects_missing_or_impossible_exact_totals(
    totals: dict[str, object],
) -> None:
    """Malformed coverage totals cannot fall back to a rounded percentage field."""
    reporter = _load_reporter()
    payload = _payload({path: _entry(100.0) for path in reporter.RISK_MODULES})
    payload["totals"] = totals

    with pytest.raises(RuntimeError, match="aggregate"):
        reporter.render_report(payload)


def test_python_coverage_suites_use_stable_rerunnable_data_files() -> None:
    """Each contextual suite overwrites one deterministic input to coverage combine."""
    runner = _load_coverage_runner()

    commands = [runner.coverage_command("python", suite) for suite in runner.SUITES["python"]]

    assert commands == [
        runner.coverage_command("python", suite) for suite in runner.SUITES["python"]
    ]
    assert all("--parallel-mode" not in command for command in commands)
    assert {
        argument
        for command in commands
        for argument in command
        if argument.startswith("--data-file=")
    } == {
        "--data-file=.work/coverage/.coverage.python.regular",
        "--data-file=.work/coverage/.coverage.python.adversarial",
        "--data-file=.work/coverage/.coverage.python.integration",
    }


def test_python_coverage_combine_requires_the_exact_suite_set(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A missing, extra, symlinked, or non-regular shard cannot yield a partial report."""
    runner = _load_coverage_runner()
    monkeypatch.setattr(runner, "_COVERAGE_DIRECTORY", tmp_path)
    expected = [tmp_path / f".coverage.python.{suite}" for suite in runner.SUITES["python"]]
    for path in expected:
        path.write_bytes(b"coverage")

    assert runner.validate_coverage_inputs("python") == tuple(expected)

    expected[0].unlink()
    with pytest.raises(RuntimeError, match="missing="):
        runner.validate_coverage_inputs("python")

    expected[0].write_bytes(b"coverage")
    unexpected = tmp_path / ".coverage.python.unexpected"
    unexpected.write_bytes(b"coverage")
    with pytest.raises(RuntimeError, match="unexpected="):
        runner.validate_coverage_inputs("python")

    unexpected.unlink()
    expected[0].unlink()
    expected[0].mkdir()
    with pytest.raises(RuntimeError, match="regular files"):
        runner.validate_coverage_inputs("python")


def test_quality_action_combines_coverage_only_after_exact_input_validation() -> None:
    """The CI command cannot let coverage.py silently combine a surviving subset."""
    action = (
        Path(__file__).parents[2] / ".github/actions/quality-validation/action.yml"
    ).read_text(encoding="utf-8")

    validate = "run_coverage_suite.py python --validate-inputs"
    combine = "coverage combine --strict"
    assert validate in action
    assert combine in action
    assert action.index(validate) < action.index(combine)
    assert "\n        coverage combine\n" not in action
