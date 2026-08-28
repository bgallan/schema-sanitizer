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


def _entry(percent: float = 75.0) -> dict[str, object]:
    """Return a minimal coverage.py JSON file entry."""
    return {
        "summary": {"percent_covered": percent},
        "missing_lines": [3],
        "missing_branches": [[5, 7]],
    }


def test_render_report_lists_every_risk_module() -> None:
    """The report keeps all configured high-risk modules visible."""
    reporter = _load_reporter()
    payload = {"files": {path: _entry() for path in reporter.RISK_MODULES}}

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
        reporter.render_report({"files": files})


def test_render_report_enforces_each_high_risk_floor() -> None:
    """A focused risk regression fails even if aggregate coverage is unchanged."""
    reporter = _load_reporter()
    files = {path: _entry(100.0) for path in reporter.RISK_MODULES}
    target = reporter.RISK_MODULES[-1]
    files[target] = _entry(reporter.MINIMUM_RISK_COVERAGE[target] - 0.1)

    with pytest.raises(RuntimeError, match="high-risk coverage floor failed"):
        reporter.render_report({"files": files})
