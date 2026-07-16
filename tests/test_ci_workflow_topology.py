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


def test_general_sanity_owns_all_validation_and_scheduled_jobs() -> None:
    """General sanity owns validation and isolates each scheduled workload."""
    ci = _workflow("ci.yml")
    validation_jobs = (
        "cloud-emulators:",
        "coverage-python:",
        "coverage-native:",
        "downstream-wheel:",
        "downstream-extras:",
        "native-sanitizers:",
    )

    for job in validation_jobs:
        assert f"  {job}" in ci
    assert "uses: ./.github/workflows/" not in ci

    schedules = ("23 3 * * 0", "30 3 * * 1", "47 4 * * 3")
    for schedule in schedules:
        assert ci.count(schedule) == 2
    assert ci.count("github.event_name != 'schedule'") >= 7
