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


def test_general_sanity_owns_all_pr_validation_modules() -> None:
    """PR validation modules run only through the general sanity orchestrator."""
    ci = _workflow("ci.yml")
    modules = (
        "cloud-emulators.yml",
        "coverage.yml",
        "downstream-packaging.yml",
        "native-sanitizers.yml",
    )

    for module in modules:
        source = _workflow(module)
        assert f"uses: ./.github/workflows/{module}" in ci
        assert "  workflow_call:" in source
        assert "  workflow_dispatch:" not in source
        assert "  pull_request:" not in source
        assert "  push:" not in source
