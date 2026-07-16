"""Tests for the shared CI release-version validator."""

from __future__ import annotations

import importlib.util
from pathlib import Path
from typing import Any

import pytest


def _validator() -> Any:
    """Load the standalone release-version validation module."""
    path = Path(__file__).resolve().parents[1] / "meta/ci/validate_release_version.py"
    spec = importlib.util.spec_from_file_location("validate_release_version", path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"Could not load {path}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_validate_release_version_accepts_matching_optional_tag(tmp_path: Path) -> None:
    """Canonical versions accept either no tag or their matching v-prefixed tag."""
    version_file = tmp_path / "VERSION"
    version_file.write_text("0.3.8\n", encoding="utf-8")

    assert _validator().validate_release_version(version_file) == "0.3.8"
    assert _validator().validate_release_version(version_file, "v0.3.8") == "0.3.8"


def test_validate_release_version_rejects_invalid_version_or_tag(tmp_path: Path) -> None:
    """Malformed versions and mismatched release tags must fail closed."""
    version_file = tmp_path / "VERSION"
    version_file.write_text("release-0.3.8\n", encoding="utf-8")
    with pytest.raises(ValueError, match="Invalid"):
        _validator().validate_release_version(version_file)

    version_file.write_text("0.3.8\n", encoding="utf-8")
    with pytest.raises(ValueError, match="does not match"):
        _validator().validate_release_version(version_file, "v0.3.7")


def test_release_tag_is_read_from_manual_workflow_event(tmp_path: Path) -> None:
    """Publish validation reads dispatch inputs without shell interpolation."""
    event_file = tmp_path / "event.json"
    event_file.write_text('{"inputs":{"release_tag":"v0.3.8"}}', encoding="utf-8")

    assert _validator().release_tag_from_event(event_file) == "v0.3.8"


def test_publish_confirmation_is_read_from_manual_workflow_event(tmp_path: Path) -> None:
    """Publishing requires the exact confirmation phrase from the event payload."""
    validator = _validator()
    event_file = tmp_path / "event.json"
    event_file.write_text(
        '{"inputs":{"confirm_publish":"publish schema-sanitizer"}}',
        encoding="utf-8",
    )
    validator.require_publish_confirmation(event_file)

    event_file.write_text('{"inputs":{"confirm_publish":"no"}}', encoding="utf-8")
    with pytest.raises(ValueError, match="Refusing upload"):
        validator.require_publish_confirmation(event_file)
