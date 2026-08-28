"""Behavioral XML scanner boundary contracts."""

from __future__ import annotations

from pathlib import Path

import pytest


@pytest.mark.parametrize("invalid_whitespace", ("\x0b", "\x0c"))
def test_xml_scanners_reject_non_xml_whitespace(
    tmp_path: Path,
    invalid_whitespace: str,
) -> None:
    """Vertical tab and form feed are not XML 1.0 whitespace."""
    from schema_sanitizer.core_impl.native_runtime import native_core

    path = tmp_path / "invalid-prefix.xml"
    path.write_text(f"{invalid_whitespace}<event/>", encoding="utf-8")

    with pytest.raises(ValueError, match="expected root element"):
        native_core.xml_folder_effective_row_tag([path], "", -1)
