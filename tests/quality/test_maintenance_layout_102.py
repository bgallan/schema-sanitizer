"""Protect ownership and hot-path cleanup introduced by maintenance layout 102."""

from __future__ import annotations

from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]


def test_analytical_conversion_has_direct_bounded_owners() -> None:
    """Analytical orchestration is direct and old package routes stay removed."""
    api_impl = ROOT / "src/schema_sanitizer/api_impl"
    owner = api_impl / "analytical.py"
    output = api_impl / "results.py"

    assert owner.is_file()
    assert output.is_file()
    assert not (api_impl / "analytical").exists()
    assert len(owner.read_text(encoding="utf-8").splitlines()) <= 600
    source = owner.read_text(encoding="utf-8")
    assert "call_options_from_locals(options, ANALYTICAL_HELPER_KEYS)" in source
    assert "call_options_from_locals(dict(options)" not in source


def test_xml_token_matching_has_one_header_only_xml_owner() -> None:
    """Shared XML matching lives with XML parsing and has no duplicate TU."""
    parsing = ROOT / "cpp/src/internal/parsing"
    owner = parsing / "xml/token_match.hh"
    old_header = parsing / "streaming/xml_token_match.hh"
    old_source = parsing / "streaming/xml_token_match.cc"

    assert owner.is_file()
    assert not old_header.exists()
    assert not old_source.exists()
    source = owner.read_text(encoding="utf-8")
    assert "std::ranges::all_of" in source
    assert "std::ranges::equal" in source
    assert "std::tolower" not in source
    assert "value == ' '" in source
    manifest = (ROOT / "cmake/SchemaSanitizerSources.cmake").read_text(encoding="utf-8")
    assert "xml_token_match.cc" not in manifest


def test_registry_stream_validation_does_not_allocate_sink_names() -> None:
    """Registry stream-only methods compare borrowed sink names directly."""
    registry = ROOT / "cpp/src/api/python_abi3/registry"
    sources = "\n".join(path.read_text(encoding="utf-8") for path in registry.glob("*.cc"))
    assert 'std::string(sink_name) != "stream"' not in sources
    assert sources.count('std::string_view(sink_name) != "stream"') >= 7


@pytest.mark.parametrize("invalid_ws", ("\v", "\f"))
def test_xml_scanners_reject_non_xml_whitespace(tmp_path: Path, invalid_ws: str) -> None:
    """Vertical tab and form feed are not XML 1.0 whitespace."""
    from schema_sanitizer.core_impl.native_runtime import native_core

    path = tmp_path / "invalid-prefix.xml"
    path.write_text(f"{invalid_ws}<event/>", encoding="utf-8")

    with pytest.raises(ValueError, match="expected root element"):
        native_core.xml_folder_effective_row_tag([path], "", -1)
