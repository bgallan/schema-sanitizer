"""Source and runtime guards for privacy-safe reader diagnostics."""

from __future__ import annotations

from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]


def test_xml_mismatch_errors_do_not_interpolate_element_names() -> None:
    """Tag mismatches identify the offset without copying user names."""
    element = (ROOT / "cpp/src/internal/parsing/xml/element.cc").read_text(encoding="utf-8")
    scanner = (ROOT / "cpp/src/internal/parsing/streaming/xml/row_scanner_markup.cc").read_text(
        encoding="utf-8"
    )

    for source in (element, scanner):
        assert "closing tag does not match the open element" in source
        assert '": closing tag </"' not in source
        assert '"> does not match <"' not in source


def test_reader_errors_expose_structural_context_without_payloads(tmp_path: Path) -> None:
    """Public parse failures carry format, source, and offsets, not input values."""
    import pytest

    import schema_sanitizer as ss

    secret = "private_value_91e8"
    cases = (
        (
            "xml",
            "xml",
            b"<rows><row><a></row></rows>",
            {"input_format": "xml", "xml_row_tag": "row"},
            14,
        ),
        (
            "csv",
            "csv",
            f'a\n"{secret}\n'.encode(),
            {"input_format": "csv"},
            2,
        ),
        (
            "json",
            "jsonl",
            f'{{"a":"{secret}\\q"}}\n'.encode(),
            {"input_format": "jsonl", "on_error": "stop"},
            25,
        ),
    )

    for expected_format, suffix, payload, options, expected_offset in cases:
        source = tmp_path / f"bad-{expected_format}.{suffix}"
        output = tmp_path / f"bad-{expected_format}.jsonl"
        source.write_bytes(payload)
        with pytest.raises(ss.SchemaSanitizerInvalidArgumentError) as captured:
            ss.to_jsonl(source, output, **options)
        detail = captured.value.detail
        assert detail is not None
        assert detail["format"] == expected_format
        assert detail["source"] == str(source)
        assert detail["stage"] == f"{expected_format}_parse"
        assert detail["byte_offset"] == expected_offset
        assert secret not in str(captured.value)
        assert secret not in repr(detail)


def test_reader_depth_limit_is_structured(tmp_path: Path) -> None:
    """Safety-limit failures expose the limit name/value programmatically."""
    import pytest

    import schema_sanitizer as ss

    source = tmp_path / "too-deep.xml"
    output = tmp_path / "too-deep.jsonl"
    source.write_bytes((b"<a>" * 513) + b"x" + (b"</a>" * 513))

    with pytest.raises(ss.SchemaSanitizerInvalidArgumentError) as captured:
        ss.to_jsonl(source, output, input_format="xml")

    detail = captured.value.detail
    assert detail is not None
    assert detail["format"] == "xml"
    assert detail["limit_name"] == "parser_depth"
    assert detail["limit_value"] == 512
    assert detail["byte_offset"] == 1536
