"""Tests for strict-first string scalar parsing with whitespace retry.

It verifies strict-first scalar parsing with ASCII-trim retry while preserving exact
Boolean-token precedence.
"""

from __future__ import annotations

import datetime as dt

import pytest
from conftest import read_test_csv, read_test_jsonl, read_test_python

pytestmark = pytest.mark.usefixtures("require_native")


def _parsing_options() -> dict[str, object]:
    """Return parsing options shared by whitespace retry cases."""
    return {
        "parse_integers": True,
        "parse_floats": True,
        "true_tokens": ("yes",),
        "false_tokens": ("no",),
        "parse_iso_timestamps": True,
        "parse_iso_dates": True,
        "parse_iso_times": True,
        "custom_date_patterns": (r"(\d{4})#(\d{2})#(\d{2})",),
    }


def _expected_row() -> dict[str, object]:
    """Return the expected typed row after whitespace retry parsing."""
    return {
        "blank": " \t\r\n",
        "boolean": True,
        "customdate": dt.date(2026, 1, 3),
        "date": dt.date(2026, 1, 2),
        "floating": 1234.5,
        "integer": 123456,
        "text": " keep surrounding spaces ",
        "time": dt.time(3, 4, 5),
        "timestamp": dt.datetime(2026, 1, 2, 3, 4, 5),
    }


def test_python_strings_retry_scalar_parsing_after_ascii_trim() -> None:
    """Verify python strings retry scalar parsing after ascii trim."""
    result = read_test_python(
        [
            {
                "integer": " \t123456\r\n",
                "floating": "\v1,234.50\f",
                "boolean": " yes ",
                "timestamp": " 2026-01-02T03:04:05Z ",
                "date": "\t2026-01-02\n",
                "time": "\r03:04:05 ",
                "custom_date": " 2026#01#03 ",
                "text": " keep surrounding spaces ",
                "blank": " \t\r\n",
            }
        ],
        **_parsing_options(),
    )

    assert result.clean_data.to_pylist() == [_expected_row()]


def test_json_strings_retry_scalar_parsing_after_ascii_trim(tmp_path) -> None:
    """Verify JSON strings retry scalar parsing after ascii trim."""
    path = tmp_path / "rows.jsonl"
    path.write_text(
        '{"integer":" 123456 ","floating":" 1,234.50 ","boolean":" yes ",'
        '"timestamp":" 2026-01-02T03:04:05Z ","date":" 2026-01-02 ",'
        '"time":" 03:04:05 ","custom_date":" 2026#01#03 ",'
        '"text":" keep surrounding spaces ","blank":" \\t\\r\\n"}\n',
        encoding="utf-8",
    )

    result = read_test_jsonl(path, **_parsing_options())

    assert result.clean_data.to_pylist() == [_expected_row()]


def test_quoted_csv_cell_retries_integer_parsing_after_trim(tmp_path) -> None:
    """Verify quoted CSV cell retries integer parsing after trim."""
    path = tmp_path / "rows.csv"
    path.write_text('value\n" 123456 "\n', encoding="utf-8")

    result = read_test_csv(path, parse_integers=True)

    assert result.clean_data.to_pylist() == [{"value": 123456}]


def test_exact_boolean_token_is_checked_before_trimmed_retry() -> None:
    """Verify exact boolean token is checked before trimmed retry."""
    result = read_test_python(
        [{"value": " yes "}],
        true_tokens=(" yes ",),
        false_tokens=(" no ",),
    )

    assert result.clean_data.to_pylist() == [{"value": True}]
