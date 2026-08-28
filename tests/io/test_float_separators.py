"""Regional decimal and thousands separator parsing tests."""

from __future__ import annotations

import pytest
from conftest import read_test_csv, read_test_jsonl, read_test_python


def test_default_float_separators_parse_grouped_strings(require_native: None) -> None:
    result = read_test_python(
        [{"value": "1,234.56"}, {"value": "12.5e2"}],
        parse_floats=True,
    )

    assert result.clean_data.to_pylist() == [{"value": 1234.56}, {"value": 1250.0}]


def test_custom_float_separators_parse_european_strings(require_native: None) -> None:
    result = read_test_python(
        [
            {"value": "1.234,56"},
            {"value": "1234,56"},
            {"value": "1.234,56e2"},
        ],
        parse_floats=True,
        parse_float_decimal_separator=",",
        parse_float_thousands_separator=".",
    )

    assert result.clean_data.to_pylist() == [
        {"value": 1234.56},
        {"value": 1234.56},
        {"value": 123456.0},
    ]


def test_comma_decimal_csv_value_is_parsed_when_quoted(tmp_path, require_native: None) -> None:
    path = tmp_path / "prices.csv"
    path.write_text('value\n"1.234,56"\n', encoding="utf-8")

    result = read_test_csv(
        path,
        parse_floats=True,
        parse_float_decimal_separator=",",
        parse_float_thousands_separator=".",
    )

    assert result.clean_data.to_pylist() == [{"value": 1234.56}]


@pytest.mark.parametrize(
    "value",
    (
        "12,34.56",
        "1,23",
        "1.234,56",
        "1,234,56",
    ),
)
def test_malformed_float_grouping_remains_string(value: str, require_native: None) -> None:
    result = read_test_python([{"value": value}], parse_floats=True)

    assert result.clean_data.to_pylist() == [{"value": value}]


def test_json_number_tokens_ignore_string_float_separators(tmp_path, require_native: None) -> None:
    path = tmp_path / "rows.jsonl"
    path.write_text('{"value":1.5}\n', encoding="utf-8")

    result = read_test_jsonl(
        path,
        parse_floats=True,
        parse_float_decimal_separator=",",
        parse_float_thousands_separator=".",
    )

    assert result.clean_data.to_pylist() == [{"value": 1.5}]


@pytest.mark.parametrize(
    ("decimal", "thousands"),
    (
        ("..", ","),
        (".", ",,"),
        (".", "."),
        ("1", ","),
        (".", " "),
        ("é", "."),
    ),
)
def test_invalid_float_separators_are_rejected(decimal: str, thousands: str) -> None:
    with pytest.raises((TypeError, ValueError), match="parse_float"):
        read_test_python(
            [{"value": "1.0"}],
            parse_floats=True,
            parse_float_decimal_separator=decimal,
            parse_float_thousands_separator=thousands,
        )
