"""Opt-in escaped-quote CSV dialect coverage.

It validates explicit escape-character configuration and confirms escaped quotes remain
correct across heterogeneous CSV directories.
"""

from __future__ import annotations

import inspect

import pytest

import schema_sanitizer as ss
from examples.example_08.local_validation import load_local_csv_directory_to_polars
from schema_sanitizer.options_impl.call_options import normalize_call_options


def test_csv_escape_char_is_explicit_and_validated() -> None:
    """The strict default is unchanged and the opt-in byte cannot be ambiguous."""
    assert normalize_call_options().csv.csv_escape_char == ""
    assert normalize_call_options(csv_escape_char="\\").csv.csv_escape_char == "\\"
    for converter in (
        ss.to_pyarrow,
        ss.to_polars,
        ss.to_pandas,
        ss.to_duckdb,
        ss.to_csv,
        ss.to_jsonl,
        ss.to_parquet,
        ss.iter_batches,
    ):
        assert inspect.signature(converter).parameters["csv_escape_char"].default is None

    with pytest.raises(TypeError, match="csv_escape_char"):
        normalize_call_options(csv_escape_char=1)
    for value in ("", "xx", '"', ",", "\n"):
        with pytest.raises(ValueError, match="csv_escape_char"):
            normalize_call_options(csv_escape_char=value)


@pytest.mark.parametrize("multi_threading", [False, True])
def test_csv_escape_char_decodes_heterogeneous_directory(tmp_path, multi_threading: bool) -> None:
    """Union-mode framing and decoding both honor escaped quotes."""
    (tmp_path / "a.csv").write_bytes(b'id,1/First event\n1,"He said \\"yes\\""\n')
    (tmp_path / "b.csv").write_bytes(b'2/Second event,id\n"line one\nline \\"two\\"",2\n')

    with pytest.raises(ss.SchemaSanitizerInvalidArgumentError):
        ss.to_polars(
            tmp_path,
            input_format="csv",
            input_mode="directory",
            field_name_policy="preserve",
            csv_header_mode="union",
            on_error="stop",
        )

    frame = ss.to_polars(
        tmp_path,
        input_format="csv",
        input_mode="directory",
        field_name_policy="preserve",
        csv_escape_char="\\",
        csv_header_mode="union",
        on_error="stop",
        multi_threading=multi_threading,
    ).clean_data
    assert frame.select(["id", "1/First event", "2/Second event"]).to_dicts() == [
        {"id": "1", "1/First event": 'He said "yes"', "2/Second event": None},
        {"id": "2", "1/First event": None, "2/Second event": 'line one\nline "two"'},
    ]
    direct = ss.to_pyarrow(
        tmp_path / "a.csv",
        input_format="csv",
        field_name_policy="preserve",
        csv_escape_char="\\",
        on_error="stop",
        multi_threading=True,
    ).clean_data
    assert direct["1/First event"].to_pylist() == ['He said "yes"']

    normalized = load_local_csv_directory_to_polars(
        tmp_path,
        multi_threading=multi_threading,
    )
    assert normalized.frame.columns == ["id", "source_file", "ingestion_timestamp", "event"]
    assert normalized.frame.get_column("event").to_list() == [
        [{"event_id": 1, "event_text": "First event", "payload": 'He said "yes"'}],
        [
            {
                "event_id": 2,
                "event_text": "Second event",
                "payload": 'line one\nline "two"',
            }
        ],
    ]
