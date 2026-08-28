#!/usr/bin/env python3
"""Exercise a release wheel without importing anything from the repository.

The program performs a public conversion and validates rows, metadata, and imports from
only the installed distribution.
"""

from __future__ import annotations

import json
import tempfile
from pathlib import Path

import pyarrow.parquet as pq

import schema_sanitizer as ss


def _assert_rows(path: Path, expected: int) -> None:
    """Assert that a JSONL output contains the expected number of rows."""
    rows = [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines()]
    assert len(rows) == expected, (path, rows)


def main() -> None:
    """Exercise public converters against an installed wheel."""
    root = Path(tempfile.mkdtemp(prefix="schema-sanitizer-downstream-"))

    csv_source = root / "events.csv"
    csv_source.write_text("id,name\n1,Alice\n2,Bob\n", encoding="utf-8")
    csv_jsonl = root / "csv.jsonl"
    ss.to_jsonl(csv_source, csv_jsonl, input_format="csv")
    _assert_rows(csv_jsonl, 2)

    jsonl_source = root / "events.jsonl"
    jsonl_source.write_text('{"id":1,"value":"x"}\n{"id":2,"value":"y"}\n', encoding="utf-8")
    jsonl_csv = root / "jsonl.csv"
    ss.to_csv(jsonl_source, jsonl_csv, input_format="jsonl")
    assert len(jsonl_csv.read_text(encoding="utf-8").splitlines()) == 3

    xml_source = root / "events.xml"
    xml_source.write_text(
        "<root><row><id>1</id><name>A</name></row><row><id>2</id><name>B</name></row></root>",
        encoding="utf-8",
    )
    xml_jsonl = root / "xml.jsonl"
    ss.to_jsonl(xml_source, xml_jsonl, input_format="xml", xml_row_tag="row")
    _assert_rows(xml_jsonl, 2)

    parquet_output = root / "events.parquet"
    ss.to_parquet(jsonl_source, parquet_output, input_format="jsonl")
    table = pq.read_table(parquet_output)
    assert table.num_rows == 2
    assert {"id", "value"}.issubset(table.column_names)

    print(f"downstream smoke passed with schema-sanitizer {ss.__version__}")


if __name__ == "__main__":
    main()
