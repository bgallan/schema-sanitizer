#!/usr/bin/env python3
"""Exercise ABI3-backed public conversion on the active downstream interpreter.

The smoke deliberately keeps its data and assertions small: it proves that an
installed wheel can cross the Python/native Arrow boundary in both directions
without repeating the broader downstream and platform test suites.
"""

from __future__ import annotations

import csv
import json
import os
import tempfile
from pathlib import Path

import schema_sanitizer as ss
from schema_sanitizer.core_impl.native_runtime import native_core


def _require_installed_extension() -> None:
    """Require an installed extension or the explicitly named sanitizer build."""
    checkout = Path.cwd().resolve()
    extension = Path(native_core.__file__).resolve()
    expected_root = os.environ.get("SCHEMA_SANITIZER_ABI_SMOKE_NATIVE_ROOT")
    if expected_root is not None:
        root = Path(expected_root).resolve(strict=True)
        if extension != root and not extension.is_relative_to(root):
            raise AssertionError(f"ABI smoke loaded {extension}, expected a build below {root}")
        return
    if extension.is_relative_to(checkout):
        raise AssertionError(f"ABI smoke loaded the checkout extension: {extension}")


def main() -> None:
    """Round-trip tiny CSV and JSONL fixtures through public native converters."""
    _require_installed_extension()
    with tempfile.TemporaryDirectory(prefix="schema-sanitizer-abi-smoke-") as raw_root:
        root = Path(raw_root)
        csv_input = root / "input.csv"
        jsonl_output = root / "output.jsonl"
        csv_input.write_text("id,name\n1,Alice\n2,Bob\n", encoding="utf-8")
        ss.to_jsonl(
            csv_input,
            jsonl_output,
            input_format="csv",
            parse_integers=True,
        )
        rows = [json.loads(line) for line in jsonl_output.read_text(encoding="utf-8").splitlines()]
        if [row["id"] for row in rows] != [1, 2]:
            raise AssertionError(f"unexpected JSONL conversion rows: {rows!r}")

        jsonl_input = root / "roundtrip-input.jsonl"
        jsonl_input.write_text(
            '{"id":1,"name":"Alice"}\n{"id":2,"name":"Bob"}\n',
            encoding="utf-8",
        )
        csv_output = root / "roundtrip.csv"
        ss.to_csv(jsonl_input, csv_output, input_format="jsonl")
        with csv_output.open(encoding="utf-8", newline="") as handle:
            roundtrip = list(csv.DictReader(handle))
        if [row["name"] for row in roundtrip] != ["Alice", "Bob"]:
            raise AssertionError(f"unexpected CSV conversion rows: {roundtrip!r}")
    print(f"ABI3 public conversion smoke passed with schema-sanitizer {ss.__version__}")


if __name__ == "__main__":
    main()
