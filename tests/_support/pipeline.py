"""Source builders shared by pipeline warm-up tests."""

from __future__ import annotations

from pathlib import Path

import pytest


def _write_warm_up_source(
    tmp_path: Path,
    input_format: str,
    input_mode: str,
    name: str,
    field_name: str,
) -> Path:
    """Write one warm-up source for a public input format/mode pair."""
    folder = tmp_path / f"{input_format}-{input_mode}-{name}"
    if input_mode == "directory":
        folder.mkdir()
        path = folder / f"part.{_input_suffix(input_format)}"
    else:
        path = folder.with_suffix(f".{_input_suffix(input_format)}")

    if input_format == "csv":
        path.write_text(
            f"alpha,beta\n{1 if field_name == 'alpha' else ''},{2 if field_name == 'beta' else ''}\n",
            encoding="utf-8",
        )
    elif input_format == "json":
        path.write_text(f'{{"{field_name}": 1}}', encoding="utf-8")
    elif input_format == "json_array":
        path.write_text(f'[{{"{field_name}": 1}}]', encoding="utf-8")
    elif input_format == "jsonl":
        path.write_text(f'{{"{field_name}": 1}}\n', encoding="utf-8")
    elif input_format == "xml":
        path.write_text(f"<row><{field_name}>1</{field_name}></row>", encoding="utf-8")
    elif input_format == "parquet":
        pa = pytest.importorskip("pyarrow")
        pq = pytest.importorskip("pyarrow.parquet")
        table = pa.table({field_name: [1]})
        pq.write_table(table, path)
    else:  # pragma: no cover - exhaustive guard for future parametrization changes
        raise AssertionError(f"Unhandled input_format={input_format!r}")
    return path if input_mode == "single_file" else folder


def _input_suffix(input_format: str) -> str:
    """Return the public suffix for one input format."""
    if input_format == "json_array":
        return "json"
    if input_format == "parquet":
        return "parquet"
    return input_format
