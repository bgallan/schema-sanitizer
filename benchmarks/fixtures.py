"""Synthetic fixture writers used by local ingestion benchmarks."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any


def write_jsonl(path: Path, rows: int, width: int) -> None:
    """Write a flat JSONL fixture with stable field names and mixed values."""
    with path.open("w", encoding="utf-8") as f:
        for row_id in range(rows):
            row: dict[str, Any] = {"id": row_id, "source": f"file-{row_id % 8}"}
            for col in range(width):
                row[f"value_{col}"] = row_id * (col + 1)
            f.write(json.dumps(row, separators=(",", ":")))
            f.write("\n")


def write_dirty_jsonl(path: Path, rows: int, width: int) -> None:
    """Write JSONL with dirty keys that exercise planned-name matching."""
    with path.open("w", encoding="utf-8") as f:
        for row_id in range(rows):
            row: dict[str, Any] = {
                "ID Value": row_id,
                "Source/File": f"file-{row_id % 8}",
            }
            for col in range(width):
                row[f"Value {col} / Gross"] = row_id * (col + 1)
            f.write(json.dumps(row, separators=(",", ":")))
            f.write("\n")


def write_nested_jsonl(path: Path, rows: int) -> None:
    """Write nested JSONL with alternating scalar/list-compatible shapes."""
    with path.open("w", encoding="utf-8") as f:
        for row_id in range(rows):
            sentence = {
                "sentiment": {
                    "magnitude": row_id / 10,
                    "score": (row_id % 11) / 10,
                }
            }
            row = {
                "id": row_id,
                "ai": {
                    "naturalLanguage": {
                        "sentimentAnalysis": [
                            {
                                "sentences": [sentence] if row_id % 2 else sentence,
                                "language": "en",
                            }
                        ]
                    }
                },
            }
            f.write(json.dumps(row, separators=(",", ":")))
            f.write("\n")


def write_json_folder(path: Path, rows: int, width: int) -> None:
    """Write a folder of one-document JSON files."""
    path.mkdir()
    for row_id in range(rows):
        row: dict[str, Any] = {"id": row_id, "source": f"file-{row_id % 8}"}
        for col in range(width):
            row[f"value_{col}"] = row_id * (col + 1)
        (path / f"row_{row_id:08d}.json").write_text(
            json.dumps(row, indent=2),
            encoding="utf-8",
        )


def write_xml_folder(path: Path, rows: int, width: int) -> None:
    """Write a folder of one-document XML files."""
    path.mkdir()
    for row_id in range(rows):
        values = "".join(f"<value_{col}>{row_id * (col + 1)}</value_{col}>" for col in range(width))
        (path / f"row_{row_id:08d}.xml").write_text(
            f'<?xml version="1.0" encoding="UTF-8"?><row>'
            f"<id>{row_id}</id><source>file-{row_id % 8}</source>{values}</row>",
            encoding="utf-8",
        )


def write_csv(path: Path, rows: int, width: int) -> None:
    """Write a flat CSV fixture with a header and numeric values."""
    headers = ["id", "source", *(f"value_{col}" for col in range(width))]
    with path.open("w", encoding="utf-8", newline="") as f:
        f.write(",".join(headers))
        f.write("\n")
        for row_id in range(rows):
            values = [str(row_id), f"file-{row_id % 8}"]
            values.extend(str(row_id * (col + 1)) for col in range(width))
            f.write(",".join(values))
            f.write("\n")


def write_parquet(path: Path, rows: int, width: int) -> None:
    """Write a flat Parquet fixture when PyArrow is installed."""
    import pyarrow as pa
    import pyarrow.parquet as pq

    data: dict[str, Any] = {
        "id": list(range(rows)),
        "source": [f"file-{row_id % 8}" for row_id in range(rows)],
    }
    for col in range(width):
        data[f"value_{col}"] = [row_id * (col + 1) for row_id in range(rows)]
    pq.write_table(pa.table(data), path)


def python_rows_fixture(rows: int, width: int) -> list[dict[str, Any]]:
    """Build Python row dictionaries for read_python benchmarks."""
    data: list[dict[str, Any]] = []
    for row_id in range(rows):
        row: dict[str, Any] = {"id": row_id, "source": f"file-{row_id % 8}"}
        for col in range(width):
            row[f"value_{col}"] = row_id * (col + 1)
        data.append(row)
    return data


def python_nested_rows_fixture(rows: int, width: int) -> list[dict[str, Any]]:
    """Build nested Python rows that stress native Python JSON encoding."""
    data: list[dict[str, Any]] = []
    for row_id in range(rows):
        row: dict[str, Any] = {
            "id": row_id,
            "groups": [
                {
                    "name": f"group_{group}",
                    "values": [row_id * (col + 1) for col in range(width)],
                }
                for group in range(3)
            ],
        }
        data.append(row)
    return data
