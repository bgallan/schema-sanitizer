"""Immutable CSV union-projection contracts."""

from __future__ import annotations

import csv
import json
from pathlib import Path

import pytest

import schema_sanitizer as ss
from schema_sanitizer.errors import (
    SchemaSanitizerInvalidArgumentError,
    SchemaSanitizerOutOfMemoryError,
)


def _write_csv(folder: Path, name: str, text: str) -> Path:
    """Write one UTF-8 CSV source and return its path."""
    path = folder / name
    path.write_text(text, encoding="utf-8")
    return path


def _business_rows(path: Path) -> list[dict[str, object]]:
    """Return rows without volatile registry and timestamp metadata."""
    rows: list[dict[str, object]] = []
    for line in path.read_text(encoding="utf-8").splitlines():
        row = json.loads(line)
        row.pop("schema_registry", None)
        row.pop("schema_drifts", None)
        row.pop("ingestion_timestamp", None)
        row["source_file"] = Path(str(row["source_file"])).name
        rows.append(row)
    return rows


def _convert_union(folder: Path, output: Path, *, multi_threading: bool = False) -> None:
    """Materialize one directory through the native CSV union path."""
    ss.to_jsonl(
        folder,
        output,
        input_format="csv",
        input_mode="directory",
        csv_header_mode="union",
        field_name_policy="preserve",
        multi_threading=multi_threading,
    )


def test_union_accepts_equal_reordered_missing_and_additive_headers(tmp_path: Path) -> None:
    """One canonical result reconciles all supported header relationships."""
    folder = tmp_path / "input"
    folder.mkdir()
    _write_csv(folder, "a.csv", "id,name\n1,Ana\n")
    _write_csv(folder, "b.csv", "name,id\nLuis,2\n")
    _write_csv(folder, "c.csv", "id\n3\n")
    _write_csv(folder, "d.csv", "id,name,extra\n4,Marta,x\n")
    output = tmp_path / "union.jsonl"

    _convert_union(folder, output)

    assert _business_rows(output) == [
        {"extra": None, "id": "1", "name": "Ana", "source_file": "a.csv"},
        {"extra": None, "id": "2", "name": "Luis", "source_file": "b.csv"},
        {"extra": None, "id": "3", "name": None, "source_file": "c.csv"},
        {"extra": "x", "id": "4", "name": "Marta", "source_file": "d.csv"},
    ]


def test_union_single_and_multi_thread_results_are_identical(tmp_path: Path) -> None:
    """Concurrent cell decoding does not change source projection selection."""
    folder = tmp_path / "input"
    folder.mkdir()
    for index in range(4):
        rows = "".join(f"n{index}_{row},{index * 10 + row},t{row}\n" for row in range(10))
        if index % 2:
            _write_csv(folder, f"{index}.csv", "name,id,tail\n" + rows)
        else:
            reordered = "".join(f"{index * 10 + row},n{index}_{row}\n" for row in range(10))
            _write_csv(folder, f"{index}.csv", "id,name\n" + reordered)
    single = tmp_path / "single.jsonl"
    multi = tmp_path / "multi.jsonl"

    _convert_union(folder, single, multi_threading=False)
    _convert_union(folder, multi, multi_threading=True)

    assert _business_rows(single) == _business_rows(multi)


def test_union_rejects_duplicate_fields_before_materialization(tmp_path: Path) -> None:
    """Duplicate names within one physical header cannot collapse silently."""
    folder = tmp_path / "input"
    folder.mkdir()
    _write_csv(folder, "a.csv", "id,id\n1,2\n")

    with pytest.raises(SchemaSanitizerInvalidArgumentError, match="duplicate non-empty name"):
        _convert_union(folder, tmp_path / "out.jsonl")


def test_union_rejects_names_colliding_after_reconciliation(tmp_path: Path) -> None:
    """The configured field policy is applied during header discovery."""
    folder = tmp_path / "input"
    folder.mkdir()
    _write_csv(folder, "a.csv", "A B,a_b\n1,2\n")

    with pytest.raises(SchemaSanitizerInvalidArgumentError, match="collide"):
        ss.to_jsonl(
            folder,
            tmp_path / "out.jsonl",
            input_format="csv",
            input_mode="directory",
            csv_header_mode="union",
            field_name_policy="lower_snake",
        )


def test_union_pads_short_rows_with_nulls(tmp_path: Path) -> None:
    """Trailing cells absent from one row materialize as canonical nulls."""
    folder = tmp_path / "input"
    folder.mkdir()
    _write_csv(folder, "a.csv", "id,name,extra\n1,Ana\n")
    output = tmp_path / "out.jsonl"

    _convert_union(folder, output)

    assert _business_rows(output) == [
        {"extra": None, "id": "1", "name": "Ana", "source_file": "a.csv"}
    ]


def test_union_rejects_rows_longer_than_their_source_header(tmp_path: Path) -> None:
    """Extra physical cells cannot acquire synthetic names in union mode."""
    folder = tmp_path / "input"
    folder.mkdir()
    _write_csv(folder, "a.csv", "id,name\n1,Ana,unexpected\n")

    with pytest.raises(
        SchemaSanitizerInvalidArgumentError, match="more fields than its source header"
    ):
        _convert_union(folder, tmp_path / "out.jsonl")


def test_union_keeps_all_null_declared_columns_as_nullable_strings(tmp_path: Path) -> None:
    """Header presence supplies string evidence without changing null values."""
    folder = tmp_path / "input"
    folder.mkdir()
    _write_csv(folder, "a.csv", "id,empty\n1,\n")
    _write_csv(folder, "b.csv", "empty,id\n,2\n")
    output = tmp_path / "out.jsonl"

    _convert_union(folder, output)

    raw_rows = [json.loads(line) for line in output.read_text(encoding="utf-8").splitlines()]
    assert [row["empty"] for row in raw_rows] == [None, None]
    registry = json.loads(raw_rows[0]["schema_registry"])
    fields = {field["name"]: field for field in registry["canonical_schema"]["fields"]}
    assert fields["empty"] == {
        "name": "empty",
        "nullable": True,
        "type": {"kind": "string"},
    }


def test_union_rejects_mixed_present_and_missing_headers(tmp_path: Path) -> None:
    """An empty source cannot be mixed with header-bearing files in union mode."""
    folder = tmp_path / "input"
    folder.mkdir()
    _write_csv(folder, "a.csv", "id\n1\n")
    _write_csv(folder, "b.csv", "")

    with pytest.raises(SchemaSanitizerInvalidArgumentError, match="with and without headers"):
        _convert_union(folder, tmp_path / "out.jsonl")


def test_union_canonical_field_order_is_deterministic(tmp_path: Path) -> None:
    """Repeated planning produces the same canonical schema order."""
    folder = tmp_path / "input"
    folder.mkdir()
    _write_csv(folder, "z.csv", "zeta,alpha\nz,a\n")
    _write_csv(folder, "a.csv", "middle,alpha\nm,a\n")
    first = tmp_path / "first.jsonl"
    second = tmp_path / "second.jsonl"

    _convert_union(folder, first)
    _convert_union(folder, second)

    def field_order(path: Path) -> list[str]:
        """Return canonical registry field order from one output file."""
        first_row = json.loads(path.read_text(encoding="utf-8").splitlines()[0])
        registry = json.loads(first_row["schema_registry"])
        return [field["name"] for field in registry["canonical_schema"]["fields"]]

    assert field_order(first) == field_order(second) == ["alpha", "middle", "zeta"]


def test_union_preserves_projection_and_provenance_across_quoted_newlines(
    tmp_path: Path,
) -> None:
    """Record framing keeps source ownership when one CSV value spans lines."""
    folder = tmp_path / "input"
    folder.mkdir()
    _write_csv(folder, "a.csv", 'id,note\n1,"first\nline"\n')
    _write_csv(folder, "b.csv", 'note,id,extra\n"second\nline",2,x\n')
    output = tmp_path / "out.jsonl"

    _convert_union(folder, output, multi_threading=True)

    assert _business_rows(output) == [
        {
            "extra": None,
            "id": "1",
            "note": "first\nline",
            "source_file": "a.csv",
        },
        {
            "extra": "x",
            "id": "2",
            "note": "second\nline",
            "source_file": "b.csv",
        },
    ]


def _registry_from_output(path: Path) -> str:
    """Return the durable registry JSON from the first materialized row."""
    row = json.loads(path.read_text(encoding="utf-8").splitlines()[0])
    return str(row["schema_registry"])


def test_union_strict_registry_rejects_unexpected_columns(tmp_path: Path) -> None:
    """Strict reconciliation rejects union fields absent from the registry."""
    base = tmp_path / "base"
    base.mkdir()
    _write_csv(base, "a.csv", "id,name\n1,Ana\n")
    base_output = tmp_path / "base.jsonl"
    _convert_union(base, base_output)

    incoming = tmp_path / "incoming"
    incoming.mkdir()
    _write_csv(incoming, "b.csv", "id,name,extra\n2,Luis,x\n")

    with pytest.raises(SchemaSanitizerInvalidArgumentError, match="extra field 'extra'"):
        ss.to_jsonl(
            incoming,
            tmp_path / "strict.jsonl",
            input_format="csv",
            input_mode="directory",
            csv_header_mode="union",
            field_name_policy="preserve",
            schema_registry=_registry_from_output(base_output),
            schema_mode="strict",
        )


def test_union_additive_registry_accepts_unexpected_columns(tmp_path: Path) -> None:
    """Additive reconciliation extends the registry with union-only fields."""
    base = tmp_path / "base"
    base.mkdir()
    _write_csv(base, "a.csv", "id,name\n1,Ana\n")
    base_output = tmp_path / "base.jsonl"
    _convert_union(base, base_output)

    incoming = tmp_path / "incoming"
    incoming.mkdir()
    _write_csv(incoming, "b.csv", "id,name,extra\n2,Luis,x\n")
    output = tmp_path / "additive.jsonl"
    ss.to_jsonl(
        incoming,
        output,
        input_format="csv",
        input_mode="directory",
        csv_header_mode="union",
        field_name_policy="preserve",
        schema_registry=_registry_from_output(base_output),
        schema_mode="additive",
    )

    assert _business_rows(output) == [
        {"extra": "x", "id": "2", "name": "Luis", "source_file": "b.csv"}
    ]


def test_union_projection_metadata_respects_memory_limit(tmp_path: Path) -> None:
    """Wide immutable header metadata is charged before schema inference."""
    folder = tmp_path / "input"
    folder.mkdir()
    header = ",".join(f"field_{index}_{'x' * 500}" for index in range(100))
    values = ",".join("value" for _ in range(100))
    _write_csv(folder, "wide.csv", f"{header}\n{values}\n")

    with pytest.raises(
        SchemaSanitizerOutOfMemoryError,
        match="CSV union source projections exceed memory_limit_bytes",
    ):
        ss.to_jsonl(
            folder,
            tmp_path / "out.jsonl",
            input_format="csv",
            input_mode="directory",
            csv_header_mode="union",
            field_name_policy="preserve",
            memory_limit_bytes=60_000,
        )


def test_union_is_shared_by_the_csv_file_sink(tmp_path: Path) -> None:
    """The public CSV sink consumes the same immutable union plan."""
    folder = tmp_path / "input"
    folder.mkdir()
    _write_csv(folder, "a.csv", "id,name\n1,Ana\n")
    _write_csv(folder, "b.csv", "name,id,extra\nLuis,2,x\n")
    output = tmp_path / "out.csv"

    ss.to_csv(
        folder,
        output,
        input_format="csv",
        input_mode="directory",
        csv_header_mode="union",
        field_name_policy="preserve",
    )

    with output.open(encoding="utf-8", newline="") as handle:
        rows = list(csv.DictReader(handle))
    assert [
        {
            "extra": row["extra"] or None,
            "id": row["id"],
            "name": row["name"],
            "source_file": Path(row["source_file"]).name,
        }
        for row in rows
    ] == [
        {"extra": None, "id": "1", "name": "Ana", "source_file": "a.csv"},
        {"extra": "x", "id": "2", "name": "Luis", "source_file": "b.csv"},
    ]
