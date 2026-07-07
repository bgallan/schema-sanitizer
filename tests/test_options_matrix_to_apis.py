"""Representative per-call option matrix across the public APIs."""

from __future__ import annotations

import csv
import itertools
import json
import re
from dataclasses import dataclass
from pathlib import Path

import pytest
from conftest import read_test_csv, require_native

import schema_sanitizer as ss

CSV_TEXT = "a,b,c\n1,true,3.5\n2,false,4.5\n"
EXPECTED_ROWS = 2


@dataclass(frozen=True)
class _Combo:
    """Test helper for Combo."""

    on_error: str
    infer_bools: bool

    @property
    def expected_rows(self) -> int:
        """Return the expected rows for this case."""
        return EXPECTED_ROWS


_OPTION_COMBOS = list(
    itertools.starmap(
        _Combo,
        itertools.product(
            ("stop", "skip_row", "emit_null_row"),
            (False, True),
        ),
    )
)


def _combo_id(c: _Combo) -> str:
    """Return combo id for the test."""
    return f"on_error={c.on_error}|infer_bools={int(c.infer_bools)}"


def _slug(s: str) -> str:
    """Return slug for the test."""
    return re.sub(r"[^a-zA-Z0-9_.-]+", "_", s)


def _kwargs_for_combo(c: _Combo) -> dict[str, object]:
    """Return kwargs for combo for the test."""
    if c.infer_bools:
        return {
            "column_order": "alphabetically",
            "true_tokens": ("true", "True"),
            "false_tokens": ("false", "False"),
            "on_error": c.on_error,
        }
    return {"column_order": "alphabetically", "on_error": c.on_error}


def _csv_path(tmp_path: Path, name: str = "rows.csv") -> Path:
    """Return csv path for the test."""
    path = tmp_path / name
    path.write_text(CSV_TEXT, encoding="utf-8")
    return path


@pytest.mark.parametrize("output_format", ("pyarrow", "pandas", "polars", "duckdb"))
@pytest.mark.parametrize("combo", _OPTION_COMBOS, ids=_combo_id)
def test_read_option_matrix(tmp_path: Path, combo: _Combo, output_format: str) -> None:
    """Verify read option matrix."""
    require_native()
    if output_format == "pyarrow":
        pytest.importorskip("pyarrow")
    elif output_format == "pandas":
        pytest.importorskip("pandas")
        pytest.importorskip("pyarrow")
    elif output_format == "polars":
        pytest.importorskip("polars")
        pytest.importorskip("pyarrow")
    elif output_format == "duckdb":
        pytest.importorskip("duckdb")
        pytest.importorskip("pyarrow")

    out = read_test_csv(
        _csv_path(tmp_path, f"{_slug(_combo_id(combo))}.csv"),
        output_format=output_format,
        **_kwargs_for_combo(combo),
    )
    if output_format == "pyarrow":
        assert out.clean_data.num_rows == combo.expected_rows
    elif output_format == "pandas":
        assert len(out.clean_data) == combo.expected_rows
    elif output_format == "polars":
        assert out.clean_data.height == combo.expected_rows
    else:
        assert len(out.clean_data.fetchall()) == combo.expected_rows


@pytest.mark.parametrize("suffix", (".csv", ".jsonl", ".parquet"))
@pytest.mark.parametrize("combo", _OPTION_COMBOS, ids=_combo_id)
def test_converter_option_matrix(tmp_path: Path, combo: _Combo, suffix: str) -> None:
    """Verify converter option matrix."""
    require_native()
    pq = pytest.importorskip("pyarrow.parquet")

    source = _csv_path(tmp_path, f"source_{_slug(_combo_id(combo))}.csv")
    out = tmp_path / f"out_{_slug(_combo_id(combo))}{suffix}"
    converter = {".csv": ss.to_csv, ".jsonl": ss.to_jsonl, ".parquet": ss.to_parquet}[suffix]
    result = converter(
        source,
        out,
        input_format="csv",
        **_kwargs_for_combo(combo),
    )
    assert result.clean_data is None

    if suffix == ".csv":
        with out.open("r", encoding="utf-8", newline="") as f:
            assert len(list(csv.DictReader(f))) == combo.expected_rows
    elif suffix == ".jsonl":
        rows = [json.loads(line) for line in out.read_text(encoding="utf-8").splitlines()]
        assert len(rows) == combo.expected_rows
    else:
        assert pq.read_table(out).num_rows == combo.expected_rows
