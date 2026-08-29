"""Public read/converter option matrices.

Its parameter matrix applies token, error, inference, output-format, and converter
combinations through the public read and write APIs.
"""

from __future__ import annotations

import csv
import itertools
import json
import re
from dataclasses import dataclass
from pathlib import Path

import pytest
from conftest import read_test_csv

import schema_sanitizer as ss

pytestmark = pytest.mark.usefixtures("require_native")

EXPECTED_ROWS = 2


@dataclass(frozen=True)
class _PolicyCombo:
    """Test helper for PolicyCombo."""

    on_error: str
    enabled: bool


@dataclass(frozen=True)
class _CsvCase:
    """Test helper for CsvCase."""

    name: str
    csv_text: str
    kwargs_when_enabled: dict[str, tuple[str, ...]]


_POLICY_COMBOS = list(
    itertools.starmap(
        _PolicyCombo,
        itertools.product(("stop", "skip_row", "emit_null_row"), (False, True)),
    )
)


_BOOL_CASES = [
    _CsvCase("default_bool_tokens", "id,b\n1,true\n2,false\n", {}),
    _CsvCase(
        "custom_bool_tokens_casefold",
        "id,b\n1,YeS\n2,nO\n",
        {"true_tokens": ("YES",), "false_tokens": ("NO",)},
    ),
]


_TEMPORAL_CASES = [
    _CsvCase(
        "custom_temporal_regex_slash_dash_pipe",
        "id,ts,d,t\n"
        "1,2024/01/02 03:04:05,2024-01-02,03|04|05\n"
        "2,2024/01/03 04:05:06,2024-01-03,04|05|06\n",
        {
            "custom_timestamp_patterns": (r"(\d{4})/(\d{2})/(\d{2}) (\d{2}):(\d{2}):(\d{2})",),
            "custom_date_patterns": (r"(\d{4})-(\d{2})-(\d{2})",),
            "custom_time_patterns": (r"(\d{2})\|(\d{2})\|(\d{2})",),
        },
    ),
    _CsvCase(
        "custom_temporal_regex_frac_tz",
        "id,ts,d,t\n"
        "1,2024*01*02 03-04-05.123456789+0130,2024#01#02,03-04-05\n"
        "2,2024*01*03 04-05-06.987654321-0215,2024#01#03,04-05-06\n",
        {
            "custom_timestamp_patterns": (
                r"(\d{4})\*(\d{2})\*(\d{2}) " r"(\d{2})-(\d{2})-(\d{2})\.(\d{1,9})([+-]\d{4})",
            ),
            "custom_date_patterns": (r"(\d{4})#(\d{2})#(\d{2})",),
            "custom_time_patterns": (r"(\d{2})-(\d{2})-(\d{2})",),
        },
    ),
]


def _token_combo_id(c: _PolicyCombo) -> str:
    """Render one token-policy combination as a stable parameter ID."""
    return f"on_error={c.on_error}|enabled={int(c.enabled)}"


def _path(tmp_path: Path, case: _CsvCase) -> Path:
    """Write one CSV option case and return its temporary path."""
    path = tmp_path / f"{case.name}.csv"
    path.write_text(case.csv_text, encoding="utf-8")
    return path


def _kwargs(case: _CsvCase, combo: _PolicyCombo) -> dict[str, object]:
    """Build public reader options for one CSV case and policy combination."""
    kwargs = dict(case.kwargs_when_enabled) if combo.enabled else {}
    if "bool" in case.name and combo.enabled:
        kwargs.setdefault("true_tokens", ("true", "True"))
        kwargs.setdefault("false_tokens", ("false", "False"))
    return {"on_error": combo.on_error, **kwargs}


@pytest.mark.parametrize("case", _BOOL_CASES + _TEMPORAL_CASES, ids=lambda c: c.name)
@pytest.mark.parametrize("combo", _POLICY_COMBOS, ids=_token_combo_id)
def test_token_option_matrix_read(tmp_path: Path, combo: _PolicyCombo, case: _CsvCase) -> None:
    """Verify token option matrix read."""
    pytest.importorskip("pyarrow")

    result = read_test_csv(_path(tmp_path, case), **_kwargs(case, combo))
    assert result.clean_data.num_rows == EXPECTED_ROWS


@pytest.mark.parametrize("case", _BOOL_CASES + _TEMPORAL_CASES, ids=lambda c: c.name)
@pytest.mark.parametrize("combo", _POLICY_COMBOS, ids=_token_combo_id)
@pytest.mark.parametrize("suffix", (".csv", ".jsonl", ".parquet"))
def test_token_option_matrix_converters(
    tmp_path: Path, combo: _PolicyCombo, case: _CsvCase, suffix: str
) -> None:
    """Verify token option matrix converters."""
    pq = pytest.importorskip("pyarrow.parquet")

    out = tmp_path / f"{case.name}_{combo.on_error}_{int(combo.enabled)}{suffix}"
    converter = {".csv": ss.to_csv, ".jsonl": ss.to_jsonl, ".parquet": ss.to_parquet}[suffix]
    result = converter(
        _path(tmp_path, case),
        out,
        input_format="csv",
        **_kwargs(case, combo),
    )
    assert result.clean_data is None

    if suffix == ".parquet":
        assert pq.read_table(out).num_rows == EXPECTED_ROWS
    else:
        lines = [line for line in out.read_text(encoding="utf-8").splitlines() if line]
        assert len(lines) == EXPECTED_ROWS + int(suffix == ".csv")


API_CSV_TEXT = "a,b,c\n1,true,3.5\n2,false,4.5\n"


@dataclass(frozen=True)
class _Combo:
    """Test helper for Combo."""

    on_error: str
    infer_bools: bool

    @property
    def expected_rows(self) -> int:
        """Return the rows expected for this option-matrix case."""
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


def _api_combo_id(c: _Combo) -> str:
    """Render one public API option combination as a stable parameter ID."""
    return f"on_error={c.on_error}|infer_bools={int(c.infer_bools)}"


def _slug(s: str) -> str:
    """Convert a parameter label into a filesystem-safe fixture name."""
    return re.sub(r"[^a-zA-Z0-9_.-]+", "_", s)


def _kwargs_for_combo(c: _Combo) -> dict[str, object]:
    """Build public converter arguments for one option combination."""
    if c.infer_bools:
        return {
            "column_order": "alphabetically",
            "true_tokens": ("true", "True"),
            "false_tokens": ("false", "False"),
            "on_error": c.on_error,
        }
    return {"column_order": "alphabetically", "on_error": c.on_error}


def _csv_path(tmp_path: Path, name: str = "rows.csv") -> Path:
    """Write the shared API matrix CSV and return its path."""
    path = tmp_path / name
    path.write_text(API_CSV_TEXT, encoding="utf-8")
    return path


@pytest.mark.parametrize("output_format", ("pyarrow", "pandas", "polars", "duckdb"))
@pytest.mark.parametrize("combo", _OPTION_COMBOS, ids=_api_combo_id)
def test_read_option_matrix(tmp_path: Path, combo: _Combo, output_format: str) -> None:
    """Verify read option matrix."""
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
        _csv_path(tmp_path, f"{_slug(_api_combo_id(combo))}.csv"),
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
@pytest.mark.parametrize("combo", _OPTION_COMBOS, ids=_api_combo_id)
def test_converter_option_matrix(tmp_path: Path, combo: _Combo, suffix: str) -> None:
    """Verify converter option matrix."""
    pq = pytest.importorskip("pyarrow.parquet")

    source = _csv_path(tmp_path, f"source_{_slug(_api_combo_id(combo))}.csv")
    out = tmp_path / f"out_{_slug(_api_combo_id(combo))}{suffix}"
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
