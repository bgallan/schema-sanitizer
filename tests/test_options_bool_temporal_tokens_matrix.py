"""Matrix coverage for bool and temporal per-call options."""

from __future__ import annotations

import itertools
from dataclasses import dataclass
from pathlib import Path

import pytest
from conftest import read_test_csv, require_native

import schema_sanitizer as ss

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


def _combo_id(c: _PolicyCombo) -> str:
    """Return combo id for the test."""
    return f"on_error={c.on_error}|enabled={int(c.enabled)}"


def _path(tmp_path: Path, case: _CsvCase) -> Path:
    """Return path for the test."""
    path = tmp_path / f"{case.name}.csv"
    path.write_text(case.csv_text, encoding="utf-8")
    return path


def _kwargs(case: _CsvCase, combo: _PolicyCombo) -> dict[str, object]:
    """Return kwargs for the test."""
    kwargs = dict(case.kwargs_when_enabled) if combo.enabled else {}
    if "bool" in case.name and combo.enabled:
        kwargs.setdefault("true_tokens", ("true", "True"))
        kwargs.setdefault("false_tokens", ("false", "False"))
    return {"on_error": combo.on_error, **kwargs}


@pytest.mark.parametrize("case", _BOOL_CASES + _TEMPORAL_CASES, ids=lambda c: c.name)
@pytest.mark.parametrize("combo", _POLICY_COMBOS, ids=_combo_id)
def test_token_option_matrix_read(tmp_path: Path, combo: _PolicyCombo, case: _CsvCase) -> None:
    """Verify Boolean and temporal token options across read policies."""
    require_native()
    pytest.importorskip("pyarrow")

    result = read_test_csv(_path(tmp_path, case), **_kwargs(case, combo))
    assert result.clean_data.num_rows == EXPECTED_ROWS


@pytest.mark.parametrize("case", _BOOL_CASES + _TEMPORAL_CASES, ids=lambda c: c.name)
@pytest.mark.parametrize("combo", _POLICY_COMBOS, ids=_combo_id)
@pytest.mark.parametrize("suffix", (".csv", ".jsonl", ".parquet"))
def test_token_option_matrix_converters(
    tmp_path: Path, combo: _PolicyCombo, case: _CsvCase, suffix: str
) -> None:
    """Verify token option matrix converters."""
    require_native()
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
        assert len([line for line in out.read_text(encoding="utf-8").splitlines() if line]) >= 2
