"""Tests CSV cleaning policy handling and schema stability."""

from __future__ import annotations

import pytest
from conftest import read_test_csv

pa = pytest.importorskip("pyarrow")

from schema_sanitizer.api_impl.execution_context import ExecutionContext
from schema_sanitizer.options_impl.call_options import normalize_call_options

_CSV_INPUT_CASES = [
    "csv_path",
    "csv_path_auto",
]


def _table_signature(table) -> tuple[str, list[dict[str, object]]]:
    """Return table signature for the test."""
    return (
        table.schema.to_string(show_field_metadata=False, show_schema_metadata=False),
        table.to_pylist(),
    )


def _csv_contract_schema():
    """Return csv contract schema for the test."""
    return pa.schema(
        [
            ("id", pa.int64()),
            ("userid", pa.int64()),
            ("username", pa.string()),
        ]
    )


def _csv_bad_text() -> str:
    """Return csv bad text for the test."""
    return "id,userid,username\n1,10,a\n2,oops,b\n3,30,c\n"


def _prepare_csv_input(case: str, tmp_path, *, content: str) -> tuple[object, str]:
    """Prepare csv input."""
    if case == "csv_path":
        p = tmp_path / "rows.csv"
        p.write_text(content, encoding="utf-8")
        return p, "csv"
    if case == "csv_path_auto":
        p = tmp_path / "rows.auto.csv"
        p.write_text(content, encoding="utf-8")
        return p, "auto"
    raise ValueError(f"unsupported csv input case: {case}")


def _read_csv_result(input_case: str, tmp_path, *, content: str, **options):
    """Read csv result."""
    data, _fmt = _prepare_csv_input(input_case, tmp_path, content=content)
    schema_contract = options.pop("schema_contract", None)
    if schema_contract is not None:
        return ExecutionContext().to_table(
            data,
            options=normalize_call_options(schema_contract=schema_contract, **options),
            format=_fmt,
            source="auto",
        )
    return read_test_csv(data, output_format="pyarrow", **options)


@pytest.mark.parametrize("input_case", _CSV_INPUT_CASES)
def test_csv_field_name_policy_matches_dirty_headers_against_strict_schema(
    input_case, tmp_path
) -> None:
    schema = pa.schema(
        [
            ("User-ID", pa.int64()),
            ("Full Name", pa.string()),
        ]
    )
    content = "User-ID,Full Name\n1,Ana\n"

    res = _read_csv_result(
        input_case,
        tmp_path,
        content=content,
        schema_contract=schema,
        schema_mode="strict",
        parse_integers=True,
    )

    assert res.clean_data is not None
    assert res.clean_data.schema.names == ["fullname", "userid"]
    assert res.clean_data.to_pylist() == [{"fullname": "Ana", "userid": 1}]


@pytest.mark.parametrize("input_case", _CSV_INPUT_CASES)
def test_csv_contract_stop_policy_raises_on_type_violation(input_case, tmp_path) -> None:
    """CSV contract + STOP must raise on type violations."""
    cfg = {"schema_contract": _csv_contract_schema(), "schema_mode": "strict", "on_error": "stop"}
    with pytest.raises(Exception, match=r"coerce string to int64|failed to coerce"):
        _read_csv_result(input_case, tmp_path, content=_csv_bad_text(), **cfg)


@pytest.mark.parametrize("input_case", _CSV_INPUT_CASES)
def test_csv_contract_skip_row_policy_keeps_only_valid_rows(input_case, tmp_path) -> None:
    """CSV contract + SKIP_ROW must keep valid rows and drop invalid rows."""
    cfg = {
        "schema_contract": _csv_contract_schema(),
        "schema_mode": "strict",
        "on_error": "skip_row",
        "parse_integers": True,
    }
    res = _read_csv_result(input_case, tmp_path, content=_csv_bad_text(), **cfg)

    assert res.clean_data is not None
    assert [row["id"] for row in res.clean_data.to_pylist()] == [1, 3]
    assert res.stats["skipped_rows"] == 1


@pytest.mark.parametrize("input_case", _CSV_INPUT_CASES)
def test_csv_contract_emit_null_row_policy_preserves_row_count(input_case, tmp_path) -> None:
    """CSV contract + EMIT_NULL_ROW must preserve row count with null row substitution."""
    cfg = {
        "schema_contract": _csv_contract_schema(),
        "schema_mode": "strict",
        "on_error": "emit_null_row",
    }
    res = _read_csv_result(input_case, tmp_path, content=_csv_bad_text(), **cfg)

    assert res.clean_data is not None
    out = res.clean_data.to_pylist()
    assert len(out) == 3
    assert out[1]["id"] is None
    assert out[1]["userid"] is None
    assert out[1]["username"] is None


@pytest.mark.parametrize("input_case", _CSV_INPUT_CASES)
def test_csv_integer_parse_policy_is_stable_and_reproducible(input_case, tmp_path) -> None:
    """CSV integer parsing toggle must deterministically alter skipped row counts."""
    with_coercion = {
        "schema_contract": _csv_contract_schema(),
        "schema_mode": "strict",
        "on_error": "skip_row",
        "parse_integers": True,
    }
    without_coercion = {
        "schema_contract": _csv_contract_schema(),
        "schema_mode": "strict",
        "on_error": "skip_row",
        "parse_integers": False,
    }
    r0 = _read_csv_result(input_case, tmp_path, content=_csv_bad_text(), **with_coercion)
    r1 = _read_csv_result(input_case, tmp_path, content=_csv_bad_text(), **with_coercion)
    r2 = _read_csv_result(input_case, tmp_path, content=_csv_bad_text(), **without_coercion)

    assert r0.clean_data is not None
    assert r1.clean_data is not None
    assert r2.clean_data is not None
    assert _table_signature(r0.clean_data) == _table_signature(r1.clean_data)
    assert r0.stats["skipped_rows"] == 1
    assert r1.stats["skipped_rows"] == 1
    assert r2.clean_data.num_rows == 0
    assert r2.stats["skipped_rows"] == 3
