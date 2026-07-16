"""Tests cleaning heuristics, policy handling, and schema stability."""

from __future__ import annotations

import pytest
from cleaning_policies_shared import INPUT_CASES as _INPUT_CASES
from cleaning_policies_shared import field_names as _field_names
from cleaning_policies_shared import nested_contract_rows as _nested_contract_rows
from cleaning_policies_shared import nested_contract_schema as _nested_contract_schema
from cleaning_policies_shared import nested_depth_rows as _nested_depth_rows
from cleaning_policies_shared import read_result as _read_result
from cleaning_policies_shared import table_signature as _table_signature
from conftest import read_test_python

pa = pytest.importorskip("pyarrow")


# Split from test_cleaning_policies.py: test_mixed_integer_float_values_infer_float_column, test_nested_versioned_sibling_fields_prefer_list_variant_for_single_values, test_field_name_policy_sanitizes_strict_schema_contract_and_matches_dirty_rows, ...


def test_mixed_integer_float_values_infer_float_column() -> None:
    """Verify integer and float observations widen to one float column."""
    res = read_test_python(
        [{"value": 10}, {"value": 37.5}, {"value": "12.5"}, {"value": "0"}],
        parse_integers=True,
        parse_floats=True,
    )

    assert res.clean_data is not None
    assert res.clean_data.schema.field("value").type == pa.float64()
    assert res.clean_data.to_pylist() == [
        {"value": 10.0},
        {"value": 37.5},
        {"value": 12.5},
        {"value": 0.0},
    ]


def test_nested_versioned_sibling_fields_prefer_list_variant_for_single_values() -> None:
    """Verify nested version families prefer the widest compatible list version."""
    sentiment_struct = pa.struct(
        [pa.field("sentiment", pa.struct([pa.field("magnitude", pa.float64())]))]
    )
    analysis_struct = pa.struct(
        [
            pa.field("sentences", sentiment_struct),
            pa.field("sentences_v2_struct_array", pa.list_(sentiment_struct)),
        ]
    )
    schema_contract = pa.schema([pa.field("sentimentanalysis", pa.list_(analysis_struct))])

    res = _read_result(
        [
            {
                "sentimentanalysis": [
                    {"sentences": {"sentiment": {"magnitude": 1.5}}},
                    {"sentences": [{"sentiment": {"magnitude": 2.5}}]},
                ]
            }
        ],
        case="python_obj",
        tmp_path=None,
        schema_contract=schema_contract,
        schema_mode="strict",
        field_name_policy="lower_snake",
        on_error="stop",
    )

    assert res.clean_data is not None
    assert res.clean_data.to_pylist() == [
        {
            "sentimentanalysis": [
                {
                    "sentences": None,
                    "sentences_v2_struct_array": [{"sentiment": {"magnitude": 1.5}}],
                },
                {
                    "sentences": None,
                    "sentences_v2_struct_array": [{"sentiment": {"magnitude": 2.5}}],
                },
            ]
        }
    ]


def test_field_name_policy_sanitizes_strict_schema_contract_and_matches_dirty_rows() -> None:
    """Verify strict schema contracts are sanitized before source-row matching."""
    schema_contract = pa.schema(
        [
            ("User-ID", pa.int64()),
            ("nested-Obj", pa.struct([("Bad.Key", pa.int64())])),
        ]
    )
    rows = [{"User-ID": 1, "nested-Obj": {"Bad.Key": 2}}]

    res = _read_result(
        rows,
        case="python_obj",
        tmp_path=None,
        schema_contract=schema_contract,
        schema_mode="strict",
    )

    assert res.clean_data is not None
    assert res.clean_data.schema.names == ["nestedobj", "userid"]
    assert _field_names(res.clean_data.schema.field("nestedobj").type) == ["badkey"]
    assert res.clean_data.to_pylist() == [{"nestedobj": {"badkey": 2}, "userid": 1}]


@pytest.mark.parametrize("input_case", _INPUT_CASES)
def test_parquet_max_depth_enforcement_flattens_nested_object_fields(input_case, tmp_path) -> None:
    """Ensure over-depth Parquet/BigQuery RECORD fields are flattened."""
    cfg = {"parquet_max_depth": 1}
    res = _read_result(_nested_depth_rows(), **cfg, case=input_case, tmp_path=tmp_path)
    table = res.clean_data
    assert table is not None

    assert table.schema.get_field_index("aflattened") >= 0
    assert table.schema.get_field_index("a") == -1

    assert res.stats["flattened_fields"] > 0
    assert res.stats["parquet_schema_depth"] == 0
    assert res.stats["arrow_schema_depth"] == 0


@pytest.mark.parametrize("input_case", _INPUT_CASES)
def test_parquet_max_depth_policy_changes_nested_schema_shape(input_case, tmp_path) -> None:
    """Verify schema shape changes when parquet_max_depth is tightened/relaxed."""
    rows = _nested_depth_rows()

    shallow = _read_result(
        rows,
        options={"parquet_max_depth": 1},
        case=input_case,
        tmp_path=tmp_path,
    )
    deep = _read_result(
        rows,
        options={"parquet_max_depth": 2},
        case=input_case,
        tmp_path=tmp_path,
    )

    assert shallow.clean_data is not None
    assert deep.clean_data is not None
    assert shallow.clean_data.schema != deep.clean_data.schema
    assert "aflattened" in shallow.clean_data.schema.to_string()
    assert "aflattened" not in deep.clean_data.schema.to_string()

    assert shallow.stats["flattened_fields"] > 0
    assert deep.stats["flattened_fields"] == 0


@pytest.mark.parametrize("input_case", _INPUT_CASES)
def test_arrow_max_depth_counts_lists_and_structs(input_case, tmp_path) -> None:
    """Verify Arrow depth counts both list and struct containers."""
    rows = [{"value": [{"a": 1}]}]

    shallow = _read_result(
        rows,
        options={"arrow_max_depth": 1},
        case=input_case,
        tmp_path=tmp_path,
    )
    deep = _read_result(
        rows,
        options={"arrow_max_depth": 2},
        case=input_case,
        tmp_path=tmp_path,
    )

    assert shallow.clean_data is not None
    assert deep.clean_data is not None
    assert "valueflattened" in shallow.clean_data.schema.to_string()
    assert "list<item: struct<a: int64>>" in deep.clean_data.schema.to_string()
    assert shallow.stats["arrow_schema_depth"] == 0
    assert deep.stats["arrow_schema_depth"] == 2
    assert deep.stats["parquet_schema_depth"] == 1


@pytest.mark.parametrize("input_case", _INPUT_CASES)
def test_mixed_integer_float_scalars_infer_as_float(input_case, tmp_path) -> None:
    """Verify integer and float scalar mixes widen to float."""
    rows = [{"value": 1}, {"value": 1.5}]

    res = _read_result(rows, options={}, case=input_case, tmp_path=tmp_path)

    assert res.clean_data is not None
    assert res.clean_data.schema.field("value").type == pa.float64()
    assert res.clean_data.column("value").to_pylist() == [1.0, 1.5]


@pytest.mark.parametrize("input_case", _INPUT_CASES)
def test_list_of_scalars_stays_typed_when_conflict_free(input_case, tmp_path) -> None:
    """Verify list of scalars stays typed when conflict free."""
    rows = [{"value": [1, 2]}, {"value": [3, 4]}]

    res = _read_result(rows, options={}, case=input_case, tmp_path=tmp_path)

    assert res.clean_data is not None
    assert pa.types.is_list(res.clean_data.schema.field("value").type)
    assert pa.types.is_int64(res.clean_data.schema.field("value").type.value_type)


@pytest.mark.parametrize("input_case", _INPUT_CASES)
def test_list_of_structs_stays_typed_when_conflict_free(input_case, tmp_path) -> None:
    """Verify list of structs stays typed when conflict free."""
    rows = [{"value": [{"a": 1}, {"a": 2}]}, {"value": [{"a": 3}]}]

    res = _read_result(rows, options={}, case=input_case, tmp_path=tmp_path)

    assert res.clean_data is not None
    assert pa.types.is_list(res.clean_data.schema.field("value").type)
    assert pa.types.is_struct(res.clean_data.schema.field("value").type.value_type)


@pytest.mark.parametrize("input_case", _INPUT_CASES)
def test_nested_lists_fall_back_to_list_of_string(input_case, tmp_path) -> None:
    """Verify nested lists fall back to list of string."""
    rows = [{"value": [[1, 2], [3]]}, {"value": [[4]]}]

    res = _read_result(rows, options={}, case=input_case, tmp_path=tmp_path)

    assert res.clean_data is not None
    field_type = res.clean_data.schema.field("value").type
    assert pa.types.is_list(field_type)
    assert pa.types.is_string(field_type.value_type) or pa.types.is_large_string(
        field_type.value_type
    )
    assert res.clean_data.column("value").to_pylist()[0] == ["[1,2]", "[3]"]


@pytest.mark.parametrize("input_case", _INPUT_CASES)
def test_list_of_structs_allows_nested_list_fields(input_case, tmp_path) -> None:
    """Verify list structs can contain typed nested list fields."""
    rows = [
        {
            "author": [
                {
                    "id": 1,
                    "signature": "El Pais",
                    "tagauthor": {
                        "image": {
                            "auth": ["abc", "def"],
                        }
                    },
                }
            ]
        },
        {"author": []},
    ]

    res = _read_result(rows, options={}, case=input_case, tmp_path=tmp_path)

    assert res.clean_data is not None
    field_type = res.clean_data.schema.field("author").type
    assert pa.types.is_list(field_type)
    assert pa.types.is_struct(field_type.value_type)

    image_type = field_type.value_type.field("tagauthor").type.field("image").type
    auth_type = image_type.field("auth").type
    assert pa.types.is_list(auth_type)
    assert pa.types.is_string(auth_type.value_type) or pa.types.is_large_string(
        auth_type.value_type
    )
    assert res.clean_data.to_pylist() == [
        rows[0],
        {"author": None},
    ]


@pytest.mark.parametrize("input_case", _INPUT_CASES)
def test_list_struct_scalar_conflict_resolves_at_nested_field(input_case, tmp_path) -> None:
    """Verify list struct scalar conflicts resolve as nested string fields."""
    rows = [{"value": [{"a": 1}]}, {"value": [{"a": "x"}]}]

    res = _read_result(rows, options={}, case=input_case, tmp_path=tmp_path)

    assert res.clean_data is not None
    field_type = res.clean_data.schema.field("value").type
    assert pa.types.is_list(field_type)
    assert pa.types.is_struct(field_type.value_type)
    value_type = field_type.value_type.field("a").type
    assert pa.types.is_string(value_type) or pa.types.is_large_string(value_type)
    assert res.clean_data.column("value").to_pylist() == [[{"a": "1"}], [{"a": "x"}]]


@pytest.mark.parametrize("input_case", _INPUT_CASES)
def test_list_struct_numeric_looking_string_conflict_stays_typed(input_case, tmp_path) -> None:
    """Verify numeric-looking strings do not stringify the parent list."""
    rows = [
        {"author": [{"tagauthor": {"externalid": "el_pais_a"}}]},
        {"author": [{"tagauthor": {"externalid": "-1"}}]},
    ]

    res = _read_result(rows, options={}, case=input_case, tmp_path=tmp_path)

    assert res.clean_data is not None
    field_type = res.clean_data.schema.field("author").type
    assert pa.types.is_list(field_type)
    assert pa.types.is_struct(field_type.value_type)
    external_id_type = field_type.value_type.field("tagauthor").type.field("externalid").type
    assert pa.types.is_string(external_id_type) or pa.types.is_large_string(external_id_type)
    assert res.clean_data.to_pylist() == rows


@pytest.mark.parametrize("input_case", _INPUT_CASES)
def test_nested_contract_stop_policy_raises_on_type_violation(input_case, tmp_path) -> None:
    """Contract + STOP must raise immediately on nested type coercion violations."""
    cfg = {
        "schema_contract": _nested_contract_schema(),
        "schema_mode": "strict",
        "on_error": "stop",
    }

    with pytest.raises(Exception, match=r"coerce string to int64|failed to coerce"):
        _read_result(_nested_contract_rows(), **cfg, case=input_case, tmp_path=tmp_path)


@pytest.mark.parametrize("input_case", _INPUT_CASES)
def test_nested_contract_skip_row_policy_keeps_only_valid_rows(input_case, tmp_path) -> None:
    """Contract + SKIP_ROW must drop only invalid rows and keep valid nested rows."""
    cfg = {
        "schema_contract": _nested_contract_schema(),
        "schema_mode": "strict",
        "on_error": "skip_row",
    }
    res = _read_result(_nested_contract_rows(), **cfg, case=input_case, tmp_path=tmp_path)

    assert res.clean_data is not None
    out = res.clean_data.to_pylist()
    assert [row["id"] for row in out] == [1, 3]
    assert res.stats["skipped_rows"] == 1


@pytest.mark.parametrize("input_case", _INPUT_CASES)
def test_nested_contract_emit_null_row_policy_preserves_row_count(input_case, tmp_path) -> None:
    """Contract + EMIT_NULL_ROW must keep row count and null out invalid rows."""
    cfg = {
        "schema_contract": _nested_contract_schema(),
        "schema_mode": "strict",
        "on_error": "emit_null_row",
    }
    res = _read_result(_nested_contract_rows(), **cfg, case=input_case, tmp_path=tmp_path)

    assert res.clean_data is not None
    out = res.clean_data.to_pylist()
    assert len(out) == 3
    assert out[1]["id"] is None
    assert out[1]["user"] is None


@pytest.mark.parametrize("input_case", _INPUT_CASES)
def test_nested_integer_parse_policy_is_stable_and_reproducible(input_case, tmp_path) -> None:
    """Integer parsing toggles must deterministically change skipped row counts."""
    rows = [
        {"id": 1, "user": {"id": "10", "name": "a"}},
        {"id": 2, "user": {"id": "bad", "name": "b"}},
    ]
    base = _nested_contract_schema()

    with_coercion = {
        "schema_contract": base,
        "schema_mode": "strict",
        "on_error": "skip_row",
        "parse_integers": True,
    }
    without_coercion = {
        "schema_contract": base,
        "schema_mode": "strict",
        "on_error": "skip_row",
        "parse_integers": False,
    }

    r0 = _read_result(rows, **with_coercion, case=input_case, tmp_path=tmp_path)
    r1 = _read_result(rows, **with_coercion, case=input_case, tmp_path=tmp_path)
    r2 = _read_result(rows, **without_coercion, case=input_case, tmp_path=tmp_path)

    assert r0.clean_data is not None
    assert r1.clean_data is not None
    assert r2.clean_data is not None

    assert _table_signature(r0.clean_data) == _table_signature(r1.clean_data)
    assert r0.stats["skipped_rows"] == 1
    assert r1.stats["skipped_rows"] == 1

    assert r2.clean_data.num_rows == 0
    assert r2.stats["skipped_rows"] == 2
