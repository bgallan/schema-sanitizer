"""Tests cleaning heuristics, policy handling, and schema stability."""

from __future__ import annotations

import json

import pytest
from conftest import read_test_json, read_test_jsonl, read_test_python, require_native

pa = pytest.importorskip("pyarrow")

import schema_sanitizer as ss
from schema_sanitizer.api_impl.context import ExecutionContext
from schema_sanitizer.api_impl.schema_registry import merge_schema_registry
from schema_sanitizer.options_impl.call_options import normalize_call_options

_INPUT_CASES = [
    "python_obj",
    "json_path",
    "json_path_auto",
    "jsonl_path",
    "jsonl_path_auto",
]


def _table_signature(table) -> tuple[str, list[dict[str, object]]]:
    """Return table signature for the test."""
    return (
        table.schema.to_string(show_field_metadata=False, show_schema_metadata=False),
        table.to_pylist(),
    )


def _nested_depth_rows() -> list[dict[str, object]]:
    """Return nested depth rows for the test."""
    return [
        {"id": 1, "a": {"b": {"c": 3}}},
        {"id": 2, "a": {"b": {"c": 4}}},
    ]


def _nested_contract_rows() -> list[dict[str, object]]:
    """Return nested contract rows for the test."""
    return [
        {"id": 1, "user": {"id": 10, "name": "a"}},
        {"id": 2, "user": {"id": "oops", "name": "b"}},
        {"id": 3, "user": {"id": 30, "name": "c"}},
    ]


def _nested_contract_schema():
    """Return nested contract schema for the test."""
    return pa.schema(
        [
            ("id", pa.int64()),
            (
                "user",
                pa.struct(
                    [
                        ("id", pa.int64()),
                        ("name", pa.string()),
                    ]
                ),
            ),
        ]
    )


def _field_names(struct_type) -> list[str]:
    """Return field names for a PyArrow schema or struct type."""
    return [field.name for field in struct_type]


def _versioned_scalar_registry(field_name: str, types: list[pa.DataType]) -> dict[str, object]:
    """Return a schema registry with one scalar variant for each type."""
    registry = None
    for typ in types:
        registry = merge_schema_registry(
            inferred_schema=pa.schema([pa.field(field_name, typ)]),
            schema_registry=registry,
            field_name_policy="lower_snake",
        ).schema_registry
    assert registry is not None
    return registry


def _prepare_input(
    rows: list[dict[str, object]],
    case: str,
    tmp_path,
) -> tuple[object, str]:
    """Prepare input."""
    json_text = json.dumps(rows)
    jsonl_text = "\n".join(json.dumps(r) for r in rows) + "\n"

    if case == "python_obj":
        return rows, "python"
    if case == "json_path":
        p = tmp_path / "rows.json"
        p.write_text(json_text, encoding="utf-8")
        return p, "json"
    if case == "json_path_auto":
        p = tmp_path / "rows.auto.json"
        p.write_text(json_text, encoding="utf-8")
        return p, "auto"
    if case == "jsonl_path":
        p = tmp_path / "rows.jsonl"
        p.write_text(jsonl_text, encoding="utf-8")
        return p, "jsonl"
    if case == "jsonl_path_auto":
        p = tmp_path / "rows.auto.jsonl"
        p.write_text(jsonl_text, encoding="utf-8")
        return p, "auto"
    raise ValueError(f"unsupported input case: {case}")


def _read_result(
    rows: list[dict[str, object]],
    case: str,
    tmp_path,
    options: dict[str, object] | None = None,
    **option_kwargs,
):
    """Read result."""
    data, fmt = _prepare_input(rows, case, tmp_path)
    options = {**(options or {}), **option_kwargs}
    schema_contract = options.pop("schema_contract", None)
    if schema_contract is not None:
        return ExecutionContext().to_table(
            data,
            options=normalize_call_options(schema_contract=schema_contract, **options),
            format=fmt,
            source="python" if fmt == "python" else "auto",
        )
    if fmt == "python":
        return read_test_python(data, output_format="pyarrow", **options)
    if case.startswith("jsonl"):
        return read_test_jsonl(data, output_format="pyarrow", **options)
    return read_test_json(data, output_format="pyarrow", **options)


def test_column_order_alphabetically_is_recursive_by_default() -> None:
    """Verify default output ordering is alphabetical at every struct depth."""
    rows = [
        {
            "z": 1,
            "a": {"z": 1, "a": 2, "m": {"z": 3, "a": 4}},
            "l": [{"z": 1, "a": 2, "m": {"z": 3, "a": 4}}],
        }
    ]

    res = read_test_python(rows, output_format="pyarrow")

    assert res.clean_data is not None
    schema = res.clean_data.schema
    assert schema.names == ["a", "l", "z"]

    a_type = schema.field("a").type
    assert _field_names(a_type) == ["a", "m", "z"]
    assert _field_names(a_type.field("m").type) == ["a", "z"]

    list_item_type = schema.field("l").type.value_type
    assert _field_names(list_item_type) == ["a", "m", "z"]
    assert _field_names(list_item_type.field("m").type) == ["a", "z"]


def test_column_order_schema_contract_first_preserves_inferred_order_without_contract() -> None:
    """Verify schema_contract_first preserves inferred order without a contract."""
    rows = [{"z": 1, "a": {"z": 1, "a": 2}, "l": [{"z": 1, "a": 2}]}]

    res = read_test_python(rows, output_format="pyarrow", column_order="schema_contract_first")

    assert res.clean_data is not None
    schema = res.clean_data.schema
    assert schema.names == ["z", "a", "l"]
    assert _field_names(schema.field("a").type) == ["z", "a"]
    assert _field_names(schema.field("l").type.value_type) == ["z", "a"]


def test_column_order_alphabetically_applies_to_strict_schema_contract() -> None:
    """Verify strict schema contracts are reordered alphabetically by default."""
    schema_contract = pa.schema(
        [
            ("z", pa.int64()),
            ("a", pa.struct([("z", pa.int64()), ("a", pa.int64())])),
        ]
    )
    rows = [{"z": 1, "a": {"z": 2, "a": 3}}]

    res = _read_result(
        rows,
        case="python_obj",
        tmp_path=None,
        schema_contract=schema_contract,
        schema_mode="strict",
    )

    assert res.clean_data is not None
    schema = res.clean_data.schema
    assert schema.names == ["a", "z"]
    assert _field_names(schema.field("a").type) == ["a", "z"]


def test_column_order_alphabetically_reorders_additive_contract_nested_fields() -> None:
    """Verify additive schema contracts merge and sort newly inferred nested fields."""
    schema_contract = pa.schema(
        [
            (
                "variables",
                pa.struct(
                    [
                        ("email", pa.string()),
                        ("phone", pa.string()),
                    ]
                ),
            )
        ]
    )
    rows = [
        {
            "variables": {
                "birthday": "2026-01-01",
                "company": "acme",
                "email": "a@example.com",
            }
        }
    ]

    res = _read_result(
        rows,
        case="python_obj",
        tmp_path=None,
        schema_contract=schema_contract,
        schema_mode="additive",
        column_order="alphabetically",
    )

    assert res.clean_data is not None
    assert _field_names(res.clean_data.schema.field("variables").type) == [
        "birthday",
        "company",
        "email",
        "phone",
    ]


def test_column_order_alphabetically_reorders_incremental_registry_struct_fields(
    tmp_path,
) -> None:
    """Verify registry-backed additive runs sort existing and newly added nested fields."""
    first_path = tmp_path / "first.jsonl"
    first_path.write_text(
        json.dumps({"variables": {"email": "a@example.com", "phone": "1"}}) + "\n",
        encoding="utf-8",
    )
    second_path = tmp_path / "second.jsonl"
    second_path.write_text(
        json.dumps(
            {
                "variables": {
                    "birthday": "2026-01-01",
                    "company": "acme",
                    "country": "ES",
                    "email": "b@example.com",
                }
            }
        )
        + "\n",
        encoding="utf-8",
    )

    first = ss.to_pyarrow(
        first_path,
        input_format="jsonl",
        field_name_policy="lower_snake",
        column_order="alphabetically",
    )
    second = ss.to_pyarrow(
        second_path,
        input_format="jsonl",
        field_name_policy="lower_snake",
        column_order="alphabetically",
        schema_registry=first.schema_registry,
    )

    assert second.clean_data is not None
    variable_names = _field_names(second.clean_data.schema.field("variables").type)
    assert variable_names == ["birthday", "company", "country", "email", "phone"]

    registry_fields = second.schema_registry["canonical_schema"]["fields"]
    variables = next(field for field in registry_fields if field["name"] == "variables")
    assert [field["name"] for field in variables["type"]["fields"]] == variable_names


@pytest.mark.parametrize("input_case", _INPUT_CASES)
def test_field_name_policy_lower_alpha_sanitizes_dirty_keys_by_default(
    input_case, tmp_path
) -> None:
    """Verify dirty source keys are sanitized and still materialized."""
    rows = [
        {
            "User-ID": 1,
            "Full Name": "Ana",
            "nested-Obj": {"Bad.Key": 2, "UPPER": 3},
            "user id": 4,
        }
    ]

    res = _read_result(rows, case=input_case, tmp_path=tmp_path)

    assert res.clean_data is not None
    table = res.clean_data
    assert table.schema.names == ["fullname", "nestedobj", "useridorojrd", "useridrpvqmr"]
    assert _field_names(table.schema.field("nestedobj").type) == ["badkey", "upper"]
    assert table.to_pylist() == [
        {
            "fullname": "Ana",
            "nestedobj": {"badkey": 2, "upper": 3},
            "useridorojrd": 1,
            "useridrpvqmr": 4,
        }
    ]


def test_field_name_policy_preserve_keeps_source_key_names() -> None:
    """Verify preserve mode disables output field-name sanitization."""
    rows = [{"User-ID": 1, "Full Name": "Ana", "nested-Obj": {"Bad.Key": 2}}]

    res = read_test_python(rows, output_format="pyarrow", field_name_policy="preserve")

    assert res.clean_data is not None
    assert res.clean_data.schema.names == ["Full Name", "User-ID", "nested-Obj"]
    assert _field_names(res.clean_data.schema.field("nested-Obj").type) == ["Bad.Key"]
    assert res.clean_data.to_pylist() == rows


@pytest.mark.parametrize("input_case", _INPUT_CASES)
def test_empty_container_elements_do_not_infer_fields(input_case, tmp_path) -> None:
    """Verify empty nested containers provide no type evidence."""
    rows = [
        {"items": [{}, {"id": 1, "details": {}}]},
        {"items": [[]]},
    ]

    res = _read_result(rows, options={}, case=input_case, tmp_path=tmp_path)

    assert res.clean_data is not None
    item_type = res.clean_data.schema.field("items").type.value_type
    assert pa.types.is_struct(item_type)
    assert item_type.names == ["id"]
    assert res.clean_data.to_pylist() == [
        {"items": [None, {"id": 1}]},
        {"items": [None]},
    ]


def test_empty_fields_do_not_affect_sanitized_name_collisions() -> None:
    """Verify ignored keys cannot reserve a cleaned output name."""
    result = read_test_python(
        [{"User-ID": {}, "user id": 1}],
        output_format="pyarrow",
    )

    assert result.clean_data.schema.names == ["userid"]
    assert result.clean_data.to_pylist() == [{"userid": 1}]


def test_field_name_policy_lower_snake_keeps_numbers_and_underscores() -> None:
    """Verify lower_snake preserves useful BigQuery-compatible separators."""
    rows = [{"User-ID 2": 1, "user_id_2": 2}]

    res = read_test_python(rows, output_format="pyarrow", field_name_policy="lower_snake")

    assert res.clean_data is not None
    assert res.clean_data.schema.names == ["user_id_2cwygmp", "user_id_2tifcvc"]


def test_field_name_collision_suffixes_are_independent_of_source_order() -> None:
    """Verify collision suffixes are derived from dirty keys, not observation order."""
    first = read_test_python([{"User-ID": 1, "user id": 2}], output_format="pyarrow")
    second = read_test_python([{"user id": 2, "User-ID": 1}], output_format="pyarrow")

    assert first.clean_data is not None
    assert second.clean_data is not None
    assert first.clean_data.schema.names == second.clean_data.schema.names
    assert first.clean_data.schema.names == ["useridorojrd", "useridrpvqmr"]
    assert first.clean_data.to_pylist() == second.clean_data.to_pylist()
    assert first.clean_data.to_pylist() == [{"useridorojrd": 1, "useridrpvqmr": 2}]


def test_versioned_sibling_fields_prefer_list_variant_for_single_values() -> None:
    """Verify list variants receive arrays and single values through wrapping."""
    sentence_struct = pa.struct([pa.field("text", pa.string())])
    schema_contract = pa.schema(
        [
            pa.field("sentences", sentence_struct),
            pa.field("sentences_v2_struct_array", pa.list_(sentence_struct)),
        ]
    )

    res = _read_result(
        [{"sentences": {"text": "one"}}, {"sentences": [{"text": "two"}]}],
        case="python_obj",
        tmp_path=None,
        schema_contract=schema_contract,
        schema_mode="strict",
        field_name_policy="lower_snake",
        on_error="stop",
    )

    assert res.clean_data is not None
    assert res.clean_data.to_pylist() == [
        {"sentences": None, "sentences_v2_struct_array": [{"text": "one"}]},
        {"sentences": None, "sentences_v2_struct_array": [{"text": "two"}]},
    ]


def test_hybrid_version_names_route_scalars_to_the_most_compatible_type() -> None:
    """Verify semantic sibling names do not change compatibility-based routing."""
    schema_contract = pa.schema(
        [
            pa.field("value", pa.string()),
            pa.field("value_v2_integer", pa.int64()),
            pa.field("value_v3_float", pa.float64()),
        ]
    )

    res = _read_result(
        [{"value": "text"}, {"value": 7}, {"value": 2.5}, {"value": " 7 "}],
        case="python_obj",
        tmp_path=None,
        schema_contract=schema_contract,
        schema_mode="strict",
        field_name_policy="lower_snake",
        on_error="stop",
        parse_integers=True,
    )

    assert res.clean_data is not None
    assert res.clean_data.to_pylist() == [
        {"value": "text", "value_v2_integer": None, "value_v3_float": None},
        {"value": None, "value_v2_integer": 7, "value_v3_float": None},
        {"value": None, "value_v2_integer": None, "value_v3_float": 2.5},
        {"value": None, "value_v2_integer": 7, "value_v3_float": None},
    ]


def test_registry_variant_routing_collapses_integer_values_to_float(tmp_path) -> None:
    """Verify registry routing sends numeric values to the single float variant."""
    require_native()
    registry = _versioned_scalar_registry(
        "phone",
        [pa.int64(), pa.string(), pa.float64()],
    )
    path = tmp_path / "phones.jsonl"
    path.write_text(
        '{"phone":" 5583993017100 "}\n'
        '{"phone":" 5583993017100.0 "}\n'
        '{"phone":5583993017100}\n'
        '{"phone":5583993017100.0}\n'
        '{"phone":"558 399 301 7100"}\n',
        encoding="utf-8",
    )

    res = ss.to_pyarrow(
        path,
        input_format="jsonl",
        schema_mode="strict",
        field_name_policy="lower_snake",
        schema_registry=registry,
        parse_integers=True,
        parse_floats=True,
    )

    assert [
        {
            "phone": row["phone"],
            "phone_v2_string": row["phone_v2_string"],
        }
        for row in res.clean_data.to_pylist()
    ] == [
        {"phone": 5583993017100.0, "phone_v2_string": None},
        {"phone": 5583993017100.0, "phone_v2_string": None},
        {"phone": 5583993017100.0, "phone_v2_string": None},
        {"phone": 5583993017100.0, "phone_v2_string": None},
        {"phone": None, "phone_v2_string": "558 399 301 7100"},
    ]


def test_nested_variant_routing_prefers_integer_string_over_float_string() -> None:
    """Verify nested scalar variants use the same native-value routing."""
    require_native()
    schema_contract = pa.schema(
        [
            pa.field(
                "contact",
                pa.struct(
                    [
                        pa.field("phone", pa.int64()),
                        pa.field("phone_v2_string", pa.string()),
                        pa.field("phone_v3_float", pa.float64()),
                    ]
                ),
            )
        ]
    )

    res = _read_result(
        [
            {"contact": {"phone": " 5583993017100 "}},
            {"contact": {"phone": " 5583993017100.0 "}},
            {"contact": {"phone": "558 399 301 7100"}},
        ],
        case="python_obj",
        tmp_path=None,
        schema_contract=schema_contract,
        schema_mode="strict",
        field_name_policy="lower_snake",
        parse_integers=True,
        parse_floats=True,
    )

    assert res.clean_data.to_pylist() == [
        {
            "contact": {
                "phone": 5583993017100,
                "phone_v2_string": None,
                "phone_v3_float": None,
            }
        },
        {
            "contact": {
                "phone": None,
                "phone_v2_string": None,
                "phone_v3_float": 5583993017100.0,
            }
        },
        {
            "contact": {
                "phone": None,
                "phone_v2_string": "558 399 301 7100",
                "phone_v3_float": None,
            }
        },
    ]


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
