"""Tests cleaning heuristics, policy handling, and schema stability.

It covers recursive column ordering, field-name policies, null containers, and
deterministic scalar or collection variant routing.
"""

from __future__ import annotations

import json

import pytest
from _support.cleaning_policies import INPUT_CASES as _INPUT_CASES
from _support.cleaning_policies import field_names as _field_names
from _support.cleaning_policies import read_result as _read_result
from _support.cleaning_policies import versioned_scalar_registry as _versioned_scalar_registry
from conftest import read_test_python

pa = pytest.importorskip("pyarrow")

import schema_sanitizer as ss


def test_column_order_alphabetically_is_recursive_by_default() -> None:
    """Verify column order alphabetically is recursive by default."""
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
    """Verify column order schema contract first preserves inferred order without contract."""
    rows = [{"z": 1, "a": {"z": 1, "a": 2}, "l": [{"z": 1, "a": 2}]}]

    res = read_test_python(rows, output_format="pyarrow", column_order="schema_contract_first")

    assert res.clean_data is not None
    schema = res.clean_data.schema
    assert schema.names == ["z", "a", "l"]
    assert _field_names(schema.field("a").type) == ["z", "a"]
    assert _field_names(schema.field("l").type.value_type) == ["z", "a"]


def test_column_order_alphabetically_applies_to_strict_schema_contract() -> None:
    """Verify column order alphabetically applies to strict schema contract."""
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
    """Verify column order alphabetically reorders additive contract nested fields."""
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
    """Verify column order alphabetically reorders incremental registry struct fields."""
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
    """Verify field name policy lower alpha sanitizes dirty keys by default."""
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
    """Verify field name policy preserve keeps source key names."""
    rows = [{"User-ID": 1, "Full Name": "Ana", "nested-Obj": {"Bad.Key": 2}}]

    res = read_test_python(rows, output_format="pyarrow", field_name_policy="preserve")

    assert res.clean_data is not None
    assert res.clean_data.schema.names == ["Full Name", "User-ID", "nested-Obj"]
    assert _field_names(res.clean_data.schema.field("nested-Obj").type) == ["Bad.Key"]
    assert res.clean_data.to_pylist() == rows


@pytest.mark.parametrize("input_case", _INPUT_CASES)
def test_null_only_fields_do_not_infer_fields(input_case, tmp_path) -> None:
    """Verify null only fields do not infer fields."""
    rows = [
        {
            "id": 1,
            "root_null": None,
            "wrapper": {"child": None},
            "items": [None],
        }
    ]

    res = _read_result(rows, options={}, case=input_case, tmp_path=tmp_path)

    assert res.clean_data is not None
    assert res.clean_data.schema.names == ["id"]
    assert res.clean_data.to_pylist() == [{"id": 1}]


@pytest.mark.parametrize("input_case", _INPUT_CASES)
def test_empty_container_elements_do_not_infer_fields(input_case, tmp_path) -> None:
    """Verify empty container elements do not infer fields."""
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
    """Verify empty fields do not affect sanitized name collisions."""
    result = read_test_python(
        [{"User-ID": {}, "user id": 1}],
        output_format="pyarrow",
    )

    assert result.clean_data.schema.names == ["userid"]
    assert result.clean_data.to_pylist() == [{"userid": 1}]


def test_field_name_policy_lower_snake_keeps_numbers_and_underscores() -> None:
    """Verify field name policy lower snake keeps numbers and underscores."""
    rows = [{"User-ID 2": 1, "user_id_2": 2}]

    res = read_test_python(rows, output_format="pyarrow", field_name_policy="lower_snake")

    assert res.clean_data is not None
    assert res.clean_data.schema.names == ["user_id_2cwygmp", "user_id_2tifcvc"]


def test_field_name_collision_suffixes_are_independent_of_source_order() -> None:
    """Verify field name collision suffixes are independent of source order."""
    first = read_test_python([{"User-ID": 1, "user id": 2}], output_format="pyarrow")
    second = read_test_python([{"user id": 2, "User-ID": 1}], output_format="pyarrow")

    assert first.clean_data is not None
    assert second.clean_data is not None
    assert first.clean_data.schema.names == second.clean_data.schema.names
    assert first.clean_data.schema.names == ["useridorojrd", "useridrpvqmr"]
    assert first.clean_data.to_pylist() == second.clean_data.to_pylist()
    assert first.clean_data.to_pylist() == [{"useridorojrd": 1, "useridrpvqmr": 2}]


def test_versioned_sibling_fields_prefer_list_variant_for_single_values() -> None:
    """Verify versioned sibling fields prefer list variant for single values."""
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
    """Verify hybrid version names route scalars to the most compatible type."""
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


def test_registry_variant_routing_collapses_integer_values_to_float(
    tmp_path, require_native: None
) -> None:
    """Verify registry variant routing collapses integer values to float."""
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


def test_nested_variant_routing_prefers_integer_string_over_float_string(
    require_native: None,
) -> None:
    """Verify nested variant routing prefers integer string over float string."""
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
