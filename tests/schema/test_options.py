"""Tests option encoding and validation."""

from __future__ import annotations

import re
from pathlib import Path

import pytest
from conftest import require_native

from schema_sanitizer.core_impl.native_options import OPTIONS
from schema_sanitizer.options_impl.call_options import (
    normalize_call_options,
    normalize_call_options_or_none,
)
from schema_sanitizer.options_impl.options import Options as InternalOptions


def _macro_calls(text: str, macro_name: str) -> list[tuple[int, str]]:
    """Return argument text for calls to a C-style macro."""
    out: list[tuple[int, str]] = []
    search_from = 0
    while True:
        start = text.find(macro_name + "(", search_from)
        if start < 0:
            return out
        line_start = text.rfind("\n", 0, start) + 1
        if text[line_start:start].strip():
            search_from = start + 1
            continue
        pos = start + len(macro_name) + 1
        depth = 1
        in_string = False
        escaped = False
        while pos < len(text) and depth:
            ch = text[pos]
            if in_string:
                if escaped:
                    escaped = False
                elif ch == "\\":
                    escaped = True
                elif ch == '"':
                    in_string = False
            elif ch == '"':
                in_string = True
            elif ch == "(":
                depth += 1
            elif ch == ")":
                depth -= 1
            pos += 1
        out.append((start, text[start + len(macro_name) + 1 : pos - 1]))
        search_from = pos


def _split_macro_args(args: str) -> list[str]:
    """Split macro arguments on top-level commas."""
    out: list[str] = []
    start = 0
    depth = 0
    in_string = False
    escaped = False
    for pos, ch in enumerate(args):
        if in_string:
            if escaped:
                escaped = False
            elif ch == "\\":
                escaped = True
            elif ch == '"':
                in_string = False
        elif ch == '"':
            in_string = True
        elif ch == "(":
            depth += 1
        elif ch == ")":
            depth -= 1
        elif ch == "," and depth == 0:
            out.append(args[start:pos].strip())
            start = pos + 1
    out.append(args[start:].strip())
    return out


def _decode_cpp_string_literals(expr: str) -> str:
    """Return concatenated contents from C++ string literals."""
    return "".join(match.group(1) for match in re.finditer(r'"((?:[^"\\]|\\.)*)"', expr))


def _normalize_cxx_default_expr(expr: str) -> str:
    """Return normalize cxx default expr for the test."""
    return " ".join(expr.split())


def _cpp_options_catalog() -> list[tuple[str, str, str, str, str]]:
    """Return cpp options catalog for the test."""
    catalog_path = (
        Path(__file__).resolve().parents[2] / "cpp/src/sanitize/options/options_catalog.def"
    )
    catalog_text = catalog_path.read_text(encoding="utf-8")
    entries: list[tuple[int, tuple[str, str, str, str, str]]] = []
    for start, call_args in _macro_calls(catalog_text, "SCHEMA_SANITIZER_OPTION"):
        cxx_type, name, default_expr, group, doc = _split_macro_args(call_args)
        entries.append(
            (
                start,
                (
                    cxx_type,
                    name,
                    _normalize_cxx_default_expr(default_expr),
                    _decode_cpp_string_literals(group),
                    _decode_cpp_string_literals(doc),
                ),
            )
        )
    for start, call_args in _macro_calls(catalog_text, "SCHEMA_SANITIZER_OPTION_DEFAULT"):
        cxx_type, name, group, doc = _split_macro_args(call_args)
        entries.append(
            (
                start,
                (
                    cxx_type,
                    name,
                    "{}",
                    _decode_cpp_string_literals(group),
                    _decode_cpp_string_literals(doc),
                ),
            )
        )
    return [entry for _, entry in sorted(entries)]


def test_native_options_catalog_matches_cpp_source_order_and_groups() -> None:
    """Verify native option metadata follows the C++ catalog without a Python copy."""
    cpp_catalog = _cpp_options_catalog()
    assert [(spec.name, spec.group) for spec in OPTIONS] == [
        (name, group) for _, name, _, group, _ in cpp_catalog
    ]
    assert len(OPTIONS) == len({spec.name for spec in OPTIONS})


def test_python_options_catalog_facades_are_removed() -> None:
    """Verify the former generated catalog and default parser stay retired."""
    package = Path(__file__).resolve().parents[2] / "src/schema_sanitizer/core_impl/native_options"
    assert not (package / "catalog.py").exists()
    assert not (package / "defaults.py").exists()


def test_memory_limit_helper_translates_only_native_unset_sentinel() -> None:
    """Python policies see an unset native memory limit as ``None``."""
    from schema_sanitizer.options_impl.options import (
        Options,
        memory_limit_bytes_or_none,
    )

    options = Options()
    assert options.performance.memory_limit_bytes == -1
    assert memory_limit_bytes_or_none(options) is None

    options.performance.memory_limit_bytes = -1
    assert memory_limit_bytes_or_none(options) is None
    options.performance.memory_limit_bytes = 1024 * 1024
    assert memory_limit_bytes_or_none(options) == 1024 * 1024
    assert memory_limit_bytes_or_none(None) is None


def test_options_validate_native_rejects_bad_enum() -> None:
    """Verify options validate native rejects bad enum."""
    require_native()
    with pytest.raises(ValueError, match="schema_evolution"):
        InternalOptions(schema={"schema_evolution": "NOT_A_MODE"})


def test_validate_native_accepts_memory_limit() -> None:
    """Verify validate native accepts memory limit."""
    require_native()
    opt = InternalOptions(performance={"memory_limit_bytes": 1024 * 1024})

    opt.validate_native()

    assert opt.performance.memory_limit_bytes == 1024 * 1024


def test_options_validate_native_rejects_negative_depth_limits() -> None:
    """Verify options validate native rejects negative depth limits."""
    require_native()

    for key in ("arrow_max_depth", "parquet_max_depth"):
        opt = InternalOptions(inference={key: -1})
        with pytest.raises(Exception, match=key):
            opt.validate_native()


def test_public_call_options_map_to_native_options() -> None:
    """Verify public call options map to native options."""
    pa = pytest.importorskip("pyarrow")
    opt = normalize_call_options(
        schema_contract=pa.schema([("a", pa.int64())]),
        schema_mode="strict",
        on_error="stop",
        multi_threading=True,
        memory_limit_bytes=1024 * 1024,
        xml_row_tag="row",
    )

    assert opt.schema.schema_evolution.name == "STRICT"
    assert opt.errors.on_error.name == "STOP"
    assert opt.performance.threading_mode.name == "MULTI"
    assert opt.performance.memory_limit_bytes == 1024 * 1024
    assert opt.performance.memory_limit_bytes == 1024 * 1024
    assert opt.xml.xml_row_tag == "row"


def test_public_call_options_reject_removed_quarantine_policy() -> None:
    """Verify the removed quarantine policy is rejected."""
    with pytest.raises(ValueError, match="on_error"):
        normalize_call_options(on_error="quarantine")


def test_public_call_options_reject_old_schema_kwarg() -> None:
    """Verify the old schema kwarg is no longer accepted."""
    with pytest.raises(TypeError, match="schema"):
        normalize_call_options(schema={"fields": [{"name": "a", "type": "int64"}]})


def test_internal_call_options_reject_object_schema_contract() -> None:
    """Verify schema_contract rejects non-PyArrow objects."""
    with pytest.raises(TypeError, match="schema_contract"):
        normalize_call_options(schema_contract=object())


def test_internal_call_options_reject_non_pyarrow_schema_contract() -> None:
    """Verify the internal schema contract only accepts PyArrow schemas."""
    pytest.importorskip("pyarrow")
    for value in (
        {"fields": [{"name": "a", "type": "int64"}]},
        b"not-a-schema",
    ):
        with pytest.raises(TypeError, match="schema_contract"):
            normalize_call_options(schema_contract=value)


def test_public_call_options_reject_strict_without_schema_contract() -> None:
    """Verify strict schema mode requires a registry-derived contract."""
    with pytest.raises(ValueError, match="schema contract"):
        normalize_call_options(schema_mode="strict")


def test_native_options_reject_strict_without_schema_contract() -> None:
    """Verify native execution rejects strict schema mode without a contract."""
    require_native()
    from schema_sanitizer.api_impl.execution_context import ExecutionContext

    with pytest.raises(Exception, match="schema contract"):
        ExecutionContext().to_table(
            [{"a": 1}],
            options=InternalOptions(schema={"schema_evolution": "strict"}),
            format="python",
            source="python",
        )


def test_public_call_options_reject_string_for_sequence_options() -> None:
    """Verify public call options reject string for sequence options."""
    for key in (
        "true_tokens",
        "false_tokens",
        "custom_timestamp_patterns",
        "custom_date_patterns",
        "custom_time_patterns",
    ):
        with pytest.raises(TypeError, match=key):
            normalize_call_options(**{key: "yes"})


def test_public_call_options_accept_list_for_sequence_options() -> None:
    """Verify public call options accept list for sequence options."""
    opt = normalize_call_options(true_tokens=["yes"], custom_timestamp_patterns=["^\\d{4}$"])

    assert opt.inference.true_tokens == ["yes"]
    assert opt.inference.timestamp_regexps == ["^\\d{4}$"]


def test_removed_temporal_pattern_names_are_rejected() -> None:
    """Verify removed public temporal pattern names have no compatibility aliases."""
    for key in ("timestamp_patterns", "date_patterns", "time_patterns"):
        with pytest.raises(TypeError, match="Unknown option"):
            normalize_call_options(**{key: ()})


def test_public_call_options_reject_non_positive_memory_limit() -> None:
    """Verify public call options reject non positive memory limit."""
    for value in (0, -1):
        with pytest.raises(ValueError, match="memory_limit_bytes"):
            normalize_call_options(memory_limit_bytes=value)


def test_public_call_options_reject_invalid_numeric_limits() -> None:
    """Verify public call options reject invalid numeric limits."""
    for key in ("arrow_max_depth", "parquet_max_depth"):
        with pytest.raises(ValueError, match=key):
            normalize_call_options(**{key: -1})
    for value in (0, -1):
        with pytest.raises(TypeError, match="read_chunk_bytes"):
            normalize_call_options(read_chunk_bytes=value)


def test_public_call_options_reject_removed_max_depth() -> None:
    """Verify the old max_depth option is no longer accepted."""
    with pytest.raises(TypeError, match="max_depth"):
        normalize_call_options(max_depth=1)


def test_public_call_options_reject_non_int_numeric_options() -> None:
    """Verify public call options reject non int numeric options."""
    for key in (
        "memory_limit_bytes",
        "arrow_max_depth",
        "parquet_max_depth",
    ):
        with pytest.raises(TypeError, match=key):
            normalize_call_options(**{key: True})
        with pytest.raises(TypeError, match=key):
            normalize_call_options(**{key: "1024"})


def test_public_call_options_reject_non_bool_boolean_options() -> None:
    """Verify public call options reject non bool boolean options."""
    for key in ("parse_integers", "parse_floats", "csv_has_header"):
        with pytest.raises(TypeError, match=key):
            normalize_call_options(**{key: "false"})


def test_public_float_separator_defaults_and_validation() -> None:
    """Verify public float separator options are explicit and deterministic."""
    defaults = normalize_call_options()
    assert defaults.inference.parse_float_decimal_separator == "."
    assert defaults.inference.parse_float_thousands_separator == ","

    custom = normalize_call_options(
        parse_float_decimal_separator=",",
        parse_float_thousands_separator=".",
    )
    assert custom.inference.parse_float_decimal_separator == ","
    assert custom.inference.parse_float_thousands_separator == "."

    with pytest.raises(ValueError, match="must differ"):
        normalize_call_options(
            parse_float_decimal_separator=".",
            parse_float_thousands_separator=".",
        )


def test_public_call_options_reject_invalid_string_options() -> None:
    """Verify public call options reject invalid string options."""
    for key in (
        "schema_mode",
        "column_order",
        "scalar_object_key",
        "csv_delimiter",
        "input_text_encoding",
        "on_error",
    ):
        with pytest.raises(TypeError, match=key):
            normalize_call_options(**{key: None})


def test_public_call_options_validate_input_text_encoding() -> None:
    """Verify public call options validate input text encoding."""
    opt = normalize_call_options(input_text_encoding=" iso8859-1 ")

    assert opt.io.input_text_encoding == "iso8859-1"

    with pytest.raises(ValueError, match="input_text_encoding"):
        normalize_call_options(input_text_encoding="")
    with pytest.raises(ValueError, match="input_text_encoding"):
        normalize_call_options(input_text_encoding="not-a-real-codec")


def test_public_call_options_validate_xml_row_tag() -> None:
    """Verify xml_row_tag accepts only explicit non-empty strings or None."""
    assert normalize_call_options(xml_row_tag=None).xml.xml_row_tag == ""
    assert normalize_call_options(xml_row_tag=" row ").xml.xml_row_tag == "row"

    with pytest.raises(ValueError, match="xml_row_tag"):
        normalize_call_options(xml_row_tag="")
    with pytest.raises(ValueError, match="xml_row_tag"):
        normalize_call_options(xml_row_tag="bad tag")
    with pytest.raises(TypeError, match="xml_row_tag"):
        normalize_call_options(xml_row_tag=True)


@pytest.mark.parametrize(
    ("group", "key", "value"),
    (
        ("io", "unknown_io_option", "streaming"),
        ("xml", "unknown_xml_option", "row"),
        ("errors", "unknown_error_option", 0),
        ("performance", "unknown_performance_option", 1),
        ("inference", "unknown_inference_option", True),
    ),
)
def test_internal_options_reject_unknown_group_fields(group: str, key: str, value: object) -> None:
    """Verify internal options reject unknown group fields."""
    with pytest.raises(AttributeError, match=key):
        InternalOptions(**{group: {key: value}})


def test_options_include_defaults_roundtrip_is_constructible() -> None:
    """Verify options include defaults roundtrip is constructible."""
    opt = InternalOptions()
    payload = opt.to_dict(include_defaults=True)
    reconstructed = InternalOptions.from_dict(payload)
    assert "true_tokens" in payload.get("inference", {})
    assert "false_tokens" in payload.get("inference", {})
    assert reconstructed.inference.true_tokens == opt.inference.true_tokens
    assert reconstructed.inference.false_tokens == opt.inference.false_tokens


def test_numeric_string_parsing_defaults_to_disabled() -> None:
    """Verify scalar string parsing features are disabled by default."""
    public = normalize_call_options()
    internal = InternalOptions()

    for key in (
        "parse_integers",
        "parse_floats",
        "parse_iso_timestamps",
        "parse_iso_dates",
        "parse_iso_times",
    ):
        assert getattr(public.inference, key) is False
        assert getattr(internal.inference, key) is False


def test_public_default_call_options_can_use_native_defaults() -> None:
    """Verify default public options can skip explicit option serialization."""
    assert normalize_call_options_or_none() is None
    assert normalize_call_options_or_none(parse_integers=False, true_tokens=[]) is None
    assert normalize_call_options_or_none(parse_integers=True) is not None
    with pytest.raises(TypeError, match="parse_integers"):
        normalize_call_options_or_none(parse_integers=0)


def test_column_order_defaults_to_alphabetically() -> None:
    """Verify public and internal field ordering default to alphabetically."""
    public = normalize_call_options()
    internal = InternalOptions()

    assert public.schema.field_order.name == "ALPHABETICALLY"
    assert internal.schema.field_order.name == "ALPHABETICALLY"


def test_column_order_rejects_removed_sorted_alias() -> None:
    """Verify the removed sorted spelling is not accepted."""
    with pytest.raises(ValueError, match="column_order"):
        normalize_call_options(column_order="sorted")
    with pytest.raises(ValueError, match="field_order"):
        InternalOptions(schema={"field_order": "sorted"})


def test_field_name_policy_defaults_to_lower_alpha() -> None:
    """Verify field-name sanitization is enabled by default."""
    public = normalize_call_options()
    internal = InternalOptions()

    assert public.schema.field_name_policy == "lower_alpha"
    assert internal.schema.field_name_policy == "lower_alpha"


def test_public_field_name_policy_accepts_only_canonical_names() -> None:
    """Verify removed compact and dashed policy spellings are rejected."""
    assert (
        normalize_call_options(field_name_policy=" PRESERVE ").schema.field_name_policy
        == "preserve"
    )
    for value in ("lower-alpha", "loweralpha", "lower-snake"):
        with pytest.raises(ValueError, match="field_name_policy"):
            normalize_call_options(field_name_policy=value)


def test_public_field_name_policy_rejects_invalid_values() -> None:
    """Verify public field-name policy rejects unsupported values."""
    with pytest.raises(ValueError, match="field_name_policy"):
        normalize_call_options(field_name_policy="camelCase")


def test_internal_options_reject_string_for_sequence_options() -> None:
    """Verify internal options reject string for sequence options."""
    for key in (
        "true_tokens",
        "false_tokens",
        "timestamp_regexps",
        "date_regexps",
        "time_regexps",
    ):
        with pytest.raises(TypeError, match=key):
            InternalOptions(inference={key: "yes"})


def test_internal_options_reject_non_bool_boolean_options() -> None:
    """Verify internal options reject non bool boolean options."""
    for key in (
        "parse_integers",
        "parse_floats",
        "parse_iso_timestamps",
        "parse_iso_dates",
        "parse_iso_times",
    ):
        with pytest.raises(TypeError, match=key):
            InternalOptions(inference={key: "false"})
    with pytest.raises(TypeError, match="csv_has_header"):
        InternalOptions(csv={"csv_has_header": "false"})


def test_internal_options_reject_non_int_integer_options() -> None:
    """Verify internal options reject non int integer options."""
    for group, key in (
        ("inference", "arrow_max_depth"),
        ("inference", "parquet_max_depth"),
        ("performance", "memory_limit_bytes"),
    ):
        with pytest.raises(TypeError, match=key):
            InternalOptions(**{group: {key: True}})
        with pytest.raises(TypeError, match=key):
            InternalOptions(**{group: {key: "1024"}})


def test_internal_options_reject_non_string_string_options() -> None:
    """Verify internal options reject non string string options."""
    for group, key in (
        ("schema", "timestamp_precision"),
        ("inference", "default_key_name"),
        ("io", "input_text_encoding"),
        ("xml", "xml_row_tag"),
        ("csv", "csv_delimiter"),
    ):
        with pytest.raises(TypeError, match=key):
            InternalOptions(**{group: {key: None}})


def test_internal_options_reject_invalid_enum_values() -> None:
    """Verify internal options reject invalid enum values."""
    for value in (True, 999, "NOT_A_MODE"):
        with pytest.raises((TypeError, ValueError), match="schema_evolution"):
            InternalOptions(schema={"schema_evolution": value})


def test_internal_options_reject_invalid_timestamp_precision_native() -> None:
    """Verify native validation rejects invalid timestamp precision strings."""
    opt = InternalOptions(schema={"timestamp_precision": "TIMESTAMP_SECONDS"})

    with pytest.raises(Exception, match="timestamp_precision"):
        opt.validate_native()


def test_internal_options_reject_invalid_field_name_policy_native() -> None:
    """Verify native validation rejects invalid field-name policies."""
    opt = InternalOptions(schema={"field_name_policy": "camelCase"})

    with pytest.raises(Exception, match="field_name_policy"):
        opt.validate_native()


@pytest.mark.parametrize("group", ("unknown_group", "output", "coercion", "strings"))
def test_unknown_groups_are_not_supported(group: str) -> None:
    """Verify unknown groups are not supported."""
    with pytest.raises(TypeError, match=group):
        InternalOptions(**{group: {"some_option": True}})


def test_unknown_option_attribute_is_not_supported() -> None:
    """Verify unknown option attribute is not supported."""
    opt = InternalOptions()
    with pytest.raises(AttributeError, match="unknown_option"):
        opt.unknown_option = True
