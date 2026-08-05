"""Tests raw option byte encoding and native payload validation."""

from __future__ import annotations

from pathlib import Path

import pytest
from conftest import require_native

_FIXED_OPTION_PAYLOAD_WIDTHS = {
    "bool": 1,
    "i32": 4,
    "i64": 8,
    "schema_evolution": 4,
    "field_order": 4,
    "on_error": 4,
    "threading_mode": 4,
}


def _skip_options_payload_value(mv: memoryview, pos: int, kind: str) -> int:
    """Skip one native option value by its catalog kind."""
    fixed_width = _FIXED_OPTION_PAYLOAD_WIDTHS.get(kind)
    if fixed_width is not None:
        return pos + fixed_width
    if kind == "string":
        size = int.from_bytes(mv[pos : pos + 4], "little")
        return pos + 4 + size
    if kind == "string_list":
        count = int.from_bytes(mv[pos : pos + 4], "little")
        pos += 4
        for _ in range(count):
            size = int.from_bytes(mv[pos : pos + 4], "little")
            pos += 4 + size
        return pos
    if kind == "logical_schema":
        has_schema = int(mv[pos])
        pos += 1
        if not has_schema:
            return pos
        size = int.from_bytes(mv[pos : pos + 4], "little")
        return pos + 4 + size
    raise AssertionError(f"unexpected option kind: {kind}")


def _corrupt_first_options_bool_byte(payload: bytes) -> bytes:
    """Corrupt first options bool byte."""
    from schema_sanitizer.core_impl import native_options as _opts_bytes

    mv = memoryview(payload)
    pos = 11
    for spec in _opts_bytes.OPTIONS:
        if spec.kind == "bool":
            out = bytearray(payload)
            out[pos] = 2
            return bytes(out)
        pos = _skip_options_payload_value(mv, pos, spec.kind)
    raise AssertionError("expected at least one bool option")


def _corrupt_first_options_enum_value(payload: bytes) -> bytes:
    """Corrupt first options enum value."""
    from schema_sanitizer.core_impl import native_options as _opts_bytes

    mv = memoryview(payload)
    pos = 11
    for spec in _opts_bytes.OPTIONS:
        if spec.kind in _opts_bytes.ENUM_BY_KIND:
            out = bytearray(payload)
            out[pos : pos + 4] = (999).to_bytes(4, "little", signed=True)
            return bytes(out)
        pos = _skip_options_payload_value(mv, pos, spec.kind)
    raise AssertionError("expected at least one enum option")


def _corrupt_first_logical_schema_nullable_byte(payload: bytes) -> bytes:
    """Corrupt first logical schema nullable byte."""
    name_len = int.from_bytes(payload[4:8], "little")
    nullable_pos = 8 + name_len
    out = bytearray(payload)
    out[nullable_pos] = 2
    return bytes(out)


def test_options_bytes_encoder_rejects_non_bool_bool_values() -> None:
    """Verify options bytes encoder rejects non bool bool values."""
    from schema_sanitizer.core_impl import native_options as _opts_bytes

    opts = _opts_bytes.Options()
    opts.parse_integers = "false"

    with pytest.raises(TypeError, match="parse_integers"):
        _opts_bytes._encode_options_bytes(opts)


def test_options_bytes_encoder_rejects_non_int_integer_values() -> None:
    """Verify options bytes encoder rejects non int integer values."""
    from schema_sanitizer.core_impl import native_options as _opts_bytes

    for key in ("arrow_max_depth", "parquet_max_depth", "memory_limit_bytes"):
        opts = _opts_bytes.Options()
        setattr(opts, key, True)
        with pytest.raises(TypeError, match=key):
            _opts_bytes._encode_options_bytes(opts)

        opts = _opts_bytes.Options()
        setattr(opts, key, "1024")
        with pytest.raises(TypeError, match=key):
            _opts_bytes._encode_options_bytes(opts)


def test_options_bytes_encoder_rejects_non_string_string_values() -> None:
    """Verify options bytes encoder rejects non string string values."""
    from schema_sanitizer.core_impl import native_options as _opts_bytes

    for key in (
        "default_key_name",
        "parse_float_decimal_separator",
        "parse_float_thousands_separator",
        "timestamp_precision",
        "input_text_encoding",
        "xml_row_tag",
        "csv_delimiter",
    ):
        opts = _opts_bytes.Options()
        setattr(opts, key, None)
        with pytest.raises(TypeError, match=key):
            _opts_bytes._encode_options_bytes(opts)


def test_options_bytes_encoder_rejects_non_string_vector_items() -> None:
    """Verify options bytes encoder rejects non string vector items."""
    from schema_sanitizer.core_impl import native_options as _opts_bytes

    opts = _opts_bytes.Options()
    opts.true_tokens = ["yes", 1]

    with pytest.raises(TypeError, match="vector<string> items"):
        _opts_bytes._encode_options_bytes(opts)

    for value in (None, "yes"):
        opts = _opts_bytes.Options()
        opts.true_tokens = value
        with pytest.raises(TypeError, match="true_tokens"):
            _opts_bytes._encode_options_bytes(opts)


def test_options_bytes_encoder_rejects_invalid_enum_values() -> None:
    """Verify options bytes encoder rejects invalid enum values."""
    from schema_sanitizer.core_impl import native_options as _opts_bytes

    for value in (True, 999):
        opts = _opts_bytes.Options()
        opts.schema_evolution = value
        with pytest.raises(ValueError, match="SchemaEvolutionMode"):
            _opts_bytes._encode_options_bytes(opts)


def test_options_byte_append_helpers_reject_out_of_range_values() -> None:
    """Verify options byte append helpers reject out of range values."""
    from schema_sanitizer.core_impl import native_options as _options_codec

    cases = (
        (_options_codec._append_u8, -1),
        (_options_codec._append_u8, 256),
        (_options_codec._append_u32, -1),
        (_options_codec._append_u32, 1 << 32),
        (_options_codec._append_i32, -(1 << 31) - 1),
        (_options_codec._append_i32, 1 << 31),
        (_options_codec._append_i64, -(1 << 63) - 1),
        (_options_codec._append_i64, 1 << 63),
    )
    for fn, value in cases:
        with pytest.raises(ValueError, match="out of range"):
            fn(bytearray(), value)


def test_options_byte_append_helpers_reject_non_int_values() -> None:
    """Verify options byte append helpers reject non int values."""
    from schema_sanitizer.core_impl import native_options as _options_codec

    for fn in (
        _options_codec._append_u8,
        _options_codec._append_u32,
        _options_codec._append_i32,
        _options_codec._append_i64,
    ):
        for value in (True, "1"):
            with pytest.raises(TypeError, match="must be an integer"):
                fn(bytearray(), value)


def test_logical_schema_payload_rejects_invalid_nullable_byte() -> None:
    """Verify logical schema payload rejects invalid nullable byte."""
    require_native()
    pa = pytest.importorskip("pyarrow")
    from schema_sanitizer.core_impl import logical_schema as _logical_schema

    payload = _logical_schema.encode_arrow_schema_payload(pa.schema([("a", pa.int64())]))
    payload = _corrupt_first_logical_schema_nullable_byte(payload)

    with pytest.raises(ValueError, match="invalid logical field nullable"):
        _logical_schema.LogicalSchemaPayload(payload)


def test_logical_schema_payload_has_no_python_decoder() -> None:
    """Verify logical-schema binary grammar is owned only by the native codec."""
    from schema_sanitizer.core_impl import logical_schema

    assert not hasattr(logical_schema, "_decode_logical_schema_payload")
    assert not (Path(logical_schema.__file__).with_name("logical_schema")).exists()


def test_raw_options_reject_unknown_attributes() -> None:
    """Verify raw options reject unknown attributes."""
    from schema_sanitizer.core_impl import native_options as _opts_bytes

    opts = _opts_bytes.Options()
    with pytest.raises(AttributeError, match="unknown_option"):
        opts.unknown_option = True


def test_native_options_prepare_rejects_truncated_payload() -> None:
    """Verify native options prepare rejects truncated payload."""
    require_native()

    from schema_sanitizer.core_impl import native_options as _opts_bytes
    from schema_sanitizer.core_impl.native_runtime import native_core as _native

    payload = _opts_bytes._encode_options_bytes(_opts_bytes.Options())

    with pytest.raises(RuntimeError, match="truncated field"):
        _native.options_prepare_bytes(payload[:-1])


def test_native_options_prepare_rejects_invalid_bool_values() -> None:
    """Verify native options prepare rejects invalid bool values."""
    require_native()

    from schema_sanitizer.core_impl import native_options as _opts_bytes
    from schema_sanitizer.core_impl.native_runtime import native_core as _native

    payload = _corrupt_first_options_bool_byte(
        _opts_bytes._encode_options_bytes(_opts_bytes.Options())
    )

    with pytest.raises(RuntimeError, match="invalid bool field"):
        _native.options_prepare_bytes(payload)


def test_native_options_prepare_rejects_invalid_schema_nullable_byte() -> None:
    """Verify native options prepare rejects invalid schema nullable byte."""
    require_native()
    pa = pytest.importorskip("pyarrow")

    from schema_sanitizer.core_impl import logical_schema as _logical_schema
    from schema_sanitizer.core_impl import native_options as _opts_bytes
    from schema_sanitizer.core_impl.native_runtime import native_core as _native

    schema = pa.schema([("a", pa.int64())])
    schema_payload = _logical_schema.encode_arrow_schema_payload(schema)
    opts = _opts_bytes.Options()
    opts.arrow_schema_contract = schema
    options_payload = bytearray(_opts_bytes._encode_options_bytes(opts))

    mv = memoryview(options_payload)
    pos = 11
    for spec in _opts_bytes.OPTIONS:
        if spec.kind == "logical_schema":
            assert options_payload[pos] == 1
            schema_size = int.from_bytes(options_payload[pos + 1 : pos + 5], "little")
            assert schema_size == len(schema_payload)
            schema_start = pos + 5
            name_len = int.from_bytes(
                options_payload[schema_start + 4 : schema_start + 8], "little"
            )
            options_payload[schema_start + 8 + name_len] = 2
            break
        pos = _skip_options_payload_value(mv, pos, spec.kind)
    else:
        raise AssertionError("expected arrow_schema_contract option")

    with pytest.raises(RuntimeError, match="invalid logical field nullable"):
        _native.options_prepare_bytes(bytes(options_payload))


def test_native_options_prepare_rejects_invalid_enum_values() -> None:
    """Verify native options prepare rejects invalid enum values."""
    require_native()

    from schema_sanitizer.core_impl import native_options as _opts_bytes
    from schema_sanitizer.core_impl.native_runtime import native_core as _native

    payload = _corrupt_first_options_enum_value(
        _opts_bytes._encode_options_bytes(_opts_bytes.Options())
    )

    with pytest.raises(RuntimeError, match="invalid enum field"):
        _native.options_prepare_bytes(payload)


def test_options_capsule_is_reused_until_an_option_changes(monkeypatch: pytest.MonkeyPatch) -> None:
    """Verify one Options instance avoids repeat encoding and native preparation."""
    require_native()

    from schema_sanitizer.core_impl import native_options as _opts_bytes

    original = _opts_bytes._encode_options_bytes
    calls = 0

    def counted(options: object) -> bytes:
        """Count option encodes while preserving the real payload."""
        nonlocal calls
        calls += 1
        return original(options)

    monkeypatch.setattr(_opts_bytes, "_encode_options_bytes", counted)
    options = _opts_bytes.Options()

    first = _opts_bytes._options_capsule(options)
    second = _opts_bytes._options_capsule(options)
    assert first is second
    assert calls == 1

    options.parse_integers = not options.parse_integers
    _opts_bytes._options_capsule(options)
    assert calls == 2

    string_list_name = next(spec.name for spec in _opts_bytes.OPTIONS if spec.kind == "string_list")
    getattr(options, string_list_name).append("cache-invalidation-token")
    _opts_bytes._options_capsule(options)
    assert calls == 3
