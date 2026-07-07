"""Tests raw option byte encoding and native payload validation."""

from __future__ import annotations

import pytest
from conftest import require_native

_FIXED_OPTION_PAYLOAD_WIDTHS = {
    "bool": 1,
    "int32_t": 4,
    "int64_t": 8,
}


def _skip_options_payload_value(mv: memoryview, pos: int, cxx_type: str) -> int:
    """Skip options payload value."""
    fixed_width = (
        4 if cxx_type.startswith("sanitize::") else _FIXED_OPTION_PAYLOAD_WIDTHS.get(cxx_type)
    )
    if fixed_width is not None:
        return pos + fixed_width
    if cxx_type == "std::string":
        size = int.from_bytes(mv[pos : pos + 4], "little")
        return pos + 4 + size
    if cxx_type == "std::vector<std::string>":
        count = int.from_bytes(mv[pos : pos + 4], "little")
        pos += 4
        for _ in range(count):
            size = int.from_bytes(mv[pos : pos + 4], "little")
            pos += 4 + size
        return pos
    if cxx_type == "std::optional<sanitize::LogicalSchema>":
        has_schema = int(mv[pos])
        pos += 1
        if not has_schema:
            return pos
        size = int.from_bytes(mv[pos : pos + 4], "little")
        return pos + 4 + size
    raise AssertionError(f"unexpected option cxx_type: {cxx_type}")


def _corrupt_first_options_bool_byte(payload: bytes) -> bytes:
    """Corrupt first options bool byte."""
    from schema_sanitizer.core_impl import options_bytes as _opts_bytes

    mv = memoryview(payload)
    pos = 11
    for spec in _opts_bytes._OPTIONS_CATALOG:
        cxx_type = spec["cxx_type"]
        if cxx_type == "bool":
            out = bytearray(payload)
            out[pos] = 2
            return bytes(out)
        pos = _skip_options_payload_value(mv, pos, cxx_type)
    raise AssertionError("expected at least one bool option")


def _corrupt_first_options_enum_value(payload: bytes) -> bytes:
    """Corrupt first options enum value."""
    from schema_sanitizer.core_impl import options_bytes as _opts_bytes

    mv = memoryview(payload)
    pos = 11
    for spec in _opts_bytes._OPTIONS_CATALOG:
        cxx_type = spec["cxx_type"]
        if cxx_type.startswith("sanitize::"):
            out = bytearray(payload)
            out[pos : pos + 4] = (999).to_bytes(4, "little", signed=True)
            return bytes(out)
        pos = _skip_options_payload_value(mv, pos, cxx_type)
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
    from schema_sanitizer.core_impl import options_bytes as _opts_bytes

    opts = _opts_bytes.Options()
    opts.parse_integers = "false"

    with pytest.raises(TypeError, match="parse_integers"):
        _opts_bytes._encode_options_bytes(opts)


def test_options_bytes_encoder_rejects_non_int_integer_values() -> None:
    """Verify options bytes encoder rejects non int integer values."""
    from schema_sanitizer.core_impl import options_bytes as _opts_bytes

    for key in ("arrow_max_depth", "parquet_max_depth", "io_chunk_bytes", "memory_limit_bytes"):
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
    from schema_sanitizer.core_impl import options_bytes as _opts_bytes

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
    from schema_sanitizer.core_impl import options_bytes as _opts_bytes

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
    from schema_sanitizer.core_impl import options_bytes as _opts_bytes

    for value in (True, 999):
        opts = _opts_bytes.Options()
        opts.schema_evolution = value
        with pytest.raises(ValueError, match="SchemaEvolutionMode"):
            _opts_bytes._encode_options_bytes(opts)


def test_options_byte_append_helpers_reject_out_of_range_values() -> None:
    """Verify options byte append helpers reject out of range values."""
    from schema_sanitizer.core_impl import options_bytes_codec as _options_codec

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
    from schema_sanitizer.core_impl import options_bytes_codec as _options_codec

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
    from schema_sanitizer.core_impl import options_logical_schema as _schema_codec

    payload = _schema_codec._encode_logical_schema_payload_from_schema(
        pa.schema([("a", pa.int64())])
    )
    payload = _corrupt_first_logical_schema_nullable_byte(payload)

    with pytest.raises(ValueError, match="invalid logical field nullable"):
        _schema_codec._decode_logical_schema_payload(payload)


def test_logical_schema_payload_requires_native_encoder(monkeypatch) -> None:
    """Verify schema payload encoding no longer falls back to the Python PyArrow walker."""
    pa = pytest.importorskip("pyarrow")
    from schema_sanitizer.core_impl import options_logical_schema as _schema_codec

    monkeypatch.setattr(_schema_codec, "_native_arrow_schema_contract_payload", lambda: None)

    with pytest.raises(RuntimeError, match="native arrow_schema_contract_payload encoder"):
        _schema_codec._encode_logical_schema_payload_from_schema(pa.schema([("a", pa.int64())]))


def test_raw_options_reject_unknown_attributes() -> None:
    """Verify raw options reject unknown attributes."""
    from schema_sanitizer.core_impl import options_bytes as _opts_bytes

    opts = _opts_bytes.Options()
    with pytest.raises(AttributeError, match="unknown_option"):
        opts.unknown_option = True


def test_native_options_prepare_rejects_truncated_payload() -> None:
    """Verify native options prepare rejects truncated payload."""
    require_native()

    from schema_sanitizer.core_impl import options_bytes as _opts_bytes
    from schema_sanitizer.core_impl.native import _native

    payload = _opts_bytes._encode_options_bytes(_opts_bytes.Options())

    with pytest.raises(RuntimeError, match="truncated field"):
        _native.options_prepare_bytes(payload[:-1])


def test_native_options_prepare_rejects_invalid_bool_values() -> None:
    """Verify native options prepare rejects invalid bool values."""
    require_native()

    from schema_sanitizer.core_impl import options_bytes as _opts_bytes
    from schema_sanitizer.core_impl.native import _native

    payload = _corrupt_first_options_bool_byte(
        _opts_bytes._encode_options_bytes(_opts_bytes.Options())
    )

    with pytest.raises(RuntimeError, match="invalid bool field"):
        _native.options_prepare_bytes(payload)


def test_native_options_prepare_rejects_invalid_schema_nullable_byte() -> None:
    """Verify native options prepare rejects invalid schema nullable byte."""
    require_native()
    pa = pytest.importorskip("pyarrow")

    from schema_sanitizer.core_impl import options_bytes as _opts_bytes
    from schema_sanitizer.core_impl import options_logical_schema as _schema_codec
    from schema_sanitizer.core_impl.native import _native

    schema = pa.schema([("a", pa.int64())])
    schema_payload = _schema_codec._encode_logical_schema_payload_from_schema(schema)
    opts = _opts_bytes.Options()
    opts.arrow_schema_contract = schema
    options_payload = bytearray(_opts_bytes._encode_options_bytes(opts))

    mv = memoryview(options_payload)
    pos = 11
    for spec in _opts_bytes._OPTIONS_CATALOG:
        cxx_type = spec["cxx_type"]
        if cxx_type == "std::optional<sanitize::LogicalSchema>":
            assert options_payload[pos] == 1
            schema_size = int.from_bytes(options_payload[pos + 1 : pos + 5], "little")
            assert schema_size == len(schema_payload)
            schema_start = pos + 5
            name_len = int.from_bytes(
                options_payload[schema_start + 4 : schema_start + 8], "little"
            )
            options_payload[schema_start + 8 + name_len] = 2
            break
        pos = _skip_options_payload_value(mv, pos, cxx_type)
    else:
        raise AssertionError("expected arrow_schema_contract option")

    with pytest.raises(RuntimeError, match="invalid logical field nullable"):
        _native.options_prepare_bytes(bytes(options_payload))


def test_native_options_prepare_rejects_invalid_enum_values() -> None:
    """Verify native options prepare rejects invalid enum values."""
    require_native()

    from schema_sanitizer.core_impl import options_bytes as _opts_bytes
    from schema_sanitizer.core_impl.native import _native

    payload = _corrupt_first_options_enum_value(
        _opts_bytes._encode_options_bytes(_opts_bytes.Options())
    )

    with pytest.raises(RuntimeError, match="invalid enum field"):
        _native.options_prepare_bytes(payload)
