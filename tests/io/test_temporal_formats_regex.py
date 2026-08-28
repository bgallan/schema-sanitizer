"""Custom temporal regex parsing tests."""

from __future__ import annotations

import pytest
from conftest import read_test_csv, read_test_python

from schema_sanitizer.api_impl.execution_context import ExecutionContext
from schema_sanitizer.options_impl.call_options import normalize_call_options

pytestmark = pytest.mark.usefixtures("require_native")


def _read_python_with_contract(rows, *, schema_contract, **options):
    """Read Python rows through the internal schema contract path."""
    return ExecutionContext().to_table(
        rows,
        options=normalize_call_options(schema_contract=schema_contract, **options),
        format="python",
        source="python",
    )


def test_iso_temporal_strings_are_used_for_opt_in_strict_coercion() -> None:
    pa = pytest.importorskip("pyarrow")

    result = _read_python_with_contract(
        [{"ts": "2024-01-02T03:04:05Z", "d": "2024-01-02", "t": "03:04:05"}],
        schema_contract=pa.schema(
            [
                ("ts", pa.timestamp("ns")),
                ("d", pa.date32()),
                ("t", pa.time32("s")),
            ]
        ),
        schema_mode="strict",
        on_error="stop",
        parse_iso_timestamps=True,
        parse_iso_dates=True,
        parse_iso_times=True,
    )

    assert result.clean_data.num_rows == 1
    assert pa.types.is_timestamp(result.clean_data.schema.field("ts").type)
    assert result.clean_data.schema.field("ts").type.unit == "us"
    assert pa.types.is_date32(result.clean_data.schema.field("d").type)
    assert pa.types.is_time32(result.clean_data.schema.field("t").type)


def test_iso_temporal_strings_remain_strings_by_default() -> None:
    pa = pytest.importorskip("pyarrow")

    result = read_test_python([{"ts": "2024-01-02T03:04:05Z", "d": "2024-01-02", "t": "03:04:05"}])

    assert all(
        pa.types.is_string(result.clean_data.schema.field(name).type) for name in ("ts", "d", "t")
    )


def test_iso_temporal_strings_are_used_for_opt_in_inference() -> None:
    pa = pytest.importorskip("pyarrow")

    result = read_test_python(
        [{"ts": "2024-01-02T03:04:05Z", "d": "2024-01-02", "t": "03:04:05"}],
        parse_iso_timestamps=True,
        parse_iso_dates=True,
        parse_iso_times=True,
    )

    assert pa.types.is_timestamp(result.clean_data.schema.field("ts").type)
    assert result.clean_data.schema.field("ts").type.unit == "us"
    assert pa.types.is_date32(result.clean_data.schema.field("d").type)
    assert pa.types.is_time32(result.clean_data.schema.field("t").type)


@pytest.mark.parametrize(
    ("timestamp_precision", "unit"),
    [
        ("TIMESTAMP_MILLIS", "ms"),
        ("TIMESTAMP_MICROS", "us"),
        ("TIMESTAMP_NANOS", "ns"),
    ],
)
def test_timestamp_precision_controls_arrow_timestamp_unit(
    timestamp_precision: str, unit: str
) -> None:

    result = read_test_python(
        [{"ts": "2024-01-02T03:04:05.123456789Z"}],
        timestamp_precision=timestamp_precision,
        parse_iso_timestamps=True,
    )

    assert result.clean_data.schema.field("ts").type.unit == unit


def test_invalid_timestamp_precision_is_rejected() -> None:

    with pytest.raises(ValueError, match="timestamp_precision"):
        read_test_python(
            [{"ts": "2024-01-02T03:04:05Z"}],
            timestamp_precision="TIMESTAMP_SECONDS",
        )


def test_custom_temporal_patterns_are_used_for_infer_and_coerce(tmp_path) -> None:
    pa = pytest.importorskip("pyarrow")

    csv_text = "ts,d,t\n2024/01/02 03:04:05,2024-01-02,03|04|05\n"
    path = tmp_path / "rows.csv"
    path.write_text(csv_text, encoding="utf-8")
    result = read_test_csv(
        path,
        custom_timestamp_patterns=(r"(\d{4})/(\d{2})/(\d{2}) (\d{2}):(\d{2}):(\d{2})",),
        custom_date_patterns=(r"(\d{4})-(\d{2})-(\d{2})",),
        custom_time_patterns=(r"(\d{2})\|(\d{2})\|(\d{2})",),
    )
    schema = result.clean_data.schema
    assert pa.types.is_timestamp(schema.field("ts").type)
    assert pa.types.is_date32(schema.field("d").type)
    assert pa.types.is_time32(schema.field("t").type)


def test_custom_temporal_patterns_with_fraction_and_timezone_are_used(tmp_path) -> None:
    pa = pytest.importorskip("pyarrow")

    csv_text = "ts,d,t\n2024/01/02 03:04:05.123456789+0130,2024-01-02,03|04|05\n"
    path = tmp_path / "rows.csv"
    path.write_text(csv_text, encoding="utf-8")
    result = read_test_csv(
        path,
        custom_timestamp_patterns=(
            r"(\d{4})/(\d{2})/(\d{2}) (\d{2}):(\d{2}):(\d{2})\.(\d{1,9})([+-]\d{4})",
        ),
        custom_date_patterns=(r"(\d{4})-(\d{2})-(\d{2})",),
        custom_time_patterns=(r"(\d{2})\|(\d{2})\|(\d{2})",),
    )
    schema = result.clean_data.schema
    assert pa.types.is_timestamp(schema.field("ts").type)
    assert pa.types.is_date32(schema.field("d").type)
    assert pa.types.is_time32(schema.field("t").type)


def test_custom_timestamp_pattern_with_z_timezone_is_used(tmp_path) -> None:
    pa = pytest.importorskip("pyarrow")

    path = tmp_path / "rows.csv"
    path.write_text("ts\n2024/01/02 03:04:05Z\n", encoding="utf-8")

    result = read_test_csv(
        path,
        custom_timestamp_patterns=(r"(\d{4})/(\d{2})/(\d{2}) (\d{2}):(\d{2}):(\d{2})Z",),
    )

    assert pa.types.is_timestamp(result.clean_data.schema.field("ts").type)


def test_invalid_temporal_pattern_fails_fast(tmp_path) -> None:

    path = tmp_path / "rows.csv"
    path.write_text("ts\n2024-01-02T03:04:05\n", encoding="utf-8")
    with pytest.raises(Exception, match="invalid timestamp_regexps regex"):
        read_test_csv(path, custom_timestamp_patterns=("(",))


def test_no_capture_temporal_regex_does_not_infer_unparseable_temporal_type() -> None:
    pa = pytest.importorskip("pyarrow")

    result = read_test_python([{"d": "2024/01/02"}], custom_date_patterns=(r"\d{4}/\d{2}/\d{2}",))

    assert pa.types.is_string(result.clean_data.schema.field("d").type)
    assert result.clean_data.to_pylist() == [{"d": "2024/01/02"}]


def test_custom_temporal_patterns_reject_invalid_calendar_dates() -> None:
    pa = pytest.importorskip("pyarrow")

    with pytest.raises(Exception, match="failed to coerce string to date32"):
        _read_python_with_contract(
            [{"d": "2024-02-31"}],
            schema_contract=pa.schema([("d", pa.date32())]),
            schema_mode="strict",
            on_error="stop",
            custom_date_patterns=(r"(\d{4})-(\d{2})-(\d{2})",),
        )


def test_custom_timestamp_patterns_reject_int64_overflow() -> None:
    pa = pytest.importorskip("pyarrow")

    with pytest.raises(Exception, match="failed to coerce string to timestamp"):
        _read_python_with_contract(
            [{"ts": "9999-12-31T00:00:00"}],
            schema_contract=pa.schema([("ts", pa.timestamp("ns"))]),
            schema_mode="strict",
            on_error="stop",
            custom_timestamp_patterns=(r"(\d{4})-(\d{2})-(\d{2})T(\d{2}):(\d{2}):(\d{2})",),
        )
