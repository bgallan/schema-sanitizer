"""Regression coverage for v68 direct logical scalar CSV output."""

from __future__ import annotations

import json
from decimal import Decimal
from pathlib import Path

import pytest
from conftest import require_native

import schema_sanitizer as ss
from schema_sanitizer.api_impl import operation_context

ROOT = Path(__file__).resolve().parents[2]
CSV_WRITER = ROOT / "cpp/src/internal/csv/csv_stream_writer.cc"
PARTS = ROOT / "cpp/src/internal/json_output/jsonl_value_writer_parts.hh"

_FIXED_TIMESTAMPS = operation_context.OperationTimestamps(
    1_767_225_600_123_456,
    "2026-01-01T00:00:00.123456Z",
)


def test_v68_csv_logical_scalars_bypass_json_temporary() -> None:
    """Logical scalar cells call shared formatters without JSON quotes."""
    source = CSV_WRITER.read_text(encoding="utf-8")
    direct = source.split("append_direct_csv_logical_scalar", 1)[1].split(
        "is_direct_csv_logical_scalar", 1
    )[0]

    for kind in (
        "kBinary",
        "kLargeBinary",
        "kFixedSizeBinary",
        "kTimestampMillis",
        "kTimestampMicros",
        "kTimestampNanos",
        "kDate32",
        "kDate64",
        "kTime32s",
        "kTime32ms",
        "kTime64us",
        "kTime64ns",
        "kDuration",
        "kDecimal",
    ):
        assert kind in direct
    assert direct.count("false") >= 14
    assert "append_csv_cell_from_json" in source  # canonical fallback retained
    assert "is_direct_csv_logical_scalar(field.kind)" in source
    assert "getenv" not in source
    assert len(source.splitlines()) <= 500


def test_v68_jsonl_formatters_default_to_quoted_output() -> None:
    """Internal quote control must not alter existing JSONL callers."""
    declarations = PARTS.read_text(encoding="utf-8")

    assert declarations.count("bool quote = true") == 11
    assert "append_interval_value" in declarations
    interval_decl = declarations.split("append_interval_value", 1)[1].split(";", 1)[0]
    assert "quote" not in interval_decl


def test_v68_public_temporal_csv_single_multi_are_identical(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Parsed temporal cells retain exact bytes, order, and unquoted CSV text."""
    require_native()
    monkeypatch.setattr(
        operation_context,
        "capture_operation_timestamps",
        lambda: _FIXED_TIMESTAMPS,
    )

    source = tmp_path / "temporal.jsonl"
    rows = 4_097
    with source.open("w", encoding="utf-8", newline="") as handle:
        for index in range(rows):
            handle.write(
                json.dumps(
                    {
                        "date_value": f"2026-01-{index % 28 + 1:02d}",
                        "timestamp_value": (
                            f"2026-01-{index % 28 + 1:02d}T"
                            f"{index % 24:02d}:{index % 60:02d}:"
                            f"{index % 60:02d}.{index % 1_000_000:06d}Z"
                        ),
                    },
                    separators=(",", ":"),
                )
                + "\n"
            )

    common = dict(
        input_format="jsonl",
        field_name_policy="preserve",
        parse_iso_timestamps=True,
        parse_iso_dates=True,
        memory_limit_bytes=64 << 20,
    )
    outputs: dict[str, bytes] = {}
    for mode in ("single", "multi"):
        destination = tmp_path / f"{mode}.csv"
        ss.to_csv(source, destination, multi_threading=mode == "multi", **common)
        outputs[mode] = destination.read_bytes()

    assert outputs["single"] == outputs["multi"]
    first_data_row = outputs["multi"].splitlines()[1]
    assert first_data_row.startswith(b"2026-01-01,2026-01-01T00:00:00,")
    assert not first_data_row.startswith(b'"2026-01-01')


def test_v68_arrow_logical_csv_single_multi_are_identical(tmp_path: Path) -> None:
    """Binary, temporal, duration, and decimal arrays use the native direct path."""
    require_native()
    pa = pytest.importorskip("pyarrow")
    from schema_sanitizer.adapters.pyarrow.csv_sink import write_csv_stream

    table = pa.table(
        {
            "bin": pa.array([b"abc", None], type=pa.binary()),
            "large_bin": pa.array([b"\x00\x01\x02\x03", b"x"], type=pa.large_binary()),
            "fixed_bin": pa.array([b"wxyz", b"1234"], type=pa.binary(4)),
            "ts": pa.array([1_767_236_486_123_456, 0], type=pa.timestamp("us")),
            "date32": pa.array([20_455, 0], type=pa.date32()),
            "date64": pa.array([20_455 * 86_400_000, 0], type=pa.date64()),
            "time32": pa.array([10_886_123, 0], type=pa.time32("ms")),
            "time64": pa.array([10_886_123_456, 0], type=pa.time64("us")),
            "duration": pa.array([123_456, -5], type=pa.duration("us")),
            "decimal": pa.array([Decimal("123.45"), Decimal("-0.01")], type=pa.decimal128(10, 2)),
        }
    )

    outputs: dict[str, bytes] = {}
    for mode in ("single", "multi"):
        reader = pa.RecordBatchReader.from_batches(table.schema, table.to_batches())
        destination = tmp_path / f"logical-{mode}.csv"
        write_csv_stream(
            reader,
            destination,
            feature="v68 logical CSV parity",
            memory_limit_bytes=64 << 20,
            threading_mode=mode,
        )
        outputs[mode] = destination.read_bytes()

    assert outputs["single"] == outputs["multi"]
    body = outputs["multi"].splitlines()[1]
    assert body.startswith(b"YWJj,AAECAw==,d3h5eg==,")
    assert b",123456us,123.45" in body
