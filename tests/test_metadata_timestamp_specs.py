"""Validation coverage for dynamic and fixed generated timestamp columns."""

from __future__ import annotations

import pytest

from schema_sanitizer.adapters.pyarrow.metadata_specs import validate_timestamp_columns


def test_fixed_timestamp_columns_preserve_order_and_values() -> None:
    """Fixed epoch-microsecond mappings remain ordered operation metadata."""
    assert validate_timestamp_columns(
        {"ingestion_timestamp": 1_700_000_000_123_456, "completed_at": -1}
    ) == {"ingestion_timestamp": 1_700_000_000_123_456, "completed_at": -1}


@pytest.mark.parametrize("value", [True, False, 1.5, "1700000000000000", None])
def test_fixed_timestamp_columns_reject_non_integer_values(value: object) -> None:
    """Booleans and non-integers cannot silently become epoch timestamps."""
    with pytest.raises(TypeError, match="integer epoch microseconds"):
        validate_timestamp_columns({"ingestion_timestamp": value})  # type: ignore[dict-item]


@pytest.mark.parametrize("value", [-(1 << 63) - 1, 1 << 63])
def test_fixed_timestamp_columns_reject_values_outside_int64(value: int) -> None:
    """The Python contract rejects values the native ABI cannot represent."""
    with pytest.raises(OverflowError, match="outside int64 range"):
        validate_timestamp_columns({"ingestion_timestamp": value})


def test_dynamic_timestamp_columns_keep_legacy_sequence_contract() -> None:
    """Internal callers may still request one clock read per emitted batch."""
    assert validate_timestamp_columns(["ingestion_timestamp", "completed_at"]) == (
        "ingestion_timestamp",
        "completed_at",
    )
