"""Hardens Parquet footer decoding against impossible or unknown lists and maps,
overflowing varints, stop-typed collections, allocator-header attacks, and retained-byte
overflow. Validation and ownership claims precede iteration or header access, and
counters saturate instead of wrapping."""

from __future__ import annotations

from pathlib import Path

import pytest


def _write_parquet_footer(path: Path, footer: bytes) -> None:
    """Write one minimal Parquet container around caller-provided footer bytes."""
    path.write_bytes(b"PAR1" + footer + len(footer).to_bytes(4, "little") + b"PAR1")


def _compact_varint(value: int) -> bytes:
    """Encode one unsigned compact-protocol varint."""
    out = bytearray()
    while value >= 0x80:
        out.append((value & 0x7F) | 0x80)
        value >>= 7
    out.append(value)
    return bytes(out)


def test_parquet_footer_rejects_impossible_list_before_reserve(
    tmp_path: Path, require_native: None
) -> None:
    """Declared metadata lists must fit in the remaining bytes before allocation."""
    from schema_sanitizer.core_impl.native_runtime import native_core

    # Footer field 2 (schema), compact list header, 65,536 struct elements, but
    # no element bytes. The parser must reject this before reserving the vector.
    footer = b"\x29\xfc" + _compact_varint(65_536)
    path = tmp_path / "impossible-schema-list.parquet"
    _write_parquet_footer(path, footer)

    with pytest.raises(RuntimeError, match="count exceeds remaining input bytes"):
        native_core.parquet_footer_info_json(str(path))


def test_parquet_footer_rejects_overflowing_varint(tmp_path: Path, require_native: None) -> None:
    """A ten-byte varint may use only one payload bit in its final byte."""
    from schema_sanitizer.core_impl.native_runtime import native_core

    # Footer field 6 (created_by binary) followed by an overflowing uint64
    # length varint. Accepting it could truncate a hostile allocation length.
    footer = b"\x68" + (b"\xff" * 9) + b"\x02"
    path = tmp_path / "overflowing-varint.parquet"
    _write_parquet_footer(path, footer)

    with pytest.raises(RuntimeError, match="varint overflow"):
        native_core.parquet_footer_info_json(str(path))


def test_retained_memory_accounting_saturates_instead_of_wrapping() -> None:
    """Builder byte estimates must never wrap to a small signed value."""
    root = Path(__file__).resolve().parents[2]
    helper = (root / "cpp/src/internal/memory/size_math.hh").read_text()
    scalar = (root / "cpp/src/internal/materialization/builders/scalar.cc").read_text()
    nested = (root / "cpp/src/internal/materialization/builders/nested.cc").read_text()
    factory = (root / "cpp/src/internal/materialization/builders/factory.cc").read_text()

    assert "saturating_capacity_bytes" in helper
    assert "saturating_add_i64" in scalar
    assert "saturating_add_i64" in nested
    assert "saturating_add_i64" in factory
    assert "capacity() * sizeof" not in scalar


def test_hardened_allocator_claims_ownership_before_reading_headers() -> None:
    """Optional double-free hardening must not inspect an unclaimed pointer."""
    root = Path(__file__).resolve().parents[2]
    source = (root / "cpp/src/internal/memory/memory_pool.cc").read_text()
    registry = (root / "cpp/src/internal/memory/memory_pool_registry.cc.inc").read_text()
    tracking = (root / "cpp/src/internal/memory/tracking_memory_pool.cc.inc").read_text()
    compact_source = " ".join(source.split())
    compact_tracking = " ".join(tracking.split())

    assert "bool hardened_allocation_registry_enabled() noexcept" in registry
    assert "return true;" in registry
    assert "getenv" not in registry
    default_free = compact_source.split(
        "void Free(uint8_t *buffer, int64_t size, int64_t alignment) noexcept override",
        maxsplit=1,
    )[1]
    assert default_free.index("claim_allocation(buffer, &record)") < default_free.index(
        "reinterpret_cast<DefaultAllocationHeader *>"
    )
    tracking_free = compact_tracking.split(
        "void Free(uint8_t *buffer, int64_t size, int64_t alignment) noexcept override",
        maxsplit=1,
    )[1]
    assert tracking_free.index("claim_allocation(buffer, &record)") < tracking_free.index(
        "reinterpret_cast<TrackingAllocationHeader *>"
    )


def test_parquet_footer_rejects_impossible_unknown_list_before_iteration(
    tmp_path: Path,
    require_native: None,
) -> None:
    """Unknown collection fields cannot claim more elements than input bytes."""
    from schema_sanitizer.core_impl.native_runtime import native_core

    # Unknown footer field 15, list type, extended size 100, byte elements, but
    # only the outer struct STOP remains after the list header.
    footer = b"\xf9\xf3" + _compact_varint(100) + b"\x00"
    path = tmp_path / "impossible-unknown-list.parquet"
    _write_parquet_footer(path, footer)

    with pytest.raises(RuntimeError, match="container count exceeds remaining input"):
        native_core.parquet_footer_info_json(str(path))


def test_parquet_footer_rejects_stop_typed_nonempty_collection(
    tmp_path: Path,
    require_native: None,
) -> None:
    """STOP is not a valid element type for a non-empty compact collection."""
    from schema_sanitizer.core_impl.native_runtime import native_core

    footer = b"\xf9\x10\x00"
    path = tmp_path / "stop-list-type.parquet"
    _write_parquet_footer(path, footer)

    with pytest.raises(RuntimeError, match="STOP element type"):
        native_core.parquet_footer_info_json(str(path))


def test_parquet_footer_rejects_impossible_unknown_map_before_iteration(
    tmp_path: Path,
    require_native: None,
) -> None:
    """Unknown maps are bounded by the bytes that could encode their pairs."""
    from schema_sanitizer.core_impl.native_runtime import native_core

    # Unknown footer field 15, map type, ten byte/byte pairs, but only the
    # outer struct STOP remains after the pair type byte.
    footer = b"\xfb" + _compact_varint(10) + b"\x33\x00"
    path = tmp_path / "impossible-unknown-map.parquet"
    _write_parquet_footer(path, footer)

    with pytest.raises(RuntimeError, match="map count exceeds remaining input"):
        native_core.parquet_footer_info_json(str(path))
