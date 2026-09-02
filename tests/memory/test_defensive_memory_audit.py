"""Exercises hostile Arrow offsets, capsule keepalives, bounded coalescing, and arena
cleanup across generator replay and allocation faults. It verifies that validation
precedes ownership transfer, foreign release callbacks stay suppressed, and backing
pools deallocate without retaining Python batches."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest
from _support.resource_fakes import CapsuleStream


def test_generator_replay_retains_no_python_row_batch() -> None:
    """Native iterator batching must not retain a Python-side row container."""
    from schema_sanitizer.core_impl.execution import PythonRowsJsonlByteReader

    yielded = 0

    def rows() -> Any:
        """Yield several distinct source objects while exposing eager retention."""
        nonlocal yielded
        for index in range(5):
            yielded += 1
            yield {"index": index, "payload": "x" * 1024}

    reader = PythonRowsJsonlByteReader(rows())
    try:
        payload = reader._produce_and_record(1)
        assert payload
        assert yielded == 1
        assert not hasattr(reader, "_iterable_chunk")
        assert reader._iterable_index == 1
    finally:
        reader.close()


def test_arrow_direct_rejects_large_absolute_offset(require_native: None) -> None:
    """A small slice beyond the derived slot budget must be rejected."""
    pa = pytest.importorskip("pyarrow")

    from schema_sanitizer.core_impl.execution import ExecutionContext
    from schema_sanitizer.options_impl.call_options import normalize_call_options

    logical_offset = 1_000_000
    values = pa.allocate_buffer((logical_offset + 1) * 8)
    sliced = pa.Array.from_buffers(
        pa.int64(),
        1,
        [None, values],
        offset=logical_offset,
    )
    batch = pa.record_batch([sliced], names=["value"])
    source = pa.RecordBatchReader.from_batches(batch.schema, [batch])
    options = normalize_call_options(memory_limit_bytes=1).raw

    output = ExecutionContext().to_sink_arrow_stream("stream", "arrow", source, options)
    with pytest.raises(pa.ArrowMemoryError, match="absolute logical range"):
        pa.RecordBatchReader.from_stream(output).read_all()


def test_coalescer_rejects_large_absolute_offset(require_native: None) -> None:
    """The coalescer validates offsets against its derived slot budget."""
    pa = pytest.importorskip("pyarrow")

    from schema_sanitizer.core_impl.native_symbols import COALESCING_STREAM_WRAP

    logical_offset = 1_000_000
    values = pa.allocate_buffer((logical_offset + 1) * 8)
    sliced = pa.Array.from_buffers(
        pa.int64(),
        1,
        [None, values],
        offset=logical_offset,
    )
    batch = pa.record_batch([sliced], names=["value"])
    source = pa.RecordBatchReader.from_batches(batch.schema, [batch])

    capsule = COALESCING_STREAM_WRAP(source, 1)
    assert capsule is not None
    with pytest.raises(pa.ArrowMemoryError, match="absolute logical range"):
        pa.RecordBatchReader.from_stream(CapsuleStream(capsule)).read_all()


def test_bump_arena_registers_new_blocks_exception_safely() -> None:
    """A failed ownership-vector growth must free the just-allocated block."""
    root = Path(__file__).resolve().parents[2]
    text = (root / "cpp/src/internal/memory/arena.cc").read_text()

    add_block = text.split("void BumpArena::add_block", maxsplit=1)[1]
    assert "try {" in add_block
    assert "blocks_.push_back" in add_block
    assert "catch (...)" in add_block
    assert "pool->Free(p, static_cast<int64_t>(want));" in add_block


def test_foreign_arrow_release_callbacks_are_suppressed() -> None:
    """RAII and C destruction paths must use no-throw release helpers."""
    root = Path(__file__).resolve().parents[2]
    callbacks = (root / "cpp/src/internal/arrow_c/cdata_stream_callbacks.cc").read_text()
    values = (
        root / "cpp/src/api/python_abi3/arrow_direct/_core_abi3_arrow_direct_values.cc"
    ).read_text()
    capsules = (root / "cpp/src/api/python_abi3/context/_core_abi3_capsules.cc").read_text()
    coalescer = (root / "cpp/src/api/python_abi3/streaming/coalesce_stream.cc").read_text()
    payload = (root / "cpp/src/api/python_abi3/logical_schema/payload.cc").read_text()

    assert "void release_stream_nothrow" in callbacks
    assert "release_array_nothrow(&array)" in values
    assert "release_stream_nothrow(stream)" in capsules
    assert "release_array_nothrow(\n      &state->pending_array)" in coalescer
    assert "release_schema_nothrow(schema)" in payload


def test_coalescer_validates_before_transferring_ownership() -> None:
    """Malformed foreign batches must never enter the pending state."""
    root = Path(__file__).resolve().parents[2]
    text = (root / "cpp/src/api/python_abi3/streaming/coalesce_stream.cc").read_text()

    validate_at = text.index("SAN_RETURN_NOT_OK(validate_arrow_node")
    move_at = text.index("move_array(batch.get(), &state->pending_array)")
    assert validate_at < move_at


def test_arrow_direct_frontend_materializes_bounded_slices() -> None:
    """One foreign batch must not create RowRef storage for every row at once."""
    root = Path(__file__).resolve().parents[2]
    frontend = (
        root / "cpp/src/api/python_abi3/arrow_direct/_core_abi3_arrow_direct.cc"
    ).read_text()
    batch_builder = (
        root / "cpp/src/api/python_abi3/arrow_direct/_core_abi3_arrow_direct_batch.cc"
    ).read_text()

    assert "const int64_t row_count = std::min(capacity, remaining);" in frontend
    assert "pending_offset_ += row_count;" in frontend
    assert "checked_field_ref_count" in batch_builder
    assert "kMaxArrowDirectFieldRefs" in batch_builder


def test_arrow_direct_scalar_values_do_not_accumulate_heap_refs() -> None:
    """Only nested container views should require stable ArrowValueRef storage."""
    root = Path(__file__).resolve().parents[2]
    values = (
        root / "cpp/src/api/python_abi3/arrow_direct/_core_abi3_arrow_direct_values.cc"
    ).read_text()

    assert "bool value_requires_stable_ref" in values
    assert "return value_from_ref(store_value_ref" in values
    assert "const ArrowValueRef ref{" in values
    assert "value_at(ref->storage, &children[i], child_array" in values
    assert "value_at(ref->storage, &child_node, child_array, i)" in values
    assert "value_from_ref(&child)" not in values


def test_sliced_fixed_size_list_round_trip(require_native: None) -> None:
    """A fixed-size-list slice must validate and read using its parent offset."""
    pa = pytest.importorskip("pyarrow")

    from schema_sanitizer.core_impl.execution import ExecutionContext

    values = pa.array(
        [[0, 1], [2, 3], [4, 5], [6, 7]],
        type=pa.list_(pa.int64(), 2),
    ).slice(2, 2)
    batch = pa.record_batch([values], names=["vector"])
    source = pa.RecordBatchReader.from_batches(batch.schema, [batch])

    output = ExecutionContext().to_sink_arrow_stream("stream", "arrow", source)
    table = pa.RecordBatchReader.from_stream(output).read_all()

    assert table.to_pylist() == [{"vector": [4, 5]}, {"vector": [6, 7]}]


def test_arrow_capsule_keepalive_outlives_pending_arrays() -> None:
    """Foreign arrays and schemas must be released before their Python capsule."""
    root = Path(__file__).resolve().parents[2]
    frontend = (
        root / "cpp/src/api/python_abi3/arrow_direct/_core_abi3_arrow_direct.cc"
    ).read_text()
    coalescer = (root / "cpp/src/api/python_abi3/streaming/coalesce_stream.cc").read_text()

    destructor = frontend.split("~ArrowDirectFrontend()", maxsplit=1)[1]
    assert destructor.index("pending_.reset();") < destructor.index("decref_with_gil(capsule_)")
    assert "capsule_owner" in frontend
    assert "capsule_owner" in coalescer


def test_fixed_size_list_validation_uses_parent_offset() -> None:
    """Validation must cover the exact child range later used by traversal."""
    root = Path(__file__).resolve().parents[2]
    validator = (
        root / "cpp/src/api/python_abi3/arrow_direct/_core_abi3_arrow_direct_validate.cc"
    ).read_text()

    assert "const auto parent_first = array.offset + first_row;" in validator
    assert "const auto child_first = parent_first * node.fixed_size_list_size;" in validator


def test_memory_pool_deallocation_is_a_noexcept_contract() -> None:
    """Allocator cleanup must never terminate a destructor by propagating an error."""
    root = Path(__file__).resolve().parents[2]
    header = (root / "cpp/src/internal/memory/memory_pool.hh").read_text()
    resource_header = (root / "cpp/src/internal/memory/pool_resource.hh").read_text()

    assert "alignment) noexcept = 0;" in header
    assert "void Free(uint8_t *buffer, int64_t size) noexcept" in header
    assert "std::size_t alignment) noexcept override;" in resource_header
