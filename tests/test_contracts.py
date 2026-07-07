"""Tests contracts."""

from __future__ import annotations

import io

import pytest
from conftest import read_test_json

pa = pytest.importorskip("pyarrow")

from schema_sanitizer.api_impl.context import ExecutionContext
from schema_sanitizer.options_impl.call_options import normalize_call_options


def _read_python_with_contract(rows, *, schema_contract: pa.Schema, **options):
    """Read Python rows through the internal schema contract path."""
    return ExecutionContext().to_table(
        rows,
        options=normalize_call_options(schema_contract=schema_contract, **options),
        format="python",
        source="python",
    )


class OneResetFile:
    """A minimal seekable file-like."""

    def __init__(self, data: bytes):
        """Initialize the test helper."""
        self._buf = io.BytesIO(data)
        self._seek0_calls = 0

    def read(self, size: int = -1) -> bytes:
        """Read from the test helper."""
        return self._buf.read(size)

    def seek(self, pos: int, whence: int = 0) -> int:
        """Seek within the test helper."""
        self._seek0_calls += 1
        return self._buf.seek(pos, whence)

    def tell(self) -> int:
        """Return the test helper position."""
        return self._buf.tell()


class NonSeekable:
    """A minimal non-seekable file-like."""

    def __init__(self, data: bytes):
        """Initialize the test helper."""
        self._data = data
        self._pos = 0

    def read(self, n: int = -1):
        """Read from the test helper."""
        if n is None or n < 0:
            n = len(self._data) - self._pos
        if self._pos >= len(self._data):
            return b""
        out = self._data[self._pos : self._pos + n]
        self._pos += len(out)
        return out

    def seek(self, *_args, **_kwargs):
        """Seek within the test helper."""
        raise OSError("non-seekable")

    def tell(self, *_args, **_kwargs):
        """Return the test helper position."""
        raise OSError("non-seekable")


def test_filelike_input_is_rejected_for_text_source() -> None:
    """Verify filelike input is rejected for text source."""
    src = OneResetFile(b'[{"a": 1}, {"a": 2}]')
    with pytest.raises(TypeError):
        read_test_json(src)


def test_filelike_input_is_rejected_for_auto_source() -> None:
    """Verify filelike input is rejected for auto source."""
    src = io.BytesIO(b'[{"a": 1}, {"a": 2}]')
    with pytest.raises(TypeError):
        read_test_json(src)


def test_filelike_input_is_rejected_with_options() -> None:
    """Verify filelike input is rejected with options."""
    src = NonSeekable(b'[{"a": 1}, {"a": 2}]')
    with pytest.raises(TypeError):
        read_test_json(src)


def test_skip_row_does_not_partially_append_columns() -> None:
    # Contract schema forces int64 materialization.
    """Verify skip row does not partially append columns."""
    schema = pa.schema([("a", pa.int64()), ("b", pa.int64()), ("c", pa.int64())])

    # Row 2 has a late-field error (c is a string). With SKIP_ROW semantics,
    # the whole row must be skipped and no earlier column may grow.
    rows = [
        {"a": 1, "b": 2, "c": 3},
        {"a": 10, "b": 20, "c": "BAD"},
        {"a": 100, "b": 200, "c": 300},
    ]

    res = _read_python_with_contract(
        rows, schema_contract=schema, schema_mode="strict", on_error="skip_row"
    )
    t = res.clean_data

    assert t is not None
    assert t.schema == schema

    # The bad row must be skipped.
    assert t.num_rows == 2

    # And all columns must have exactly num_rows entries.
    assert t.column("a").length() == t.num_rows
    assert t.column("b").length() == t.num_rows
    assert t.column("c").length() == t.num_rows

    assert t.column("a").to_pylist() == [1, 100]
    assert t.column("b").to_pylist() == [2, 200]
    assert t.column("c").to_pylist() == [3, 300]


def test_result_does_not_expose_bad_rows() -> None:
    """Verify removed bad row queue is not exposed on Result."""
    schema = pa.schema([("a", pa.int64()), ("b", pa.int64()), ("c", pa.int64())])

    rows = [
        {"a": 1, "b": 2, "c": 3},
        {"a": 10, "b": 20, "c": "BAD"},
        {"a": 100, "b": 200, "c": 300},
    ]

    res = _read_python_with_contract(
        rows,
        schema_contract=schema,
        schema_mode="strict",
        on_error="skip_row",
    )

    t = res.clean_data
    assert t is not None
    assert t.schema == schema
    # The bad row must be skipped.
    assert t.num_rows == 2
    assert res.stats["skipped_rows"] == 1
    assert not hasattr(res, "bad_rows")
