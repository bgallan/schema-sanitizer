"""Optional PyArrow imports and fixtures shared by Parquet runtime tests."""

from __future__ import annotations

from typing import Any

import pytest

try:
    import pyarrow as pa
    import pyarrow.feather as feather
    import pyarrow.parquet as pq

    HAVE_PYARROW = True
except ModuleNotFoundError:  # pragma: no cover
    pa = feather = pq = None  # type: ignore[assignment]
    HAVE_PYARROW = False

requires_pyarrow = pytest.mark.skipif(
    not HAVE_PYARROW,
    reason="pyarrow not installed",
)


def sample_table(pyarrow: Any) -> Any:
    """Return the canonical two-column Parquet runtime test table."""
    return pyarrow.table({"a": [1, 2, 3], "b": ["x", "y", "z"]})


def recursive_arrow_type(spec: object) -> object:
    """Build a PyArrow type from the recursive runtime-test grammar."""
    kind = spec[0]  # type: ignore[index]
    if kind == "int64":
        return pa.int64()
    if kind == "string":
        return pa.string()
    if kind == "bool":
        return pa.bool_()
    if kind == "float64":
        return pa.float64()
    if kind == "list":
        return pa.list_(recursive_arrow_type(spec[1]))  # type: ignore[index]
    if kind == "map":
        return pa.map_(pa.string(), recursive_arrow_type(spec[1]))  # type: ignore[index]
    if kind == "struct":
        return pa.struct(
            [
                pa.field(name, recursive_arrow_type(child))
                for name, child in spec[1]  # type: ignore[index]
            ]
        )
    raise AssertionError(kind)
