"""Native scalar, list, page, projection, and staging Parquet cases.

Its named matrix covers scalar and list types, page encodings, projections, staging, and
exact native outcomes.
"""

from __future__ import annotations

import inspect

import pytest
from _support.parquet_runtime import NATIVE_SCALAR_CASES, requires_pyarrow

pytestmark = pytest.mark.usefixtures("require_native")


@requires_pyarrow
@pytest.mark.parametrize(
    ("case_id", "run_case"),
    NATIVE_SCALAR_CASES,
    ids=[case_id for case_id, _run_case in NATIVE_SCALAR_CASES],
)
def test_native_scalar_case(
    case_id: str,
    run_case,
    request: pytest.FixtureRequest,
) -> None:
    """Verify native scalar case."""
    del case_id
    kwargs = {
        name: request.getfixturevalue(name)
        for name, parameter in inspect.signature(run_case).parameters.items()
        if parameter.default is inspect.Parameter.empty
    }
    run_case(**kwargs)
