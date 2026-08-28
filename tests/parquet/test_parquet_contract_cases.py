"""Parquet contract certification, filtering, and runtime-status cases.

Its parameterized cases certify required contract families, filtering behavior,
readiness status, and expected runtime outcomes.
"""

from __future__ import annotations

import inspect

import pytest
from _support.parquet_contracts import PARQUET_CONTRACT_CASES


@pytest.mark.parametrize(
    ("case_id", "run_case"),
    PARQUET_CONTRACT_CASES,
    ids=[case_id for case_id, _run_case in PARQUET_CONTRACT_CASES],
)
def test_parquet_contract_case(
    case_id: str,
    run_case: object,
    request: pytest.FixtureRequest,
) -> None:
    """Verify Parquet contract case."""
    del case_id
    kwargs = {
        name: request.getfixturevalue(name)
        for name, parameter in inspect.signature(run_case).parameters.items()
        if parameter.default is inspect.Parameter.empty
    }
    run_case(**kwargs)
