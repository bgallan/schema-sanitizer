"""Recursive Parquet layout and projection contract cases."""

from __future__ import annotations

from collections.abc import Callable

import pytest
from _support.parquet_contracts import RECURSIVE_LAYOUT_CASES


@pytest.mark.parametrize(
    ("case_id", "run_case"),
    RECURSIVE_LAYOUT_CASES,
    ids=[case_id for case_id, _run_case in RECURSIVE_LAYOUT_CASES],
)
def test_recursive_layout_case(case_id: str, run_case: Callable[[], None]) -> None:
    del case_id
    run_case()
