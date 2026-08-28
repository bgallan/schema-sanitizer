"""Native Parquet nested-shape materialization cases."""

from __future__ import annotations

from pathlib import Path

import pytest
from _support.parquet_nested_cases import NATIVE_NESTED_CASES
from _support.parquet_runtime import requires_pyarrow

pytestmark = pytest.mark.usefixtures("require_native")


@requires_pyarrow
@pytest.mark.parametrize(
    ("case_id", "run_case"),
    NATIVE_NESTED_CASES,
    ids=[case_id for case_id, _run_case in NATIVE_NESTED_CASES],
)
def test_native_nested_case(case_id: str, run_case, tmp_path: Path) -> None:
    del case_id
    run_case(tmp_path)
