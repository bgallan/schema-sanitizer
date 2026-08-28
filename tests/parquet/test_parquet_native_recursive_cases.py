"""Generated and adversarial native recursive Parquet cases."""

from __future__ import annotations

from collections.abc import Callable
from pathlib import Path

import pytest
from _support.parquet_recursive_cases import RECURSIVE_RUNTIME_CASES
from _support.parquet_runtime import requires_pyarrow

pytestmark = pytest.mark.usefixtures("require_native")


@requires_pyarrow
@pytest.mark.parametrize(
    ("case_id", "run_case"),
    RECURSIVE_RUNTIME_CASES,
    ids=[case_id for case_id, _run_case in RECURSIVE_RUNTIME_CASES],
)
def test_native_recursive_case(
    case_id: str,
    run_case: Callable[[Path], None],
    tmp_path: Path,
) -> None:
    del case_id
    run_case(tmp_path)
