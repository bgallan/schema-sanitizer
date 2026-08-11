"""Release certification: all 56 public format pairs execute real shared admissions."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest

import schema_sanitizer as ss
from schema_sanitizer.core_impl import concurrency_coverage as coverage
from schema_sanitizer.core_impl.concurrency_contracts import (
    runtime_pair_payload_contract_observations,
    runtime_pair_stage_observations,
)
from schema_sanitizer.core_impl.concurrency_coverage import (
    INPUT_CONCURRENCY_COVERAGE,
    OUTPUT_CONCURRENCY_COVERAGE,
    payload_observed_concurrency_pair_guarantees,
    validate_format_pair_release_contracts,
)


def _require_release_adapters() -> None:
    """Require the same adapter set installed by the platform-wheel full suite."""
    pytest.importorskip("pyarrow")
    pytest.importorskip("pandas")
    pytest.importorskip("polars")
    pytest.importorskip("duckdb")


def _prepare_sources(root: Path) -> dict[str, tuple[Any, dict[str, Any], int]]:
    csv_path = root / "input.csv"
    csv_path.write_text("a,b\n1,x\n2,y\n", encoding="utf-8")

    json_path = root / "input.json"
    json_path.write_text('{"a":1,"b":"x"}', encoding="utf-8")

    json_array_path = root / "input-array.json"
    json_array_path.write_text('[{"a":1,"b":"x"},{"a":2,"b":"y"}]', encoding="utf-8")

    jsonl_path = root / "input.jsonl"
    jsonl_path.write_text('{"a":1,"b":"x"}\n{"a":2,"b":"y"}\n', encoding="utf-8")

    ndjson_path = root / "input.ndjson"
    ndjson_path.write_text('{"a":1,"b":"x"}\n{"a":2,"b":"y"}\n', encoding="utf-8")

    xml_path = root / "input.xml"
    xml_path.write_text(
        "<root><row><a>1</a><b>x</b></row><row><a>2</a><b>y</b></row></root>",
        encoding="utf-8",
    )

    parquet_path = root / "input.parquet"
    pa = pytest.importorskip("pyarrow")
    parquet = pytest.importorskip("pyarrow.parquet")
    parquet.write_table(
        pa.table({"a": [1, 2], "b": ["x", "y"]}),
        parquet_path,
    )

    return {
        "csv": (csv_path, {}, 2),
        # A top-level JSON object is one document row.  Keep this distinct from
        # the two-row top-level array exercised by ``json_array`` below.
        "json": (json_path, {}, 1),
        "json_array": (json_array_path, {}, 2),
        "jsonl": (jsonl_path, {}, 2),
        "ndjson": (ndjson_path, {}, 2),
        "xml": (xml_path, {"xml_row_tag": "row"}, 2),
        "parquet": (parquet_path, {}, 2),
        # A fresh immutable list is safe to reuse; the public Python-input path
        # creates its own replay/stream ownership per call.
        "python": ([{"a": 1, "b": "x"}, {"a": 2, "b": "y"}], {}, 2),
    }


def _run_pair(
    root: Path,
    input_name: str,
    output_name: str,
    source: Any,
    input_kwargs: dict[str, Any],
    expected_rows: int,
) -> None:
    common: dict[str, Any] = {
        "input_format": input_name,
        "multi_threading": True,
        "memory_limit_bytes": 64 << 20,
        **input_kwargs,
    }
    if output_name in {"csv", "jsonl", "parquet"}:
        suffix = {"csv": ".csv", "jsonl": ".jsonl", "parquet": ".parquet"}[output_name]
        target = root / f"{input_name}-to-{output_name}{suffix}"
        converter = {
            "csv": ss.to_csv,
            "jsonl": ss.to_jsonl,
            "parquet": ss.to_parquet,
        }[output_name]
        result = converter(source, target, **common)
        try:
            assert target.exists()
        finally:
            result.close()
        return

    converter = {
        "pyarrow": ss.to_pyarrow,
        "pandas": ss.to_pandas,
        "polars": ss.to_polars,
        "duckdb": ss.to_duckdb,
    }[output_name]
    result = converter(source, **common)
    try:
        clean = result.clean_data
        if output_name == "pyarrow":
            assert clean.num_rows == expected_rows
        elif output_name == "pandas":
            assert len(clean) == expected_rows
        elif output_name == "polars":
            assert clean.height == expected_rows
        else:
            assert len(clean.fetchall()) == expected_rows
    finally:
        result.close()


def test_release_gate_executes_real_public_8x7_format_matrix(tmp_path: Path) -> None:
    """No structural bootstrap can certify a release without all real payload paths."""
    _require_release_adapters()
    sources = _prepare_sources(tmp_path)
    assert set(sources) == set(INPUT_CONCURRENCY_COVERAGE)

    for input_name in INPUT_CONCURRENCY_COVERAGE:
        source, kwargs, expected_rows = sources[input_name]
        for output_name in OUTPUT_CONCURRENCY_COVERAGE:
            _run_pair(
                tmp_path,
                input_name,
                output_name,
                source,
                kwargs,
                expected_rows,
            )

    # Transport/lifetime profiles are an orthogonal release dimension and are
    # certified by the strict global gate after their own real integrations run.
    assert validate_format_pair_release_contracts() == 56
    evidence = payload_observed_concurrency_pair_guarantees()
    assert all(
        all(count > 0 for count in evidence[input_name][output_name].values())
        for input_name in INPUT_CONCURRENCY_COVERAGE
        for output_name in OUTPUT_CONCURRENCY_COVERAGE
    )


def test_parquet_pandas_admits_reader_and_adapter_on_shared_pyarrow_pool(
    tmp_path: Path,
) -> None:
    """The Parquet reader and pandas handoff share one governed PyArrow pool."""
    pa = pytest.importorskip("pyarrow")
    parquet = pytest.importorskip("pyarrow.parquet")
    source = tmp_path / "shared-pyarrow.parquet"
    parquet.write_table(pa.table({"a": [1, 2], "b": ["x", "y"]}), source)

    pair = ("parquet", "pandas")
    before_payload = runtime_pair_payload_contract_observations().get(pair, {})
    before_stages = runtime_pair_stage_observations().get(pair, {})
    before_claims = int(before_payload.get("external_runtime_pool_claim", 0))
    before_threaded = int(before_stages.get("threaded_adapter_conversion", 0))

    result = ss.to_pandas(
        source,
        input_format="parquet",
        multi_threading=True,
        memory_limit_bytes=64 << 20,
    )
    try:
        assert len(result.clean_data) == 2
    finally:
        result.close()

    after_payload = runtime_pair_payload_contract_observations().get(pair, {})
    after_stages = runtime_pair_stage_observations().get(pair, {})
    assert int(after_payload.get("external_runtime_pool_claim", 0)) >= before_claims + 2
    assert int(after_stages.get("threaded_adapter_conversion", 0)) == before_threaded + 1


def test_global_release_gate_still_requires_orthogonal_route_profiles(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The strict release gate cannot silently skip transport/lifetime proof."""
    monkeypatch.setattr(coverage, "validate_format_pair_release_contracts", lambda: 56)

    def reject_missing_routes() -> int:
        """Represent an incomplete real route-profile integration run."""
        raise RuntimeError("orthogonal route-profile evidence is incomplete")

    monkeypatch.setattr(
        coverage,
        "validate_route_profile_runtime_contracts",
        reject_missing_routes,
    )
    with pytest.raises(RuntimeError, match="orthogonal route-profile evidence"):
        coverage.validate_release_concurrency_pair_contracts()
