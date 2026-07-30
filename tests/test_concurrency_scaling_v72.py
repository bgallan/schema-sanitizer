"""Regression coverage for v72 JSON and analytical-output concurrency."""

from __future__ import annotations

import csv
import inspect
import json
from pathlib import Path

import pytest
from conftest import require_native
from threading_golden import assert_exceptions_equivalent

import schema_sanitizer as ss
from schema_sanitizer.api_impl import results as result_adapters
from schema_sanitizer.api_impl.execution_context import default_pool
from schema_sanitizer.core_impl.concurrency_coverage import (
    INPUT_CONCURRENCY_COVERAGE,
    OUTPUT_CONCURRENCY_COVERAGE,
    concurrency_guarantees,
)

ROOT = Path(__file__).resolve().parents[1]
PARALLEL_PREPARER = ROOT / "cpp/src/internal/materialization/ingest_stream/parallel_preparer.cc"
PARALLEL_SOURCE = ROOT / "cpp/src/internal/materialization/ingest_stream/parallel_source.cc"
TEXT_PIPELINE = ROOT / "cpp/src/frontends/json/text_row_pipeline.cc"
DIRECT_ROWS = ROOT / "cpp/src/internal/materialization/direct_rows.cc"
ROW_STREAM = ROOT / "cpp/src/sanitize/core/row_stream.hh"

_GENERATED_COLUMNS = {
    "schema_registry",
    "schema_drifts",
    "source_file",
    "ingestion_timestamp",
}


def _user_csv_rows(path: Path) -> list[dict[str, str]]:
    """Return ordered user columns while excluding operation metadata."""
    with path.open("r", encoding="utf-8", newline="") as handle:
        return [
            {key: value for key, value in row.items() if key not in _GENERATED_COLUMNS}
            for row in csv.DictReader(handle)
        ]


def _materialization_stats() -> tuple[int, int]:
    """Return submitted materialization tasks and peak active task count."""
    stats = default_pool().get().performance_stats()
    submitted = int(stats.get("tasks", {}).get("materialization", {}).get("submitted", 0))
    peak = int(stats.get("counters", {}).get("peak_active_tasks", 0))
    return submitted, peak


def _write_flat_array(path: Path, *, rows: int = 4_096, columns: int = 24) -> None:
    """Write one deterministic flat JSON array suitable for columnar packets."""
    payload = [
        {
            f"field_{column:02d}": (
                row * (column + 3) if column % 3 == 0 else f"row-{row}-column-{column}"
            )
            for column in range(columns)
        }
        for row in range(rows)
    ]
    path.write_text(
        json.dumps(payload, ensure_ascii=False, separators=(",", ":")),
        encoding="utf-8",
    )


def test_v72_every_format_has_parallel_work_and_documented_boundaries() -> None:
    """The benefit contract covers every input/output without hidden exceptions."""
    guarantees = concurrency_guarantees()
    assert set(guarantees["inputs"]) == set(INPUT_CONCURRENCY_COVERAGE)
    assert set(guarantees["outputs"]) == set(OUTPUT_CONCURRENCY_COVERAGE)
    for family in guarantees.values():
        for contract in family.values():
            assert contract["parallel_stages"]
            assert contract["serial_boundaries"]
            assert contract["adaptive_small_work_fallback"] is True


def test_v72_coverage_matrix_declares_real_json_and_pandas_parallel_stages() -> None:
    """Coverage names the worker-owned work added by v72."""
    for input_name in ("json", "json_array"):
        stages = INPUT_CONCURRENCY_COVERAGE[input_name]
        assert "worker_local_row_parse" in stages
        assert "direct_columnar_materialization" in stages
    assert "threaded_adapter_conversion" in OUTPUT_CONCURRENCY_COVERAGE["pandas"]


def test_v72_json_sources_defer_authoritative_parse_to_workers() -> None:
    """JSON document arrays no longer parse each row in both coordinator and worker."""
    source = PARALLEL_SOURCE.read_text(encoding="utf-8")
    pipeline = TEXT_PIPELINE.read_text(encoding="utf-8")
    direct = DIRECT_ROWS.read_text(encoding="utf-8")
    row_stream = ROW_STREAM.read_text(encoding="utf-8")

    assert 'frontend_name == "json" || frontend_name == "json_array"' in source
    assert "FrontendMaterializationMode::kWorkerAuthoritativeRaw" in source
    assert "kWorkerAuthoritativeRaw" in pipeline
    assert "policy.raw_only = plan != nullptr" in pipeline
    assert "kJsonObjectRequired" in row_stream
    assert "json_array requires object elements" in direct


def test_v72_flat_json_arrays_use_adaptive_columnar_packets() -> None:
    """Flat JSON arrays build Arrow columns directly only for useful packet sizes."""
    source = PARALLEL_PREPARER.read_text(encoding="utf-8")
    assert 'frontend_name_ == "json"' in source
    assert 'frontend_name_ == "json_array"' in source
    assert 'frontend_name_ == "jsonl" || owned.rows.size() >= 64U' in source
    assert "return prepare_columnar(std::move(owned), worker_index, stop)" in source


@pytest.mark.parametrize("input_format", ["json", "json_array"])
def test_v72_large_flat_arrays_parallelize_with_exact_user_data(
    tmp_path: Path, input_format: str
) -> None:
    """Both JSON array routes publish work and preserve ordered user data."""
    require_native()
    source = tmp_path / f"array-{input_format}.json"
    _write_flat_array(source)

    outputs: dict[str, Path] = {}
    telemetry: dict[str, tuple[int, int]] = {}
    for mode in ("single", "multi"):
        output = tmp_path / f"{input_format}-{mode}.csv"
        outputs[mode] = output
        ss.to_csv(
            source,
            output,
            input_format=input_format,
            multi_threading=mode == "multi",
            memory_limit_bytes=256 * 1024 * 1024,
            parse_integers=True,
            on_error="stop",
        )
        telemetry[mode] = _materialization_stats()

    assert _user_csv_rows(outputs["multi"]) == _user_csv_rows(outputs["single"])
    assert telemetry["single"][0] == 0
    assert telemetry["multi"][0] >= 2
    assert telemetry["multi"][1] >= 2


def test_v72_small_json_document_avoids_artificial_column_parallelism(
    tmp_path: Path,
) -> None:
    """One-row documents keep a single useful materialization task in multi mode."""
    require_native()
    source = tmp_path / "single-document.json"
    source.write_text(
        json.dumps({f"field_{index:04d}": index for index in range(1_024)}),
        encoding="utf-8",
    )
    output = tmp_path / "single-document.csv"
    ss.to_csv(
        source,
        output,
        input_format="json",
        multi_threading=True,
        memory_limit_bytes=128 * 1024 * 1024,
        parse_integers=True,
    )
    submitted, peak = _materialization_stats()
    assert submitted == 1
    assert peak == 1


def test_v72_json_array_object_contract_has_exact_single_multi_error(
    tmp_path: Path,
) -> None:
    """Deferred parsing preserves the json_array object-only public contract."""
    require_native()
    source = tmp_path / "invalid-array.json"
    source.write_text('[{"value":1},2,{"value":3}]', encoding="utf-8")

    def run(mode: str) -> None:
        """Execute the invalid array under one threading mode."""
        ss.to_csv(
            source,
            tmp_path / f"invalid-{mode}.csv",
            input_format="json_array",
            multi_threading=mode == "multi",
            memory_limit_bytes=128 * 1024 * 1024,
            on_error="stop",
        )

    assert_exceptions_equivalent(lambda: run("single"), lambda: run("multi"))


class _RecordingArrowTable:
    """Minimal Arrow-like table recording pandas conversion policy."""

    def __init__(self) -> None:
        """Initialize the recorded keyword arguments."""
        self.calls: list[dict[str, object]] = []

    def to_pandas(self, **kwargs: object) -> object:
        """Record conversion keywords and return a sentinel object."""
        self.calls.append(dict(kwargs))
        return object()


def test_v72_pandas_adapter_obeys_single_multi_threading_policy(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The final pandas conversion explicitly disables/enables PyArrow threads."""
    monkeypatch.setattr(
        result_adapters,
        "ensure_optional_dependency",
        lambda *_args, **_kwargs: object(),
    )
    table = _RecordingArrowTable()

    result_adapters.convert_arrow_table_output(
        table, "pandas", feature="test", threading_mode="single"
    )
    result_adapters.convert_arrow_table_output(
        table, "pandas", feature="test", threading_mode="multi"
    )

    assert table.calls == [{"use_threads": False}, {"use_threads": True}]


def test_v72_all_public_outputs_keep_multi_threading() -> None:
    """Every output remains addressable through the common public policy switch."""
    for output_name in OUTPUT_CONCURRENCY_COVERAGE:
        converter = getattr(ss, f"to_{output_name}")
        parameters = inspect.signature(converter).parameters
        assert parameters["multi_threading"].default is False
        assert "threading_mode" not in parameters
