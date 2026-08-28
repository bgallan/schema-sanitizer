"""Regression coverage for concurrency complete format matrix names worker structural framing."""

from __future__ import annotations

import csv
import json
from pathlib import Path

import pytest
from _support.threading_goldens import assert_exceptions_equivalent
from conftest import require_native

import schema_sanitizer as ss
from schema_sanitizer.api_impl.execution_context import default_pool
from schema_sanitizer.core_impl.concurrency_coverage import (
    INPUT_CONCURRENCY_COVERAGE,
    OUTPUT_CONCURRENCY_COVERAGE,
    concurrency_guarantees,
)

ROOT = Path(__file__).resolve().parents[2]
SCANNER_HEADER = ROOT / "cpp/src/internal/parsing/streaming/json/scanner.hh"
SCANNER_VALUE = ROOT / "cpp/src/internal/parsing/streaming/json/scanner_value.cc"
JSON_FRONTEND = ROOT / "cpp/src/frontends/json/text_frontend.cc"

_GENERATED_COLUMNS = {
    "schema_registry",
    "schema_drifts",
    "source_file",
    "ingestion_timestamp",
}


def _user_rows(path: Path) -> list[dict[str, str]]:
    """Return ordered user values while excluding invocation metadata."""
    with path.open("r", encoding="utf-8", newline="") as handle:
        return [
            {key: value for key, value in row.items() if key not in _GENERATED_COLUMNS}
            for row in csv.DictReader(handle)
        ]


def _materialization_stats() -> tuple[int, int]:
    """Return submitted materialization tasks and peak active tasks."""
    stats = default_pool().get().performance_stats()
    return (
        int(stats["tasks"]["materialization"]["submitted"]),
        int(stats["counters"]["peak_active_tasks"]),
    )


def _write_object_array(path: Path, *, rows: int = 8_192) -> None:
    """Write a sub-chunk array with enough work for observable worker overlap."""
    payload = [
        {
            "ordinal": row,
            "text": f'row-{row}-{{object}}-[array]-"quote"-\\slash',
            "nested": {"value": row % 11, "items": [row, row + 1]},
        }
        for row in range(rows)
    ]
    path.write_text(
        json.dumps(payload, ensure_ascii=False, separators=(",", ":")),
        encoding="utf-8",
    )


def test_complete_format_matrix_names_worker_structural_framing() -> None:
    """JSON gains framing acceleration without weakening any format contract."""
    guarantees = concurrency_guarantees()
    assert len(INPUT_CONCURRENCY_COVERAGE) == 7
    assert len(OUTPUT_CONCURRENCY_COVERAGE) == 7
    for name in ("json", "json_array"):
        assert "worker_authoritative_structural_framing" in INPUT_CONCURRENCY_COVERAGE[name]
        assert guarantees["inputs"][name]["benefit_proof"] == (
            "worker_structural_framing_plus_direct_columnar_runtime"
        )
    for family in guarantees.values():
        for contract in family.values():
            assert contract["eligible_multi_benefit"] is True
            assert contract["parallel_stages"]
            assert contract["benefit_proof"]


def test_scanner_is_bounded_worker_authoritative_and_has_canonical_fallback() -> None:
    """The coordinator frames shallow values and delegates validation to workers."""
    header = SCANNER_HEADER.read_text(encoding="utf-8")
    scanner = SCANNER_VALUE.read_text(encoding="utf-8")
    frontend = JSON_FRONTEND.read_text(encoding="utf-8")

    assert "set_worker_authoritative_framing" in header
    assert "worker_authoritative_framing_" in header
    assert "kWorkerFramingInlineDepth = 64" in scanner
    assert "find_worker_framed_value_end" in scanner
    assert "return std::nullopt" in scanner
    assert "json_skip_value" in scanner
    assert "scan_json_value_span" in scanner
    assert "*end == data.size() && !eof_ && primitive" in scanner
    assert "FrontendMaterializationMode::kWorkerAuthoritativeRaw" in frontend
    combined = header + scanner + frontend
    assert "std::thread" not in combined
    assert "getenv" not in combined


@pytest.mark.parametrize("input_format", ["json", "json_array"])
def test_object_arrays_keep_exact_single_multi_data_and_real_work(
    tmp_path: Path, input_format: str
) -> None:
    """Both array aliases frame cheaply and preserve authoritative worker output."""
    require_native()
    source = tmp_path / f"{input_format}.json"
    _write_object_array(source)
    assert source.stat().st_size < 1 << 20

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
            memory_limit_bytes=128 << 20,
            parse_integers=True,
            on_error="stop",
        )
        telemetry[mode] = _materialization_stats()

    assert _user_rows(outputs["multi"]) == _user_rows(outputs["single"])
    assert telemetry["single"][0] == 0
    assert telemetry["multi"][0] >= 2
    assert telemetry["multi"][1] >= 2


def test_deep_values_fall_back_without_changing_results(tmp_path: Path) -> None:
    """Nesting beyond the inline stack uses the canonical span scanner."""
    require_native()
    value: object = 7
    for depth in range(70):
        value = {f"level_{depth}": value}
    source = tmp_path / "deep.json"
    source.write_text(json.dumps([value], separators=(",", ":")), encoding="utf-8")

    rows: dict[str, list[dict[str, str]]] = {}
    for mode in ("single", "multi"):
        output = tmp_path / f"deep-{mode}.csv"
        ss.to_csv(
            source,
            output,
            input_format="json",
            multi_threading=mode == "multi",
            memory_limit_bytes=64 << 20,
            arrow_max_depth=96,
            on_error="stop",
        )
        rows[mode] = _user_rows(output)
    assert rows["multi"] == rows["single"]


def test_scalar_split_at_chunk_boundary_is_complete_and_parallel(tmp_path: Path) -> None:
    """A primitive spanning the 1 MiB chunk boundary is never emitted partially."""
    require_native()
    source = tmp_path / "scalars.json"
    source.write_text(
        "[" + ",".join(str(index) for index in range(200_000)) + "]",
        encoding="utf-8",
    )
    assert source.stat().st_size > 1 << 20

    rows: dict[str, list[dict[str, str]]] = {}
    for mode in ("single", "multi"):
        output = tmp_path / f"scalars-{mode}.csv"
        ss.to_csv(
            source,
            output,
            input_format="json",
            multi_threading=mode == "multi",
            memory_limit_bytes=128 << 20,
            parse_integers=True,
            on_error="stop",
        )
        rows[mode] = _user_rows(output)
    assert len(rows["single"]) == 200_000
    assert rows["multi"] == rows["single"]


@pytest.mark.parametrize(
    "payload",
    [
        '[{"value":1} {"value":2}]',
        '[{"value":1],{"value":2}]',
        '[{"value":tru},{"value":2}]',
        '[{"value":"unterminated}]',
    ],
)
def test_malformed_arrays_keep_exact_single_multi_errors(tmp_path: Path, payload: str) -> None:
    """Light framing never weakens deterministic public parse failures."""
    require_native()
    source = tmp_path / "invalid.json"
    source.write_text(payload, encoding="utf-8")

    def run(mode: str) -> None:
        """Execute one malformed array under the requested threading mode."""
        ss.to_csv(
            source,
            tmp_path / f"invalid-{mode}.csv",
            input_format="json",
            multi_threading=mode == "multi",
            memory_limit_bytes=64 << 20,
            on_error="stop",
        )

    assert_exceptions_equivalent(lambda: run("single"), lambda: run("multi"))
