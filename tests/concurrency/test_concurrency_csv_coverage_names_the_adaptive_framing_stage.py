"""Certify adaptive CSV framing as the concrete parallel stage recorded by coverage.

The scanner must adapt without private threads, preserve values across chunk boundaries, and keep
wide-schema decoding and probing identical between single- and multi-worker execution.
"""

from __future__ import annotations

import csv
from pathlib import Path

from _support.diagnostics import assert_diagnostics_semantically_equal

import schema_sanitizer as ss
from schema_sanitizer.core_impl.concurrency_coverage import (
    INPUT_CONCURRENCY_COVERAGE,
    OUTPUT_CONCURRENCY_COVERAGE,
    concurrency_guarantees,
)
from schema_sanitizer.core_impl.execution import ExecutionContext
from schema_sanitizer.options_impl.call_options import normalize_call_options

ROOT = Path(__file__).resolve().parents[2]
RECORD_SPAN = ROOT / "cpp/src/internal/parsing/streaming/csv/record_span.cc"
SCANNER_HEADER = ROOT / "cpp/src/internal/parsing/streaming/csv/scanner.hh"
SCANNER_SOURCE = ROOT / "cpp/src/internal/parsing/streaming/csv/scanner.cc"

_GENERATED_COLUMNS = {
    "schema_registry",
    "schema_drifts",
    "source_file",
    "ingestion_timestamp",
}


def _user_rows(path: Path) -> list[dict[str, str]]:
    """Read ordered user columns from one generated CSV file."""
    csv.field_size_limit(16 << 20)
    with path.open("r", encoding="utf-8", newline="") as handle:
        return [
            {key: value for key, value in row.items() if key not in _GENERATED_COLUMNS}
            for row in csv.DictReader(handle)
        ]


def _probe(path: Path, mode: str):
    """Run a CSV schema probe and return result plus native telemetry."""
    context = ExecutionContext()
    options = normalize_call_options(
        multi_threading=mode == "multi",
        memory_limit_bytes=64 << 20,
        on_error="stop",
        field_name_policy="preserve",
    ).raw
    result = context.schema_probe_paths("csv", [str(path)], options)
    return result, context.performance_stats()


def test_csv_coverage_names_the_adaptive_framing_stage() -> None:
    """The complete format matrix records the new ordered CSV acceleration."""
    guarantees = concurrency_guarantees()
    assert "adaptive_vector_record_framing" in INPUT_CONCURRENCY_COVERAGE["csv"]
    assert (
        guarantees["inputs"]["csv"]["benefit_proof"]
        == "adaptive_vector_framing_plus_parallel_decode_runtime"
    )
    for family in guarantees.values():
        for contract in family.values():
            assert contract["eligible_multi_benefit"] is True
            assert contract["parallel_stages"]
            assert contract["benefit_proof"]
    assert len(INPUT_CONCURRENCY_COVERAGE) == 7
    assert len(OUTPUT_CONCURRENCY_COVERAGE) == 7


def test_csv_scanner_is_adaptive_without_private_threads() -> None:
    """Wide records vectorize while short and quote-dense records stay scalar."""
    record_span = RECORD_SPAN.read_text(encoding="utf-8")
    scanner_header = SCANNER_HEADER.read_text(encoding="utf-8")
    scanner_source = SCANNER_SOURCE.read_text(encoding="utf-8")

    assert "std::memchr" in record_span
    assert "kCsvVectorScanMinimum" in record_span
    assert "kCsvDenseQuoteGap" in record_span
    assert "kCsvDenseQuoteRun" in record_span
    assert "prefer_vector_scan_" in record_span
    assert "prefer_vector_scan_" in scanner_header
    assert "prefer_vector_scan_ = false" in scanner_source
    combined = record_span + scanner_header + scanner_source
    assert "std::thread" not in combined
    assert "getenv" not in combined


def test_csv_chunk_boundaries_preserve_exact_single_multi_values(
    tmp_path: Path,
    require_native: None,
) -> None:
    """Escaped quotes, CRLF, and multiline fields survive chunk transitions."""
    chunk_bytes = 1 << 20
    header = "id,text\n"
    prefix = '1,"'
    pad_len = chunk_bytes - 1 - len(header.encode()) - len(prefix.encode())
    assert pad_len > 0

    expected = [
        {"id": "1", "text": "a" * pad_len + '"boundary'},
        {"id": "2", "text": 'hello, "quoted" value'},
        {"id": "3", "text": "multi\nline\nvalue"},
        {"id": "4", "text": "x" * (2 * chunk_bytes + 17)},
    ]
    source = tmp_path / "chunk-boundaries.csv"
    with source.open("w", encoding="utf-8", newline="") as handle:
        handle.write(header)
        handle.write(prefix)
        handle.write("a" * pad_len)
        handle.write('""boundary"\r\n')
        handle.write('2,"hello, ""quoted"" value"\n')
        handle.write('3,"multi\nline\r\nvalue"\r\n')
        handle.write('4,"')
        handle.write("x" * (2 * chunk_bytes + 17))
        handle.write('"\n')

    actual: dict[str, list[dict[str, str]]] = {}
    for mode in ("single", "multi"):
        output = tmp_path / f"{mode}.csv"
        ss.to_csv(
            source,
            output,
            input_format="csv",
            multi_threading=mode == "multi",
            # This test exercises multi-megabyte record framing, not the
            # minimum-memory contract. Leave enough headroom for the input
            # owner and the encoded output packet on every allocator.
            memory_limit_bytes=64 << 20,
            on_error="stop",
        )
        actual[mode] = _user_rows(output)

    assert actual["single"] == expected
    assert actual["multi"] == expected


def test_wide_csv_keeps_parallel_decode_and_exact_probe_parity(
    tmp_path: Path,
    require_native: None,
) -> None:
    """Adaptive framing continues feeding real input tasks in multi mode."""
    source = tmp_path / "wide.csv"
    columns = [f"field_{index:02d}" for index in range(12)]
    payload = "abcdefghijklmnopqrstuvwxyz0123456789" * 8
    with source.open("w", encoding="utf-8", newline="") as handle:
        handle.write(",".join(columns) + "\n")
        for row in range(2_048):
            values = [
                f'"row {row}, column {column}, ""quoted"" {payload}"'
                for column in range(len(columns))
            ]
            handle.write(",".join(values) + "\n")

    single, single_stats = _probe(source, "single")
    multi, multi_stats = _probe(source, "multi")

    assert multi.schema_payload == single.schema_payload
    assert multi.field_names == single.field_names
    assert_diagnostics_semantically_equal(multi.diagnostics, single.diagnostics)
    assert int(single_stats["tasks"]["input"]["submitted"]) == 0
    assert int(multi_stats["tasks"]["input"]["submitted"]) >= 2
    assert int(multi_stats["counters"]["peak_active_tasks"]) >= 2
    assert int(multi_stats["memory"]["peak_bytes"]) <= 64 << 20
