"""Reader hardening resource diagnostics regressions."""

from __future__ import annotations

import json
from pathlib import Path

import pytest


@pytest.mark.parametrize(
    ("input_format", "suffix", "payload", "options", "expected_depth", "expected_nodes"),
    [
        (
            "xml",
            ".xml",
            "<rows><row><outer><inner>x&amp;y</inner></outer></row>"
            "<row><outer><inner>z</inner></outer></row></rows>",
            {"xml_row_tag": "row"},
            3,
            6,
        ),
        ("csv", ".csv", "a,b\n1,2\n3,4\n", {}, 0, 0),
        ("jsonl", ".jsonl", '{"a":1}\n{"a":2}\n', {}, 0, 0),
    ],
)
@pytest.mark.parametrize("multi_threading", [False, True])
def test_reader_resource_diagnostics_report_bounded_operation_metrics(
    tmp_path: Path,
    input_format: str,
    suffix: str,
    payload: str,
    options: dict[str, object],
    expected_depth: int,
    expected_nodes: int,
    multi_threading: bool,
    require_native: None,
) -> None:
    """Every text reader reports stable counters without retaining pool bytes."""
    import schema_sanitizer as ss

    source = tmp_path / f"source{suffix}"
    output = tmp_path / f"output-{input_format}-{multi_threading}.jsonl"
    source.write_text(payload, encoding="utf-8")
    limit = 8 << 20

    result = ss.to_jsonl(
        source,
        output,
        input_format=input_format,
        memory_limit_bytes=limit,
        multi_threading=multi_threading,
        **options,
    )
    stats = result.stats

    assert stats["reader_records"] == 2
    assert stats["decoded_bytes"] > 0
    assert stats["parser_max_depth"] == expected_depth
    assert stats["reader_nodes"] == expected_nodes
    assert stats["current_charged_memory_bytes"] == 0
    assert 0 < stats["peak_charged_memory_bytes"] <= limit
    assert stats["operation_memory_limit_bytes"] == limit
    assert stats["cancellations"] == 0
    assert stats["cancellation_reason"] == ""
    assert stats["decompression_ratio"] == 0.0


def test_reader_resource_diagnostics_are_mode_independent_except_peak(
    tmp_path: Path,
    require_native: None,
) -> None:
    """Serial and parallel execution expose identical semantic reader metrics."""
    import schema_sanitizer as ss

    source = tmp_path / "source.jsonl"
    source.write_text("".join(f'{{"value":{index}}}\n' for index in range(5000)))
    results = []
    for multi_threading in (False, True):
        results.append(
            ss.to_jsonl(
                source,
                tmp_path / f"out-{multi_threading}.jsonl",
                input_format="jsonl",
                multi_threading=multi_threading,
                memory_limit_bytes=32 << 20,
            ).stats
        )

    volatile = {"peak_charged_memory_bytes"}
    left = {key: value for key, value in results[0].items() if key not in volatile}
    right = {key: value for key, value in results[1].items() if key not in volatile}
    assert left == right


def test_consumer_close_records_cancellation_and_releases_pool(require_native: None) -> None:
    """Closing an unconsumed native stream records a privacy-safe reason code."""
    from schema_sanitizer.api_impl.execution_context import ExecutionContext
    from schema_sanitizer.options_impl.call_options import normalize_call_options

    options = normalize_call_options(
        memory_limit_bytes=8 << 20,
        multi_threading=False,
    ).raw
    raw = ExecutionContext()._raw.to_registry_sink_from_source(
        "stream",
        "jsonl",
        "text",
        '{"a":1}\n{"a":2}\n',
        options,
        registry_json="{}",
        field_name_policy="lower_alpha",
        schema_mode="additive",
        first_row_columns={},
        all_row_columns={},
        row_span_columns={},
        timestamp_columns=(),
    )
    try:
        raw.close_main_stream()
        diagnostics = json.loads(raw.diagnostics.to_json())
        assert diagnostics["cancellations"] == 1
        assert diagnostics["cancellation_reason"] == "consumer_close"
        assert diagnostics["current_charged_memory_bytes"] == 0
        assert diagnostics["reader_records"] == 2
    finally:
        raw.close()


def test_parquet_resource_diagnostics_sum_footer_sizes() -> None:
    """Parquet telemetry derives compression counters without reading payloads."""
    from schema_sanitizer.adapters.parquet.native_reader import (
        parquet_resource_diagnostics,
    )

    diagnostics = parquet_resource_diagnostics(
        {
            "row_groups": [
                {
                    "columns": [
                        {"total_compressed_size": 25, "total_uncompressed_size": 100},
                        {"total_compressed_size": 75, "total_uncompressed_size": 200},
                    ]
                },
                {
                    "columns": [
                        {"total_compressed_size": -1, "total_uncompressed_size": 50},
                        {"total_compressed_size": "bad", "total_uncompressed_size": None},
                    ]
                },
            ]
        }
    )

    assert diagnostics == {
        "compressed_bytes": 100,
        "decompressed_bytes": 350,
        "decompression_ratio": 3.5,
    }
