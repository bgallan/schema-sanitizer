"""Hold fixed-wide parallel JSONL processing to a strict single-stage oracle.

Row, column, parse, validation, and conversion failures must keep canonical precedence through
nested fallbacks and empty containers; low-budget replay also proves Arrow ownership, bounded
reordering, and exact packet reparenting.
"""

from __future__ import annotations

import gc
import json
import os
from pathlib import Path

import pytest
from _support.threading_goldens import (
    assert_exceptions_equivalent,
    assert_logical_files_equivalent,
    semantic_stats,
)

import schema_sanitizer as ss
from schema_sanitizer.api_impl.execution_context import ExecutionContext
from schema_sanitizer.api_impl.file_conversion.writers import write_jsonl_native_first_stream
from schema_sanitizer.api_impl.stream_output import write_raw_stream_to_file
from schema_sanitizer.core_impl.schema_registry import schema_contract_from_registry_json
from schema_sanitizer.options_impl.call_options import normalize_call_options

pytestmark = pytest.mark.usefixtures("fixed_operation_clock")

_MEMORY_LIMIT = 256 * 1024 * 1024
_LOW_MEMORY_LIMIT = (256 if os.name == "nt" else 64) * 1024 * 1024
_COLUMNS = tuple(
    f"field{chr(ord('a') + index // 26)}{chr(ord('a') + index % 26)}" for index in range(128)
)


def _write_wide_jsonl(path: Path, rows: int) -> None:
    """Write deterministic fixed-width-dominant JSONL rows."""
    with path.open("w", encoding="utf-8") as handle:
        for row_index in range(rows):
            row = {name: str(row_index + column) for column, name in enumerate(_COLUMNS)}
            handle.write(json.dumps(row, separators=(",", ":")) + "\n")


def _strict_contract(tmp_path: Path):
    """Create one native logical-schema payload without requiring PyArrow."""
    source = tmp_path / "contract-source.jsonl"
    source.write_text(
        json.dumps({name: index for index, name in enumerate(_COLUMNS)}) + "\n",
        encoding="utf-8",
    )
    result = ss.to_jsonl(
        source,
        tmp_path / "contract-output.jsonl",
        input_format="jsonl",
        multi_threading=False,
        memory_limit_bytes=_MEMORY_LIMIT,
    )
    contract = schema_contract_from_registry_json(result.schema_registry_json)
    assert contract is not None
    return contract


def _consume_strict_jsonl(
    source: Path, output: Path, *, mode: str, contract: object, memory_limit: int = _MEMORY_LIMIT
):
    """Consume a strict native stream through Arrow C Data without PyArrow."""
    options = normalize_call_options(
        schema_contract=contract,
        schema_mode="strict",
        on_error="stop",
        parse_integers=False,
        multi_threading=mode == "multi",
        memory_limit_bytes=memory_limit,
    )
    context = ExecutionContext()
    sink = context.to_sink(
        source,
        sink="stream",
        options=options,
        format="jsonl",
        source="path",
    )
    return write_raw_stream_to_file(
        sink.raw,
        output,
        writer=write_jsonl_native_first_stream,
        feature="wide-fixed-jsonl-matches-single-oracle column partition regression",
        first_row_columns=None,
        memory_limit_bytes=memory_limit,
        threading_mode=mode,
    )


def test_wide_fixed_jsonl_matches_single_oracle(tmp_path: Path, require_native: None) -> None:
    """Disjoint column owners preserve every logical row and diagnostic."""
    source = tmp_path / "wide.jsonl"
    _write_wide_jsonl(source, 12_000)

    results = {}
    outputs = {}
    for mode in ("single", "multi"):
        outputs[mode] = tmp_path / f"wide-{mode}.jsonl"
        results[mode] = ss.to_jsonl(
            source,
            outputs[mode],
            input_format="jsonl",
            parse_integers=True,
            on_error="stop",
            multi_threading=mode == "multi",
            memory_limit_bytes=_MEMORY_LIMIT,
        )

    assert_logical_files_equivalent(outputs["single"], outputs["multi"])
    assert semantic_stats(results["multi"].stats) == semantic_stats(results["single"].stats)
    assert results["multi"].schema_registry_json == results["single"].schema_registry_json
    assert results["multi"].stats["materialized_rows"] == 12_000


def test_partitioned_first_error_is_row_then_column_ordinal(
    tmp_path: Path, require_native: None
) -> None:
    """A later low column cannot overtake an earlier high-column failure."""
    contract = _strict_contract(tmp_path)
    rows = [
        {name: row_index + column for column, name in enumerate(_COLUMNS)}
        for row_index in range(256)
    ]
    rows[17][_COLUMNS[20]] = "earlier-high-column"
    rows[33][_COLUMNS[0]] = "later-low-column"
    source = tmp_path / "ordered-errors.jsonl"
    source.write_text(
        "\n".join(json.dumps(row, separators=(",", ":")) for row in rows) + "\n",
        encoding="utf-8",
    )

    def run(mode: str):
        """Run the ordered-error fixture in one execution mode."""
        return _consume_strict_jsonl(
            source,
            tmp_path / f"ordered-errors-{mode}.jsonl",
            mode=mode,
            contract=contract,
        )

    assert_exceptions_equivalent(lambda: run("single"), lambda: run("multi"))
    with pytest.raises(RuntimeError, match=f"field '{_COLUMNS[20]}'"):
        run("multi")


def test_row_validation_precedes_same_row_conversion(tmp_path: Path, require_native: None) -> None:
    """Strict extra-field validation keeps serial priority within one row."""
    contract = _strict_contract(tmp_path)
    rows = [
        {name: row_index + column for column, name in enumerate(_COLUMNS)}
        for row_index in range(64)
    ]
    rows[9][_COLUMNS[20]] = "same-row-conversion"
    rows[9]["unexpected_extra"] = 1
    source = tmp_path / "same-row-errors.jsonl"
    source.write_text(
        "\n".join(json.dumps(row, separators=(",", ":")) for row in rows) + "\n",
        encoding="utf-8",
    )

    def run(mode: str):
        """Run the same-row validation fixture in one execution mode."""
        return _consume_strict_jsonl(
            source,
            tmp_path / f"same-row-errors-{mode}.jsonl",
            mode=mode,
            contract=contract,
        )

    assert_exceptions_equivalent(lambda: run("single"), lambda: run("multi"))
    with pytest.raises(RuntimeError, match="observed extra field 'unexpected_extra'"):
        run("multi")


def test_earlier_conversion_precedes_later_row_validation(
    tmp_path: Path, require_native: None
) -> None:
    """Complete row validation cannot reorder a later strict error forward."""
    contract = _strict_contract(tmp_path)
    rows = [
        {name: row_index + column for column, name in enumerate(_COLUMNS)}
        for row_index in range(128)
    ]
    rows[7][_COLUMNS[20]] = "earlier-conversion"
    rows[19]["unexpected_extra"] = 1
    source = tmp_path / "cross-row-errors.jsonl"
    source.write_text(
        "\n".join(json.dumps(row, separators=(",", ":")) for row in rows) + "\n",
        encoding="utf-8",
    )

    def run(mode: str):
        """Run the cross-row validation fixture in one execution mode."""
        return _consume_strict_jsonl(
            source,
            tmp_path / f"cross-row-errors-{mode}.jsonl",
            mode=mode,
            contract=contract,
        )

    assert_exceptions_equivalent(lambda: run("single"), lambda: run("multi"))
    with pytest.raises(RuntimeError, match=f"field '{_COLUMNS[20]}'"):
        run("multi")


def test_later_scanner_parse_error_matches_single_stage_oracle(
    tmp_path: Path, require_native: None
) -> None:
    """Scanner-stage failures retain the established single-mode precedence."""
    contract = _strict_contract(tmp_path)
    lines = [
        json.dumps(
            {name: row_index + column for column, name in enumerate(_COLUMNS)},
            separators=(",", ":"),
        )
        for row_index in range(64)
    ]
    row = {name: 7 + column for column, name in enumerate(_COLUMNS)}
    row[_COLUMNS[20]] = "earlier-conversion"
    lines[7] = json.dumps(row, separators=(",", ":"))
    lines[19] = '{"fieldaa":19'
    source = tmp_path / "conversion-before-parse.jsonl"
    source.write_text("\n".join(lines) + "\n", encoding="utf-8")

    def run(mode: str):
        """Run the conversion-before-parse fixture in one execution mode."""
        return _consume_strict_jsonl(
            source,
            tmp_path / f"conversion-before-parse-{mode}.jsonl",
            mode=mode,
            contract=contract,
        )

    assert_exceptions_equivalent(lambda: run("single"), lambda: run("multi"))
    with pytest.raises(RuntimeError, match="JSON parse error"):
        run("multi")


def test_earlier_json_parse_error_precedes_later_conversion(
    tmp_path: Path, require_native: None
) -> None:
    """Raw fallback keeps a malformed earlier row as the global first error."""
    contract = _strict_contract(tmp_path)
    lines = [
        json.dumps(
            {name: row_index + column for column, name in enumerate(_COLUMNS)},
            separators=(",", ":"),
        )
        for row_index in range(64)
    ]
    lines[7] = '{"fieldaa":7'
    row = {name: 19 + column for column, name in enumerate(_COLUMNS)}
    row[_COLUMNS[0]] = "later-conversion"
    lines[19] = json.dumps(row, separators=(",", ":"))
    source = tmp_path / "parse-before-conversion.jsonl"
    source.write_text("\n".join(lines) + "\n", encoding="utf-8")

    def run(mode: str):
        """Run the parse-before-conversion fixture in one execution mode."""
        return _consume_strict_jsonl(
            source,
            tmp_path / f"parse-before-conversion-{mode}.jsonl",
            mode=mode,
            contract=contract,
        )

    assert_exceptions_equivalent(lambda: run("single"), lambda: run("multi"))
    with pytest.raises(RuntimeError, match="JSON parse error"):
        run("multi")


def test_observed_nested_value_falls_back_without_reordering(
    tmp_path: Path, require_native: None
) -> None:
    """A scalar contract with nested observed data returns to the serial oracle."""
    contract = _strict_contract(tmp_path)
    rows = [
        {name: row_index + column for column, name in enumerate(_COLUMNS)}
        for row_index in range(64)
    ]
    rows[11][_COLUMNS[12]] = {"nested": 1}
    source = tmp_path / "nested-fallback.jsonl"
    source.write_text(
        "\n".join(json.dumps(row, separators=(",", ":")) for row in rows) + "\n",
        encoding="utf-8",
    )

    def run(mode: str):
        """Run the nested-fallback fixture in one execution mode."""
        return _consume_strict_jsonl(
            source,
            tmp_path / f"nested-fallback-{mode}.jsonl",
            mode=mode,
            contract=contract,
        )

    assert_exceptions_equivalent(lambda: run("single"), lambda: run("multi"))


def test_empty_containers_preserve_duplicate_and_strict_semantics(
    tmp_path: Path, require_native: None
) -> None:
    """Empty containers stay ignorable and cannot mask a later scalar duplicate."""
    contract = _strict_contract(tmp_path)
    rows = [
        json.dumps(
            {name: row_index + column for column, name in enumerate(_COLUMNS)},
            separators=(",", ":"),
        )
        for row_index in range(32)
    ]
    tail = ",".join(
        f"{json.dumps(name)}:{column + 7}" for column, name in enumerate(_COLUMNS[1:], start=1)
    )
    rows[7] = (
        "{"
        f"{json.dumps(_COLUMNS[0])}:{{}},"
        f"{json.dumps(_COLUMNS[0])}:7,"
        f"{tail},"
        '"ignored_extra":{}'
        "}"
    )
    source = tmp_path / "empty-container-duplicates.jsonl"
    source.write_text("\n".join(rows) + "\n", encoding="utf-8")

    outputs = {}
    for mode in ("single", "multi"):
        outputs[mode] = tmp_path / f"empty-container-duplicates-{mode}.jsonl"
        _consume_strict_jsonl(
            source,
            outputs[mode],
            mode=mode,
            contract=contract,
        )

    assert_logical_files_equivalent(outputs["single"], outputs["multi"])
    decoded = json.loads(outputs["multi"].read_text(encoding="utf-8").splitlines()[7])
    assert decoded[_COLUMNS[0]] == 7


def test_low_budget_repeated_consumption_preserves_arrow_ownership(
    tmp_path: Path, require_native: None
) -> None:
    """Merged children survive producer reuse and release exactly once."""
    source = tmp_path / "bounded.jsonl"
    _write_wide_jsonl(source, 4_000)
    single = tmp_path / "bounded-single.jsonl"
    ss.to_jsonl(
        source,
        single,
        input_format="jsonl",
        parse_integers=True,
        on_error="stop",
        multi_threading=False,
        memory_limit_bytes=_LOW_MEMORY_LIMIT,
    )

    for repetition in range(3):
        output = tmp_path / f"bounded-multi-{repetition}.jsonl"
        result = ss.to_jsonl(
            source,
            output,
            input_format="jsonl",
            parse_integers=True,
            on_error="stop",
            multi_threading=True,
            memory_limit_bytes=_LOW_MEMORY_LIMIT,
        )
        gc.collect()
        assert result.stats["materialized_rows"] == 4_000
        assert_logical_files_equivalent(single, output)


def test_sources_encode_bounded_reorder_and_exact_reparenting() -> None:
    """Keep the architectural invariants visible against accidental rollback."""
    root = Path(__file__).resolve().parents[2]
    header = (
        root / "cpp/src/internal/materialization/ingest_stream/column_partition.hh"
    ).read_text()
    implementation = (
        root / "cpp/src/internal/materialization/ingest_stream/column_partition.cc"
    ).read_text()
    coordinator = (
        root / "cpp/src/internal/materialization/ingest_stream/parallel_source_impl.hh"
    ).read_text()
    dispatch = (
        root / "cpp/src/internal/materialization/ingest_stream/parallel_source_dispatch.cc"
    ).read_text()

    row_stream = (root / "cpp/src/sanitize/core/row_stream.hh").read_text()
    frontend = (root / "cpp/src/frontends/json/text_row_materializer.hh").read_text()

    assert "std::pmr::vector<std::int32_t> field_indices" in header
    assert "kMinimumPartitionColumns = 128" in implementation
    assert "kPlanOrdered = 2" in row_stream
    assert "rewrite_current_row_as_raw" in frontend
    assert "child = nullptr" in implementation
    assert "cdata_stream::release_array_nothrow(array)" in implementation
    assert "std::deque<ColumnPartitionAssembly>" in coordinator
    assert "return workers >= 16 ? 2" in implementation
    assert "groups > executor_->dispatch_window() / packet_window" in dispatch
