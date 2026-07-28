"""Regression coverage for v79 high-core CSV output and chunk retention."""

from __future__ import annotations

import json
from pathlib import Path

import pytest
from conftest import require_native

import schema_sanitizer as ss
from schema_sanitizer.api_impl import results as result_adapters
from schema_sanitizer.api_impl.execution_context import default_pool
from schema_sanitizer.core_impl.concurrency_coverage import (
    concurrency_pair_guarantees,
)

ROOT = Path(__file__).resolve().parents[1]
CSV_WRITER = ROOT / "cpp/src/internal/csv/csv_stream_writer.cc"
ESTIMATOR = ROOT / "cpp/src/internal/output/text_output_estimator.hh"
CSV_FIXED_ESTIMATOR = ROOT / "cpp/src/internal/output/csv_fixed_estimator.hh"


class _FakePolarsFrame:
    """Minimal Polars-like frame exposing a stable row count."""

    height = 3


class _FakeReader:
    """Minimal record-batch reader double with lifecycle telemetry."""

    num_record_batches = 2

    def __init__(self) -> None:
        """Initialize the reader as open."""
        self.closed = False

    def close(self) -> None:
        """Record deterministic reader closure."""
        self.closed = True


def test_v79_csv_high_core_policy_is_adaptive_not_globally_wider() -> None:
    """Only wide fixed-cost CSV grows beyond the proven four-worker path."""
    source = CSV_WRITER.read_text(encoding="utf-8")

    assert "kCsvOutputWorkerCeiling = 4" in source
    assert "kMaximumWideFixedCsvWorkers = 16" in source
    assert "wide_fixed_csv_worker_ceiling_for(8) == 4" in source
    assert "wide_fixed_csv_worker_ceiling_for(16) == 8" in source
    assert "wide_fixed_csv_worker_ceiling_for(32) == 16" in source
    assert "high_core_eligible" in source
    assert "output_worker_ceiling" in source
    assert "scale_wide_fixed" in source
    assert "getenv" not in source
    assert "std::thread" not in source


def test_v79_csv_fixed_schema_packet_planning_is_constant_per_row() -> None:
    """Wide fixed CSV reuses one conservative schema-derived row estimate."""
    writer = CSV_WRITER.read_text(encoding="utf-8")
    estimator = ESTIMATOR.read_text(encoding="utf-8")
    fixed_estimator = CSV_FIXED_ESTIMATOR.read_text(encoding="utf-8")

    assert "class CsvRowEstimator" in writer
    assert "plan_" in writer
    assert "make_csv_fixed_estimate_plan" in writer
    assert "estimate_csv_row_bytes_from_plan" in writer
    assert "estimate_csv_row_bytes" in estimator
    assert "fixed_cost_csv_scalar_kind" in fixed_estimator
    assert "fixed_csv_scalar_output_upper_bound" in fixed_estimator
    assert "Nulls can only reduce fixed output" in fixed_estimator
    assert "return multiply_capped(total, 2, cap);" in fixed_estimator
    assert "public outputs with four metadata fields are O(4)" in fixed_estimator


def test_v79_polars_disables_full_frame_rechunk_when_supported(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Polars preserves Arrow batch chunks instead of forcing one serial rechunk."""
    reader = _FakeReader()
    calls: list[tuple[object, dict[str, object]]] = []

    class FakePolars:
        """Polars double accepting the modern rechunk keyword."""

        @staticmethod
        def from_arrow(value: object, **kwargs: object) -> _FakePolarsFrame:
            """Record conversion arguments and return a deterministic frame."""
            calls.append((value, dict(kwargs)))
            return _FakePolarsFrame()

    monkeypatch.setattr(
        result_adapters._pyarrow_streams,
        "reader_from_stream_like",
        lambda _stream, *, feature: reader,
    )
    monkeypatch.setattr(
        result_adapters,
        "ensure_optional_dependency",
        lambda name, **_kwargs: FakePolars if name == "polars" else None,
    )

    conversion = result_adapters.convert_arrow_stream_output(
        "stream", "polars", feature="v79", threading_mode="multi"
    )

    assert calls == [(reader, {"rechunk": False})]
    assert conversion.route == "record_batch_reader_to_polars"
    assert conversion.diagnostics_shape.num_rows == 3
    assert conversion.diagnostics_shape.batch_count == 2
    assert reader.closed is True


def test_v79_polars_keeps_compatibility_with_older_from_arrow(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A version without the rechunk keyword retains the established fallback."""
    reader = _FakeReader()
    calls: list[object] = []

    class FakePolars:
        """Polars double exposing the legacy one-argument conversion API."""

        @staticmethod
        def from_arrow(value: object) -> _FakePolarsFrame:
            """Record the legacy conversion and return a deterministic frame."""
            calls.append(value)
            return _FakePolarsFrame()

    monkeypatch.setattr(
        result_adapters._pyarrow_streams,
        "reader_from_stream_like",
        lambda _stream, *, feature: reader,
    )
    monkeypatch.setattr(
        result_adapters,
        "ensure_optional_dependency",
        lambda name, **_kwargs: FakePolars if name == "polars" else None,
    )

    conversion = result_adapters.convert_arrow_stream_output(
        "stream", "polars", feature="v79", threading_mode="multi"
    )

    assert calls == [reader]
    assert conversion.route == "record_batch_reader_to_polars"
    assert reader.closed is True


def test_v79_polars_does_not_hide_conversion_type_errors(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Only an unsupported rechunk keyword may activate compatibility fallback."""
    reader = _FakeReader()
    calls = 0

    class FakePolars:
        """Polars double failing for a real conversion problem."""

        @staticmethod
        def from_arrow(value: object, **kwargs: object) -> _FakePolarsFrame:
            """Raise a non-signature TypeError without accepting a retry."""
            nonlocal calls
            calls += 1
            raise TypeError("cannot convert incompatible Arrow value")

    monkeypatch.setattr(
        result_adapters._pyarrow_streams,
        "reader_from_stream_like",
        lambda _stream, *, feature: reader,
    )
    monkeypatch.setattr(
        result_adapters,
        "ensure_optional_dependency",
        lambda name, **_kwargs: FakePolars if name == "polars" else None,
    )

    with pytest.raises(RuntimeError, match="could not convert.*Polars"):
        result_adapters.convert_arrow_stream_output(
            "stream", "polars", feature="v79", threading_mode="multi"
        )

    assert calls == 1
    assert reader.closed is True


def test_v79_all_56_pairs_inherit_the_new_shared_guarantee() -> None:
    """Every input keeps a complete route to every improved output."""
    pairs = concurrency_pair_guarantees()

    assert sum(len(outputs) for outputs in pairs.values()) == 56
    for input_name, outputs in pairs.items():
        assert len(outputs) == 7, input_name
        csv = outputs["csv"]
        assert "wide_fixed_schema_o1_packet_planning" in csv["output_parallel_stages"]
        assert "adaptive_high_core_output_workers" in csv["output_parallel_stages"]
        assert csv["source_to_sink_parallel_path"] is True
        assert csv["eligible_multi_benefit"] is True

        polars = outputs["polars"]
        assert "chunk_preserving_no_rechunk_conversion" in polars["output_parallel_stages"]
        assert polars["source_to_sink_parallel_path"] is True
        assert polars["eligible_multi_benefit"] is True


def test_v79_public_wide_csv_uses_hybrid_plan_and_parallel_output(
    tmp_path: Path,
) -> None:
    """The real public path reuses fixed columns and publishes output tasks."""
    require_native()
    source = tmp_path / "wide.jsonl"
    output = tmp_path / "wide.csv"
    with source.open("w", encoding="utf-8") as handle:
        for row in range(4_096):
            handle.write(
                json.dumps(
                    {f"field_{column:02d}": row * 48 + column for column in range(48)},
                    separators=(",", ":"),
                )
                + "\n"
            )

    ss.to_csv(
        source,
        output,
        input_format="jsonl",
        threading_mode="multi",
        memory_limit_bytes=64 << 20,
        parse_integers=True,
        field_name_policy="preserve",
    )
    stats = default_pool().get().performance_stats()
    counters = stats["counters"]
    output_tasks = stats["tasks"]["output"]

    assert counters["csv_fixed_plan_fixed_fields"] >= 48
    assert counters["csv_fixed_plan_dynamic_fields"] <= 4
    assert 4 <= counters["csv_output_worker_ceiling"] <= 16
    assert output_tasks["submitted"] > 1
    assert output_tasks["submitted"] == output_tasks["finished"]
    assert output.exists()
