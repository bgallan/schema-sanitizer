"""Compare single- and multi-worker behavior under a fixed operation clock.

Analytical and file frontends, directory and Parquet fallbacks, error policies, registry
generation and strict preconditions, reused native state, nested schemas, and partition warm-up
must all produce equivalent golden results.
"""

from __future__ import annotations

import json
from datetime import date, datetime
from pathlib import Path
from typing import Any

import pytest

pa = pytest.importorskip("pyarrow")
pq = pytest.importorskip("pyarrow.parquet")

from _support.threading_goldens import (
    assert_exceptions_equivalent,
    assert_logical_files_equivalent,
    assert_results_equivalent,
    canonical_json,
)

import schema_sanitizer as ss
from schema_sanitizer.api_impl.execution_context import ExecutionContext
from schema_sanitizer.options_impl.call_options import normalize_call_options
from schema_sanitizer.pipeline import PartitionRunPlan
from schema_sanitizer.pipeline.advanced import infer_warm_up_schema_registry_state

pytestmark = pytest.mark.usefixtures("fixed_operation_clock")

_FIXED_TIME = datetime(2023, 11, 14, 22, 13, 20, 123456)
_FIXED_DETECTED_AT = "2023-11-14T22:13:20.123456Z"
_SECOND_FIXED_TIME_NS = 1_700_000_111_654_321_000
_SECOND_FIXED_DETECTED_AT = "2023-11-14T22:15:11.654321Z"
_MEMORY_LIMIT = 256 * 1024 * 1024


def _write_input(path: Path, input_format: str, rows: int = 513) -> dict[str, Any]:
    """Write deterministic input for one public frontend."""
    records = [
        {
            "ordinal": index,
            "label": f"row-{index}",
            "value": index % 17,
            "nested": {"items": [index, index + 1]},
        }
        for index in range(rows)
    ]
    options: dict[str, Any] = {"input_format": input_format}
    if input_format in {"jsonl", "json"}:
        path.write_text(
            "\n".join(json.dumps(record, separators=(",", ":")) for record in records) + "\n",
            encoding="utf-8",
        )
    elif input_format == "json_array":
        path.write_text(json.dumps(records, separators=(",", ":")), encoding="utf-8")
    elif input_format == "csv":
        path.write_text(
            "ordinal,label,value\n"
            + "".join(f"{index},row-{index},{index % 17}\n" for index in range(rows)),
            encoding="utf-8",
        )
    elif input_format == "xml":
        path.write_text(
            "<rows>"
            + "".join(
                f"<row><ordinal>{index}</ordinal><label>row-{index}</label>"
                f"<value>{index % 17}</value></row>"
                for index in range(rows)
            )
            + "</rows>",
            encoding="utf-8",
        )
        options["xml_row_tag"] = "row"
    else:  # pragma: no cover - test matrix owns the values
        raise AssertionError(input_format)
    return options


@pytest.mark.parametrize(
    ("input_format", "suffix"),
    [
        ("json", ".json"),
        ("json_array", ".json"),
        ("jsonl", ".jsonl"),
        ("csv", ".csv"),
        ("xml", ".xml"),
    ],
)
def test_fixed_clock_analytical_frontend_matrix(
    tmp_path: Path,
    input_format: str,
    suffix: str,
) -> None:
    """All local text frontends preserve complete logical result state."""
    source = tmp_path / f"source{suffix}"
    options = _write_input(source, input_format)

    single = ss.to_pyarrow(
        source,
        multi_threading=False,
        memory_limit_bytes=_MEMORY_LIMIT,
        **options,
    )
    multi = ss.to_pyarrow(
        source,
        multi_threading=True,
        memory_limit_bytes=_MEMORY_LIMIT,
        **options,
    )

    assert_results_equivalent(single, multi)
    assert set(single.clean_data.column("ingestion_timestamp").to_pylist()) == {_FIXED_TIME}
    assert set(multi.clean_data.column("ingestion_timestamp").to_pylist()) == {_FIXED_TIME}


@pytest.mark.parametrize(
    ("converter_name", "suffix", "extra"),
    [
        ("to_jsonl", ".jsonl", {}),
        ("to_csv", ".csv", {}),
        ("to_parquet", ".parquet", {"parquet_compression": "snappy"}),
    ],
)
def test_fixed_clock_file_output_matrix(
    tmp_path: Path,
    converter_name: str,
    suffix: str,
    extra: dict[str, Any],
) -> None:
    """Text and Parquet outputs preserve result metadata and logical files."""
    source = tmp_path / "source.jsonl"
    options = _write_input(source, "jsonl", rows=3_200)
    converter = getattr(ss, converter_name)
    outputs = {mode: tmp_path / f"{mode}{suffix}" for mode in ("single", "multi")}

    results = {
        mode: converter(
            source,
            outputs[mode],
            multi_threading=mode == "multi",
            memory_limit_bytes=_MEMORY_LIMIT,
            **options,
            **extra,
        )
        for mode in ("single", "multi")
    }

    assert_results_equivalent(results["single"], results["multi"])
    assert_logical_files_equivalent(outputs["single"], outputs["multi"])


def test_fixed_clock_directory_source_plan_equivalence(tmp_path: Path) -> None:
    """Directory discovery and native multi-source ingestion preserve ordering."""
    folder = tmp_path / "directory"
    folder.mkdir()
    for ordinal in range(4):
        path = folder / f"part-{ordinal:02d}.jsonl"
        path.write_text(
            "\n".join(
                json.dumps({"partition": ordinal, "row": row}, separators=(",", ":"))
                for row in range(400)
            )
            + "\n",
            encoding="utf-8",
        )

    single = ss.to_pyarrow(
        folder,
        input_format="jsonl",
        input_mode="directory",
        multi_threading=False,
        memory_limit_bytes=_MEMORY_LIMIT,
    )
    multi = ss.to_pyarrow(
        folder,
        input_format="jsonl",
        input_mode="directory",
        multi_threading=True,
        memory_limit_bytes=_MEMORY_LIMIT,
    )

    assert_results_equivalent(single, multi)


def test_fixed_clock_parquet_input_fallback_equivalence(tmp_path: Path) -> None:
    """INT96 fallback input preserves exact rows, metadata, and ordering."""
    from schema_sanitizer.adapters.parquet.telemetry import (
        last_parquet_stream_factory_route,
    )

    source = tmp_path / "spark-int96.parquet"
    values = [
        datetime(2024, 1, 1, 1, 2, 3, 123456),
        datetime(2024, 1, 2, 4, 5, 6, 654321),
    ] * 600
    pq.write_table(
        pa.table({"ordinal": range(len(values)), "event_time": values}),
        source,
        flavor="spark",
        row_group_size=257,
        use_deprecated_int96_timestamps=True,
    )

    results = {}
    routes = {}
    for mode in ("single", "multi"):
        results[mode] = ss.to_pyarrow(
            source,
            input_format="parquet",
            multi_threading=mode == "multi",
            memory_limit_bytes=_MEMORY_LIMIT,
        )
        routes[mode] = last_parquet_stream_factory_route()

    assert_results_equivalent(results["single"], results["multi"])
    assert routes == {
        "single": "pyarrow_dataset_scanner",
        "multi": "pyarrow_dataset_scanner",
    }


def test_fixed_clock_parquet_output_fallback_equivalence(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Forced PyArrow output fallback preserves replay and logical files."""
    from schema_sanitizer.api_impl.file_conversion import direct_writers

    def native_writer_unavailable(*_args: Any, **_kwargs: Any) -> bool:
        """Force the supported replayable PyArrow fallback route."""
        return False

    monkeypatch.setattr(
        direct_writers,
        "try_write_parquet_raw_direct_native",
        native_writer_unavailable,
    )
    monkeypatch.setattr(
        direct_writers,
        "try_write_parquet_direct_native",
        native_writer_unavailable,
    )
    source = tmp_path / "source.jsonl"
    options = _write_input(source, "jsonl", rows=3_200)
    outputs = {mode: tmp_path / f"fallback-{mode}.parquet" for mode in ("single", "multi")}
    results = {}
    for mode in ("single", "multi"):
        results[mode] = ss.to_parquet(
            source,
            outputs[mode],
            multi_threading=mode == "multi",
            memory_limit_bytes=_MEMORY_LIMIT,
            parquet_compression="snappy",
            **options,
        )

    assert_results_equivalent(results["single"], results["multi"])
    assert_logical_files_equivalent(outputs["single"], outputs["multi"])
    assert {mode: result.stats["file_output_route"] for mode, result in results.items()} == {
        "single": "pyarrow",
        "multi": "pyarrow",
    }


def _strict_materialization(rows: list[dict[str, Any]], mode: str, on_error: str) -> Any:
    """Materialize rows against one strict schema for error-policy goldens."""
    schema = pa.schema(
        [
            pa.field("ordinal", pa.int64(), nullable=False),
            pa.field("value", pa.int64(), nullable=False),
        ]
    )
    options = normalize_call_options(
        schema_contract=schema,
        schema_mode="strict",
        on_error=on_error,
        parse_integers=False,
        multi_threading=mode == "multi",
        memory_limit_bytes=_MEMORY_LIMIT,
    )
    return ExecutionContext().to_table(rows, options=options, format="python", source="python")


@pytest.mark.parametrize("on_error", ["skip_row", "emit_null_row"])
def test_error_policy_result_goldens(on_error: str) -> None:
    """Recoverable row errors preserve rows and diagnostics across modes."""
    rows = [
        {"ordinal": index, "value": "bad" if index % 257 == 0 else index} for index in range(4_500)
    ]

    single = _strict_materialization(rows, "single", on_error)
    multi = _strict_materialization(rows, "multi", on_error)

    assert_results_equivalent(single, multi)


def test_stop_policy_exception_golden() -> None:
    """The earliest strict conversion failure has identical public semantics."""
    rows = [{"ordinal": index, "value": index} for index in range(4_500)]
    rows[1_025]["value"] = "first-failure"
    rows[1_026].pop("value")

    assert_exceptions_equivalent(
        lambda: _strict_materialization(rows, "single", "stop"),
        lambda: _strict_materialization(rows, "multi", "stop"),
    )


def test_fixed_clock_additive_registry_generation_equivalence(tmp_path: Path) -> None:
    """Additive registry evolution preserves generations and drift semantics."""
    seed = tmp_path / "seed.jsonl"
    seed.write_text('{"alpha":1}\n', encoding="utf-8")
    baseline = ss.to_pyarrow(
        seed,
        input_format="jsonl",
        multi_threading=False,
        memory_limit_bytes=_MEMORY_LIMIT,
    )

    source = tmp_path / "evolved.jsonl"
    source.write_text('{"alpha":2,"beta":"x"}\n', encoding="utf-8")
    results = {
        mode: ss.to_pyarrow(
            source,
            input_format="jsonl",
            schema_mode="additive",
            schema_registry=baseline.schema_registry,
            multi_threading=mode == "multi",
            memory_limit_bytes=_MEMORY_LIMIT,
        )
        for mode in ("single", "multi")
    }

    assert_results_equivalent(results["single"], results["multi"])
    registry = results["single"].schema_registry
    assert registry["schema_generation"] == baseline.schema_registry["schema_generation"] + 1
    assert [field["name"] for field in registry["canonical_schema"]["fields"]] == [
        "alpha",
        "beta",
    ]
    assert [drift["drift_type"] for drift in results["single"].schema_drifts] == ["newly_added"]
    assert {drift["detected_at"] for drift in results["single"].schema_drifts} == {
        _FIXED_DETECTED_AT
    }


def test_fixed_clock_strict_registry_precondition_equivalence(tmp_path: Path) -> None:
    """Strict registry mode requires the same canonical baseline in both modes."""
    source = tmp_path / "strict-missing-canonical.jsonl"
    source.write_text('{"alpha":1}\n', encoding="utf-8")
    incomplete_registry = {"schema_generation": 1}

    assert_exceptions_equivalent(
        lambda: ss.to_pyarrow(
            source,
            input_format="jsonl",
            schema_mode="strict",
            schema_registry=incomplete_registry,
            multi_threading=False,
            memory_limit_bytes=_MEMORY_LIMIT,
        ),
        lambda: ss.to_pyarrow(
            source,
            input_format="jsonl",
            schema_mode="strict",
            schema_registry=incomplete_registry,
            multi_threading=True,
            memory_limit_bytes=_MEMORY_LIMIT,
        ),
    )


def test_fixed_clock_strict_registry_file_precondition_equivalence(
    tmp_path: Path,
) -> None:
    """Strict file conversion fails equally and removes partial outputs."""
    source = tmp_path / "strict-file-missing-canonical.jsonl"
    source.write_text('{"alpha":1}\n', encoding="utf-8")
    incomplete_registry = {"schema_generation": 1}
    outputs = {mode: tmp_path / f"strict-{mode}.parquet" for mode in ("single", "multi")}

    assert_exceptions_equivalent(
        lambda: ss.to_parquet(
            source,
            outputs["single"],
            input_format="jsonl",
            schema_mode="strict",
            schema_registry=incomplete_registry,
            multi_threading=False,
            memory_limit_bytes=_MEMORY_LIMIT,
        ),
        lambda: ss.to_parquet(
            source,
            outputs["multi"],
            input_format="jsonl",
            schema_mode="strict",
            schema_registry=incomplete_registry,
            multi_threading=True,
            memory_limit_bytes=_MEMORY_LIMIT,
        ),
    )
    assert not outputs["single"].exists()
    assert not outputs["multi"].exists()


def test_reused_native_registry_state_uses_current_operation_clock(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """New drift from reusable native state uses the current operation clock."""
    from schema_sanitizer.api_impl import operation_context
    from schema_sanitizer.core_impl.schema_registry import native_registry_state_context

    seed = tmp_path / "state-seed"
    seed.mkdir()
    (seed / "seed.jsonl").write_text('{"alpha":1}\n', encoding="utf-8")
    baseline = ss.to_parquet(
        seed,
        tmp_path / "state-seed.parquet",
        input_format="jsonl",
        input_mode="directory",
        multi_threading=False,
        memory_limit_bytes=_MEMORY_LIMIT,
    )
    assert baseline.native_registry_state is not None

    monkeypatch.setattr(operation_context, "time_ns", lambda: _SECOND_FIXED_TIME_NS)
    source = tmp_path / "state-evolved"
    source.mkdir()
    (source / "evolved.jsonl").write_text(
        '{"alpha":2,"beta":"current"}\n',
        encoding="utf-8",
    )

    results = {}
    for mode in ("single", "multi"):
        with native_registry_state_context(baseline.native_registry_state):
            results[mode] = ss.to_parquet(
                source,
                tmp_path / f"state-evolved-{mode}.parquet",
                input_format="jsonl",
                input_mode="directory",
                schema_mode="additive",
                schema_registry=baseline.schema_registry_json,
                multi_threading=mode == "multi",
                memory_limit_bytes=_MEMORY_LIMIT,
            )

    assert_results_equivalent(results["single"], results["multi"])
    assert [drift["drift_type"] for drift in results["single"].schema_drifts] == ["newly_added"]
    assert {drift["detected_at"] for drift in results["single"].schema_drifts} == {
        _SECOND_FIXED_DETECTED_AT
    }


def test_fixed_clock_nested_registry_version_equivalence(tmp_path: Path) -> None:
    """Nested incompatible evolution creates the same named schema version."""
    seed = tmp_path / "nested-seed.jsonl"
    seed.write_text('{"payload":{"text":"before"}}\n', encoding="utf-8")
    baseline = ss.to_pyarrow(
        seed,
        input_format="jsonl",
        multi_threading=False,
        memory_limit_bytes=_MEMORY_LIMIT,
    )

    source = tmp_path / "nested-evolved.jsonl"
    source.write_text('{"payload":[{"text":"after"}]}\n', encoding="utf-8")
    results = {
        mode: ss.to_pyarrow(
            source,
            input_format="jsonl",
            schema_mode="additive",
            schema_registry=baseline.schema_registry,
            multi_threading=mode == "multi",
            memory_limit_bytes=_MEMORY_LIMIT,
        )
        for mode in ("single", "multi")
    }

    assert_results_equivalent(results["single"], results["multi"])
    registry = results["single"].schema_registry
    versions = registry["variants"]["payload"]["versions"]
    assert [version["output_name"] for version in versions] == [
        "payload",
        "payload_v2_struct_array",
    ]
    assert [drift["drift_type"] for drift in results["single"].schema_drifts] == [
        "new_version_generated"
    ]
    assert {drift["detected_at"] for drift in results["single"].schema_drifts} == {
        _FIXED_DETECTED_AT
    }


def test_fixed_clock_partition_warm_up_registry_equivalence(tmp_path: Path) -> None:
    """Multi-partition warm-up compiles equivalent reusable registry state."""
    sources = []
    for ordinal, payload in enumerate(({"alpha": 1}, {"beta": "x"}), start=1):
        source = tmp_path / f"warm-{ordinal}.jsonl"
        source.write_text(json.dumps(payload, separators=(",", ":")) + "\n", encoding="utf-8")
        sources.append(source)
    plans = [
        PartitionRunPlan(
            date(2026, 1, ordinal),
            str(source),
            str(tmp_path / f"warm-{ordinal}.parquet"),
        )
        for ordinal, source in enumerate(sources, start=1)
    ]

    states = {}
    drifts: dict[str, list[dict[str, Any]]] = {"single": [], "multi": []}
    for mode in ("single", "multi"):
        states[mode] = infer_warm_up_schema_registry_state(
            plans,
            input_format="jsonl",
            input_mode="single_file",
            options={
                "multi_threading": mode == "multi",
                "memory_limit_bytes": _MEMORY_LIMIT,
            },
            schema_registry={},
            field_name_policy="lower_snake",
            after_schema_drifts=lambda _index, _total, _plan, raw, mode=mode: drifts[mode].extend(
                json.loads(raw)
            ),
        )

    assert canonical_json(states["multi"].schema_registry_json, empty={}) == canonical_json(
        states["single"].schema_registry_json,
        empty={},
    )
    assert states["single"].native_registry_state is not None
    assert states["multi"].native_registry_state is not None
    assert [
        field["name"] for field in states["single"].schema_registry["canonical_schema"]["fields"]
    ] == ["alpha", "beta"]
    assert drifts["multi"] == drifts["single"]
    assert {drift["detected_at"] for drift in drifts["single"]} == {_FIXED_DETECTED_AT}
