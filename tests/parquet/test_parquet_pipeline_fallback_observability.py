"""Parquet fallback observability and route-annotation tests."""

from __future__ import annotations

from types import SimpleNamespace

import pytest

from schema_sanitizer.adapters.parquet import telemetry as recording


def _native_try_state() -> object:
    """Build the state consumed by the native-stream decision helper."""
    return SimpleNamespace(
        _filters=None,
        _local_path="native-candidate.parquet",
        _source="path",
        _native_source_kind="path",
        _columns=None,
        _batch_size=1024,
        _memory_limit_bytes=None,
        _keepalive=(),
    )


def test_parquet_stream_factory_observability_counts_are_defensive() -> None:
    from schema_sanitizer.adapters.parquet import telemetry as observability

    observability.reset_parquet_stream_factory_observability()

    recording.set_parquet_stream_factory_route("native_parquet_stream")
    recording.set_parquet_stream_factory_route("pyarrow_dataset_scanner")
    recording.set_parquet_native_reader_diagnostics(
        attempted=True,
        ready=False,
        reason="not_ready",
        blockers=["unsupported writer"],
    )

    snapshot = observability.parquet_stream_factory_observability()

    assert snapshot["last_route"] == "pyarrow_dataset_scanner"
    assert snapshot["route_counts"] == {
        "native_parquet_stream": 1,
        "pyarrow_dataset_scanner": 1,
    }
    assert snapshot["native_reader_reason_counts"] == {"not_ready": 1}
    assert snapshot["last_native_reader_diagnostics"]["blockers"] == ["unsupported writer"]

    snapshot["route_counts"]["native_parquet_stream"] = 99
    snapshot["native_reader_reason_counts"]["not_ready"] = 99
    snapshot["last_native_reader_diagnostics"]["blockers"].append("mutated")

    fresh_snapshot = observability.parquet_stream_factory_observability()

    assert fresh_snapshot["route_counts"]["native_parquet_stream"] == 1
    assert fresh_snapshot["native_reader_reason_counts"]["not_ready"] == 1
    assert fresh_snapshot["last_native_reader_diagnostics"]["blockers"] == ["unsupported writer"]

    recording.update_parquet_native_reader_diagnostics(
        nested_detail={"routes": ["pyarrow_dataset_scanner"]},
    )
    nested_snapshot = observability.parquet_stream_factory_observability()
    nested_snapshot["last_native_reader_diagnostics"]["nested_detail"]["routes"].append("mutated")
    assert observability.parquet_stream_factory_observability()["last_native_reader_diagnostics"][
        "nested_detail"
    ] == {"routes": ["pyarrow_dataset_scanner"]}

    observability.reset_parquet_stream_factory_observability()

    reset_snapshot = observability.parquet_stream_factory_observability()
    assert reset_snapshot["last_route"] == "none"
    assert reset_snapshot["route_counts"] == {}
    assert reset_snapshot["native_reader_reason_counts"] == {}
    assert reset_snapshot["fallback_attempt_counts"] == {}
    assert reset_snapshot["fallback_success_counts"] == {}
    assert reset_snapshot["fallback_failure_counts"] == {}
    assert reset_snapshot["last_native_reader_diagnostics"] == {
        "attempted": False,
        "ready": False,
        "reason": "none",
        "blockers": [],
        "fallback_expected": False,
        "fallback_attempted": False,
        "fallback_succeeded": False,
        "fallback_route": None,
        "fallback_error": None,
        "fallback_attempt_history": [],
        "pipeline_contract_satisfied": False,
        "pipeline_contract_route": None,
        "pipeline_contract_error": None,
        "native_reader_contract_satisfied": False,
        "safe_fallback_contract_satisfied": False,
        "created_by": None,
        "native_writer_detected": False,
        "native_writer_contract_satisfied": False,
        "native_nested_contract_applicable": False,
        "native_nested_contract_satisfied": False,
        "native_nested_contract_issues": [],
        "compressed_bytes": 0,
        "decompressed_bytes": 0,
        "decompression_ratio": 0.0,
    }


def test_parquet_fallback_observability_counts_attempts_successes_and_failures() -> None:
    from schema_sanitizer.adapters.parquet import telemetry as observability

    observability.reset_parquet_stream_factory_observability()
    recording.set_parquet_native_reader_diagnostics(
        attempted=True,
        ready=False,
        reason="not_ready",
        blockers=["external writer"],
        fallback_expected=True,
        fallback_route="pyarrow_dataset_scanner",
    )

    recording.record_parquet_fallback_attempt("pyarrow_dataset_scanner")
    recording.record_parquet_fallback_failure(
        "pyarrow_dataset_scanner",
        ValueError("dataset unavailable"),
    )
    recording.record_parquet_fallback_attempt("pyarrow_parquetfile_iter_batches")
    recording.record_parquet_fallback_success("pyarrow_parquetfile_iter_batches")

    snapshot = observability.parquet_stream_factory_observability()

    assert snapshot["fallback_attempt_counts"] == {
        "pyarrow_dataset_scanner": 1,
        "pyarrow_parquetfile_iter_batches": 1,
    }
    assert snapshot["fallback_failure_counts"] == {"pyarrow_dataset_scanner": 1}
    assert snapshot["fallback_success_counts"] == {"pyarrow_parquetfile_iter_batches": 1}
    assert snapshot["route_counts"] == {"pyarrow_parquetfile_iter_batches": 1}
    diagnostics = snapshot["last_native_reader_diagnostics"]
    assert diagnostics["fallback_attempt_history"] == [
        {"route": "pyarrow_dataset_scanner", "status": "attempted"},
        {
            "route": "pyarrow_dataset_scanner",
            "status": "failed",
            "error": "ValueError: dataset unavailable",
        },
        {"route": "pyarrow_parquetfile_iter_batches", "status": "attempted"},
        {"route": "pyarrow_parquetfile_iter_batches", "status": "succeeded"},
    ]
    assert diagnostics["pipeline_contract_satisfied"] is True
    assert diagnostics["pipeline_contract_route"] == "pyarrow_parquetfile_iter_batches"
    assert diagnostics["pipeline_contract_error"] is None
    assert diagnostics["safe_fallback_contract_satisfied"] is True
    assert diagnostics["native_reader_contract_satisfied"] is False

    snapshot["fallback_attempt_counts"]["pyarrow_dataset_scanner"] = 99
    snapshot["fallback_success_counts"]["pyarrow_parquetfile_iter_batches"] = 99
    snapshot["fallback_failure_counts"]["pyarrow_dataset_scanner"] = 99

    fresh = observability.parquet_stream_factory_observability()
    assert fresh["fallback_attempt_counts"]["pyarrow_dataset_scanner"] == 1
    assert fresh["fallback_success_counts"]["pyarrow_parquetfile_iter_batches"] == 1
    assert fresh["fallback_failure_counts"]["pyarrow_dataset_scanner"] == 1


def test_native_parquet_stream_marks_schema_sanitizer_writer_contract(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from schema_sanitizer.adapters.parquet import record_batch_factory as stream_factory
    from schema_sanitizer.adapters.parquet import telemetry as observability

    observability.reset_parquet_stream_factory_observability()
    factory = _native_try_state()

    monkeypatch.setattr(
        stream_factory,
        "native_parquet_stream_preflight_info",
        lambda *args, **kwargs: {
            "created_by": "schema-sanitizer native parquet writer",
            "native_reader_ready": 1,
            "row_group_count": 2,
            "num_rows": 7,
            "row_groups": [{"num_rows": 3}, {"num_rows": 4}],
            "native_reader_blockers": [],
        },
    )
    monkeypatch.setattr(stream_factory, "PARQUET_STREAM_READ", lambda *args: "capsule")

    assert stream_factory.ParquetRecordBatchStreamFactory._try_native_stream(factory) == "capsule"

    snapshot = observability.parquet_stream_factory_observability()
    assert snapshot["last_route"] == "native_parquet_stream"
    assert snapshot["route_counts"] == {"native_parquet_stream": 1}
    assert snapshot["fallback_attempt_counts"] == {}
    diagnostics = snapshot["last_native_reader_diagnostics"]
    assert diagnostics["ready"] is True
    assert diagnostics["reason"] == "native_stream"
    assert diagnostics["fallback_expected"] is False
    assert diagnostics["pipeline_contract_satisfied"] is True
    assert diagnostics["pipeline_contract_route"] == "native_parquet_stream"
    assert diagnostics["pipeline_contract_error"] is None
    assert diagnostics["native_reader_contract_satisfied"] is True
    assert diagnostics["safe_fallback_contract_satisfied"] is False
    assert diagnostics["created_by"] == "schema-sanitizer native parquet writer"
    assert diagnostics["native_writer_detected"] is True
    assert diagnostics["native_writer_contract_satisfied"] is True


def test_parquet_native_reader_non_runtime_errors_fall_back(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from schema_sanitizer.adapters.parquet import record_batch_factory as stream_factory
    from schema_sanitizer.adapters.parquet import telemetry as observability

    observability.reset_parquet_stream_factory_observability()
    factory = _native_try_state()

    monkeypatch.setattr(
        stream_factory,
        "native_parquet_stream_preflight_info",
        lambda *args, **kwargs: {
            "native_reader_ready": 1,
            "row_group_count": 1,
            "num_rows": 3,
            "row_groups": [{"num_rows": 3}],
            "native_reader_blockers": [],
        },
    )

    def failing_native_read(*args: object) -> object:
        """Internal test helper."""
        raise ValueError("corrupt native capsule")

    monkeypatch.setattr(stream_factory, "PARQUET_STREAM_READ", failing_native_read)

    assert stream_factory.ParquetRecordBatchStreamFactory._try_native_stream(factory) is None

    diagnostics = observability.last_parquet_native_reader_diagnostics()
    assert diagnostics["attempted"] is True
    assert diagnostics["ready"] is False
    assert diagnostics["reason"] == "native_error"
    assert diagnostics["fallback_expected"] is True
    assert diagnostics["fallback_route"] == "pyarrow_dataset_scanner"
    assert diagnostics["error"] == "ValueError: corrupt native capsule"
    assert diagnostics["blockers"] == []
    assert observability.parquet_stream_factory_observability()["native_reader_reason_counts"] == {
        "native_error": 1,
    }


def test_parquet_native_footer_errors_fall_back(monkeypatch: pytest.MonkeyPatch) -> None:
    from schema_sanitizer.adapters.parquet import record_batch_factory as stream_factory
    from schema_sanitizer.adapters.parquet import telemetry as observability

    observability.reset_parquet_stream_factory_observability()
    factory = _native_try_state()

    def failing_footer_info(*args: object, **kwargs: object) -> object:
        """Internal test helper."""
        raise OSError("footer read failed")

    monkeypatch.setattr(stream_factory, "native_parquet_stream_preflight_info", failing_footer_info)
    monkeypatch.setattr(stream_factory, "PARQUET_STREAM_READ", object())

    assert stream_factory.ParquetRecordBatchStreamFactory._try_native_stream(factory) is None

    diagnostics = observability.last_parquet_native_reader_diagnostics()
    assert diagnostics["attempted"] is True
    assert diagnostics["ready"] is False
    assert diagnostics["reason"] == "footer_info_error"
    assert diagnostics["fallback_expected"] is True
    assert diagnostics["fallback_route"] == "pyarrow_dataset_scanner"
    assert diagnostics["blockers"] == ["OSError: footer read failed"]


def test_parquet_fallback_route_annotation_is_defensive() -> None:
    from schema_sanitizer.adapters.parquet import telemetry as observability

    observability.reset_parquet_stream_factory_observability()
    recording.set_parquet_native_reader_diagnostics(
        attempted=True,
        ready=False,
        reason="not_ready",
        blockers=["external writer"],
        fallback_expected=True,
        fallback_route="pyarrow_dataset_scanner",
    )

    recording.record_parquet_fallback_success("pyarrow_dataset_scanner")
    snapshot = observability.parquet_stream_factory_observability()

    assert snapshot["last_route"] == "pyarrow_dataset_scanner"
    assert snapshot["route_counts"] == {"pyarrow_dataset_scanner": 1}
    assert snapshot["native_reader_reason_counts"] == {"not_ready": 1}
    diagnostics = snapshot["last_native_reader_diagnostics"]
    assert diagnostics["reason"] == "not_ready"
    assert diagnostics["fallback_expected"] is True
    assert diagnostics["fallback_attempted"] is True
    assert diagnostics["fallback_succeeded"] is True
    assert diagnostics["fallback_route"] == "pyarrow_dataset_scanner"

    diagnostics["blockers"].append("mutated")
    snapshot["route_counts"]["pyarrow_dataset_scanner"] = 99

    fresh = observability.parquet_stream_factory_observability()
    assert fresh["route_counts"] == {"pyarrow_dataset_scanner": 1}
    assert fresh["last_native_reader_diagnostics"]["blockers"] == ["external writer"]
