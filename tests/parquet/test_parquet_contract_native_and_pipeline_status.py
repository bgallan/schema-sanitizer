"""Tests for Parquet contract gates and certificates.

These core-only contract tests are separated from the large public Parquet API
runtime suite so certification logic can evolve without making the main Parquet
test module harder to navigate.
"""

from __future__ import annotations

import pytest
from _support.parquet_contracts import (
    stable_native_nested_contract_summary as _stable_native_nested_contract_summary,
)
from _support.parquet_contracts import (
    stable_native_writer_footer_info as _stable_native_writer_footer_info,
)

from schema_sanitizer.adapters.parquet import telemetry as recording


def test_native_nested_contract_status_certifies_stable_recursive_summary() -> None:
    """Verify the compact nested contract gate certifies stable recursive layouts."""
    from schema_sanitizer.adapters.parquet.contract_gates.native import (
        _native_nested_contract_status_from_summary,
    )

    status = _native_nested_contract_status_from_summary(_stable_native_nested_contract_summary())

    assert status["applicable"] is True
    assert status["satisfied"] is True
    assert status["issues"] == []
    assert status["row_group_count"] == 2
    assert status["decoded_row_group_count"] == 2
    assert status["field_count"] == 1
    assert status["canonical_layout_fingerprint"] == "payload=field-fp"
    assert status["canonical_leaf_contract_fingerprint"] == "payload=leaf-fp"
    assert status["canonical_root_contract_fingerprint"] == "payload=root-fp"


def test_native_nested_contract_status_fails_closed_on_recursive_drift() -> None:
    """Verify the compact nested contract gate rejects partial/drifted layouts."""
    from schema_sanitizer.adapters.parquet.contract_gates.native import (
        _native_nested_contract_status_from_summary,
    )

    summary = _stable_native_nested_contract_summary()
    summary.update(
        {
            "decoded_row_group_count": 1,
            "stable_across_row_groups": False,
            "mismatches": ["root-contract drifted"],
            "row_group_leaf_contract_fingerprints_stable": False,
            "leaf_path_collisions": [
                {"leaf_path": "payload.list.element", "first_field": "a", "other_field": "b"}
            ],
        }
    )

    status = _native_nested_contract_status_from_summary(summary)

    assert status["applicable"] is True
    assert status["satisfied"] is False
    assert any("decoded row-group count" in issue for issue in status["issues"])
    assert "recursive layout is not stable across row groups" in status["issues"]
    assert "root-contract drifted" in status["issues"]
    assert "leaf contract fingerprints drifted" in status["issues"]
    assert "leaf path ownership collisions detected" in status["issues"]


def test_native_parquet_nested_contract_status_uses_public_summary(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Verify the public nested contract API reduces recursive summaries to a verdict."""
    from schema_sanitizer.adapters.parquet import status as parquet_footer

    monkeypatch.setattr(
        parquet_footer,
        "native_parquet_recursive_layout_summary",
        lambda *args, **kwargs: _stable_native_nested_contract_summary(),
    )

    status = parquet_footer.native_parquet_nested_contract_status(
        "nested.parquet",
        columns=["payload"],
    )

    assert status["applicable"] is True
    assert status["satisfied"] is True
    assert status["field_order"] == ["payload"]


def test_parquet_fallback_failure_marks_pipeline_contract_failed() -> None:
    """Verify final fallback failure makes the pipeline contract fail closed."""
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
        OSError("dataset scanner unavailable"),
    )

    diagnostics = observability.last_parquet_native_reader_diagnostics()
    assert diagnostics["pipeline_contract_satisfied"] is False
    assert diagnostics["pipeline_contract_route"] == "pyarrow_dataset_scanner"
    assert diagnostics["pipeline_contract_error"] == "OSError: dataset scanner unavailable"
    assert diagnostics["safe_fallback_contract_satisfied"] is False
    assert diagnostics["fallback_succeeded"] is False


def test_last_parquet_pipeline_contract_status_certifies_native_success() -> None:
    """Verify the compact pipeline gate accepts a clean native-reader success."""
    from schema_sanitizer.adapters.parquet import telemetry as observability

    observability.reset_parquet_stream_factory_observability()
    recording.set_parquet_native_reader_diagnostics(
        attempted=True,
        ready=True,
        reason="native_stream",
        fallback_expected=False,
        fallback_attempted=False,
        fallback_succeeded=False,
        pipeline_contract_satisfied=True,
        pipeline_contract_route="native_parquet_stream",
        pipeline_contract_error=None,
        native_reader_contract_satisfied=True,
        safe_fallback_contract_satisfied=False,
    )

    status = observability.last_parquet_pipeline_contract_status()

    assert status["satisfied"] is True
    assert status["route"] == "native_parquet_stream"
    assert status["issues"] == []
    assert status["native_reader_contract_satisfied"] is True
    assert status["safe_fallback_contract_satisfied"] is False


def test_last_parquet_pipeline_contract_status_certifies_safe_fallback_success() -> None:
    """Verify the compact pipeline gate accepts a successful PyArrow fallback."""
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
    recording.record_parquet_fallback_success("pyarrow_dataset_scanner")

    status = observability.last_parquet_pipeline_contract_status()

    assert status["satisfied"] is True
    assert status["route"] == "pyarrow_dataset_scanner"
    assert status["issues"] == []
    assert status["fallback_attempted"] is True
    assert status["fallback_succeeded"] is True
    assert status["safe_fallback_contract_satisfied"] is True


def test_last_parquet_pipeline_contract_status_fails_closed_on_inconsistent_state() -> None:
    """Verify the compact pipeline gate rejects inconsistent fallback diagnostics."""
    from schema_sanitizer.adapters.parquet import telemetry as observability

    observability.reset_parquet_stream_factory_observability()
    recording.set_parquet_native_reader_diagnostics(
        attempted=True,
        ready=False,
        reason="not_ready",
        fallback_expected=True,
        fallback_route="pyarrow_dataset_scanner",
        pipeline_contract_satisfied=True,
        pipeline_contract_route="pyarrow_dataset_scanner",
        safe_fallback_contract_satisfied=True,
        fallback_attempted=False,
        fallback_succeeded=True,
    )

    status = observability.last_parquet_pipeline_contract_status()

    assert status["satisfied"] is False
    assert "PyArrow fallback route did not record an attempt" in status["issues"]


def test_native_parquet_writer_contract_status_certifies_stable_native_nested_file() -> None:
    """Verify writer-native footer diagnostics produce a satisfied contract status."""
    from schema_sanitizer.adapters.parquet.contract_gates.native import (
        _native_parquet_writer_contract_status_from_footer_info,
    )

    status = _native_parquet_writer_contract_status_from_footer_info(
        _stable_native_writer_footer_info(),
        native_stream_available=True,
    )

    assert status["applicable"] is True
    assert status["satisfied"] is True
    assert status["issues"] == []
    assert status["native_writer_detected"] is True
    assert status["native_reader_ready"] is True
    assert status["native_stream_available"] is True
    assert status["nested_contract_applicable"] is True
    assert status["nested_contract_satisfied"] is True
    assert status["canonical_leaf_contract_fingerprint"]
    assert status["canonical_root_contract_fingerprint"]


def test_native_parquet_writer_contract_status_fails_closed_on_missing_native_stream() -> None:
    """Verify writer-native certification fails closed if native streaming is absent."""
    from schema_sanitizer.adapters.parquet.contract_gates.native import (
        _native_parquet_writer_contract_status_from_footer_info,
    )

    status = _native_parquet_writer_contract_status_from_footer_info(
        _stable_native_writer_footer_info(),
        native_stream_available=False,
    )

    assert status["applicable"] is True
    assert status["satisfied"] is False
    assert "native Parquet stream function is unavailable" in status["issues"]


def test_native_parquet_writer_contract_status_fails_closed_on_external_writer() -> None:
    """Verify writer-native certification does not bless external writer layouts."""
    from schema_sanitizer.adapters.parquet.contract_gates.native import (
        _native_parquet_writer_contract_status_from_footer_info,
    )

    info = _stable_native_writer_footer_info()
    info["created_by"] = "spark-3.x"

    status = _native_parquet_writer_contract_status_from_footer_info(
        info,
        native_stream_available=True,
    )

    assert status["applicable"] is False
    assert status["satisfied"] is False
    assert any("not created by schema-sanitizer" in issue for issue in status["issues"])


def test_native_parquet_writer_contract_status_uses_public_footer_info(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Verify the public writer-native contract gate reduces footer diagnostics."""
    from schema_sanitizer.adapters.parquet import status as parquet_footer

    monkeypatch.setattr(
        parquet_footer,
        "native_parquet_footer_info",
        lambda *args, **kwargs: _stable_native_writer_footer_info(),
    )

    status = parquet_footer.native_parquet_writer_contract_status(
        "writer-native.parquet",
        columns=["payload"],
    )

    assert status["satisfied"] is True
    assert status["native_writer_detected"] is True
    assert status["nested_contract_satisfied"] is True


def test_parquet_preflight_contract_status_certifies_native_without_pyarrow() -> None:
    """Verify preflight accepts a schema-sanitizer-native file without PyArrow."""
    from schema_sanitizer.adapters.parquet.contract_gates.native import (
        _native_parquet_writer_contract_status_from_footer_info,
    )
    from schema_sanitizer.adapters.parquet.status import (
        _parquet_preflight_contract_status_from_writer_status,
    )

    writer_status = _native_parquet_writer_contract_status_from_footer_info(
        _stable_native_writer_footer_info(),
        native_stream_available=True,
    )

    status = _parquet_preflight_contract_status_from_writer_status(
        writer_status,
        pyarrow_available=False,
    )

    assert status["satisfied"] is True
    assert status["route"] == "native_parquet_stream"
    assert status["pyarrow_available"] is False
    assert status["native_writer_contract_satisfied"] is True
    assert status["safe_fallback_contract_satisfied"] is False
    assert status["nested_contract_satisfied"] is True
    assert status["issues"] == []


def test_parquet_preflight_contract_status_certifies_external_with_pyarrow() -> None:
    """Verify preflight accepts external/unsupported files only with PyArrow."""
    from schema_sanitizer.adapters.parquet.contract_gates.native import (
        _native_parquet_writer_contract_status_from_footer_info,
    )
    from schema_sanitizer.adapters.parquet.status import (
        _parquet_preflight_contract_status_from_writer_status,
    )

    info = _stable_native_writer_footer_info()
    info["created_by"] = "spark-3.x"
    writer_status = _native_parquet_writer_contract_status_from_footer_info(
        info,
        native_stream_available=True,
    )

    status = _parquet_preflight_contract_status_from_writer_status(
        writer_status,
        pyarrow_available=True,
    )

    assert status["satisfied"] is True
    assert status["route"] == "pyarrow_fallback_available"
    assert status["pyarrow_available"] is True
    assert status["native_writer_contract_satisfied"] is False
    assert status["safe_fallback_contract_satisfied"] is True
    assert any(
        "not created by schema-sanitizer" in issue for issue in status["native_writer_issues"]
    )
    assert status["issues"] == []


def test_parquet_preflight_contract_status_fails_without_native_or_pyarrow() -> None:
    """Verify preflight fails closed when neither native nor PyArrow can cover input."""
    from schema_sanitizer.adapters.parquet.contract_gates.native import (
        _native_parquet_writer_contract_status_from_footer_info,
    )
    from schema_sanitizer.adapters.parquet.status import (
        _parquet_preflight_contract_status_from_writer_status,
    )

    info = _stable_native_writer_footer_info()
    info["created_by"] = "spark-3.x"
    writer_status = _native_parquet_writer_contract_status_from_footer_info(
        info,
        native_stream_available=True,
    )

    status = _parquet_preflight_contract_status_from_writer_status(
        writer_status,
        pyarrow_available=False,
    )

    assert status["satisfied"] is False
    assert status["route"] is None
    assert status["pyarrow_available"] is False
    assert status["native_writer_contract_satisfied"] is False
    assert status["safe_fallback_contract_satisfied"] is False
    assert any("PyArrow is not installed" in issue for issue in status["issues"])
    assert any("not created by schema-sanitizer" in issue for issue in status["issues"])


def test_parquet_preflight_contract_status_uses_public_writer_gate(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Verify the public preflight gate combines writer status and PyArrow availability."""
    from schema_sanitizer.adapters.parquet import status as parquet_runtime

    monkeypatch.setattr(
        parquet_runtime,
        "native_parquet_writer_contract_status",
        lambda *args, **kwargs: {
            "applicable": False,
            "satisfied": False,
            "issues": ["external writer"],
            "created_by": "external",
            "native_reader_ready": False,
            "nested_contract_applicable": False,
            "nested_contract_satisfied": False,
        },
    )
    monkeypatch.setattr(parquet_runtime, "pyarrow_importable", lambda: True)

    status = parquet_runtime.parquet_preflight_contract_status("external.parquet")

    assert status["satisfied"] is True
    assert status["route"] == "pyarrow_fallback_available"
    assert status["safe_fallback_contract_satisfied"] is True


def test_native_writer_nested_contract_blockers_fail_closed_on_drift() -> None:
    """Verify native-writer nested drift becomes a native-read blocker."""
    from schema_sanitizer.adapters.parquet.record_batch_factory import (
        ParquetRecordBatchStreamFactory,
    )

    info = _stable_native_writer_footer_info()
    second_layout = info["row_groups"][1]["native_recursive_output_layout"]
    second_layout["fields"][0]["leaf_path_repetition_levels"] = [[0, 1, 1, 1, 1, 1]]

    blockers = ParquetRecordBatchStreamFactory._native_nested_contract_blockers(info)

    assert blockers
    assert any("native nested contract" in blocker for blocker in blockers)
    assert any("repetition" in blocker or "stable" in blocker for blocker in blockers)
