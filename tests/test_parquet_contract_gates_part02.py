"""Tests for Parquet contract gates and certificates.

These core-only contract tests are separated from the large public Parquet API
runtime suite so certification logic can evolve without making the main Parquet
test module harder to navigate.
"""

from __future__ import annotations

import pytest
from parquet_contract_shared import (
    stable_native_writer_footer_info as _stable_native_writer_footer_info,
)

# Split from test_parquet_contract_gates.py: test_parquet_contract_certification_status_certifies_native_writer_contract, test_parquet_contract_certification_status_certifies_external_pyarrow_fallback, test_parquet_contract_certification_status_fails_native_nested_drift, ...


def test_parquet_contract_certification_status_certifies_native_writer_contract() -> None:
    """Verify the combined certificate accepts a stable schema-sanitizer-native file."""
    from schema_sanitizer.adapters.parquet.contract_gates.native import (
        _native_parquet_writer_contract_status_from_footer_info,
    )
    from schema_sanitizer.adapters.parquet.status import (
        _parquet_contract_certification_status_from_parts,
        _parquet_preflight_contract_status_from_writer_status,
    )

    writer_status = _native_parquet_writer_contract_status_from_footer_info(
        _stable_native_writer_footer_info(),
        native_stream_available=True,
    )
    preflight_status = _parquet_preflight_contract_status_from_writer_status(
        writer_status,
        pyarrow_available=False,
    )

    certificate = _parquet_contract_certification_status_from_parts(
        preflight_status=preflight_status,
        writer_status=writer_status,
    )

    assert certificate["satisfied"] is True
    assert certificate["route"] == "native_parquet_stream"
    assert certificate["pipeline_safe_with_fallback"] is True
    assert certificate["native_writer_contract_applicable"] is True
    assert certificate["native_writer_contract_satisfied"] is True
    assert certificate["nested_contract_applicable"] is True
    assert certificate["nested_contract_satisfied"] is True
    assert certificate["safe_fallback_contract_satisfied"] is False
    assert certificate["issues"] == []


def test_parquet_contract_certification_status_certifies_external_pyarrow_fallback() -> None:
    """Verify the combined certificate accepts external files when PyArrow covers them."""
    from schema_sanitizer.adapters.parquet.contract_gates.native import (
        _native_parquet_writer_contract_status_from_footer_info,
    )
    from schema_sanitizer.adapters.parquet.status import (
        _parquet_contract_certification_status_from_parts,
        _parquet_preflight_contract_status_from_writer_status,
    )

    info = _stable_native_writer_footer_info()
    info["created_by"] = "spark-3.x"
    writer_status = _native_parquet_writer_contract_status_from_footer_info(
        info,
        native_stream_available=True,
    )
    preflight_status = _parquet_preflight_contract_status_from_writer_status(
        writer_status,
        pyarrow_available=True,
    )

    certificate = _parquet_contract_certification_status_from_parts(
        preflight_status=preflight_status,
        writer_status=writer_status,
    )

    assert certificate["satisfied"] is True
    assert certificate["route"] == "pyarrow_fallback_available"
    assert certificate["pipeline_safe_with_fallback"] is True
    assert certificate["native_writer_contract_applicable"] is False
    assert certificate["native_writer_contract_satisfied"] is False
    assert certificate["safe_fallback_contract_satisfied"] is True
    assert certificate["issues"] == []


def test_parquet_contract_certification_status_fails_native_nested_drift() -> None:
    """Verify schema-sanitizer-native nested drift fails the combined certificate."""
    from schema_sanitizer.adapters.parquet.contract_gates.native import (
        _native_parquet_writer_contract_status_from_footer_info,
    )
    from schema_sanitizer.adapters.parquet.status import (
        _parquet_contract_certification_status_from_parts,
        _parquet_preflight_contract_status_from_writer_status,
    )

    info = _stable_native_writer_footer_info()
    second_layout = info["row_groups"][1]["native_recursive_output_layout"]
    second_layout["fields"][0]["leaf_path_repetition_levels"] = [[0, 1, 1, 1, 1, 1]]
    writer_status = _native_parquet_writer_contract_status_from_footer_info(
        info,
        native_stream_available=True,
    )
    preflight_status = _parquet_preflight_contract_status_from_writer_status(
        writer_status,
        pyarrow_available=True,
    )

    certificate = _parquet_contract_certification_status_from_parts(
        preflight_status=preflight_status,
        writer_status=writer_status,
    )

    assert certificate["satisfied"] is False
    assert certificate["pipeline_safe_with_fallback"] is True
    assert certificate["native_writer_contract_applicable"] is True
    assert certificate["native_writer_contract_satisfied"] is False
    assert certificate["nested_contract_applicable"] is True
    assert certificate["nested_contract_satisfied"] is False
    assert any("native-writer:" in issue for issue in certificate["issues"])
    assert any("nested" in issue for issue in certificate["issues"])


def test_parquet_contract_certification_status_fails_projection_drift() -> None:
    """Verify optional projection contract audits participate in the certificate."""
    from schema_sanitizer.adapters.parquet.contract_gates.native import (
        _native_parquet_writer_contract_status_from_footer_info,
    )
    from schema_sanitizer.adapters.parquet.status import (
        _parquet_contract_certification_status_from_parts,
        _parquet_preflight_contract_status_from_writer_status,
    )

    writer_status = _native_parquet_writer_contract_status_from_footer_info(
        _stable_native_writer_footer_info(),
        native_stream_available=True,
    )
    preflight_status = _parquet_preflight_contract_status_from_writer_status(
        writer_status,
        pyarrow_available=False,
    )

    certificate = _parquet_contract_certification_status_from_parts(
        preflight_status=preflight_status,
        writer_status=writer_status,
        projection_audit={"stable": False, "mismatches": ["root contract drifted"]},
    )

    assert certificate["satisfied"] is False
    assert certificate["projection_contract_applicable"] is True
    assert certificate["projection_contract_satisfied"] is False
    assert "projection: root contract drifted" in certificate["issues"]


def test_parquet_contract_certification_status_public_gate(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Verify the public combined certificate wires writer/preflight/projection gates."""
    from schema_sanitizer.adapters.parquet import status as parquet_runtime
    from schema_sanitizer.adapters.parquet.contract_gates.native import (
        _native_parquet_writer_contract_status_from_footer_info,
    )

    writer_status = _native_parquet_writer_contract_status_from_footer_info(
        _stable_native_writer_footer_info(),
        native_stream_available=True,
    )
    monkeypatch.setattr(
        parquet_runtime,
        "native_parquet_writer_contract_status",
        lambda *args, **kwargs: writer_status,
    )
    monkeypatch.setattr(parquet_runtime, "pyarrow_importable", lambda: False)
    monkeypatch.setattr(
        parquet_runtime,
        "native_parquet_recursive_projection_coverage_contract_audit",
        lambda *args, **kwargs: {"stable": True, "mismatches": []},
    )

    certificate = parquet_runtime.parquet_contract_certification_status(
        "writer-native.parquet",
        projections=[["payload"]],
        require_full_projection_coverage=True,
        allow_projection_overlaps=False,
    )

    assert certificate["satisfied"] is True
    assert certificate["route"] == "native_parquet_stream"
    assert certificate["projection_contract_applicable"] is True
    assert certificate["projection_contract_satisfied"] is True


def test_native_parquet_writer_contract_status_enforces_runtime_batch_size() -> None:
    """Verify preflight native certification uses the same row-group batch contract as runtime."""
    from schema_sanitizer.adapters.parquet.contract_gates.native import (
        _native_parquet_writer_contract_status_from_footer_info,
    )

    info = _stable_native_writer_footer_info()
    info["row_groups"][0]["num_rows"] = 3
    info["row_groups"][1]["num_rows"] = 1

    blocked = _native_parquet_writer_contract_status_from_footer_info(
        info,
        native_stream_available=True,
        batch_size=2,
    )
    allowed = _native_parquet_writer_contract_status_from_footer_info(
        info,
        native_stream_available=True,
        batch_size=3,
    )

    assert blocked["applicable"] is True
    assert blocked["satisfied"] is False
    assert blocked["batch_size"] == 2
    assert blocked["max_row_group_rows"] == 3
    assert blocked["batch_size_contract_satisfied"] is False
    assert any("batch-size contract" in issue for issue in blocked["issues"])
    assert allowed["satisfied"] is True
    assert allowed["batch_size_contract_satisfied"] is True


def test_parquet_contract_certification_status_fails_native_batch_size_contract() -> None:
    """Verify certificates fail schema-sanitizer-native files that cannot satisfy runtime batch semantics."""
    from schema_sanitizer.adapters.parquet.contract_gates.native import (
        _native_parquet_writer_contract_status_from_footer_info,
    )
    from schema_sanitizer.adapters.parquet.status import (
        _parquet_contract_certification_status_from_parts,
        _parquet_preflight_contract_status_from_writer_status,
    )

    info = _stable_native_writer_footer_info()
    info["row_groups"][0]["num_rows"] = 4
    info["row_groups"][1]["num_rows"] = 1
    writer_status = _native_parquet_writer_contract_status_from_footer_info(
        info,
        native_stream_available=True,
        batch_size=2,
    )
    preflight_status = _parquet_preflight_contract_status_from_writer_status(
        writer_status,
        pyarrow_available=True,
    )

    certificate = _parquet_contract_certification_status_from_parts(
        preflight_status=preflight_status,
        writer_status=writer_status,
    )

    assert preflight_status["satisfied"] is True
    assert preflight_status["route"] == "pyarrow_fallback_available"
    assert certificate["satisfied"] is False
    assert certificate["pipeline_safe_with_fallback"] is True
    assert certificate["native_writer_contract_applicable"] is True
    assert certificate["native_writer_contract_satisfied"] is False
    assert certificate["safe_fallback_contract_satisfied"] is True
    assert certificate["batch_size"] == 2
    assert certificate["max_row_group_rows"] == 4
    assert certificate["batch_size_contract_satisfied"] is False
    assert any("batch-size contract" in issue for issue in certificate["issues"])


def test_parquet_preflight_contract_status_passes_batch_size_to_writer_gate(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Verify the public preflight gate certifies the same batch-size contract as runtime."""
    from schema_sanitizer.adapters.parquet import status as parquet_runtime

    captured: dict[str, object] = {}

    def fake_writer_status(*args: object, **kwargs: object) -> dict[str, object]:
        """Internal test helper."""
        captured.update(kwargs)
        return {
            "applicable": True,
            "satisfied": False,
            "issues": ["native reader batch-size contract: too small"],
            "created_by": "schema-sanitizer native parquet writer",
            "native_reader_ready": True,
            "nested_contract_applicable": True,
            "nested_contract_satisfied": True,
        }

    monkeypatch.setattr(
        parquet_runtime, "native_parquet_writer_contract_status", fake_writer_status
    )
    monkeypatch.setattr(parquet_runtime, "pyarrow_importable", lambda: True)

    status = parquet_runtime.parquet_preflight_contract_status(
        "native.parquet",
        columns=["payload"],
        batch_size=128,
    )

    assert captured["columns"] == ["payload"]
    assert captured["batch_size"] == 128
    assert status["satisfied"] is True
    assert status["route"] == "pyarrow_fallback_available"
    assert status["native_writer_contract_satisfied"] is False
    assert status["safe_fallback_contract_satisfied"] is True


def test_native_parquet_writer_contract_status_rejects_runtime_filters() -> None:
    """Verify native-writer certification models the runtime filter fallback contract."""
    from schema_sanitizer.adapters.parquet.contract_gates.native import (
        _native_parquet_writer_contract_status_from_footer_info,
    )

    filtered = _native_parquet_writer_contract_status_from_footer_info(
        _stable_native_writer_footer_info(),
        native_stream_available=True,
        filters=object(),
    )
    unfiltered = _native_parquet_writer_contract_status_from_footer_info(
        _stable_native_writer_footer_info(),
        native_stream_available=True,
        filters=None,
    )

    assert filtered["applicable"] is True
    assert filtered["satisfied"] is False
    assert filtered["filters_present"] is True
    assert filtered["filter_contract_satisfied"] is False
    assert any("filter contract" in issue for issue in filtered["issues"])
    assert unfiltered["satisfied"] is True
    assert unfiltered["filter_contract_satisfied"] is True


def test_parquet_contract_certification_status_fails_native_filter_contract() -> None:
    """Verify filters keep the pipeline safe through PyArrow but fail the native guarantee."""
    from schema_sanitizer.adapters.parquet.contract_gates.native import (
        _native_parquet_writer_contract_status_from_footer_info,
    )
    from schema_sanitizer.adapters.parquet.status import (
        _parquet_contract_certification_status_from_parts,
        _parquet_preflight_contract_status_from_writer_status,
    )

    writer_status = _native_parquet_writer_contract_status_from_footer_info(
        _stable_native_writer_footer_info(),
        native_stream_available=True,
        filters=object(),
    )
    preflight_status = _parquet_preflight_contract_status_from_writer_status(
        writer_status,
        pyarrow_available=True,
    )

    certificate = _parquet_contract_certification_status_from_parts(
        preflight_status=preflight_status,
        writer_status=writer_status,
    )

    assert preflight_status["satisfied"] is True
    assert preflight_status["route"] == "pyarrow_fallback_available"
    assert preflight_status["filters_present"] is True
    assert preflight_status["filter_contract_satisfied"] is False
    assert certificate["satisfied"] is False
    assert certificate["pipeline_safe_with_fallback"] is True
    assert certificate["native_writer_contract_applicable"] is True
    assert certificate["native_writer_contract_satisfied"] is False
    assert certificate["safe_fallback_contract_satisfied"] is True
    assert certificate["filters_present"] is True
    assert certificate["filter_contract_satisfied"] is False
    assert any("filter contract" in issue for issue in certificate["issues"])


def test_parquet_preflight_contract_status_passes_filters_to_writer_gate(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Verify public preflight certifies the same filter contract as runtime."""
    from schema_sanitizer.adapters.parquet import status as parquet_runtime

    captured: dict[str, object] = {}
    sentinel_filter = object()

    def fake_writer_status(*args: object, **kwargs: object) -> dict[str, object]:
        """Internal test helper."""
        captured.update(kwargs)
        return {
            "applicable": True,
            "satisfied": False,
            "issues": ["native reader filter contract: predicate filters require PyArrow"],
            "created_by": "schema-sanitizer native parquet writer",
            "native_reader_ready": True,
            "filters_present": True,
            "filter_contract_satisfied": False,
            "nested_contract_applicable": True,
            "nested_contract_satisfied": True,
        }

    monkeypatch.setattr(
        parquet_runtime, "native_parquet_writer_contract_status", fake_writer_status
    )
    monkeypatch.setattr(parquet_runtime, "pyarrow_importable", lambda: True)

    status = parquet_runtime.parquet_preflight_contract_status(
        "native.parquet",
        columns=["payload"],
        batch_size=128,
        filters=sentinel_filter,
    )

    assert captured["columns"] == ["payload"]
    assert captured["batch_size"] == 128
    assert captured["filters"] is sentinel_filter
    assert status["satisfied"] is True
    assert status["route"] == "pyarrow_fallback_available"
    assert status["native_writer_contract_satisfied"] is False
    assert status["safe_fallback_contract_satisfied"] is True
    assert status["filters_present"] is True
    assert status["filter_contract_satisfied"] is False
