"""Tests for Parquet contract gates and certificates.

These core-only contract tests are separated from the large public Parquet API
runtime suite so certification logic can evolve without making the main Parquet
test module harder to navigate.
"""

from __future__ import annotations

from functools import partial

import pytest
from parquet_contract_shared import filter_rejecting_writer_status

# Split from test_parquet_contract_gates.py: test_parquet_contract_certification_status_passes_filters_to_writer_gate, test_parquet_contract_runtime_readiness_status_from_capabilities_accepts_full_runtime, test_parquet_contract_runtime_readiness_status_fails_without_pyarrow, ...


def test_parquet_contract_certification_status_passes_filters_to_writer_gate(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Verify public certificate models filter-driven fallback in the native contract."""
    from schema_sanitizer.adapters.parquet import status as parquet_runtime

    captured: dict[str, object] = {}
    sentinel_filter = object()
    fake_writer_status = partial(filter_rejecting_writer_status, captured)

    monkeypatch.setattr(
        parquet_runtime, "native_parquet_writer_contract_status", fake_writer_status
    )
    monkeypatch.setattr(parquet_runtime, "pyarrow_importable", lambda: True)

    certificate = parquet_runtime.parquet_contract_certification_status(
        "native.parquet",
        columns=["payload"],
        batch_size=128,
        filters=sentinel_filter,
    )

    assert captured["filters"] is sentinel_filter
    assert certificate["pipeline_safe_with_fallback"] is True
    assert certificate["native_writer_contract_applicable"] is True
    assert certificate["native_writer_contract_satisfied"] is False
    assert certificate["safe_fallback_contract_satisfied"] is True
    assert certificate["filters_present"] is True
    assert certificate["filter_contract_satisfied"] is False
    assert certificate["satisfied"] is False


def test_parquet_contract_runtime_readiness_status_from_capabilities_accepts_full_runtime() -> None:
    """Verify runtime readiness can certify PyArrow fallback plus native gates."""
    from schema_sanitizer.adapters.parquet.status import (
        _parquet_contract_runtime_readiness_status_from_capabilities,
    )

    status = _parquet_contract_runtime_readiness_status_from_capabilities(
        pyarrow_available=True,
        native_footer_available=True,
        native_stream_available=True,
    )

    assert status["satisfied"] is True
    assert status["issues"] == []
    assert status["safe_fallback_runtime_available"] is True
    assert status["native_reader_runtime_available"] is True
    assert status["schema_sanitizer_native_contracts_gateable"] is True
    assert status["nested_native_contracts_gateable"] is True


def test_parquet_contract_runtime_readiness_status_fails_without_pyarrow() -> None:
    """Verify the safe fallback contract fails closed when PyArrow is required."""
    from schema_sanitizer.adapters.parquet.status import (
        _parquet_contract_runtime_readiness_status_from_capabilities,
    )

    status = _parquet_contract_runtime_readiness_status_from_capabilities(
        pyarrow_available=False,
        native_footer_available=True,
        native_stream_available=True,
        require_pyarrow=True,
    )

    assert status["satisfied"] is False
    assert status["safe_fallback_runtime_available"] is False
    assert any("PyArrow is required" in issue for issue in status["issues"])


def test_parquet_contract_runtime_readiness_status_fails_without_native_gates() -> None:
    """Verify native/nested guarantees fail closed when native ABI hooks are absent."""
    from schema_sanitizer.adapters.parquet.status import (
        _parquet_contract_runtime_readiness_status_from_capabilities,
    )

    status = _parquet_contract_runtime_readiness_status_from_capabilities(
        pyarrow_available=True,
        native_footer_available=False,
        native_stream_available=False,
        require_native=True,
    )

    assert status["satisfied"] is False
    assert status["safe_fallback_runtime_available"] is True
    assert status["native_reader_runtime_available"] is False
    assert status["schema_sanitizer_native_contracts_gateable"] is False
    assert status["nested_native_contracts_gateable"] is False
    assert any("footer diagnostics" in issue for issue in status["issues"])
    assert any("stream reader" in issue for issue in status["issues"])


def test_parquet_contract_runtime_readiness_status_can_relax_native_requirement() -> None:
    """Verify callers can gate only the safe-fallback pipeline when desired."""
    from schema_sanitizer.adapters.parquet.status import (
        _parquet_contract_runtime_readiness_status_from_capabilities,
    )

    status = _parquet_contract_runtime_readiness_status_from_capabilities(
        pyarrow_available=True,
        native_footer_available=False,
        native_stream_available=False,
        require_native=False,
    )

    assert status["satisfied"] is True
    assert status["safe_fallback_runtime_available"] is True
    assert status["native_reader_runtime_available"] is False


def test_parquet_contract_runtime_readiness_status_public_gate(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Verify the public readiness gate reduces installed runtime capabilities."""
    from schema_sanitizer.adapters.parquet import status as parquet_runtime

    monkeypatch.setattr(parquet_runtime, "pyarrow_importable", lambda: True)

    status = parquet_runtime.parquet_contract_runtime_readiness_status()

    assert status["satisfied"] is True
    assert status["pyarrow_available"] is True
    assert status["native_footer_available"] is True
    assert status["native_stream_available"] is True
