"""Regression coverage for registry metadata-stream memory-limit propagation."""

from __future__ import annotations

from io import BytesIO
from pathlib import Path
from typing import Any

import pytest
from conftest import require_native


@pytest.mark.parametrize("source_kind", ["path", "stream", "text"])
def test_registry_metadata_wrapper_preserves_explicit_memory_limit(
    tmp_path: Path,
    source_kind: str,
) -> None:
    """Every source-selected registry wrapper must keep the caller's budget."""
    require_native()
    from schema_sanitizer.api_impl.execution_context import ExecutionContext
    from schema_sanitizer.core_impl.native_symbols import PARQUET_STREAM_WRITE
    from schema_sanitizer.options_impl.call_options import normalize_call_options

    source_path = tmp_path / "one-row.jsonl"
    source_path.write_text('{"a":1}\n', encoding="utf-8")
    payloads: dict[str, Any] = {
        "path": source_path,
        "stream": BytesIO(b'{"a":1}\n'),
        "text": '{"a":1}\n',
    }
    options = normalize_call_options(
        memory_limit_bytes=8192,
        multi_threading=False,
    ).raw
    raw = ExecutionContext()._raw.to_registry_sink_from_source(
        "stream",
        "jsonl",
        source_kind,
        payloads[source_kind],
        options,
        registry_json="{}",
        field_name_policy="lower_alpha",
        schema_mode="additive",
        first_row_columns={},
        all_row_columns={},
        row_span_columns={},
        timestamp_columns=("ingestion_timestamp",),
    )
    output = tmp_path / f"{source_kind}.parquet"
    try:
        with pytest.raises(RuntimeError) as caught:
            PARQUET_STREAM_WRITE(
                raw,
                str(output),
                "uncompressed",
                -1,
                64 << 20,
            )
    finally:
        raw.close()
        output.unlink(missing_ok=True)

    message = str(caught.value)
    assert "generated metadata batch exceeds byte safety limit" in message
    assert "configured_memory_limit_bytes=8192" in message
    assert "limit_bytes=1024" in message


def test_public_parquet_conversion_keeps_metadata_budget_and_atomicity(
    tmp_path: Path,
) -> None:
    """The public path preserves the low budget and removes staged output."""
    require_native()
    import schema_sanitizer as ss

    source = tmp_path / "one-row.jsonl"
    destination = tmp_path / "output.parquet"
    source.write_text('{"a":1}\n', encoding="utf-8")

    with pytest.raises(ss.SchemaSanitizerOutOfMemoryError) as caught:
        ss.to_parquet(
            source,
            destination,
            input_format="jsonl",
            memory_limit_bytes=8192,
            multi_threading=False,
        )

    message = str(caught.value)
    assert "generated metadata batch exceeds byte safety limit" in message
    assert "configured_memory_limit_bytes=8192" in message
    assert "limit_bytes=1024" in message
    assert not destination.exists()
