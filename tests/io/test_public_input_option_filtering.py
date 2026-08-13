"""Tests explicit public input formats, extensions, and directory mode."""

from __future__ import annotations

import json
from pathlib import Path
from types import SimpleNamespace

import pytest
from _support.public_input_modes import GENERATED_COLUMNS as GENERATED
from _support.public_input_modes import data_rows as _data_rows
from conftest import require_native

import schema_sanitizer as ss


def test_file_conversion_core_filters_helper_and_writer_options_before_schema_options(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Verify internal file conversion ignores non-schema options defensively."""
    from schema_sanitizer.api_impl.file_conversion import converters as file_convert_core
    from schema_sanitizer.api_impl.results import Result

    source = tmp_path / "rows.jsonl"
    source.write_text('{"alpha":1}\n', encoding="utf-8")
    captured_options = []

    def fake_normalize(**kwargs):
        """Capture schema option input."""
        captured_options.append(kwargs)
        return None

    def fake_writer(_data, output_path, **_kwargs):
        """Write a marker and return a minimal result."""
        Path(output_path).write_text("ok", encoding="utf-8")
        return Result(SimpleNamespace(diagnostics=None), schema_registry_json="{}")

    monkeypatch.setattr(file_convert_core, "normalize_call_options_or_none", fake_normalize)
    process_clock = iter((5.0, 7.0))
    monkeypatch.setattr(file_convert_core, "process_time", lambda: next(process_clock))

    result = file_convert_core.convert_file_with_options(
        source,
        tmp_path / "out.parquet",
        input_format="jsonl",
        input_mode="single_file",
        options={
            "input_path": source,
            "output_path": tmp_path / "out.parquet",
            "input_format": "jsonl",
            "input_mode": "single_file",
            "field_name_policy": "lower_snake",
            "parquet_compression": "gzip",
            "parquet_gzip_level": 6,
        },
        writer=fake_writer,
        source_plan_writer=fake_writer,
        feature="to_parquet",
        schema_registry={},
        writer_options={"parquet_compression": "gzip", "parquet_gzip_level": 6},
    )

    assert len(captured_options) == 1
    resolved_memory_limit = captured_options[0].pop("memory_limit_bytes")
    assert isinstance(resolved_memory_limit, int) and resolved_memory_limit > 0
    assert captured_options == [
        {
            "field_name_policy": "lower_snake",
            "schema_contract": None,
            "schema_mode": "additive",
        }
    ]
    assert result.conversion_cpu_seconds == pytest.approx(2.0)
    assert result.file_io_seconds is not None
    assert result.file_io_seconds >= 0.0


def test_analytical_core_filters_helper_options_before_schema_options(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Verify internal analytical conversion ignores helper options defensively."""
    require_native()
    import schema_sanitizer.api_impl.analytical as analytical_conversion

    source = tmp_path / "rows.jsonl"
    source.write_text('{"alpha":1}\n', encoding="utf-8")
    captured_options = []
    real_normalize = analytical_conversion.normalize_call_options_or_none

    def tracking_normalize(**kwargs):
        """Capture schema option input and preserve behavior."""
        captured_options.append(kwargs)
        return real_normalize(**kwargs)

    monkeypatch.setattr(analytical_conversion, "normalize_call_options_or_none", tracking_normalize)

    result = analytical_conversion.convert_analytical_with_options(
        source,
        target="pyarrow",
        input_format="jsonl",
        input_mode="single_file",
        options={
            "input_path": source,
            "target": "pyarrow",
            "input_format": "jsonl",
            "input_mode": "single_file",
            "field_name_policy": "lower_snake",
        },
        schema_registry=None,
    )

    assert _data_rows(result) == [{"alpha": 1}]
    assert len(captured_options) == 1
    resolved_memory_limit = captured_options[0].pop("memory_limit_bytes")
    assert isinstance(resolved_memory_limit, int) and resolved_memory_limit > 0
    assert captured_options == [
        {
            "field_name_policy": "lower_snake",
            "schema_contract": None,
            "schema_mode": "additive",
        }
    ]


def test_file_converter_accepts_json_array(tmp_path: Path) -> None:
    """Verify JSON-array input works with a file output sink."""
    source = tmp_path / "rows.json"
    output = tmp_path / "rows.jsonl"
    source.write_text('[{"a":1},{"a":2}]', encoding="utf-8")

    ss.to_jsonl(source, output, input_format="json_array")
    rows = [json.loads(line) for line in output.read_text(encoding="utf-8").splitlines()]
    assert [{key: value for key, value in row.items() if key not in GENERATED} for row in rows] == [
        {"a": 1},
        {"a": 2},
    ]
