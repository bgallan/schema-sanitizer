"""Protect consolidation and hot-path ownership from maintenance layout 89."""

from __future__ import annotations

from pathlib import Path

import pytest

from schema_sanitizer.input_impl.selection import (
    FORMAT_SUFFIXES,
    input_format_extensions,
)
from schema_sanitizer.pipeline.types import PartitionRunPlan

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src/schema_sanitizer"
CPP = ROOT / "cpp/src"


def test_input_selection_has_one_bounded_owner_without_facades() -> None:
    """Closely coupled selector and path rules must have one neutral owner."""
    owner = SRC / "input_impl/selection.py"
    retired = (
        SRC / "input_impl/selection",
        SRC / "input_impl/path_inputs.py",
        SRC / "input_impl/errors.py",
    )
    assert owner.is_file()
    assert len(owner.read_text(encoding="utf-8").splitlines()) <= 500
    assert all(not path.exists() for path in retired)


def test_retired_single_call_input_facades_stay_absent() -> None:
    """Private stream and JSON-array wrappers must not regain ownership."""
    assert not (SRC / "api_impl/file_conversion/stream_writers.py").exists()
    assert not (SRC / "api_impl/input/json_array.py").exists()


def test_input_extension_catalog_has_one_owner() -> None:
    """Hive planning and discovery derive extensions from selector metadata."""
    assert input_format_extensions("parquet") == ("parquet", "pq")
    assert input_format_extensions("jsonl") == ("jsonl",)
    assert FORMAT_SUFFIXES["ndjson"] == (".ndjson",)

    hive = (SRC / "pipeline/hive.py").read_text(encoding="utf-8")
    discovery = (SRC / "pipeline/source_discovery.py").read_text(encoding="utf-8")
    assert "FORMAT_EXTENSIONS" not in hive + discovery
    assert "input_format_extensions" in hive
    assert "input_format_extensions" in discovery


def test_retired_python_owners_have_no_importers() -> None:
    """No source module may preserve old implementation paths as aliases."""
    retired_names = (
        "input_impl.path_inputs",
        "input_impl.errors",
        "input_impl.selection.",
        "api_impl.input.json_array",
        "api_impl.file_conversion.stream_writers",
    )
    source = "\n".join(path.read_text(encoding="utf-8") for path in SRC.rglob("*.py"))
    assert not [name for name in retired_names if name in source]


def test_hive_output_validation_is_linear_and_reports_duplicates() -> None:
    """Duplicate-output validation must use one pass rather than nested scans."""
    from schema_sanitizer.pipeline.hive import _validate_unique_outputs

    plans = [PartitionRunPlan(None, f"input-{index}", f"output-{index % 2}") for index in range(4)]
    with pytest.raises(ValueError, match="output-0.*output-1"):
        _validate_unique_outputs(plans)

    source = (SRC / "pipeline/hive.py").read_text(encoding="utf-8")
    function = source[
        source.index("def _validate_unique_outputs") : source.index(
            "\ndef ", source.index("def _validate_unique_outputs") + 1
        )
    ]
    assert "sum(" not in function
    assert "seen:" in function


def test_python_row_shape_validation_has_one_native_owner() -> None:
    """Python orchestration validates the container; C++ validates each row."""
    selection = (SRC / "input_impl/selection.py").read_text(encoding="utf-8")
    native = (CPP / "api/python_abi3/json/_core_abi3_python_rows.cc").read_text(encoding="utf-8")
    assert "all(isinstance(row, dict)" not in selection
    assert "PyDict_Check(item)" in native
    assert "row %zd " in native
    assert "is not a dict" in native


def test_prepared_options_resolution_has_one_abi3_owner() -> None:
    """Default preparation and capsule unwrapping must not be reimplemented per method."""
    c_api = (CPP / "api/c/schema_sanitizer_c.cc").read_text(encoding="utf-8")
    capsules = (CPP / "api/python_abi3/context/_core_abi3_capsules.cc").read_text(encoding="utf-8")
    abi3 = "\n".join(
        path.read_text(encoding="utf-8") for path in (CPP / "api/python_abi3").rglob("*.cc")
    )
    assert "static const auto prepared" in c_api
    assert "bool resolve_prepared_options(" in capsules
    assert "resolve_chunk_provider_prepared_options" not in abi3
    assert abi3.count("default_prepared_options()") == 1


def test_cpp23_enum_serialization_uses_to_underlying() -> None:
    """Parquet enum serialization should use the C++23 enum conversion utility."""
    source = (
        CPP / "internal/parquet/stream_writer/stream_writer_schema_elements.cc.inc"
    ).read_text(encoding="utf-8")
    assert source.count("std::to_underlying") >= 4
    assert "static_cast<std::int32_t>(node.physical_type)" not in source


def test_product_files_remain_bounded_including_textual_cpp_fragments() -> None:
    """No product owner may hide a monolith behind a .inc include."""
    candidates = [*SRC.rglob("*.py"), *CPP.rglob("*.cc"), *CPP.rglob("*.hh"), *CPP.rglob("*.inc")]
    oversized = {
        str(path.relative_to(ROOT)): len(path.read_text(encoding="utf-8").splitlines())
        for path in candidates
        if len(path.read_text(encoding="utf-8").splitlines()) > 500
    }
    assert oversized == {}
