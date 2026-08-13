"""Contracts for bounded native build parallelism."""

from __future__ import annotations

import re
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]


def test_msvc_translation_units_compile_in_four_bounded_processes() -> None:
    """Release and diagnostic targets parallelize MSVC without changing LTO."""
    project = (ROOT / "CMakeLists.txt").read_text(encoding="utf-8")
    options = (ROOT / "cmake/SchemaSanitizerTargetOptions.cmake").read_text(encoding="utf-8")
    helper = options.split("function(schema_sanitizer_enable_msvc_parallel_compile target)", 1)[
        1
    ].split("endfunction()", 1)[0]

    assert re.search(
        r'set\(SCHEMA_SANITIZER_MSVC_COMPILE_PROCESSES\s+"4"\s+CACHE STRING',
        project,
    )
    assert 'MATCHES "^[1-9][0-9]*$"' in project
    assert "if(NOT MSVC)" in helper
    assert 'PRIVATE "/MP${SCHEMA_SANITIZER_MSVC_COMPILE_PROCESSES}"' in helper

    main_targets = project.split("foreach(_schema_sanitizer_target sanitize_core", 1)[1].split(
        "endforeach()", 1
    )[0]
    assert "schema_sanitizer_enable_msvc_parallel_compile(${_schema_sanitizer_target})" in (
        main_targets
    )
    sanitizer_target = project.split(
        "add_executable(\n    schema_sanitizer_sanitized_ordered_executor", 1
    )[1].split('if(NOT (MSVC AND SCHEMA_SANITIZER_SANITIZER STREQUAL "asan"))', 1)[0]
    assert (
        "schema_sanitizer_enable_msvc_parallel_compile(\n"
        "    schema_sanitizer_sanitized_ordered_executor)"
    ) in sanitizer_target
    fuzzer_factory = project.split("function(schema_sanitizer_add_fuzzer target source)", 1)[
        1
    ].split("endfunction()", 1)[0]
    assert "schema_sanitizer_enable_msvc_parallel_compile(${target})" in fuzzer_factory
    assert project.count("schema_sanitizer_enable_msvc_parallel_compile(") == 3
    assert "set(CMAKE_INTERPROCEDURAL_OPTIMIZATION_RELEASE TRUE)" in project
    assert "set(CMAKE_INTERPROCEDURAL_OPTIMIZATION_RELWITHDEBINFO TRUE)" in project
