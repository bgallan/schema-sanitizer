"""Local, infrastructure-free validation path for example 08."""

from __future__ import annotations

from pathlib import Path

import schema_sanitizer as ss

try:
    from examples.example_08.question_normalization import (
        QuestionNormalizationResult,
        normalize_question_columns_inferred,
    )
except ModuleNotFoundError:  # pragma: no cover - direct script execution
    from question_normalization import (
        QuestionNormalizationResult,
        normalize_question_columns_inferred,
    )


def load_local_csv_directory_to_polars(
    source_directory: str | Path,
    *,
    question_separator: str = "/",
    questions_column: str = "questions",
    omit_null_answers: bool = True,
    csv_delimiter: str = ",",
    csv_escape_char: str | None = "\\",
    multi_threading: bool = True,
    memory_limit_bytes: int | None = None,
) -> QuestionNormalizationResult:
    """Sanitize and normalize every CSV in one local directory into one frame."""
    converted = ss.to_polars(
        Path(source_directory),
        input_format="csv",
        input_mode="directory",
        schema_mode="additive",
        column_order="schema_contract_first",
        field_name_policy="preserve",
        csv_has_header=True,
        csv_delimiter=csv_delimiter,
        csv_escape_char=csv_escape_char,
        csv_header_mode="union",
        on_error="stop",
        multi_threading=multi_threading,
        memory_limit_bytes=memory_limit_bytes,
    )
    return normalize_question_columns_inferred(
        converted.clean_data,
        separator=question_separator,
        output_column=questions_column,
        omit_null_answers=omit_null_answers,
    )


__all__ = ["load_local_csv_directory_to_polars"]
