"""Vectorized Polars normalization for wide question columns."""

from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass
from importlib import import_module
from typing import Any

_SCHEMA_REGISTRY_COLUMN = "schema_registry"
_SCHEMA_DRIFTS_COLUMN = "schema_drifts"


@dataclass(frozen=True, slots=True)
class QuestionColumn:
    """One parsed ``<integer><separator><question text>`` source column."""

    name: str
    question_id: int
    question_text: str


@dataclass(frozen=True, slots=True)
class QuestionNormalizationResult:
    """A normalized Polars frame and the source columns it consumed."""

    frame: Any
    question_columns: tuple[QuestionColumn, ...]


def parse_question_column(name: str, *, separator: str = "/") -> QuestionColumn | None:
    """Parse a question header, splitting only on the first separator."""
    if not isinstance(name, str):
        raise TypeError("column names must be strings")
    if not separator:
        raise ValueError("question separator must not be empty")
    raw_id, found, question_text = name.partition(separator)
    if not found or not raw_id.strip() or not question_text:
        return None
    try:
        question_id = int(raw_id.strip())
    except ValueError:
        return None
    return QuestionColumn(
        name=name,
        question_id=question_id,
        question_text=question_text,
    )


def detect_question_columns(
    column_names: Iterable[str],
    *,
    separator: str = "/",
) -> tuple[QuestionColumn, ...]:
    """Return question columns in deterministic input-column order."""
    detected: list[QuestionColumn] = []
    for name in column_names:
        parsed = parse_question_column(name, separator=separator)
        if parsed is not None:
            detected.append(parsed)
    return tuple(detected)


def normalize_question_columns(
    frame: Any,
    final_schema: Any,
    *,
    separator: str = "/",
    output_column: str = "questions",
    omit_null_answers: bool = False,
) -> QuestionNormalizationResult:
    """Replace wide question columns with one vectorized list-of-struct field.

    The returned frame contains every final non-metadata field exactly once.
    Registry columns are intentionally removed because
    ``finalize_analytical_output`` regenerates them from the normalized schema.
    """
    pl = import_module("polars")
    pa = import_module("pyarrow")
    if type(frame).__module__.split(".", 1)[0] != "polars":
        raise TypeError("frame must be a polars.DataFrame")
    if not isinstance(final_schema, pa.Schema):
        raise TypeError("final_schema must be a pyarrow.Schema")
    if output_column not in final_schema.names:
        raise ValueError(f"final schema has no question output column {output_column!r}")
    _validate_question_output_type(pa, final_schema.field(output_column).type)

    question_columns = detect_question_columns(frame.columns, separator=separator)
    source_question_names = {column.name for column in question_columns}
    final_data_names = [
        field.name
        for field in final_schema
        if field.name not in {_SCHEMA_REGISTRY_COLUMN, _SCHEMA_DRIFTS_COLUMN}
    ]
    passthrough_names = [name for name in final_data_names if name != output_column]
    missing = [
        name
        for name in passthrough_names
        if name not in frame.columns
        or (frame.height > 0 and frame.get_column(name).null_count() == frame.height)
    ]
    if missing:
        raise ValueError(f"wide CSV result is missing final scalar fields: {missing!r}")

    unexpected = [
        name
        for name in frame.columns
        if name not in passthrough_names
        and name not in source_question_names
        and name not in {_SCHEMA_REGISTRY_COLUMN, _SCHEMA_DRIFTS_COLUMN}
    ]
    if unexpected:
        raise ValueError(
            "wide CSV result contains non-question columns outside the final schema: "
            f"{unexpected!r}"
        )

    question_expr = _questions_expression(
        pl,
        question_columns,
        output_column=output_column,
        omit_null_answers=omit_null_answers,
    )
    normalized = frame.with_columns(question_expr).select(final_data_names)
    return QuestionNormalizationResult(normalized, question_columns)


def normalize_question_columns_inferred(
    frame: Any,
    *,
    separator: str = "/",
    output_column: str = "questions",
    omit_null_answers: bool = True,
) -> QuestionNormalizationResult:
    """Normalize a sanitized wide frame without requiring a target schema.

    This local-validation variant retains every non-question data/provenance
    column. Intermediate registry metadata is removed because it describes the
    wide ingress shape, not the normalized analytical result.
    """
    pl = import_module("polars")
    if type(frame).__module__.split(".", 1)[0] != "polars":
        raise TypeError("frame must be a polars.DataFrame")

    question_columns = detect_question_columns(frame.columns, separator=separator)
    question_names = {column.name for column in question_columns}
    if output_column in frame.columns and output_column not in question_names:
        raise ValueError(f"input already contains output column {output_column!r}")
    passthrough = [
        name
        for name in frame.columns
        if name not in question_names
        and name not in {_SCHEMA_REGISTRY_COLUMN, _SCHEMA_DRIFTS_COLUMN}
    ]
    question_expr = _questions_expression(
        pl,
        question_columns,
        output_column=output_column,
        omit_null_answers=omit_null_answers,
    )
    normalized = frame.with_columns(question_expr).select([*passthrough, output_column])
    return QuestionNormalizationResult(normalized, question_columns)


def _questions_expression(
    pl: Any,
    columns: tuple[QuestionColumn, ...],
    *,
    output_column: str,
    omit_null_answers: bool,
) -> Any:
    """Build one vectorized Polars expression for all question columns."""
    struct_type = pl.Struct(
        [
            pl.Field("question_id", pl.Int64),
            pl.Field("question_text", pl.String),
            pl.Field("answer", pl.String),
        ]
    )
    if not columns:
        return pl.lit([], dtype=pl.List(struct_type)).alias(output_column)

    items: list[Any] = []
    for column in columns:
        struct_expr = pl.struct(
            [
                pl.lit(column.question_id, dtype=pl.Int64).alias("question_id"),
                pl.lit(column.question_text, dtype=pl.String).alias("question_text"),
                pl.col(column.name).cast(pl.String, strict=False).alias("answer"),
            ]
        )
        if omit_null_answers:
            struct_expr = pl.when(pl.col(column.name).is_not_null()).then(struct_expr)
        items.append(struct_expr)
    questions = pl.concat_list(items)
    if omit_null_answers:
        questions = questions.list.drop_nulls()
    return questions.alias(output_column)


def _validate_question_output_type(pa: Any, data_type: Any) -> None:
    """Require the documented list-of-question-struct target shape."""
    if not (pa.types.is_list(data_type) or pa.types.is_large_list(data_type)):
        raise ValueError("question output field must be list<struct<...>>")
    struct_type = data_type.value_type
    if not pa.types.is_struct(struct_type):
        raise ValueError("question output field must be list<struct<...>>")
    names = [field.name for field in struct_type]
    if names != ["question_id", "question_text", "answer"]:
        raise ValueError(
            "question struct fields must be question_id, question_text, answer in that order"
        )
    if not pa.types.is_int64(struct_type.field("question_id").type):
        raise ValueError("question_id must use int64")
    if not pa.types.is_string(struct_type.field("question_text").type):
        raise ValueError("question_text must use string")
    if not pa.types.is_string(struct_type.field("answer").type):
        raise ValueError("answer must use nullable string")


__all__ = [
    "QuestionColumn",
    "QuestionNormalizationResult",
    "detect_question_columns",
    "normalize_question_columns",
    "normalize_question_columns_inferred",
    "parse_question_column",
]
