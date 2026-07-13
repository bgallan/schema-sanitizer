"""BigQuery Standard SQL quoting and canonical type names."""

from __future__ import annotations

_BQ_TYPE_SYNONYMS = {
    "INTEGER": "INT64",
    "INT": "INT64",
    "BOOLEAN": "BOOL",
    "BOOL": "BOOL",
    "FLOAT": "FLOAT64",
    "DOUBLE": "FLOAT64",
}


def _validate_identifier_component(value: str) -> None:
    """Reject identifier components that cannot be quoted safely."""
    if not value or "`" in value:
        raise ValueError(f"Invalid BigQuery identifier component: {value!r}")


def quote_bq_identifier_component(value: str) -> str:
    """Quote one BigQuery identifier component."""
    _validate_identifier_component(value)
    return f"`{value}`"


def quote_bq_identifier(parts: list[str] | tuple[str, ...]) -> str:
    """Quote a BigQuery identifier path."""
    for part in parts:
        _validate_identifier_component(part)
    return f"`{'.'.join(parts)}`"


def quote_bq_string(value: str) -> str:
    """Quote a BigQuery Standard SQL string literal."""
    return "'" + value.replace("'", "''") + "'"


def normalize_bq_type(data_type: str) -> str:
    """Normalize BigQuery scalar type synonyms returned by clients."""
    normalized = data_type.strip().upper()
    return _BQ_TYPE_SYNONYMS.get(normalized, normalized)


def normalize_external_format(value: str) -> str:
    """Normalize a BigQuery external-table format name."""
    return value.strip().upper()
