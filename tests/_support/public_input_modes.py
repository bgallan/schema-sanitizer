"""Result normalization shared by the split public input-mode tests."""

from __future__ import annotations

GENERATED_COLUMNS = {
    "schema_registry",
    "schema_drifts",
    "source_file",
    "ingestion_timestamp",
}


def data_rows(result: object) -> list[dict[str, object]]:
    """Return analytical rows without generated metadata columns."""
    clean_data = result.clean_data  # type: ignore[attr-defined]
    return [
        {key: value for key, value in row.items() if key not in GENERATED_COLUMNS}
        for row in clean_data.to_pylist()
    ]
