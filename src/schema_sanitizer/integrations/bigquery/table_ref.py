"""Validated BigQuery table references."""

from __future__ import annotations

from dataclasses import dataclass

from .sql import quote_bq_identifier


@dataclass(frozen=True)
class BigQueryTableRef:
    """Fully qualified BigQuery table reference."""

    project: str
    dataset: str
    table: str

    @property
    def sql_identifier(self) -> str:
        """Return a quoted BigQuery table identifier."""
        return quote_bq_identifier([self.project, self.dataset, self.table])

    @property
    def information_schema_tables_identifier(self) -> str:
        """Return the quoted INFORMATION_SCHEMA.TABLES identifier."""
        return quote_bq_identifier([self.project, self.dataset, "INFORMATION_SCHEMA", "TABLES"])

    @property
    def information_schema_columns_identifier(self) -> str:
        """Return the quoted INFORMATION_SCHEMA.COLUMNS identifier."""
        return quote_bq_identifier([self.project, self.dataset, "INFORMATION_SCHEMA", "COLUMNS"])

    @property
    def display_name(self) -> str:
        """Return project.dataset.table for logs."""
        return f"{self.project}.{self.dataset}.{self.table}"


def parse_table_ref(raw: str, *, default_project: str | None = None) -> BigQueryTableRef:
    """Parse project.dataset.table or dataset.table into a table reference."""
    parts = [part.strip("` ") for part in raw.split(".")]
    if len(parts) == 3:
        project, dataset, table = parts
    elif len(parts) == 2 and default_project:
        project = default_project
        dataset, table = parts
    else:
        raise ValueError(
            "target table must be project.dataset.table, or dataset.table with a project"
        )
    return BigQueryTableRef(project=project, dataset=dataset, table=table)


def maybe_parse_table_ref(
    raw: str | BigQueryTableRef | None,
    *,
    default_project: str | None = None,
) -> BigQueryTableRef | None:
    """Parse an optional BigQuery table reference."""
    if raw is None or raw == "":
        return None
    if isinstance(raw, BigQueryTableRef):
        return raw
    return parse_table_ref(str(raw), default_project=default_project)
