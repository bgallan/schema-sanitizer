"""Reusable schema drift comparison helpers."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class SchemaDriftDiff:
    """Field-level schema drift diff."""

    added_paths: list[str]
    removed_paths: list[str]
    changed_paths: list[str]

    @property
    def has_changes(self) -> bool:
        """Return whether any path was added, removed, or changed."""
        return bool(self.added_paths or self.removed_paths or self.changed_paths)


def flatten_arrow_schema_paths(schema: Any) -> dict[str, str]:
    """Flatten a PyArrow schema to {field_path: type_string}."""
    import pyarrow as pa

    result: dict[str, str] = {}

    def visit_field(field: Any, prefix: str = "") -> None:
        """Append one field and nested child paths."""
        path = f"{prefix}.{field.name}" if prefix else field.name
        result[path] = str(field.type)

        data_type = field.type
        if pa.types.is_struct(data_type):
            for child in data_type:
                visit_field(child, path)
        elif pa.types.is_list(data_type) or pa.types.is_large_list(data_type):
            value_type = data_type.value_type
            list_path = f"{path}[]"
            result[list_path] = str(value_type)
            if pa.types.is_struct(value_type):
                for child in value_type:
                    visit_field(child, list_path)
        elif hasattr(pa.types, "is_fixed_size_list") and pa.types.is_fixed_size_list(data_type):
            value_type = data_type.value_type
            list_path = f"{path}[]"
            result[list_path] = str(value_type)
            if pa.types.is_struct(value_type):
                for child in value_type:
                    visit_field(child, list_path)

    for field in schema:
        visit_field(field)
    return result


def diff_flat_schema_paths(
    before_paths: dict[str, str],
    after_paths: dict[str, str],
) -> SchemaDriftDiff:
    """Return added, removed, and changed paths between flattened schemas."""
    before_keys = set(before_paths)
    after_keys = set(after_paths)
    return SchemaDriftDiff(
        added_paths=sorted(after_keys - before_keys),
        removed_paths=sorted(before_keys - after_keys),
        changed_paths=sorted(
            path for path in before_keys & after_keys if before_paths[path] != after_paths[path]
        ),
    )


def diff_arrow_schemas(before_schema: Any, after_schema: Any) -> SchemaDriftDiff:
    """Return added, removed, and changed paths between PyArrow schemas."""
    return diff_flat_schema_paths(
        flatten_arrow_schema_paths(before_schema),
        flatten_arrow_schema_paths(after_schema),
    )
