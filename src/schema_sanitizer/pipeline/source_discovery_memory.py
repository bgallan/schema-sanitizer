"""Bounded metadata helpers shared by source-discovery phases."""

from __future__ import annotations

from collections.abc import Iterable
from itertools import chain
from typing import cast
from urllib.parse import urlparse

from ..core_impl.uris import LocationKind, location_kind
from ..input_impl.directory_inputs import (
    DiscoveredDirectoryInput,
    current_directory_metadata_budget,
)
from ..input_impl.directory_metadata_budget import DirectoryMetadataBudget
from .types import PartitionRunPlan


def source_summary(
    discovered: DiscoveredDirectoryInput | None,
) -> tuple[int | None, int | None]:
    """Return file count and bytes without materializing an auxiliary size list."""
    if discovered is None:
        return None, None
    count = total = 0
    complete = True
    for size in chain(
        (file.size for file in discovered.local_files),
        (file.size for file in discovered.remote_files),
    ):
        count += 1
        if size is None:
            complete = False
        else:
            total += size
    return count, total if complete else None


def cached_source_summary(
    cache: dict[int, tuple[int | None, int | None]],
    discovered: DiscoveredDirectoryInput | None,
) -> tuple[int | None, int | None]:
    """Reuse one streaming summary for plans sharing the same discovered object."""
    if discovered is None:
        return None, None
    identity = id(discovered)
    summary = cache.get(identity)
    if summary is None:
        summary = source_summary(discovered)
        cache[identity] = summary
    return summary


def precharge_source_locations(
    plans: list[PartitionRunPlan],
    *,
    memory_limit_bytes: int | None,
) -> tuple[DirectoryMetadataBudget, dict[str, LocationKind], object | None]:
    """Pre-admit the retained URI/association graph and classify unique sources."""
    budget = current_directory_metadata_budget(memory_limit_bytes)
    locations: dict[str, LocationKind] = {}
    for source_uri in budget.charge_uris(plan.source_uri for plan in plans):
        if source_uri in locations:
            continue
        kind = location_kind(source_uri)
        if kind is None:
            scheme = urlparse(source_uri).scheme
            if scheme:
                raise ValueError(f"Unsupported source URI scheme: {scheme!r}")
            kind = "path"
        locations[source_uri] = kind
    budget.charge_associations(len(locations) * 12 + len(plans) * 2)
    return budget, locations, budget.retention_owner


def bounded_remaining_sources(
    locations: dict[str, LocationKind],
    checked_uris: set[str],
    budget: DirectoryMetadataBudget,
) -> tuple[tuple[str, LocationKind], ...]:
    """Retain only unresolved source references after pre-admitting their metadata."""
    values: Iterable[tuple[str, LocationKind]] = (
        item for item in locations.items() if item[0] not in checked_uris
    )
    return cast(
        tuple[tuple[str, LocationKind], ...],
        budget.charge_references(values, references_per_item=2),
    )
