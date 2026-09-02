"""Deterministic file-and-byte packetization for remote staging.

It groups files by count and known bytes, isolates oversized objects, and keeps
unknown-size inputs within the hard file cap.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Sequence

from ..core_impl.memory_budget import memory_budget
from ..sources.models import RemoteFile


@dataclass(frozen=True, slots=True)
class RemoteStagingPacketPolicy:
    """Maximum file count and known bytes for one remote staging packet."""

    max_files: int
    target_bytes: int


def remote_staging_packet_policy(
    memory_limit_bytes: int | None,
) -> RemoteStagingPacketPolicy:
    """Derive stable remote packet limits from the operation memory budget."""
    budget = memory_budget(memory_limit_bytes)
    concurrent_packets = max(1, budget.remote_chunk_prefetch)
    fair_share = budget.total_bytes // max(4, concurrent_packets * 2)
    return RemoteStagingPacketPolicy(
        max_files=budget.async_prefetch_files * 4,
        target_bytes=max(
            budget.io_chunk_bytes,
            min(64 * 1024 * 1024, fair_share),
        ),
    )


def remote_file_packet(
    files: Sequence[RemoteFile],
    start: int,
    *,
    max_files: int,
    target_bytes: int,
) -> list[RemoteFile]:
    """Return one ordered packet bounded by file count and known bytes."""
    if start < 0 or start >= len(files):
        return []
    file_limit = max(1, max_files)
    byte_limit = max(1, target_bytes)
    unknown_size_estimate = max(1, byte_limit // file_limit)
    selected: list[RemoteFile] = []
    known_bytes = 0
    for file in files[start : start + file_limit]:
        estimate = (
            file.size if isinstance(file.size, int) and file.size >= 0 else unknown_size_estimate
        )
        if selected and known_bytes + estimate > byte_limit:
            break
        selected.append(file)
        known_bytes += estimate
        if known_bytes >= byte_limit:
            break
    return selected


def remote_file_packet_estimated_bytes(
    files: Sequence[RemoteFile],
    start: int,
    *,
    max_files: int,
    target_bytes: int,
) -> int:
    """Return the same conservative byte estimate used to form one packet."""
    selected = remote_file_packet(
        files,
        start,
        max_files=max_files,
        target_bytes=target_bytes,
    )
    if not selected:
        return 0
    unknown_size_estimate = max(1, max(1, target_bytes) // max(1, max_files))
    return sum(
        file.size if isinstance(file.size, int) and file.size >= 0 else unknown_size_estimate
        for file in selected
    )


__all__ = [
    "RemoteStagingPacketPolicy",
    "remote_file_packet",
    "remote_file_packet_estimated_bytes",
    "remote_staging_packet_policy",
]
