"""Remote staging packet boundaries and memory-derived policy."""

from __future__ import annotations

from schema_sanitizer.input_impl.directory_inputs import RemoteFile
from schema_sanitizer.remote_impl.packetization import (
    remote_file_packet,
    remote_staging_packet_policy,
)


def _file(index: int, size: int | None) -> RemoteFile:
    """Build one ordered remote-file descriptor."""
    return RemoteFile(f"s3://bucket/{index}.jsonl", f"{index}.jsonl", size)


def test_remote_packets_are_bounded_by_files_and_known_bytes() -> None:
    """Known sizes stop a packet before either configured boundary is crossed."""
    files = [_file(0, 40), _file(1, 40), _file(2, 40), _file(3, 1)]
    assert [
        file.name
        for file in remote_file_packet(
            files,
            0,
            max_files=3,
            target_bytes=100,
        )
    ] == ["0.jsonl", "1.jsonl"]
    assert [
        file.name
        for file in remote_file_packet(
            files,
            2,
            max_files=3,
            target_bytes=100,
        )
    ] == ["2.jsonl", "3.jsonl"]


def test_oversized_remote_file_is_isolated() -> None:
    """One file larger than the target remains processable as a singleton packet."""
    files = [_file(0, 200), _file(1, 10)]
    assert [
        file.name
        for file in remote_file_packet(
            files,
            0,
            max_files=8,
            target_bytes=100,
        )
    ] == ["0.jsonl"]


def test_unknown_remote_sizes_remain_file_count_bounded() -> None:
    """Unknown metadata uses a fair-share estimate instead of forcing singletons."""
    files = [_file(index, None) for index in range(6)]
    assert len(remote_file_packet(files, 0, max_files=4, target_bytes=100)) == 4


def test_remote_packet_policy_scales_without_exceeding_hard_caps() -> None:
    """The single memory knob derives both remote packet controls."""
    small = remote_staging_packet_policy(32 * 1024 * 1024)
    large = remote_staging_packet_policy(512 * 1024 * 1024)
    assert 1 <= small.max_files <= large.max_files <= 4096
    assert 1 <= small.target_bytes <= large.target_bytes <= 64 * 1024 * 1024
