"""Bounded replay-spool policy for one-shot Python row iterables."""

from __future__ import annotations

import shutil
import tempfile
from tempfile import SpooledTemporaryFile


def ensure_replay_spool_capacity(
    directory: str | None,
    *,
    payload_bytes: int,
    next_size: int,
    memory_bytes: int,
    minimum_free_bytes: int,
) -> None:
    """Reject disk-backed replay growth before exhausting temporary storage."""
    if next_size <= memory_bytes or payload_bytes <= 0:
        return
    target = directory or tempfile.gettempdir()
    try:
        free_bytes = shutil.disk_usage(target).free
    except OSError as exc:
        raise OSError(f"Unable to inspect replay spool filesystem {target!r}") from exc
    required = payload_bytes + minimum_free_bytes
    if free_bytes < required:
        raise OSError(
            "Replay spool filesystem has insufficient free space: "
            f"{free_bytes} bytes available, {required} bytes required"
        )


def close_replay_spool(spool: SpooledTemporaryFile[bytes], spool_bytes: int) -> None:
    """Best-effort overwrite replay bytes, then close the spool."""
    if spool_bytes > 0:
        try:
            spool.seek(0)
            zero_chunk = b"\x00" * min(1024 * 1024, spool_bytes)
            remaining = spool_bytes
            while remaining > 0:
                size = min(len(zero_chunk), remaining)
                written = spool.write(zero_chunk[:size])
                if written != size:
                    break
                remaining -= written
            spool.flush()
        except OSError:
            # Closing still removes the temporary file. Secure overwrite is
            # best-effort because filesystems may be copy-on-write.
            pass
    spool.close()
