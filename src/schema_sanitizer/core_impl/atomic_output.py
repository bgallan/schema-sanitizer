"""Atomic local-file publication for native output writers.

It reserves a unique sibling file, preserves an existing target's permissions, replaces the
target only after success, and cleans abandoned staging files.
"""

from __future__ import annotations

import os
import secrets
import stat
from contextlib import contextmanager, suppress
from pathlib import Path
from typing import Iterator

_MAX_TEMP_NAME_ATTEMPTS = 128


def _create_sibling_temp(target: Path) -> Path:
    """Reserve a unique sibling file using normal output-file permissions."""
    existing_mode: int | None = None
    with suppress(OSError):
        existing_mode = stat.S_IMODE(target.stat().st_mode)

    for _ in range(_MAX_TEMP_NAME_ATTEMPTS):
        candidate = target.parent / f".{target.name}.{secrets.token_hex(8)}.tmp"
        try:
            from .process_resources import governed_os_descriptor

            with governed_os_descriptor(
                lambda: os.open(
                    candidate,
                    os.O_CREAT | os.O_EXCL | os.O_WRONLY,
                    0o666,
                ),
                teardown=True,
                label="atomic-output-staging",
            ) as descriptor:
                if existing_mode is not None:
                    fchmod = getattr(os, "fchmod", None)
                    if fchmod is not None:
                        fchmod(descriptor, existing_mode)
                    else:
                        os.chmod(candidate, existing_mode)
        except FileExistsError:
            continue
        except Exception:
            with suppress(OSError):
                candidate.unlink()
            raise
        return candidate
    raise FileExistsError(f"unable to reserve temporary output beside {target}")


@contextmanager
def atomic_local_output(path: str | os.PathLike[str]) -> Iterator[str]:
    """Yield a sibling staging path and atomically publish it on success."""
    target = Path(os.fspath(path))
    staged = _create_sibling_temp(target)
    try:
        yield os.fspath(staged)
        os.replace(staged, target)
    finally:
        with suppress(OSError):
            staged.unlink()
