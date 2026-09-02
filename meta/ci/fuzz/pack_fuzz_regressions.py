#!/usr/bin/env python3
"""Pack immutable content-addressed fuzz regressions deterministically.

The command validates one target corpus and writes its canonical archive without
timestamps or ordering drift.
"""

from __future__ import annotations

import argparse
import os
import stat
import tempfile
import zipfile
from pathlib import Path

try:
    from meta.ci.fuzz.corpus_io import (
        ARCHIVE_TIMESTAMP,
        TARGETS,
        archive_path,
        is_content_addressed_name,
        target_inputs,
    )
except ModuleNotFoundError:
    from corpus_io import (
        ARCHIVE_TIMESTAMP,
        TARGETS,
        archive_path,
        is_content_addressed_name,
        target_inputs,
    )


def pack_target(regression_root: Path, target: str, *, remove_loose: bool) -> int:
    """Write one target archive and optionally remove its packed loose inputs."""
    target_root = regression_root / target
    if not target_root.is_dir() or target_root.is_symlink():
        raise ValueError(f"missing regular fuzz target directory: {target_root}")
    cases = [
        case
        for case in target_inputs(
            regression_root,
            target,
            allow_identical_archive_duplicates=remove_loose,
        )
        if is_content_addressed_name(case.name)
    ]
    if not cases:
        raise ValueError(f"no content-addressed regressions found under {target_root}")

    destination = archive_path(regression_root, target)
    destination.parent.mkdir(parents=True, exist_ok=True)
    descriptor, temporary_name = tempfile.mkstemp(
        dir=destination.parent,
        prefix=f".{destination.name}.",
        suffix=".tmp",
    )
    os.close(descriptor)
    temporary = Path(temporary_name)
    try:
        with zipfile.ZipFile(
            temporary,
            "w",
            compression=zipfile.ZIP_DEFLATED,
            compresslevel=9,
            strict_timestamps=True,
        ) as archive:
            for case in cases:
                info = zipfile.ZipInfo(case.name, date_time=ARCHIVE_TIMESTAMP)
                info.compress_type = zipfile.ZIP_DEFLATED
                info.create_system = 3
                info.external_attr = 0o100644 << 16
                archive.writestr(
                    info,
                    case.data,
                    compress_type=zipfile.ZIP_DEFLATED,
                    compresslevel=9,
                )
        archive_mode = stat.S_IRUSR | stat.S_IWUSR | stat.S_IRGRP | stat.S_IROTH
        temporary.chmod(archive_mode)
        if destination.is_symlink():
            raise ValueError(f"refusing symlinked fuzz archive: {destination}")
        if not destination.is_file() or destination.read_bytes() != temporary.read_bytes():
            os.replace(temporary, destination)
        else:
            destination.chmod(archive_mode)
    finally:
        temporary.unlink(missing_ok=True)

    if remove_loose:
        # Archive replacement commits every byte before retryable loose cleanup.
        for case in cases:
            if not case.archived:
                case.origin.unlink(missing_ok=True)
    return len(cases)


def main() -> None:
    """Pack every parser target from the requested fuzz tree."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--fuzz-root", type=Path, default=Path("fuzz"))
    parser.add_argument("--remove-loose", action="store_true")
    args = parser.parse_args()
    regression_root = args.fuzz_root / "regressions"
    total = 0
    for target in TARGETS:
        count = pack_target(regression_root, target, remove_loose=args.remove_loose)
        total += count
        print(f"Packed {count} {target} regressions into {archive_path(regression_root, target)}")
    print(f"Packed {total} content-addressed regressions.")


if __name__ == "__main__":
    main()
