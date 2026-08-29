#!/usr/bin/env python3
"""Validate the CI layout and byte integrity of native fuzz inputs.

It checks root and target layouts, content-addressed names, digest manifests, and
deterministic archive contents before fuzzing.
"""

from __future__ import annotations

import argparse
import hashlib
import re
from pathlib import Path
from typing import NamedTuple

try:
    from meta.ci.fuzz.corpus_io import ARCHIVE_SUFFIX, TARGETS, target_inputs
except ModuleNotFoundError:
    from corpus_io import ARCHIVE_SUFFIX, TARGETS, target_inputs

ROLES = ("corpus", "regressions")
LEGACY_LIBFUZZER_NAME = re.compile(r"[0-9a-f]{40}")
EXPECTED_INPUT_COUNTS = {
    ("corpus", "json"): 7,
    ("corpus", "csv"): 4,
    ("corpus", "xml"): 5,
    ("corpus", "parquet"): 9,
    ("regressions", "json"): 67,
    ("regressions", "csv"): 119,
    ("regressions", "xml"): 84,
    ("regressions", "parquet"): 5,
}
EXPECTED_TREE_SHA256 = "6f0edba72c6823df471cd0ab3392d5f095449938d1ace52f38b21cbc3eb40831"
DESCRIPTIVE_REGRESSION_SHA256 = {
    "csv/unterminated.csv": "17a373d8a99cbc0238d1b1b87088915e04040adec99d3605d930099ee4f42df0",
    "json/truncated.json": "ee3bb016ee1b1e395152b5db18af8d7e785aa19a2ab541c9bd9d13dfa8a2a0f0",
    "parquet/truncated.parquet": "9b11cf33087ace2044cce38e4d53afbed5672d0ee26d8eed68203d7d75004a3f",
    "xml/mismatched.xml": "a493b1c7f5c6595b8bd3cdc1fdf90e926a7201283dba19abbc0d30cfd31684f9",
}


class FuzzCorpusError(ValueError):
    """Raised when the checked fuzz tree violates its repository contract."""


class FuzzInventory(NamedTuple):
    """Summary of the validated fuzz inputs."""

    corpus_inputs: int
    regression_inputs: int
    legacy_libfuzzer_inputs: int
    unique_campaign_inputs: int


def _sha256(data: bytes) -> str:
    """Return the manifest digest for a descriptive regression fixture."""
    return hashlib.sha256(data).hexdigest()


def _tree_sha256(entries: list[tuple[str, int, bytes]]) -> str:
    """Hash the canonical relative paths, byte lengths, and content digests."""
    digest = hashlib.sha256(b"schema-sanitizer-fuzz-tree-v1\0")
    for relative, size, content_digest in sorted(entries):
        encoded = relative.encode("utf-8")
        digest.update(len(encoded).to_bytes(4, "big"))
        digest.update(encoded)
        digest.update(size.to_bytes(8, "big"))
        digest.update(content_digest)
    return digest.hexdigest()


def _validate_root_entries(fuzz_root: Path, errors: list[str]) -> None:
    """Reject unexpected files or directories at the fuzz corpus root."""
    expected = {"README.md", *ROLES}
    actual = {path.name for path in fuzz_root.iterdir()}
    for name in sorted(expected - actual):
        errors.append(f"missing fuzz root entry: {name}")
    for name in sorted(actual - expected):
        errors.append(f"unexpected fuzz root entry: {name}")


def _validate_role_entries(fuzz_root: Path, role: str, errors: list[str]) -> Path | None:
    """Validate one role directory without traversing rejected symlinks."""
    role_root = fuzz_root / role
    if not role_root.is_dir() or role_root.is_symlink():
        errors.append(f"missing regular directory: {role}")
        return None

    expected = set(TARGETS)
    allowed = set(expected)
    if role == "regressions":
        expected.add("README.md")
        allowed.add("README.md")
        archives = {f"{target}{ARCHIVE_SUFFIX}" for target in TARGETS}
        present_archives = {path.name for path in role_root.iterdir()} & archives
        if present_archives and present_archives != archives:
            missing = sorted(archives - present_archives)
            errors.append(f"regression archives must be all-or-none; missing: {missing}")
        allowed.update(archives)
    actual = {path.name for path in role_root.iterdir()}
    for name in sorted(expected - actual):
        errors.append(f"missing {role} entry: {name}")
    for name in sorted(actual - allowed):
        errors.append(f"unexpected {role} entry: {name}")
    return role_root


def check_fuzz_tree(fuzz_root: Path) -> FuzzInventory:
    """Validate one repository fuzz tree and return its input inventory."""
    if not fuzz_root.is_dir() or fuzz_root.is_symlink():
        raise FuzzCorpusError(f"missing regular fuzz directory: {fuzz_root}")

    errors: list[str] = []
    _validate_root_entries(fuzz_root, errors)
    role_roots = {role: _validate_role_entries(fuzz_root, role, errors) for role in ROLES}
    counts = {role: 0 for role in ROLES}
    legacy_libfuzzer = 0
    campaign_inputs = {target: set() for target in TARGETS}
    tree_entries: list[tuple[str, int, bytes]] = []

    for role, role_root in role_roots.items():
        if role_root is None:
            continue
        for target in TARGETS:
            target_root = role_root / target
            if not target_root.is_dir() or target_root.is_symlink():
                errors.append(f"missing regular directory: {role}/{target}")
                continue
            target_count = 0
            cases = target_inputs(role_root, target, errors=errors)
            archive = role_root / f"{target}{ARCHIVE_SUFFIX}"
            archive_present = not archive.is_symlink() and archive.is_file()
            if archive_present:
                loose_hashes = [
                    case.name
                    for case in cases
                    if not case.archived and LEGACY_LIBFUZZER_NAME.fullmatch(case.name)
                ]
                if loose_hashes:
                    errors.append(
                        f"content-addressed regressions must be packed under {role}/{target}: "
                        f"{loose_hashes[:5]}"
                    )
            for case in cases:
                relative = Path(role, target, case.name)
                data = case.data
                counts[role] += 1
                target_count += 1
                content_digest = hashlib.sha256(data).digest()
                campaign_inputs[target].add(content_digest)
                tree_entries.append((relative.as_posix(), len(data), content_digest))
                if LEGACY_LIBFUZZER_NAME.fullmatch(case.name) is not None:
                    legacy_libfuzzer += 1
                elif role == "regressions":
                    manifest_name = f"{target}/{case.name}"
                    expected = DESCRIPTIVE_REGRESSION_SHA256.get(manifest_name)
                    actual = _sha256(data)
                    if expected is not None and actual != expected:
                        errors.append(
                            f"descriptive fixture hash mismatch: {relative} "
                            f"(actual SHA-256: {actual})"
                        )
            if target_count == 0:
                errors.append(f"no fuzz inputs found under {role}/{target}")
            expected_count = EXPECTED_INPUT_COUNTS[(role, target)]
            if target_count != expected_count:
                errors.append(
                    f"unexpected input count under {role}/{target}: "
                    f"expected {expected_count}, found {target_count}"
                )

    regression_root = role_roots["regressions"]
    if regression_root is not None:
        for manifest_name in sorted(DESCRIPTIVE_REGRESSION_SHA256):
            path = regression_root / manifest_name
            if path.is_symlink() or not path.is_file():
                errors.append(
                    f"missing descriptive regression fixture: regressions/{manifest_name}"
                )

    tree_sha256 = _tree_sha256(tree_entries)
    if tree_sha256 != EXPECTED_TREE_SHA256:
        errors.append(
            f"fuzz tree fingerprint mismatch: expected {EXPECTED_TREE_SHA256}, actual {tree_sha256}"
        )

    if errors:
        raise FuzzCorpusError("\n".join(errors))
    return FuzzInventory(
        corpus_inputs=counts["corpus"],
        regression_inputs=counts["regressions"],
        legacy_libfuzzer_inputs=legacy_libfuzzer,
        unique_campaign_inputs=sum(map(len, campaign_inputs.values())),
    )


def parse_args() -> argparse.Namespace:
    """Parse the fuzz tree to validate."""
    parser = argparse.ArgumentParser()
    parser.add_argument("--fuzz-root", type=Path, default=Path("fuzz"))
    return parser.parse_args()


def main() -> None:
    """Validate the configured fuzz tree and print a compact inventory."""
    args = parse_args()
    try:
        inventory = check_fuzz_tree(args.fuzz_root)
    except FuzzCorpusError as error:
        raise SystemExit(f"fuzz corpus integrity failed:\n{error}") from error
    print(
        "Fuzz corpus integrity passed: "
        f"corpus={inventory.corpus_inputs}, "
        f"regressions={inventory.regression_inputs}, "
        f"legacy_libfuzzer={inventory.legacy_libfuzzer_inputs}, "
        f"unique_campaign_inputs={inventory.unique_campaign_inputs}."
    )


if __name__ == "__main__":
    main()
