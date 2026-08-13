#!/usr/bin/env python3
"""Validate the CI layout and byte integrity of native fuzz inputs."""

from __future__ import annotations

import argparse
import hashlib
import re
from pathlib import Path
from typing import NamedTuple

TARGETS = ("json", "csv", "xml", "parquet")
ROLES = ("corpus", "regressions")
SHA1_NAME = re.compile(r"[0-9a-f]{40}")
HEX40_NAME = re.compile(r"[0-9A-Fa-f]{40}")
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
EXPECTED_TREE_SHA256 = "7c6167755bf670e838b2ef95491a3c54df4fd07064520999e0c5e72efeae5f36"
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
    content_addressed_inputs: int
    unique_campaign_inputs: int


def _sha1(data: bytes) -> str:
    """Return the libFuzzer-compatible content identifier for one input."""
    return hashlib.sha1(data, usedforsecurity=False).hexdigest()


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
    expected = {"README.md", *ROLES}
    actual = {path.name for path in fuzz_root.iterdir()}
    for name in sorted(expected - actual):
        errors.append(f"missing fuzz root entry: {name}")
    for name in sorted(actual - expected):
        errors.append(f"unexpected fuzz root entry: {name}")


def _validate_role_entries(fuzz_root: Path, role: str, errors: list[str]) -> Path:
    role_root = fuzz_root / role
    if not role_root.is_dir() or role_root.is_symlink():
        errors.append(f"missing regular directory: {role}")
        return role_root

    expected = set(TARGETS)
    if role == "regressions":
        expected.add("README.md")
    actual = {path.name for path in role_root.iterdir()}
    for name in sorted(expected - actual):
        errors.append(f"missing {role} entry: {name}")
    for name in sorted(actual - expected):
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
    content_addressed = 0
    campaign_inputs = {target: set() for target in TARGETS}
    tree_entries: list[tuple[str, int, bytes]] = []

    for role, role_root in role_roots.items():
        for target in TARGETS:
            target_root = role_root / target
            if not target_root.is_dir() or target_root.is_symlink():
                errors.append(f"missing regular directory: {role}/{target}")
                continue
            target_count = 0
            for path in sorted(target_root.iterdir()):
                relative = path.relative_to(fuzz_root)
                if path.is_symlink() or not path.is_file():
                    errors.append(f"fuzz target directories must stay flat: {relative}")
                    continue
                if path.name.startswith("."):
                    errors.append(f"hidden fuzz input is not allowed: {relative}")
                    continue

                data = path.read_bytes()
                counts[role] += 1
                target_count += 1
                content_digest = hashlib.sha256(data).digest()
                campaign_inputs[target].add(content_digest)
                tree_entries.append((relative.as_posix(), len(data), content_digest))
                if HEX40_NAME.fullmatch(path.name) is not None:
                    content_addressed += 1
                    actual = _sha1(data)
                    if SHA1_NAME.fullmatch(path.name) is None or actual != path.name:
                        errors.append(f"content hash mismatch: {relative} (actual SHA-1: {actual})")
                elif role == "regressions":
                    manifest_name = f"{target}/{path.name}"
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

    for manifest_name in sorted(DESCRIPTIVE_REGRESSION_SHA256):
        path = role_roots["regressions"] / manifest_name
        if not path.is_file():
            errors.append(f"missing descriptive regression fixture: regressions/{manifest_name}")

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
        content_addressed_inputs=content_addressed,
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
        f"content_addressed={inventory.content_addressed_inputs}, "
        f"unique_campaign_inputs={inventory.unique_campaign_inputs}."
    )


if __name__ == "__main__":
    main()
