"""Parse exact owner locks and bind them to the reviewed artifact-digest allowlist.

Repository owner locks intentionally remain compact, one requirement per line.  This
module joins those exact pins with the deduplicated artifact lock and renders the
native pip requirements syntax needed for fail-closed ``--require-hashes`` installs.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable

_PIN_PATTERN = re.compile(
    r"^(?P<name>[A-Za-z0-9][A-Za-z0-9._-]*)==(?P<version>[^;\s]+)(?P<marker>\s*;.*)?$"
)
_HASH_PATTERN = re.compile(r"^sha256:[0-9a-f]{64}$")


@dataclass(frozen=True)
class LockedRequirement:
    """Describe one exact requirement while preserving its environment marker."""

    name: str
    version: str
    source: str

    @property
    def key(self) -> str:
        """Return the canonical package/version key used by the artifact lock."""
        return f"{self.name}=={self.version}"


def canonicalize_name(name: str) -> str:
    """Return the normalized spelling defined by Python package metadata."""
    return re.sub(r"[-_.]+", "-", name).lower()


def read_owner_lock(path: Path) -> tuple[LockedRequirement, ...]:
    """Read a sorted owner lock and reject non-exact or duplicate requirements."""
    requirements: list[LockedRequirement] = []
    seen: set[str] = set()
    for line_number, raw_line in enumerate(path.read_text(encoding="utf-8").splitlines(), 1):
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue
        match = _PIN_PATTERN.fullmatch(line)
        if match is None:
            raise ValueError(
                f"owner lock entry must be one exact pin: {path}:{line_number}: {line}"
            )
        name = canonicalize_name(match.group("name"))
        if name in seen:
            raise ValueError(f"duplicate owner lock package: {path}:{line_number}: {name}")
        seen.add(name)
        requirements.append(
            LockedRequirement(name=name, version=match.group("version"), source=line)
        )
    if not requirements:
        raise ValueError(f"owner lock cannot be empty: {path}")
    if [requirement.name for requirement in requirements] != sorted(seen):
        raise ValueError(f"owner lock packages must be sorted canonically: {path}")
    return tuple(requirements)


def read_artifact_lock(path: Path) -> dict[str, tuple[str, ...]]:
    """Read the canonical package/version to SHA-256 artifact-digest allowlist."""
    entries: dict[str, tuple[str, ...]] = {}
    previous_key = ""
    for line_number, raw_line in enumerate(path.read_text(encoding="utf-8").splitlines(), 1):
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue
        fields = line.split()
        key = fields[0]
        match = _PIN_PATTERN.fullmatch(key)
        if match is None or match.group("marker"):
            raise ValueError(f"invalid artifact-lock key: {path}:{line_number}: {key}")
        canonical_key = f"{canonicalize_name(match.group('name'))}=={match.group('version')}"
        hashes = tuple(fields[1:])
        if canonical_key != key or canonical_key <= previous_key:
            raise ValueError(f"artifact-lock keys must be canonical, unique, and sorted: {key}")
        if not hashes or hashes != tuple(sorted(set(hashes))):
            raise ValueError(f"artifact-lock hashes must be nonempty, unique, and sorted: {key}")
        if any(_HASH_PATTERN.fullmatch(digest) is None for digest in hashes):
            raise ValueError(f"invalid artifact-lock SHA-256 digest: {key}")
        entries[key] = hashes
        previous_key = key
    if not entries:
        raise ValueError(f"artifact lock cannot be empty: {path}")
    return entries


def render_hashed_requirements(
    requirements: Iterable[LockedRequirement], artifact_hashes: dict[str, tuple[str, ...]]
) -> str:
    """Render exact requirements with every reviewed artifact hash for native pip."""
    rendered: list[str] = []
    for requirement in requirements:
        try:
            hashes = artifact_hashes[requirement.key]
        except KeyError as error:
            raise ValueError(f"artifact lock has no hashes for {requirement.key}") from error
        rendered.append(f"{requirement.source} \\")
        for index, digest in enumerate(hashes):
            continuation = " \\" if index + 1 < len(hashes) else ""
            rendered.append(f"    --hash={digest}{continuation}")
    if not rendered:
        raise ValueError("hashed requirement selection cannot be empty")
    return "\n".join(rendered) + "\n"


def select_requirements(
    requirements: tuple[LockedRequirement, ...], names: Iterable[str]
) -> tuple[LockedRequirement, ...]:
    """Select requested canonical packages and reject unknown owner-lock names."""
    requested = {canonicalize_name(name) for name in names}
    available = {requirement.name: requirement for requirement in requirements}
    unknown = sorted(requested - available.keys())
    if unknown:
        raise ValueError(f"packages are absent from owner lock: {', '.join(unknown)}")
    return tuple(available[name] for name in sorted(requested))
