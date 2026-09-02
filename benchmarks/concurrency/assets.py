"""Load, verify, stage, and deterministically pack concurrency benchmark assets.

The module validates catalog metadata and integrity hashes before staging deterministic
probe archives for isolated runs.
"""

from __future__ import annotations

import argparse
import copy
import json
import os
import re
import stat
import tempfile
import zipfile
from collections.abc import Iterable, Mapping, Sequence
from pathlib import Path, PurePosixPath
from typing import Any

PROJECT_ROOT = Path(__file__).resolve().parents[2]
CATALOG_PATH = PROJECT_ROOT / "benchmarks/evidence/concurrency/catalog.json"
PROBE_ARCHIVE = PROJECT_ROOT / "benchmarks/probes/concurrency.zip"
_ID = re.compile(r"[a-z0-9]+(?:-[a-z0-9]+)*")
_FIXED_TIMESTAMP = (1980, 1, 1, 0, 0, 0)
_MAX_PROBE_BYTES = 2 * 1024 * 1024
_MAX_ARCHIVE_BYTES = 32 * 1024 * 1024


def _require(condition: bool, message: str) -> None:
    """Return required catalog metadata or reject the missing key."""
    if not condition:
        raise ValueError(message)


def _safe_probe_name(name: str, *, domain: str | None = None) -> bool:
    """Validate and return a path-safe benchmark probe name."""
    path = PurePosixPath(name)
    return (
        len(path.parts) == 2
        and not path.is_absolute()
        and all(part not in {"", ".", ".."} for part in path.parts)
        and path.suffix == ".cc"
        and (domain is None or path.parts[0] == domain)
    )


def load_catalog(path: Path = CATALOG_PATH) -> dict[str, Any]:
    """Return the validated, human-readable concurrency evidence catalog."""
    catalog = json.loads(path.read_text(encoding="utf-8"))
    _require(isinstance(catalog, dict), "concurrency catalog must be an object")
    _require(
        set(catalog) == {"schema_version", "description", "domains", "records"},
        "unexpected concurrency catalog fields",
    )
    _require(catalog["schema_version"] == 1, "unsupported concurrency catalog schema")
    domains = catalog["domains"]
    _require(
        isinstance(domains, list)
        and all(isinstance(domain, str) for domain in domains)
        and domains == sorted(set(domains)),
        "catalog domains must be a sorted unique list",
    )
    records = catalog["records"]
    _require(isinstance(records, list), "catalog records must be a list")
    _require(
        all(isinstance(record, dict) for record in records),
        "catalog records must be objects",
    )
    _require(
        records == sorted(records, key=lambda record: (record["domain"], record["id"])),
        "catalog records must be sorted by domain and id",
    )

    ids: set[str] = set()
    probes: set[str] = set()
    for record in records:
        _require(
            set(record) == {"id", "domain", "evidence", "probes"},
            "unexpected concurrency record fields",
        )
        record_id = record["id"]
        domain = record["domain"]
        _require(
            isinstance(record_id, str) and _ID.fullmatch(record_id) is not None,
            f"invalid concurrency record id: {record_id!r}",
        )
        _require(record_id not in ids, f"duplicate concurrency record id: {record_id}")
        ids.add(record_id)
        _require(domain in domains, f"unknown concurrency domain: {domain!r}")
        _require(
            record["evidence"] is None or isinstance(record["evidence"], dict),
            f"evidence for {record_id} must be an object or null",
        )
        _require(isinstance(record["probes"], list), f"probes for {record_id} must be a list")
        _require(
            record["probes"] == sorted(set(record["probes"])),
            f"probes for {record_id} must be sorted and unique",
        )
        for name in record["probes"]:
            _require(
                isinstance(name, str) and _safe_probe_name(name, domain=domain),
                f"unsafe probe name for {record_id}: {name!r}",
            )
            _require(name not in probes, f"duplicate concurrency probe: {name}")
            probes.add(name)
    return catalog


def _record(record_id: str, catalog: Mapping[str, Any]) -> Mapping[str, Any]:
    """Record one result in the current analysis."""
    for record in catalog["records"]:
        if record["id"] == record_id:
            return record
    raise KeyError(f"unknown concurrency evidence id: {record_id}")


def load_evidence(record_id: str) -> dict[str, Any]:
    """Return an isolated copy of one retained evidence document by stable ID."""
    evidence = _record(record_id, load_catalog())["evidence"]
    if evidence is None:
        raise KeyError(f"concurrency record has no retained evidence: {record_id}")
    return copy.deepcopy(evidence)


def _probe_bytes() -> dict[str, bytes]:
    """Read and validate the selected benchmark probe payload."""
    catalog = load_catalog()
    expected = sorted(probe for record in catalog["records"] for probe in record["probes"])
    probes: dict[str, bytes] = {}
    total = 0
    with zipfile.ZipFile(PROBE_ARCHIVE) as archive:
        members = archive.infolist()
        _require(
            [member.orig_filename for member in members] == expected,
            "probe archive inventory differs from the catalog",
        )
        for member in members:
            name = member.orig_filename
            mode = member.external_attr >> 16
            _require(_safe_probe_name(name), f"unsafe archived probe name: {name!r}")
            _require(
                not member.is_dir() and not (member.flag_bits & 0x1),
                f"probe must be an unencrypted regular file: {name}",
            )
            _require(member.date_time == _FIXED_TIMESTAMP, f"unstable timestamp: {name}")
            _require(
                stat.S_ISREG(mode) and stat.S_IMODE(mode) == 0o644,
                f"unsafe archived probe mode: {name}",
            )
            _require(
                member.compress_type == zipfile.ZIP_DEFLATED,
                f"unexpected probe compression: {name}",
            )
            _require(member.file_size <= _MAX_PROBE_BYTES, f"oversized probe: {name}")
            total += member.file_size
            _require(total <= _MAX_ARCHIVE_BYTES, "oversized concurrency probe archive")
            payload = archive.read(member)
            payload.decode("utf-8")
            probes[name] = payload
    return probes


def load_probe(name: str) -> str:
    """Return one verified standalone probe source by catalog-relative name."""
    if not _safe_probe_name(name):
        raise ValueError(f"unsafe concurrency probe name: {name!r}")
    try:
        return _probe_bytes()[name].decode("utf-8")
    except KeyError as error:
        raise KeyError(f"unknown concurrency probe: {name}") from error


def stage_probes(destination: Path, record_ids: Iterable[str] = ()) -> tuple[Path, ...]:
    """Materialize selected probe sources for compilation without unpacking by hand."""
    catalog = load_catalog()
    selected = set(record_ids)
    if selected:
        records = [_record(record_id, catalog) for record_id in sorted(selected)]
    else:
        records = catalog["records"]
    names = sorted({name for record in records for name in record["probes"]})
    payloads = _probe_bytes()
    destination = destination.resolve()
    staged: list[Path] = []
    for name in names:
        target = destination.joinpath(*PurePosixPath(name).parts)
        target.parent.mkdir(parents=True, exist_ok=True)
        _require(
            target.parent.resolve().is_relative_to(destination),
            f"probe staging path escapes destination: {name}",
        )
        _require(not target.is_symlink(), f"refusing to stage through a symlink: {target}")
        payload = payloads[name]
        if target.exists() and target.read_bytes() != payload:
            raise FileExistsError(f"refusing to overwrite changed probe: {target}")
        target.write_bytes(payload)
        staged.append(target)
    return tuple(staged)


def _write_probe_archive(probes: Mapping[str, bytes]) -> None:
    """Write a deterministic archive for the selected probe payload."""
    total = 0
    for name, payload in probes.items():
        _require(_safe_probe_name(name), f"unsafe concurrency probe name: {name!r}")
        _require(len(payload) <= _MAX_PROBE_BYTES, f"oversized probe: {name}")
        total += len(payload)
        _require(total <= _MAX_ARCHIVE_BYTES, "oversized concurrency probe archive")
        try:
            payload.decode("utf-8")
        except UnicodeDecodeError as error:
            raise ValueError(f"probe is not UTF-8: {name}") from error
    PROBE_ARCHIVE.parent.mkdir(parents=True, exist_ok=True)
    descriptor, temporary_name = tempfile.mkstemp(
        prefix=f".{PROBE_ARCHIVE.name}.", suffix=".tmp", dir=PROBE_ARCHIVE.parent
    )
    os.close(descriptor)
    temporary = Path(temporary_name)
    try:
        with zipfile.ZipFile(
            temporary, "w", compression=zipfile.ZIP_DEFLATED, compresslevel=9
        ) as archive:
            for name in sorted(probes):
                info = zipfile.ZipInfo(name, _FIXED_TIMESTAMP)
                info.create_system = 3
                info.external_attr = (stat.S_IFREG | 0o644) << 16
                info.compress_type = zipfile.ZIP_DEFLATED
                archive.writestr(info, probes[name], compresslevel=9)
        os.replace(temporary, PROBE_ARCHIVE)
    finally:
        temporary.unlink(missing_ok=True)


def pack_staged_probes(source: Path) -> None:
    """Replace the archive from a complete staged tree after validating its inventory."""
    catalog = load_catalog()
    expected = sorted(probe for record in catalog["records"] for probe in record["probes"])
    source = source.resolve()
    source_paths = list(source.rglob("*.cc"))
    _require(
        all(
            not path.is_symlink() and path.resolve().is_relative_to(source) for path in source_paths
        ),
        "staged probes must be regular files inside the source tree",
    )
    actual = sorted(path.relative_to(source).as_posix() for path in source_paths)
    if actual != expected:
        raise ValueError("staged probe inventory does not match the catalog")
    _write_probe_archive({name: (source / name).read_bytes() for name in expected})
    _probe_bytes()


def _parser() -> argparse.ArgumentParser:
    """Build the command-line argument parser."""
    parser = argparse.ArgumentParser(description=__doc__)
    subparsers = parser.add_subparsers(dest="command", required=True)
    subparsers.add_parser("verify", help="validate the catalog and probe archive")
    stage = subparsers.add_parser("stage", help="materialize probes for compilation")
    stage.add_argument("destination", type=Path)
    stage.add_argument("record_ids", nargs="*")
    pack = subparsers.add_parser("pack", help="repack a complete staged probe tree")
    pack.add_argument("source", type=Path)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    """Run the asset verification, staging, or deterministic packing command."""
    arguments = _parser().parse_args(argv)
    if arguments.command == "verify":
        catalog = load_catalog()
        probes = _probe_bytes()
        print(f"records={len(catalog['records'])} probes={len(probes)}")
    elif arguments.command == "stage":
        staged = stage_probes(arguments.destination, arguments.record_ids)
        print(f"staged={len(staged)} destination={arguments.destination}")
    elif arguments.command == "pack":
        pack_staged_probes(arguments.source)
        print(f"packed={PROBE_ARCHIVE}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
