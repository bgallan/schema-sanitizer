"""Exercise the fail-closed Windows CPython package installer used by wheel builds.

The tests cover byte identity, Windows path aliases, archive entry types, PE machine
identity, failure rollback, and atomic publication without launching a Windows binary.
"""

from __future__ import annotations

import hashlib
import stat
import struct
import zipfile
from pathlib import Path

import pytest
from cibuildwheel.util import file as cibuildwheel_file

from meta.ci.native import install_windows_cpython as installer


def _amd64_pe(machine: int = 0x8664) -> bytes:
    """Return the smallest fixture carrying a readable PE machine header."""
    payload = bytearray(256)
    payload[:2] = b"MZ"
    struct.pack_into("<I", payload, 0x3C, 0x80)
    payload[0x80:0x84] = b"PE\0\0"
    struct.pack_into("<H", payload, 0x84, machine)
    return bytes(payload)


def _archive(
    path: Path,
    members: list[tuple[str | zipfile.ZipInfo, bytes]] | None = None,
) -> str:
    """Write a deterministic package fixture and return its SHA-256 digest."""
    package_members = members or [
        ("python.nuspec", b"<package />"),
        ("tools/python.exe", _amd64_pe()),
        ("tools/python311.dll", b"runtime"),
    ]
    with zipfile.ZipFile(path, "w", compression=zipfile.ZIP_DEFLATED) as archive:
        for member, payload in package_members:
            archive.writestr(member, payload)
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _accept_runtime(executable: Path) -> None:
    """Stand in for Windows execution while checking the candidate location."""
    assert executable.name == "python.exe"
    assert executable.parent.name == "tools"
    assert installer._sha256(executable) == hashlib.sha256(_amd64_pe()).hexdigest()


def test_verified_archive_is_published_only_after_candidate_certification(tmp_path: Path) -> None:
    """A complete certified tree atomically replaces an owned partial installation."""
    archive = tmp_path / "python.nupkg"
    expected = _archive(archive)
    package_parent = tmp_path / "cibuildwheel" / "nuget-cpython"
    package_root = package_parent / "python.3.11.9"
    package_root.mkdir(parents=True)
    (package_root / "stale.txt").write_text("partial", encoding="utf-8")

    executable = installer.install_archive(
        archive,
        package_root,
        expected,
        candidate_validator=_accept_runtime,
    )

    assert executable == package_root / "tools" / "python.exe"
    assert executable.read_bytes() == _amd64_pe()
    assert not (package_root / "stale.txt").exists()
    assert not list(package_parent.glob(".python.3.11.9.extract-*"))


def test_cli_emits_only_the_certified_executable_path(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Command substitution receives one POSIX-style path without a CRLF suffix."""
    archive = tmp_path / "python.nupkg"
    expected = _archive(archive)
    cache_root = tmp_path / "cibuildwheel"
    monkeypatch.setattr(cibuildwheel_file, "CIBW_CACHE_PATH", cache_root)

    def fixture_archive(_workspace: Path) -> Path:
        """Route the CLI's owned cache lookup to the immutable fixture package."""
        return archive

    def preserve_fixture(_path: Path, _url: str, _expected: str) -> None:
        """Keep the already verified fixture in place without producing output."""

    monkeypatch.setattr(installer, "_workspace_archive", fixture_archive)
    monkeypatch.setattr(installer, "ensure_verified_archive", preserve_fixture)

    assert installer.main([expected]) == 0

    executable = cache_root / "nuget-cpython" / "python.3.11.9" / "tools" / "python.exe"
    captured = capsys.readouterr()
    assert captured.out == executable.as_posix()
    assert captured.err == ""


def test_failed_candidate_certification_preserves_existing_installation(tmp_path: Path) -> None:
    """A runtime failure cleans its temporary tree before touching the prior package."""
    archive = tmp_path / "python.nupkg"
    expected = _archive(archive)
    package_parent = tmp_path / "cibuildwheel" / "nuget-cpython"
    package_root = package_parent / "python.3.11.9"
    package_root.mkdir(parents=True)
    sentinel = package_root / "preserved.txt"
    sentinel.write_text("keep", encoding="utf-8")

    def reject_runtime(_executable: Path) -> None:
        """Inject a certification failure before the atomic publication point."""
        raise installer.WindowsCpythonInstallError("injected runtime mismatch")

    with pytest.raises(installer.WindowsCpythonInstallError, match="injected runtime mismatch"):
        installer.install_archive(
            archive,
            package_root,
            expected,
            candidate_validator=reject_runtime,
        )

    assert sentinel.read_text(encoding="utf-8") == "keep"
    assert not list(package_parent.glob(".python.3.11.9.extract-*"))


@pytest.mark.parametrize(
    "unsafe_name",
    [
        "../escape",
        "/absolute",
        "C:/drive",
        "tools\\python.exe",
        "tools/./python.exe",
        "tools/../python.exe",
        "tools/python.exe:stream",
        "tools/NUL.txt",
        "tools/trailing. ",
        "tools//python.exe",
    ],
)
def test_extractor_rejects_traversal_and_windows_aliases(
    tmp_path: Path,
    unsafe_name: str,
) -> None:
    """Raw member names cannot escape or alias another Windows destination."""
    archive = tmp_path / "unsafe.nupkg"
    _archive(archive, [(unsafe_name, b"payload")])
    destination = tmp_path / "extract"
    destination.mkdir()

    with pytest.raises(installer.WindowsCpythonInstallError, match="unsafe archive member path"):
        installer.extract_archive(archive, destination)

    assert not any(destination.iterdir())
    assert not (tmp_path / "escape").exists()


def test_extractor_rejects_exact_and_casefolded_duplicates(tmp_path: Path) -> None:
    """Duplicate logical names fail before extraction on case-insensitive filesystems."""
    exact = tmp_path / "exact.nupkg"
    with pytest.warns(UserWarning, match="Duplicate name"):
        _archive(exact, [("tools/runtime.dll", b"one"), ("tools/runtime.dll", b"two")])
    folded = tmp_path / "folded.nupkg"
    _archive(folded, [("Tools/runtime.dll", b"one"), ("tools/RUNTIME.dll", b"two")])

    for archive in (exact, folded):
        destination = tmp_path / archive.stem
        destination.mkdir()
        with pytest.raises(
            installer.WindowsCpythonInstallError,
            match="duplicate or case-colliding",
        ):
            installer.extract_archive(archive, destination)
        assert not any(destination.iterdir())


def test_extractor_rejects_casefolded_implicit_directory_aliases(tmp_path: Path) -> None:
    """Distinct files cannot spell the same implicit Windows directory differently."""
    archive = tmp_path / "prefixes.nupkg"
    _archive(archive, [("Tools/one.dll", b"one"), ("tools/two.dll", b"two")])
    destination = tmp_path / "extract"
    destination.mkdir()

    with pytest.raises(installer.WindowsCpythonInstallError, match="path prefixes"):
        installer.extract_archive(archive, destination)

    assert not any(destination.iterdir())


@pytest.mark.parametrize("mode", [stat.S_IFLNK | 0o777, stat.S_IFIFO | 0o600])
def test_extractor_rejects_links_and_nonregular_members(tmp_path: Path, mode: int) -> None:
    """ZIP metadata cannot materialize a link, device, socket, or pipe."""
    member = zipfile.ZipInfo("tools/python.exe")
    member.create_system = 3
    member.external_attr = mode << 16
    archive = tmp_path / "typed.nupkg"
    _archive(archive, [(member, b"target")])
    destination = tmp_path / "extract"
    destination.mkdir()

    with pytest.raises(installer.WindowsCpythonInstallError, match="linked, or non-regular"):
        installer.extract_archive(archive, destination)

    assert not any(destination.iterdir())


def test_extractor_rejects_a_file_used_as_a_member_parent(tmp_path: Path) -> None:
    """A file/directory collision is rejected before either member is written."""
    archive = tmp_path / "collision.nupkg"
    _archive(archive, [("tools", b"file"), ("tools/python.exe", b"nested")])
    destination = tmp_path / "extract"
    destination.mkdir()

    with pytest.raises(installer.WindowsCpythonInstallError, match="also a member parent"):
        installer.extract_archive(archive, destination)

    assert not any(destination.iterdir())


def test_installation_rejects_digest_drift_before_extraction(tmp_path: Path) -> None:
    """A package whose bytes differ from its immutable pin cannot create a tree."""
    archive = tmp_path / "python.nupkg"
    _archive(archive)
    package_parent = tmp_path / "cibuildwheel" / "nuget-cpython"
    package_parent.mkdir(parents=True)
    package_root = package_parent / "python.3.11.9"

    with pytest.raises(installer.WindowsCpythonInstallError, match="unverified CPython package"):
        installer.install_archive(
            archive,
            package_root,
            "0" * 64,
            candidate_validator=_accept_runtime,
        )

    assert not package_root.exists()
    assert not list(package_parent.glob(".python.3.11.9.extract-*"))


def test_pe_certification_rejects_a_non_amd64_machine(tmp_path: Path) -> None:
    """An otherwise structured PE image cannot substitute an x86 executable."""
    executable = tmp_path / "python.exe"
    executable.write_bytes(_amd64_pe(machine=0x014C))

    with pytest.raises(installer.WindowsCpythonInstallError, match="not AMD64"):
        installer.validate_amd64_pe(executable)


def test_explicit_directory_after_child_is_portable(tmp_path: Path) -> None:
    """An explicit directory entry may follow a child that created it implicitly."""
    directory = zipfile.ZipInfo("tools/")
    directory.external_attr = (stat.S_IFDIR | 0o755) << 16
    archive = tmp_path / "directories.nupkg"
    _archive(archive, [("tools/runtime.dll", b"runtime"), (directory, b"")])
    destination = tmp_path / "extract"
    destination.mkdir()

    installer.extract_archive(archive, destination)

    assert (destination / "tools" / "runtime.dll").read_bytes() == b"runtime"
