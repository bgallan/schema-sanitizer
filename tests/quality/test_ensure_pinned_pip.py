"""Tests for idempotent, wheel-only pip bootstrapping in repository automation.

The contracts prove that matching runner images perform no installation and that a
version mismatch is repaired with the exact binary-only command before succeeding.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from meta.ci.quality import ensure_pinned_pip

ROOT = Path(__file__).resolve().parents[2]


def test_bootstrap_pin_matches_every_runtime_lock() -> None:
    """The bootstrap constant cannot drift from any environment's exact pip pin."""
    lock_names = (
        "build-tools.txt",
        "platform-tests.txt",
        "quality.txt",
        "release-verification.txt",
    )
    observed = {
        line.split("==", 1)[1]
        for name in lock_names
        for line in (ROOT / "meta" / "ci" / "requirements" / name)
        .read_text(encoding="utf-8")
        .splitlines()
        if line.startswith("pip==")
    }

    assert observed == {ensure_pinned_pip.PIP_VERSION}


def test_matching_pip_pin_is_a_no_op(monkeypatch: pytest.MonkeyPatch) -> None:
    """An already-correct runner must not launch a redundant pip subprocess."""
    monkeypatch.setattr(
        ensure_pinned_pip,
        "installed_pip_version",
        lambda: ensure_pinned_pip.PIP_VERSION,
    )

    def unexpected_spawn(*_args: object, **_kwargs: object) -> int:
        """Fail if the idempotent path attempts to invoke an installer."""
        raise AssertionError("matching pip pin unexpectedly invoked an installer")

    monkeypatch.setattr(ensure_pinned_pip.os, "spawnv", unexpected_spawn)

    assert ensure_pinned_pip.ensure_pinned_pip() is False


def test_mismatched_pip_is_repaired_from_an_exact_wheel(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A mismatched runner installs the exact wheel and verifies its postcondition."""
    observed_versions = iter(["25.0", ensure_pinned_pip.PIP_VERSION])
    monkeypatch.setattr(
        ensure_pinned_pip,
        "installed_pip_version",
        lambda: next(observed_versions),
    )
    calls: list[tuple[int, str, list[str]]] = []

    def record_spawn(mode: int, executable: str, command: list[str]) -> int:
        """Record the deterministic installer command without changing the environment."""
        calls.append((mode, executable, command))
        return 0

    monkeypatch.setattr(ensure_pinned_pip.os, "spawnv", record_spawn)

    assert ensure_pinned_pip.ensure_pinned_pip() is True
    assert calls == [
        (
            ensure_pinned_pip.os.P_WAIT,
            ensure_pinned_pip.sys.executable,
            [
                ensure_pinned_pip.sys.executable,
                "-m",
                "pip",
                "install",
                "--no-deps",
                "--only-binary=:all:",
                f"pip=={ensure_pinned_pip.PIP_VERSION}",
            ],
        )
    ]


def test_failed_pip_postcondition_is_fatal(monkeypatch: pytest.MonkeyPatch) -> None:
    """A successful installer exit cannot hide a mismatched final pip version."""
    observed_versions = iter([None, "25.0"])
    monkeypatch.setattr(
        ensure_pinned_pip,
        "installed_pip_version",
        lambda: next(observed_versions),
    )
    monkeypatch.setattr(ensure_pinned_pip.os, "spawnv", lambda *_args, **_kwargs: 0)

    with pytest.raises(RuntimeError, match="pip bootstrap postcondition failed"):
        ensure_pinned_pip.ensure_pinned_pip()


def test_failed_pip_process_is_fatal(monkeypatch: pytest.MonkeyPatch) -> None:
    """A nonzero installer status fails before accepting an unverifiable environment."""
    monkeypatch.setattr(ensure_pinned_pip, "installed_pip_version", lambda: "25.0")
    monkeypatch.setattr(ensure_pinned_pip.os, "spawnv", lambda *_args, **_kwargs: 7)

    with pytest.raises(RuntimeError, match="pip bootstrap failed with status 7"):
        ensure_pinned_pip.ensure_pinned_pip()
