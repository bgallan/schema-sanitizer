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
        "pre-commit-hooks.txt",
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

    def unexpected_run(*_args: object, **_kwargs: object) -> None:
        """Fail if the idempotent path attempts to invoke an installer."""
        raise AssertionError("matching pip pin unexpectedly invoked an installer")

    monkeypatch.setattr(ensure_pinned_pip, "run_bounded", unexpected_run)

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
    calls: list[tuple[list[str], int, str, str]] = []

    def record_run(command: list[str], *, timeout_seconds: int, label: str) -> None:
        """Record the deterministic installer command without changing the environment."""
        requirement = Path(command[command.index("--requirement") + 1])
        calls.append((command, timeout_seconds, label, requirement.read_text(encoding="utf-8")))

    monkeypatch.setattr(ensure_pinned_pip, "run_bounded", record_run)

    assert ensure_pinned_pip.ensure_pinned_pip() is True
    assert len(calls) == 1
    command, timeout_seconds, label, requirement = calls[0]
    assert (timeout_seconds, label) == (300, "pinned-pip-bootstrap")
    assert command[:6] == [
        ensure_pinned_pip.sys.executable,
        "-m",
        "pip",
        "install",
        "--no-deps",
        "--only-binary=:all:",
    ]
    assert command[6:8] == ["--require-hashes", "--requirement"]
    assert requirement == (
        f"pip=={ensure_pinned_pip.PIP_VERSION} --hash=sha256:{ensure_pinned_pip.PIP_SHA256}\n"
    )


def test_failed_pip_postcondition_is_fatal(monkeypatch: pytest.MonkeyPatch) -> None:
    """A successful installer exit cannot hide a mismatched final pip version."""
    observed_versions = iter([None, "25.0"])
    monkeypatch.setattr(
        ensure_pinned_pip,
        "installed_pip_version",
        lambda: next(observed_versions),
    )
    monkeypatch.setattr(ensure_pinned_pip, "run_bounded", lambda *_args, **_kwargs: None)

    with pytest.raises(RuntimeError, match="pip bootstrap postcondition failed"):
        ensure_pinned_pip.ensure_pinned_pip()


def test_failed_pip_process_is_fatal(monkeypatch: pytest.MonkeyPatch) -> None:
    """A nonzero installer status fails before accepting an unverifiable environment."""
    monkeypatch.setattr(ensure_pinned_pip, "installed_pip_version", lambda: "25.0")

    def fail_process(*_args: object, **_kwargs: object) -> None:
        """Simulate the bounded runner rejecting a nonzero pip process."""
        raise RuntimeError("bounded command failed with status 7: pinned-pip-bootstrap")

    monkeypatch.setattr(ensure_pinned_pip, "run_bounded", fail_process)

    with pytest.raises(RuntimeError, match="failed with status 7"):
        ensure_pinned_pip.ensure_pinned_pip()
