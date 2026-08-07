"""Protect the portable C++ thread and cancellation boundary."""

from __future__ import annotations

from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
CPP = ROOT / "cpp"
COMPAT = CPP / "src/internal/runtime/thread_compat.hh"


def test_nonportable_standard_thread_types_are_isolated() -> None:
    """AppleClang-facing sources use the internal compatibility vocabulary."""
    forbidden = (
        "#include <stop_token>",
        "std::jthread",
        "std::move_only_function",
        "std::stop_callback",
        "std::stop_source",
        "std::stop_token",
    )

    for source in CPP.rglob("*"):
        if source == COMPAT or source.suffix not in {".cc", ".hh", ".inc"}:
            continue
        text = source.read_text(encoding="utf-8")
        for token in forbidden:
            assert token not in text, f"{source.relative_to(ROOT)} uses {token}"


def test_thread_compat_has_standard_and_portable_routes() -> None:
    """Modern libraries stay native while older libc++ receives owned fallbacks."""
    text = COMPAT.read_text(encoding="utf-8")

    assert "__cpp_lib_jthread" in text
    assert "__cpp_lib_move_only_function" in text
    assert "SCHEMA_SANITIZER_FORCE_PORTABLE_THREAD_COMPAT" in text
    for owner in (
        "class StopToken final",
        "class StopSource final",
        "class StopCallback final",
        "class JThread final",
        "class MoveOnlyFunction<Result(Args...)> final",
        "bool WaitWithStop(",
    ):
        assert owner in text
