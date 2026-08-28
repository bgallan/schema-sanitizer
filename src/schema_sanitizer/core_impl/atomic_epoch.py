"""Fixed-width runtime counters with an ABI-backed atomic fast path.

AtomicEpoch uses a native capsule when available and a locked saturating uint64 fallback while
preserving the same success semantics.
"""

from __future__ import annotations

from threading import Lock
from typing import Callable

_MAX_U64 = (1 << 64) - 1


class AtomicEpoch:
    """Saturating uint64 counter with non-allocating cached ABI callables."""

    __slots__ = ("_capsule", "_value", "_lock", "_inc", "_dec", "_read", "_reset", "_write")

    def __init__(self) -> None:
        """Initialize the atomic epoch and its owned runtime state."""
        self._capsule = None
        self._value = 0
        self._lock = Lock()
        self._inc: Callable[[object], object] | None = None
        self._dec: Callable[[object], object] | None = None
        self._read: Callable[[object], int] | None = None
        self._reset: Callable[[object], object] | None = None
        self._write: Callable[[object, bytearray, int], object] | None = None
        try:
            from .native_runtime import native_core

            create = getattr(native_core, "atomic_epoch_create", None)
            inc = getattr(native_core, "atomic_epoch_increment", None)
            dec = getattr(native_core, "atomic_epoch_decrement", None)
            read = getattr(native_core, "atomic_epoch_value", None)
            reset = getattr(native_core, "atomic_epoch_reset", None)
            write = getattr(native_core, "atomic_epoch_write_le", None)
            if (
                callable(create)
                and callable(inc)
                and callable(dec)
                and callable(read)
                and callable(reset)
                and callable(write)
            ):
                self._capsule = create()
                self._inc = inc
                self._dec = dec
                self._read = read
                self._reset = reset
                self._write = write
        except BaseException:
            self._capsule = None

    def increment(self) -> bool:
        """Increment the epoch and return whether saturation was avoided."""
        capsule = self._capsule
        inc = self._inc
        if capsule is not None and inc is not None:
            try:
                return bool(inc(capsule))
            except BaseException:
                return False
        with self._lock:
            if self._value >= _MAX_U64:
                return False
            self._value += 1
            return True

    def decrement(self) -> bool:
        """Decrement the epoch and return whether underflow was avoided."""
        capsule = self._capsule
        dec = self._dec
        if capsule is not None and dec is not None:
            try:
                return bool(dec(capsule))
            except BaseException:
                return False
        with self._lock:
            if self._value <= 0:
                return False
            self._value -= 1
            return True

    def value(self) -> int:
        """Return the current atomic epoch value."""
        capsule = self._capsule
        read = self._read
        if capsule is not None and read is not None:
            return int(read(capsule))
        with self._lock:
            return self._value

    def write_into(self, target: bytearray, offset: int) -> None:
        """Write one little-endian uint64 directly into an existing buffer.

        With the ABI fast path this creates no Python integer for the counter
        value; correctness barriers can therefore observe epochs under OOM.
        """
        capsule = self._capsule
        write = self._write
        if capsule is not None and write is not None:
            write(capsule, target, offset)
            return
        with self._lock:
            value = self._value
        for shift in range(0, 64, 8):
            target[offset] = (value >> shift) & 0xFF
            offset += 1

    @property
    def native_capsule(self) -> object | None:
        """Return the native capsule."""
        return self._capsule

    def reset_after_fork(self) -> None:
        """Reset process-local state inherited across a fork."""
        self._lock = Lock()
        capsule = self._capsule
        reset = self._reset
        if capsule is not None and reset is not None:
            try:
                reset(capsule)
                return
            except BaseException:
                self._capsule = None
                self._inc = self._dec = self._read = self._reset = self._write = None
        self._value = 0


__all__ = ["AtomicEpoch"]
