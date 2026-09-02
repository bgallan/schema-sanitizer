"""Provide fixed-width runtime counters and retry-visible native commits.

AtomicEpoch uses a native capsule when available and a locked saturating uint64
fallback for ordinary counter operations. Its paired counter/byte-marker commit
requires the native primitive and fails closed when that indivisible operation
is unavailable.
"""

from __future__ import annotations

from threading import Lock
from typing import Callable

_MAX_U64 = (1 << 64) - 1


class AtomicEpoch:
    """Saturating uint64 counter with non-allocating cached ABI callables."""

    __slots__ = (
        "_capsule",
        "_value",
        "_lock",
        "_fork_locks",
        "_fork_lock_index",
        "_inc",
        "_inc_marked",
        "_dec",
        "_read",
        "_set",
        "_write",
    )

    def __init__(self) -> None:
        """Initialize the atomic epoch and its owned runtime state."""
        self._capsule = None
        self._value = 0
        self._lock = Lock()
        # Fallback counters must not allocate or decref an inherited lock in a
        # child. The escrow has two usable prepared child generations, so keep
        # one initial and two one-way child locks; never cycle to a potentially
        # poisoned ancestor lock.
        self._fork_locks = (self._lock, Lock(), Lock())
        self._fork_lock_index = 0
        self._inc: Callable[[object], object] | None = None
        self._inc_marked: Callable[[object, bytearray, int], object] | None = None
        self._dec: Callable[[object], object] | None = None
        self._read: Callable[[object], int] | None = None
        self._set: Callable[[object, int], object] | None = None
        self._write: Callable[[object, bytearray, int], object] | None = None
        try:
            from .native_runtime import native_core

            create = getattr(native_core, "atomic_epoch_create", None)
            inc = getattr(native_core, "atomic_epoch_increment", None)
            inc_marked = getattr(native_core, "atomic_epoch_increment_marked", None)
            dec = getattr(native_core, "atomic_epoch_decrement", None)
            read = getattr(native_core, "atomic_epoch_value", None)
            set_exact = getattr(native_core, "atomic_epoch_set_exact", None)
            write = getattr(native_core, "atomic_epoch_write_le", None)
            if (
                callable(create)
                and callable(inc)
                and callable(inc_marked)
                and callable(dec)
                and callable(read)
                and callable(set_exact)
                and callable(write)
            ):
                self._capsule = create()
                self._inc = inc
                self._inc_marked = inc_marked
                self._dec = dec
                self._read = read
                self._set = set_exact
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

    def increment_marked(self, markers: bytearray, index: int) -> bool:
        """Increment once and atomically advance marker byte 1 to byte 2.

        The marker is the retry authority if an asynchronous exception reaches
        Python after the native commit. Without that composite ABI, returning
        false leaves the caller's conservative pre-commit state untouched.
        """
        if not isinstance(markers, bytearray):
            raise TypeError("atomic epoch commit markers must be bytearray")
        if type(index) is not int or index < 0 or index >= len(markers):
            raise ValueError("atomic epoch commit marker index is invalid")
        capsule = self._capsule
        increment_marked = self._inc_marked
        if capsule is None or increment_marked is None:
            return False
        try:
            increment_marked(capsule, markers, index)
        except BaseException:
            pass
        # Bypass a bytearray subclass's Python hooks: the exact native marker
        # postcondition, not the extension call's return path, acknowledges the
        # indivisible commit.
        return bytearray.__getitem__(markers, index) == 2

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

    def set_exact(self, value: int) -> bool:
        """Set this counter to an exact bounded value without replacing its capsule.

        Admission-side recovery uses this to reconcile derived mirrors while
        retaining the native capsule captured by finalizer shutdown. The ABI
        performs one release-store, so a frozen native activity reader
        can observe only the prior or the exact rebuilt value, never a transient
        zero produced by reset-plus-increment reconstruction.
        """
        if type(value) is not int or value < 0 or value > _MAX_U64:
            raise ValueError("atomic epoch exact value must be a uint64")
        capsule = self._capsule
        set_exact = self._set
        if capsule is not None and set_exact is not None:
            try:
                set_exact(capsule, value)
            except BaseException:
                return False
            return True
        with self._lock:
            self._value = value
        return True

    @property
    def native_capsule(self) -> object | None:
        """Return the native capsule."""
        return self._capsule

    def reset_after_fork(self) -> bool:
        """Reset process-local state inherited across a fork in place."""
        next_index = self._fork_lock_index + 1
        if next_index >= len(self._fork_locks):
            return False
        self._lock = self._fork_locks[next_index]
        self._fork_lock_index = next_index
        return self.set_exact(0)

    def replenish_fork_locks(self) -> None:
        """Rearm the bounded child-lock bank from a normal-runtime safe point."""
        # Keep the currently selected child lock as generation zero. This is
        # safe in normal runtime and avoids changing the lock used by a fallback
        # reader that may already be active.
        self._fork_locks = (self._lock, Lock(), Lock())
        self._fork_lock_index = 0


__all__ = ["AtomicEpoch"]
