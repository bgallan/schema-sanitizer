"""Operation-owned pooling for loop-affine remote provider clients."""

from __future__ import annotations

import asyncio
import hashlib
import math
import os
from collections.abc import AsyncIterator, Awaitable, Callable
from contextlib import asynccontextmanager, contextmanager
from contextvars import ContextVar
from dataclasses import dataclass
from typing import Any, AsyncContextManager, TypeVar

from ..core_impl.fork_safety import quarantine_inherited_state
from ..core_impl.process_resources import acquire_file_descriptors
from ..core_impl.safe_errors import add_bounded_note

T = TypeVar("T")
_KEY_CHUNK_CHARS = 4096
_MAX_KEY_DEPTH = 32


def _key_is_compactable(value: Any, depth: int = 0) -> bool:
    """Return whether a key can be hashed without retaining unknown objects."""
    if depth > _MAX_KEY_DEPTH:
        return False
    if value is None or isinstance(value, (str, bytes)):
        return True
    if isinstance(value, bool):
        return True
    if isinstance(value, int):
        return value.bit_length() <= 4096
    if isinstance(value, float):
        return not math.isnan(value)
    if isinstance(value, tuple):
        return all(_key_is_compactable(item, depth + 1) for item in value)
    return False


def _feed_key_digest(digest: Any, value: Any, depth: int = 0) -> None:
    """Hash supported immutable key values incrementally and unambiguously."""
    if value is None:
        digest.update(b"n;")
        return
    numerator: int | None
    denominator: int | None
    if isinstance(value, (bool, int)):
        numerator, denominator = int(value), 1
    elif isinstance(value, float):
        if math.isinf(value):
            digest.update(b"q+inf;" if value > 0 else b"q-inf;")
            return
        numerator, denominator = value.as_integer_ratio()
    else:
        numerator = denominator = None
    if numerator is not None:
        encoded_numerator = str(numerator).encode("ascii")
        encoded_denominator = str(denominator).encode("ascii")
        digest.update(
            b"q"
            + str(len(encoded_numerator)).encode("ascii")
            + b":"
            + encoded_numerator
            + b"/"
            + str(len(encoded_denominator)).encode("ascii")
            + b":"
            + encoded_denominator
            + b";"
        )
        return
    if isinstance(value, str):
        digest.update(b"s" + str(len(value)).encode("ascii") + b":")
        for offset in range(0, len(value), _KEY_CHUNK_CHARS):
            digest.update(
                value[offset : offset + _KEY_CHUNK_CHARS].encode("utf-8", errors="surrogatepass")
            )
        digest.update(b";")
        return
    if isinstance(value, bytes):
        digest.update(b"y" + str(len(value)).encode("ascii") + b":")
        digest.update(memoryview(value))
        digest.update(b";")
        return
    if isinstance(value, tuple):
        digest.update(b"t" + str(len(value)).encode("ascii") + b"[")
        for item in value:
            _feed_key_digest(digest, item, depth + 1)
        digest.update(b"]")
        return
    raise TypeError(f"unsupported provider session pool key component: {type(value)!r}")


def _compact_pool_key(key: tuple[Any, ...]) -> tuple[Any, ...]:
    """Return a fixed-size identity for supported potentially huge pool keys."""
    if not _key_is_compactable(key):
        return key
    digest = hashlib.blake2b(digest_size=32, person=b"ss-provider-pool")
    _feed_key_digest(digest, key)
    return ("pool-key-blake2b-v1", digest.digest())


def _validate_descriptor_weight(weight: int) -> int:
    """Return one exact logical descriptor request without coercion."""
    if isinstance(weight, bool) or not isinstance(weight, int):
        raise TypeError("provider descriptor weight must be an integer")
    if weight <= 0:
        raise ValueError("provider descriptor weight must be > 0")
    return weight


_CURRENT_POOL: ContextVar[RemoteProviderSessionPool | None] = ContextVar(
    "schema_sanitizer_remote_provider_pool",
    default=None,
)
_FORKED_PROVIDER_POOLS_KEEPALIVE: list[RemoteProviderSessionPool] = []


@dataclass(slots=True)
class _PoolEntry:
    """One shared provider value plus its operation-final close callback."""

    value: Any
    close: Callable[[], Awaitable[Any]]
    descriptor_lease: Any | None = None


@dataclass(slots=True)
class _KeyGate:
    """One active single-flight lock plus its current borrower count."""

    lock: asyncio.Lock
    users: int = 0


async def _close_client_value(value: Any) -> None:
    """Close one directly created provider value exactly once."""
    close = getattr(value, "close", None)
    if close is None:
        exit_fn = getattr(value, "__aexit__", None)
        if exit_fn is not None:
            await exit_fn(None, None, None)
        return
    result = close()
    if asyncio.iscoroutine(result):
        await result


class _BorrowedClient:
    """Forward provider methods while making per-call close a no-op."""

    def __init__(self, value: Any) -> None:
        """Store the operation-owned underlying client."""
        self._value = value

    def __getattr__(self, name: str) -> Any:
        """Forward provider methods and properties."""
        return getattr(self._value, name)

    def __setattr__(self, name: str, value: Any) -> None:
        """Forward mutable provider attributes after initialization."""
        if name == "_value" or "_value" not in self.__dict__:
            object.__setattr__(self, name, value)
            return
        setattr(self._value, name, value)

    async def __aenter__(self) -> _BorrowedClient:
        """Return this borrowed view without reopening the client."""
        return self

    async def __aexit__(self, *_exc: object) -> None:
        """Keep the operation-owned client alive."""

    async def close(self) -> None:
        """Keep the operation-owned client alive until pool shutdown."""


class _BorrowedManager:
    """Expose an already-entered async manager without closing it per use."""

    def __init__(self, value: Any) -> None:
        """Store the shared entered value."""
        self._value = value

    async def __aenter__(self) -> Any:
        """Return the already-entered provider client."""
        return self._value

    async def __aexit__(self, *_exc: object) -> None:
        """Keep the provider manager alive until operation shutdown."""


class RemoteProviderSessionPool:
    """Reuse demonstrably concurrent-safe provider sessions for one operation."""

    def __init__(self) -> None:
        """Initialize empty loop-affine provider storage."""
        self._entries: dict[tuple[Any, ...], _PoolEntry] = {}
        self._pid = os.getpid()
        self._loop: asyncio.AbstractEventLoop | None = None
        self._lock: asyncio.Lock | None = None
        self._key_locks: dict[tuple[Any, ...], _KeyGate] = {}
        self._closed = False

    async def __aenter__(self) -> RemoteProviderSessionPool:
        """Bind the pool to exactly one operation coordinator event loop."""
        self._ensure_owner_process()
        if self._loop is not None or self._lock is not None:
            raise RuntimeError("remote provider session pool is already open")
        self._loop = asyncio.get_running_loop()
        self._lock = asyncio.Lock()
        return self

    async def __aexit__(self, *_exc: object) -> None:
        """Close every shared client once in reverse creation order."""
        self._ensure_owner_loop()
        if self._closed:
            return
        self._closed = True
        self._key_locks.clear()
        first_error: BaseException | None = None
        while self._entries:
            _key, entry = self._entries.popitem()
            try:
                await entry.close()
            except BaseException as exc:
                if first_error is None:
                    first_error = exc
            finally:
                if entry.descriptor_lease is not None:
                    entry.descriptor_lease.release()
        if first_error is not None:
            raise first_error

    async def borrow_client(
        self,
        key: tuple[Any, ...],
        factory: Callable[[], Awaitable[Any]],
        *,
        descriptor_weight: int = 1,
    ) -> Any:
        """Return a borrowed client, retaining its descriptor budget once."""
        descriptor_weight = _validate_descriptor_weight(descriptor_weight)
        pool_key = _compact_pool_key(key)
        entry = await self._get_or_create_client(pool_key, factory, descriptor_weight)
        return _BorrowedClient(entry.value)

    async def borrow_manager(
        self,
        key: tuple[Any, ...],
        factory: Callable[[], Awaitable[AsyncContextManager[Any]]],
        *,
        descriptor_weight: int = 1,
    ) -> AsyncContextManager[Any]:
        """Return a borrowed view of one entered provider context manager."""
        descriptor_weight = _validate_descriptor_weight(descriptor_weight)
        pool_key = _compact_pool_key(key)
        async with self._key_guard(pool_key):
            self._ensure_open()
            entry = self._entries.get(pool_key)
            if entry is None:
                descriptor_lease = acquire_file_descriptors(descriptor_weight)
                try:
                    manager = await factory()
                    value = await manager.__aenter__()
                except BaseException:
                    descriptor_lease.release()
                    raise
                if self._closed:
                    try:
                        await manager.__aexit__(None, None, None)
                    finally:
                        descriptor_lease.release()
                    raise RuntimeError("remote provider session pool is closed")

                async def close_manager() -> None:
                    """Close the retained provider manager once."""
                    await manager.__aexit__(None, None, None)

                try:
                    entry = _PoolEntry(value, close_manager, descriptor_lease)
                    self._entries[pool_key] = entry
                except BaseException as exc:
                    try:
                        await manager.__aexit__(None, None, None)
                    except BaseException as cleanup_error:
                        add_bounded_note(
                            exc,
                            "provider manager cleanup also failed after pool insertion failure",
                            cleanup_error,
                        )
                    finally:
                        descriptor_lease.release()
                    raise
        return _BorrowedManager(entry.value)

    async def _get_or_create_client(
        self,
        key: tuple[Any, ...],
        factory: Callable[[], Awaitable[Any]],
        descriptor_weight: int,
    ) -> _PoolEntry:
        """Create one directly closable client under its key-local lock."""
        async with self._key_guard(key):
            self._ensure_open()
            entry = self._entries.get(key)
            if entry is not None:
                return entry
            descriptor_lease = acquire_file_descriptors(descriptor_weight)
            try:
                value = await factory()
            except BaseException:
                descriptor_lease.release()
                raise
            if self._closed:
                try:
                    await _close_client_value(value)
                finally:
                    descriptor_lease.release()
                raise RuntimeError("remote provider session pool is closed")

            async def close_client() -> None:
                """Close a retained provider client once."""
                await _close_client_value(value)

            try:
                entry = _PoolEntry(value, close_client, descriptor_lease)
                self._entries[key] = entry
            except BaseException as exc:
                try:
                    await _close_client_value(value)
                except BaseException as cleanup_error:
                    add_bounded_note(
                        exc,
                        "provider client cleanup also failed after pool insertion failure",
                        cleanup_error,
                    )
                finally:
                    descriptor_lease.release()
                raise
            return entry

    @asynccontextmanager
    async def _key_guard(self, key: tuple[Any, ...]) -> AsyncIterator[None]:
        """Retain one key gate only while creators or waiters actively use it."""
        lock = self._require_lock()
        async with lock:
            self._ensure_open()
            gate = self._key_locks.get(key)
            if gate is None:
                gate = _KeyGate(asyncio.Lock())
                self._key_locks[key] = gate
            gate.users += 1
        try:
            async with gate.lock:
                yield
        finally:
            # This pool is event-loop affine, so synchronous mutation here is
            # atomic with respect to every other pool operation. Avoiding an
            # await makes gate retirement immune to repeated task cancellation.
            gate.users = max(0, gate.users - 1)
            if gate.users == 0 and self._key_locks.get(key) is gate:
                self._key_locks.pop(key, None)

    def _require_lock(self) -> asyncio.Lock:
        """Return the event-loop lock after context entry."""
        self._ensure_owner_loop()
        if self._lock is None:
            raise RuntimeError("remote provider session pool is not open")
        return self._lock

    def _ensure_owner_process(self) -> None:
        """Reject direct pool references inherited by a forked child."""
        if os.getpid() != self._pid:
            raise RuntimeError("remote provider session pool cannot be reused after fork")

    def _ensure_owner_loop(self) -> None:
        """Reject cross-loop reuse before touching loop-affine locks or clients."""
        self._ensure_owner_process()
        loop = self._loop
        if loop is None:
            raise RuntimeError("remote provider session pool is not open")
        if asyncio.get_running_loop() is not loop:
            raise RuntimeError(
                "remote provider session pool cannot be reused from another event loop"
            )

    def _ensure_open(self) -> None:
        """Reject borrows after operation shutdown."""
        self._ensure_owner_process()
        if self._closed:
            raise RuntimeError("remote provider session pool is closed")


@contextmanager
def activate_provider_session_pool(pool: Any):
    """Expose one operation pool to provider factories in the current task."""
    if not isinstance(pool, RemoteProviderSessionPool):
        yield
        return
    owner_pid = os.getpid()
    token = _CURRENT_POOL.set(pool)
    try:
        yield
    finally:
        if os.getpid() == owner_pid:
            _CURRENT_POOL.reset(token)
        else:
            _reset_current_pool_after_fork()


def _reset_current_pool_after_fork() -> None:
    """Detach inherited clients without finalizing parent loop-affine objects."""
    inherited = _CURRENT_POOL.get()
    if inherited is not None:
        quarantine_inherited_state("provider-session-pool", inherited)
    _CURRENT_POOL.set(None)


if hasattr(os, "register_at_fork"):
    os.register_at_fork(after_in_child=_reset_current_pool_after_fork)


def current_provider_session_pool() -> RemoteProviderSessionPool | None:
    """Return the provider pool active for the current coordinator task."""
    return _CURRENT_POOL.get()


__all__ = [
    "RemoteProviderSessionPool",
    "activate_provider_session_pool",
    "current_provider_session_pool",
]
