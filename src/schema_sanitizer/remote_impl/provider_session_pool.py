"""Operation-owned pooling for loop-affine remote provider clients."""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
from contextlib import contextmanager
from contextvars import ContextVar
from dataclasses import dataclass
from typing import Any, AsyncContextManager, TypeVar

T = TypeVar("T")
_CURRENT_POOL: ContextVar[RemoteProviderSessionPool | None] = ContextVar(
    "schema_sanitizer_remote_provider_pool",
    default=None,
)


@dataclass(slots=True)
class _PoolEntry:
    """One shared provider value plus its operation-final close callback."""

    value: Any
    close: Callable[[], Awaitable[Any]]


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
        self._lock: asyncio.Lock | None = None
        self._key_locks: dict[tuple[Any, ...], asyncio.Lock] = {}
        self._closed = False

    async def __aenter__(self) -> RemoteProviderSessionPool:
        """Bind the pool to the operation coordinator event loop."""
        self._lock = asyncio.Lock()
        return self

    async def __aexit__(self, *_exc: object) -> None:
        """Close every shared client once in reverse creation order."""
        if self._closed:
            return
        self._closed = True
        entries = tuple(reversed(tuple(self._entries.values())))
        self._entries.clear()
        self._key_locks.clear()
        first_error: BaseException | None = None
        for entry in entries:
            try:
                await entry.close()
            except BaseException as exc:
                if first_error is None:
                    first_error = exc
        if first_error is not None:
            raise first_error

    async def borrow_client(
        self,
        key: tuple[Any, ...],
        factory: Callable[[], Awaitable[Any]],
    ) -> Any:
        """Return a borrowed client, creating and owning it exactly once."""
        entry = await self._get_or_create_client(key, factory)
        return _BorrowedClient(entry.value)

    async def borrow_manager(
        self,
        key: tuple[Any, ...],
        factory: Callable[[], Awaitable[AsyncContextManager[Any]]],
    ) -> AsyncContextManager[Any]:
        """Return a borrowed view of one entered provider context manager."""
        key_lock = await self._key_lock(key)
        async with key_lock:
            self._ensure_open()
            entry = self._entries.get(key)
            if entry is None:
                manager = await factory()
                value = await manager.__aenter__()

                async def close_manager() -> None:
                    """Close the retained provider manager once."""
                    await manager.__aexit__(None, None, None)

                entry = _PoolEntry(value, close_manager)
                self._entries[key] = entry
        return _BorrowedManager(entry.value)

    async def _get_or_create_client(
        self,
        key: tuple[Any, ...],
        factory: Callable[[], Awaitable[Any]],
    ) -> _PoolEntry:
        """Create one directly closable client under its key-local lock."""
        key_lock = await self._key_lock(key)
        async with key_lock:
            self._ensure_open()
            entry = self._entries.get(key)
            if entry is not None:
                return entry
            value = await factory()

            async def close_client() -> None:
                """Close a retained provider client once."""
                close = getattr(value, "close", None)
                if close is None:
                    exit_fn = getattr(value, "__aexit__", None)
                    if exit_fn is not None:
                        await exit_fn(None, None, None)
                    return
                result = close()
                if asyncio.iscoroutine(result):
                    await result

            entry = _PoolEntry(value, close_client)
            self._entries[key] = entry
            return entry

    async def _key_lock(self, key: tuple[Any, ...]) -> asyncio.Lock:
        """Return one single-flight lock without serializing unrelated keys."""
        lock = self._require_lock()
        async with lock:
            self._ensure_open()
            return self._key_locks.setdefault(key, asyncio.Lock())

    def _require_lock(self) -> asyncio.Lock:
        """Return the event-loop lock after context entry."""
        if self._lock is None:
            raise RuntimeError("remote provider session pool is not open")
        return self._lock

    def _ensure_open(self) -> None:
        """Reject borrows after operation shutdown."""
        if self._closed:
            raise RuntimeError("remote provider session pool is closed")


@contextmanager
def activate_provider_session_pool(pool: Any):
    """Expose one operation pool to provider factories in the current task."""
    if not isinstance(pool, RemoteProviderSessionPool):
        yield
        return
    token = _CURRENT_POOL.set(pool)
    try:
        yield
    finally:
        _CURRENT_POOL.reset(token)


def current_provider_session_pool() -> RemoteProviderSessionPool | None:
    """Return the provider pool active for the current coordinator task."""
    return _CURRENT_POOL.get()


__all__ = [
    "RemoteProviderSessionPool",
    "activate_provider_session_pool",
    "current_provider_session_pool",
]
