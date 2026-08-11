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
from ..core_impl.memory_budget import acquire_operation_memory
from ..core_impl.process_resources import acquire_file_descriptors
from ..core_impl.safe_errors import add_bounded_note
from ..errors import SchemaSanitizerResourceError

T = TypeVar("T")
_KEY_CHUNK_CHARS = 4096
_MAX_KEY_DEPTH = 32
_MAX_POOL_ENTRIES = 1024
_MAX_PENDING_KEY_GATES = 1024


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
    """Preallocated construction/cleanup escrow for one provider resource.

    The slot is reserved *before* any SDK resource is created.  Until the
    resource is successfully inserted into ``_entries`` the slot itself is the
    authoritative owner, so dict insertion OOM, partial ``__aenter__`` failure,
    and cleanup failure can never drop the only strong reference.
    """

    value: Any | None = None
    resource: Any | None = None
    kind: str = "free"
    descriptor_lease: Any | None = None
    control_lease: Any | None = None
    close_attempts: int = 0
    physical_closed: bool = False
    published: bool = False

    def reserve(self) -> None:
        if self.kind != "free":
            raise RuntimeError("provider cleanup escrow slot is not free")
        self.kind = "reserved"
        self.value = None
        self.resource = None
        self.descriptor_lease = None
        self.control_lease = None
        self.close_attempts = 0
        self.physical_closed = False
        self.published = False

    def bind_owners(self, *, descriptor_lease: Any, control_lease: Any | None) -> None:
        if self.kind != "reserved":
            raise RuntimeError("provider cleanup escrow slot is not reserved")
        self.descriptor_lease = descriptor_lease
        self.control_lease = control_lease

    def bind_client(self, value: Any) -> None:
        if self.kind != "reserved":
            raise RuntimeError("provider cleanup escrow slot is not reserved")
        self.resource = value
        self.value = value
        self.kind = "client"

    def bind_manager(self, manager: AsyncContextManager[Any]) -> None:
        if self.kind != "reserved":
            raise RuntimeError("provider cleanup escrow slot is not reserved")
        self.resource = manager
        self.kind = "manager"

    def publish_value(self, value: Any) -> None:
        if self.kind != "manager":
            raise RuntimeError("provider manager escrow is not bound")
        self.value = value

    async def _close_physical(self) -> None:
        resource = self.resource
        if resource is None:
            return
        self.close_attempts += 1
        if self.kind == "manager":
            await resource.__aexit__(None, None, None)
        elif self.kind == "client":
            await _close_client_value(resource)
        else:
            raise RuntimeError("provider cleanup escrow kind is invalid")

    async def close_and_commit(self) -> None:
        """Close physically once, then retire logical owners transactionally."""
        if not self.physical_closed and self.resource is not None:
            await self._close_physical()
            self.physical_closed = True

        first_error: BaseException | None = None
        for attribute in ("descriptor_lease", "control_lease"):
            owner = getattr(self, attribute)
            if owner is None:
                continue
            try:
                owner.release()
            except BaseException as exc:
                if first_error is None:
                    first_error = exc
                else:
                    add_bounded_note(
                        first_error,
                        f"provider pool {attribute} cleanup also failed",
                        exc,
                    )
            else:
                setattr(self, attribute, None)
        if first_error is not None:
            raise first_error

    def recycle(self) -> None:
        if not self.physical_closed and self.resource is not None:
            raise RuntimeError("cannot recycle a physically live provider resource")
        if self.descriptor_lease is not None or self.control_lease is not None:
            raise RuntimeError("cannot recycle provider escrow with live logical owners")
        self.value = None
        self.resource = None
        self.kind = "free"
        self.close_attempts = 0
        self.physical_closed = False
        self.published = False


@dataclass(slots=True)
class _KeyGate:
    """One active single-flight lock plus its bounded control-memory owner."""

    lock: asyncio.Lock
    users: int = 0
    control_lease: Any | None = None


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

    def __init__(self, *, default_descriptor_weight: int = 1) -> None:
        """Initialize storage with one explicit SDK transport-capacity charge.

        Each provider entry reserves its worst-case connection-pool width once.
        Async operations therefore do not reserve the same network socket again;
        their composite footprint only adds local-file descriptors.
        """
        self._default_descriptor_weight = _validate_descriptor_weight(default_descriptor_weight)
        self._entries: dict[tuple[Any, ...], _PoolEntry] = {}
        # Construction escrow is physically allocated before any SDK resource.
        # A resource that cannot be published remains rooted in one of these
        # slots and is retried during pool shutdown.
        self._entry_escrow: list[_PoolEntry] = [_PoolEntry() for _ in range(_MAX_POOL_ENTRIES)]
        self._pid = os.getpid()
        self._loop: asyncio.AbstractEventLoop | None = None
        self._lock: asyncio.Lock | None = None
        self._key_locks: dict[tuple[Any, ...], _KeyGate] = {}
        self._closed = False
        self._close_generation = 0
        self._protocol_violations = 0

    async def __aenter__(self) -> RemoteProviderSessionPool:
        """Bind the pool to exactly one operation coordinator event loop."""
        self._ensure_owner_process()
        if self._loop is not None or self._lock is not None:
            raise RuntimeError("remote provider session pool is already open")
        self._loop = asyncio.get_running_loop()
        self._lock = asyncio.Lock()
        return self

    async def __aexit__(self, *_exc: object) -> None:
        """Close published and construction-escrow resources transactionally."""
        self._ensure_owner_loop()
        if (
            self._closed
            and not self._entries
            and not any(slot.kind != "free" for slot in self._entry_escrow)
        ):
            return
        self._closed = True
        self._close_generation += 1
        first_error: BaseException | None = None

        # Published entries and unpublished construction debt share the same
        # preallocated slot type.  Iterate the bank directly so an insertion
        # failure cannot make an owner invisible to shutdown.
        for entry in self._entry_escrow:
            if entry.kind == "free":
                continue
            # A RESERVED slot with no resource belongs to an in-flight factory.
            # Do not recycle it from shutdown: the factory will bind its result
            # into this exact slot and observe _closed before publication returns.
            if entry.kind == "reserved" and entry.resource is None:
                continue
            try:
                await entry.close_and_commit()
            except BaseException as exc:
                if first_error is None:
                    first_error = exc
                if isinstance(exc, (asyncio.CancelledError, KeyboardInterrupt, SystemExit)):
                    break
                continue
            for key, candidate in tuple(self._entries.items()):
                if candidate is entry:
                    self._entries.pop(key, None)
                    break
            entry.recycle()

        # Idle key gates own real control-memory leases. Retire the lease before
        # removing the gate so shutdown cannot publish a false memory commit.
        for key, gate in tuple(self._key_locks.items()):
            if gate.users != 0:
                # A suspended constructor still owns this gate and its control
                # lease. Its finally block will retire the gate after it resumes.
                continue
            lease = gate.control_lease
            if lease is not None:
                try:
                    lease.release()
                except BaseException as exc:
                    if first_error is None:
                        first_error = exc
                    else:
                        add_bounded_note(first_error, "provider key-gate cleanup also failed", exc)
                    continue
                gate.control_lease = None
            self._key_locks.pop(key, None)

        # Azure constructor rollback uses preallocated terminal slots. Drive any
        # slot whose retry Task could not be created before claiming pool quiescence.
        try:
            from .providers.azure import drain_azure_credential_rollbacks
        except ImportError:
            # Source-only test environments intentionally omit the native core.
            # The static Azure escrow remains authoritative until its own task or
            # a later provider safe point can drive it.
            pass
        else:
            try:
                await drain_azure_credential_rollbacks()
            except BaseException as exc:
                if first_error is None:
                    first_error = exc
                else:
                    add_bounded_note(first_error, "Azure rollback safe-point also failed", exc)
        if first_error is not None:
            raise first_error
        if self._protocol_violations:
            raise RuntimeError("provider key-gate protocol violation prevents clean close")

    def _reserve_entry_escrow(self) -> _PoolEntry:
        """Reserve one preallocated owner before creating a physical resource."""
        for entry in self._entry_escrow:
            if entry.kind == "free":
                entry.reserve()
                return entry
        raise SchemaSanitizerResourceError(
            "remote provider construction escrow exhausted",
            detail={
                "stage": "remote_provider_pool",
                "limit_name": "provider_cleanup_escrow",
                "limit": _MAX_POOL_ENTRIES,
                "actual": _MAX_POOL_ENTRIES,
            },
        )

    @staticmethod
    def _acquire_entry_control_lease(descriptor_weight: int) -> Any | None:
        """Charge a conservative SDK/transport footprint, scaled by pool width."""
        control_bytes = min(1 << 20, (16 << 10) + descriptor_weight * (8 << 10))
        return acquire_operation_memory(control_bytes, stage="remote_provider_pool_entry")

    @property
    def transport_capacity(self) -> int:
        """Worst-case per-entry SDK connection width already FD-admitted."""
        return self._default_descriptor_weight

    def _ensure_entry_capacity(self) -> None:
        """Keep loop-affine provider metadata bounded even without an operation ledger."""
        active = sum(1 for entry in self._entry_escrow if entry.kind != "free")
        if active < _MAX_POOL_ENTRIES:
            return
        raise SchemaSanitizerResourceError(
            "remote provider session pool entry capacity exhausted",
            detail={
                "stage": "remote_provider_pool",
                "limit_name": "provider_pool_entries",
                "limit": _MAX_POOL_ENTRIES,
                "actual": _MAX_POOL_ENTRIES,
            },
        )

    async def borrow_client(
        self,
        key: tuple[Any, ...],
        factory: Callable[[], Awaitable[Any]],
        *,
        descriptor_weight: int | None = None,
    ) -> Any:
        """Return a borrowed client, retaining its transport capacity once."""
        descriptor_weight = _validate_descriptor_weight(
            self._default_descriptor_weight if descriptor_weight is None else descriptor_weight
        )
        pool_key = _compact_pool_key(key)
        entry = await self._get_or_create_client(pool_key, factory, descriptor_weight)
        return _BorrowedClient(entry.value)

    async def borrow_manager(
        self,
        key: tuple[Any, ...],
        factory: Callable[[], Awaitable[AsyncContextManager[Any]]],
        *,
        descriptor_weight: int | None = None,
    ) -> AsyncContextManager[Any]:
        """Return a borrowed manager, retaining its transport capacity once."""
        descriptor_weight = _validate_descriptor_weight(
            self._default_descriptor_weight if descriptor_weight is None else descriptor_weight
        )
        pool_key = _compact_pool_key(key)
        async with self._key_guard(pool_key):
            self._ensure_open()
            entry = self._entries.get(pool_key)
            if entry is None:
                self._ensure_entry_capacity()
                entry = self._reserve_entry_escrow()
                try:
                    control_lease = self._acquire_entry_control_lease(descriptor_weight)
                    try:
                        descriptor_lease = acquire_file_descriptors(descriptor_weight)
                    except BaseException:
                        if control_lease is not None:
                            control_lease.release()
                        raise
                    entry.bind_owners(
                        descriptor_lease=descriptor_lease, control_lease=control_lease
                    )
                    manager = await factory()
                    # Publish the raw manager into escrow before __aenter__. A
                    # partially successful __aenter__ therefore remains cleanup-owned.
                    entry.bind_manager(manager)
                    try:
                        value = await manager.__aenter__()
                    except BaseException as primary:
                        try:
                            await entry.close_and_commit()
                        except BaseException as cleanup_error:
                            add_bounded_note(
                                primary,
                                "provider manager rollback also failed after __aenter__",
                                cleanup_error,
                            )
                            raise
                        else:
                            entry.recycle()
                            raise
                    entry.publish_value(value)
                    try:
                        self._entries[pool_key] = entry
                    except BaseException as primary:
                        # The preallocated escrow slot remains authoritative even
                        # if dict growth and immediate cleanup both fail.
                        try:
                            await entry.close_and_commit()
                        except BaseException as cleanup_error:
                            add_bounded_note(
                                primary,
                                "provider manager cleanup also failed after pool insertion failure",
                                cleanup_error,
                            )
                        else:
                            entry.recycle()
                        raise
                    entry.published = True
                except BaseException:
                    # Slots with any live owner stay published for __aexit__; an
                    # untouched reservation can be recycled immediately.
                    if (
                        entry.kind == "reserved"
                        and entry.resource is None
                        and entry.descriptor_lease is None
                        and entry.control_lease is None
                    ):
                        entry.physical_closed = True
                        entry.recycle()
                    raise
                if self._closed:
                    try:
                        await entry.close_and_commit()
                    except BaseException as cleanup_error:
                        closed = RuntimeError("remote provider session pool is closed")
                        add_bounded_note(
                            closed, "provider manager close also failed", cleanup_error
                        )
                        raise closed
                    else:
                        self._entries.pop(pool_key, None)
                        entry.recycle()
                    raise RuntimeError("remote provider session pool is closed")
        assert entry.value is not None
        return _BorrowedManager(entry.value)

    async def _get_or_create_client(
        self,
        key: tuple[Any, ...],
        factory: Callable[[], Awaitable[Any]],
        descriptor_weight: int,
    ) -> _PoolEntry:
        """Create one client with construction escrow reserved pre-resource."""
        async with self._key_guard(key):
            self._ensure_open()
            entry = self._entries.get(key)
            if entry is not None:
                return entry
            self._ensure_entry_capacity()
            entry = self._reserve_entry_escrow()
            try:
                control_lease = self._acquire_entry_control_lease(descriptor_weight)
                try:
                    descriptor_lease = acquire_file_descriptors(descriptor_weight)
                except BaseException:
                    if control_lease is not None:
                        control_lease.release()
                    raise
                entry.bind_owners(descriptor_lease=descriptor_lease, control_lease=control_lease)
                value = await factory()
                # Attribute assignment into a preallocated slot is the first
                # post-construction action; no fallible container publication is
                # needed to retain the physical resource.
                entry.bind_client(value)
                try:
                    self._entries[key] = entry
                except BaseException as primary:
                    try:
                        await entry.close_and_commit()
                    except BaseException as cleanup_error:
                        add_bounded_note(
                            primary,
                            "provider client cleanup also failed after pool insertion failure",
                            cleanup_error,
                        )
                    else:
                        entry.recycle()
                    raise
                entry.published = True
            except BaseException:
                if (
                    entry.kind == "reserved"
                    and entry.resource is None
                    and entry.descriptor_lease is None
                    and entry.control_lease is None
                ):
                    entry.physical_closed = True
                    entry.recycle()
                raise
            if self._closed:
                try:
                    await entry.close_and_commit()
                except BaseException as cleanup_error:
                    closed = RuntimeError("remote provider session pool is closed")
                    add_bounded_note(closed, "provider client close also failed", cleanup_error)
                    raise closed
                else:
                    self._entries.pop(key, None)
                    entry.recycle()
                raise RuntimeError("remote provider session pool is closed")
            return entry

    @asynccontextmanager
    async def _key_guard(self, key: tuple[Any, ...]) -> AsyncIterator[None]:
        """Retain one key gate only while creators or waiters actively use it."""
        lock = self._require_lock()
        async with lock:
            self._ensure_open()
            gate = self._key_locks.get(key)
            if gate is None:
                if len(self._key_locks) >= _MAX_PENDING_KEY_GATES:
                    raise SchemaSanitizerResourceError(
                        "remote provider key-gate capacity exhausted",
                        detail={
                            "stage": "remote_provider_pool",
                            "limit_name": "provider_pending_key_gates",
                            "limit": _MAX_PENDING_KEY_GATES,
                            "actual": len(self._key_locks) + 1,
                        },
                    )
                gate_lease = acquire_operation_memory(512, stage="remote_provider_key_gate")
                try:
                    gate = _KeyGate(asyncio.Lock(), control_lease=gate_lease)
                    self._key_locks[key] = gate
                except BaseException as primary:
                    if gate_lease is not None:
                        try:
                            gate_lease.release()
                        except BaseException as cleanup_error:
                            add_bounded_note(
                                primary, "provider key-gate rollback also failed", cleanup_error
                            )
                    raise
            gate.users += 1
        try:
            async with gate.lock:
                yield
        finally:
            # This pool is event-loop affine, so synchronous mutation here is
            # atomic with respect to every other pool operation. Avoiding an
            # await makes gate retirement immune to repeated task cancellation.
            if gate.users <= 0:
                self._protocol_violations += 1
            else:
                gate.users -= 1
            if gate.users == 0 and self._key_locks.get(key) is gate:
                lease = gate.control_lease
                if lease is not None:
                    lease.release()
                    gate.control_lease = None
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


_FORK_CURRENT_POOL_BANKS: tuple[ContextVar[RemoteProviderSessionPool | None], ...] = (
    ContextVar[RemoteProviderSessionPool | None](
        "schema_sanitizer_provider_session_pool_child_0", default=None
    ),
    ContextVar[RemoteProviderSessionPool | None](
        "schema_sanitizer_provider_session_pool_child_1", default=None
    ),
)
_FORK_CURRENT_POOL_BANK_INDEX = 0
_FORK_PREPARED_CURRENT_POOL: ContextVar[RemoteProviderSessionPool | None] | None = None


def _prepare_provider_session_pool_for_fork() -> None:
    global _FORK_PREPARED_CURRENT_POOL
    _FORK_PREPARED_CURRENT_POOL = _FORK_CURRENT_POOL_BANKS[_FORK_CURRENT_POOL_BANK_INDEX]


def _clear_provider_session_pool_fork_preparation() -> None:
    global _FORK_PREPARED_CURRENT_POOL
    _FORK_PREPARED_CURRENT_POOL = None


def _reset_current_pool_after_fork() -> None:
    """Swap to a preallocated empty child ContextVar without decrefing owners."""
    global _CURRENT_POOL, _FORK_PREPARED_CURRENT_POOL
    global _FORK_CURRENT_POOL_BANK_INDEX
    prepared = _FORK_PREPARED_CURRENT_POOL
    if prepared is None:
        return
    inherited = _CURRENT_POOL.get()
    if inherited is not None:
        quarantine_inherited_state("provider-session-pool", inherited, _CURRENT_POOL)
    _CURRENT_POOL = prepared
    _FORK_PREPARED_CURRENT_POOL = None
    _FORK_CURRENT_POOL_BANK_INDEX = 1 - _FORK_CURRENT_POOL_BANK_INDEX


from ..core_impl.fork_manager import register_fork_handler as _register_fork_handler  # noqa: E402

_register_fork_handler(
    "provider-session-pool",
    before=_prepare_provider_session_pool_for_fork,
    after_in_parent=_clear_provider_session_pool_fork_preparation,
    after_in_child=_reset_current_pool_after_fork,
)


def current_provider_session_pool() -> RemoteProviderSessionPool | None:
    """Return the provider pool active for the current coordinator task."""
    return _CURRENT_POOL.get()


__all__ = [
    "RemoteProviderSessionPool",
    "activate_provider_session_pool",
    "current_provider_session_pool",
]
