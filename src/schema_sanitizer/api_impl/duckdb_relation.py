"""Owned DuckDB relations backed by private, bounded connections."""

from __future__ import annotations

import os
from contextlib import suppress
from dataclasses import dataclass
from threading import Lock
from typing import Any

from ..core_impl.finalization import runtime_is_finalizing
from ..core_impl.finalizer_cleanup import (
    PreparedFinalizerCleanup,
    acknowledge_prepared_finalizer_cleanup,
    defer_prepared_finalizer_cleanup,
    reserve_finalizer_cleanup,
)


class _DuckDBConnectionOwner:
    """Own one private DuckDB connection used by a lazy relation."""

    __slots__ = ("connection",)

    def __init__(self, connection: Any) -> None:
        self.connection = connection

    def close(self) -> None:
        """Close the private connection exactly after its relation is dropped."""
        connection = self.connection
        if connection is None:
            return
        close = getattr(connection, "close", None)
        if callable(close):
            close()
        if self.connection is connection:
            self.connection = None


class _DuckDBSharedLifetime:
    """Reference-count one connection and the upstream lazy resource chain."""

    __slots__ = ("_lock", "_owner", "_keepalive", "_references", "_closing", "relation_type")

    def __init__(self, owner: _DuckDBConnectionOwner, relation_type: type) -> None:
        self._lock = Lock()
        self._owner: _DuckDBConnectionOwner | None = owner
        self._keepalive: Any = None
        self._references = 0
        self._closing = False
        self.relation_type = relation_type

    def retain(self) -> None:
        with self._lock:
            if self._closing or self._owner is None:
                raise RuntimeError("DuckDB relation lifetime is already closing")
            if self._references >= (1 << 63) - 1:
                raise RuntimeError("DuckDB relation lifetime reference capacity exhausted")
            self._references += 1

    def attach_keepalive(self, keepalive: Any) -> None:
        """Attach the operation chain before the public relation escapes."""
        with self._lock:
            existing = self._keepalive
            if existing is not None and existing is not keepalive:
                raise RuntimeError("DuckDB relation lifetime already owns a keepalive")
            if self._closing or self._owner is None:
                raise RuntimeError("DuckDB relation lifetime is already closing")
            self._keepalive = keepalive

    def retains(self, first: object, second: object) -> bool:
        """Report whether the attached chain owns both exact objects."""
        with self._lock:
            items = getattr(self._keepalive, "_items", None)
            if not isinstance(items, list):
                return False
            found_first = False
            found_second = False
            for item in items:
                if item is first:
                    found_first = True
                elif item is second:
                    found_second = True
            return found_first and found_second

    def release(self) -> None:
        """Release one wrapper reference and close the final owner retryably."""
        with self._lock:
            if self._references <= 0:
                return
            if self._references > 1:
                self._references -= 1
                return
            if self._closing:
                raise RuntimeError("DuckDB relation lifetime close is already in progress")
            self._closing = True
            owner = self._owner
            keepalive = self._keepalive
        try:
            if owner is not None:
                owner.close()
            close_keepalive = getattr(keepalive, "close", None)
            if callable(close_keepalive):
                close_keepalive()
        except BaseException:
            with self._lock:
                self._closing = False
            raise
        with self._lock:
            self._references = 0
            self._owner = None
            self._keepalive = None
            self._closing = False


@dataclass(slots=True)
class _OwnedDuckDBRelationState:
    """Detached relation graph and one shared-lifetime reference."""

    relation: Any
    lifetime: _DuckDBSharedLifetime
    retained: bool = False


def _close_owned_duckdb_relation_capsule(capsule: PreparedFinalizerCleanup) -> None:
    """Drop a lazy relation before closing its connection at a safe point."""
    state = capsule.arg0
    if not isinstance(state, _OwnedDuckDBRelationState):
        return
    state.relation = None
    if state.retained:
        state.lifetime.release()
        state.retained = False


class _OwnedDuckDBRelation:
    """DuckDB relation proxy retaining connection and every derived relation."""

    def __init__(self, relation: Any, owner: _DuckDBConnectionOwner) -> None:
        lifetime = _DuckDBSharedLifetime(owner, type(relation))
        self._initialize(relation, lifetime)

    @classmethod
    def _from_shared(cls, relation: Any, lifetime: _DuckDBSharedLifetime) -> _OwnedDuckDBRelation:
        wrapper = object.__new__(cls)
        wrapper._initialize(relation, lifetime)
        return wrapper

    def _initialize(self, relation: Any, lifetime: _DuckDBSharedLifetime) -> None:
        capsule = reserve_finalizer_cleanup(_close_owned_duckdb_relation_capsule)
        state = _OwnedDuckDBRelationState(relation, lifetime)
        capsule.arg0 = state
        self._relation = relation
        self._lifetime: _DuckDBSharedLifetime | None = lifetime
        self._finalizer_capsule: PreparedFinalizerCleanup | None = capsule
        self._finalizer_state: _OwnedDuckDBRelationState | None = state
        self._pid = os.getpid()
        try:
            lifetime.retain()
            state.retained = True
        except BaseException:
            acknowledge_prepared_finalizer_cleanup(capsule)
            self._relation = None
            self._lifetime = None
            self._finalizer_capsule = None
            self._finalizer_state = None
            raise

    def __getattr__(self, name: str) -> Any:
        relation = self._relation
        if relation is None:
            raise RuntimeError("DuckDB relation is already closed")
        attribute = getattr(relation, name)
        if not callable(attribute):
            return self._wrap_relation_result(attribute)

        def delegated(*args: Any, **kwargs: Any) -> Any:
            unwrapped_args = tuple(self._unwrap_relation_argument(value) for value in args)
            unwrapped_kwargs = {
                key: self._unwrap_relation_argument(value) for key, value in kwargs.items()
            }
            return self._wrap_relation_result(attribute(*unwrapped_args, **unwrapped_kwargs))

        return delegated

    def _unwrap_relation_argument(self, value: Any) -> Any:
        if not isinstance(value, _OwnedDuckDBRelation):
            return value
        if value._lifetime is not self._lifetime:
            raise ValueError(
                "DuckDB relations owned by different private connections cannot be combined"
            )
        relation = value._relation
        if relation is None:
            raise RuntimeError("DuckDB relation is already closed")
        return relation

    def _wrap_relation_result(self, value: Any) -> Any:
        lifetime = self._lifetime
        if lifetime is not None and isinstance(value, lifetime.relation_type):
            if value is self._relation:
                return self
            return type(self)._from_shared(value, lifetime)
        return value

    def _attach_keepalive(self, keepalive: Any) -> None:
        lifetime = self._lifetime
        if lifetime is None:
            raise RuntimeError("DuckDB relation is already closed")
        lifetime.attach_keepalive(keepalive)

    def _retains_resources(self, first: object, second: object) -> bool:
        lifetime = self._lifetime
        return lifetime is not None and lifetime.retains(first, second)

    def __arrow_c_stream__(self, requested_schema: object | None = None) -> Any:
        exporter = getattr(self._relation, "__arrow_c_stream__")
        if requested_schema is not None:
            with suppress(TypeError):
                return exporter(requested_schema)
        return exporter()

    def __len__(self) -> int:
        return len(self._relation)

    def __iter__(self):
        # Keep this proxy (and therefore its private connection) alive for the
        # complete lifetime of any iterator exposed by a DuckDB release.
        yield from self._relation

    def __getitem__(self, key: Any) -> Any:
        return self._wrap_relation_result(self._relation[key])

    def __contains__(self, value: Any) -> bool:
        return value in self._relation

    def __bool__(self) -> bool:
        return bool(self._relation)

    def __hash__(self) -> int:
        return hash(self._relation)

    def __eq__(self, other: object) -> bool:
        return bool(self._relation == self._unwrap_relation_argument(other))

    def __ne__(self, other: object) -> bool:
        return bool(self._relation != self._unwrap_relation_argument(other))

    def __dir__(self) -> list[str]:
        return sorted(set(super().__dir__()) | set(dir(self._relation)))

    def __format__(self, format_spec: str) -> str:
        return format(self._relation, format_spec)

    def __repr__(self) -> str:
        return repr(self._relation)

    def __str__(self) -> str:
        return str(self._relation)

    def close(self) -> None:
        """Release the relation graph, then its private connection."""
        self._relation = None
        state = self._finalizer_state
        if state is not None:
            state.relation = None
        lifetime = self._lifetime
        if lifetime is not None and state is not None and state.retained:
            lifetime.release()
            state.retained = False
            self._lifetime = None
        capsule = self._finalizer_capsule
        if capsule is not None and self._lifetime is None:
            acknowledge_prepared_finalizer_cleanup(capsule)
            self._finalizer_capsule = None
            self._finalizer_state = None

    def __del__(self) -> None:
        """Publish the relation and connection without closing on the GC thread."""
        try:
            if runtime_is_finalizing() or os.getpid() != getattr(self, "_pid", os.getpid()):
                return
            capsule = getattr(self, "_finalizer_capsule", None)
            state = getattr(self, "_finalizer_state", None)
            if capsule is None or state is None:
                return
            state.relation = getattr(self, "_relation", None)
            if defer_prepared_finalizer_cleanup(capsule):
                self._relation = None
                self._lifetime = None
                self._finalizer_capsule = None
                self._finalizer_state = None
        except BaseException:
            pass


def _duckdb_from_arrow_serial(duckdb: Any, value: Any) -> tuple[Any, Any | None]:
    """Bind Arrow to a private one-thread DuckDB connection."""
    connect = getattr(duckdb, "connect", None)
    if callable(connect):
        try:
            connection = connect(database=":memory:", config={"threads": 1})
        except TypeError:
            connection = connect(database=":memory:")
            execute = getattr(connection, "execute", None)
            if callable(execute):
                execute("SET threads=1")
        try:
            owner = _DuckDBConnectionOwner(connection)
        except BaseException:
            close = getattr(connection, "close", None)
            if callable(close):
                close()
            raise
        try:
            relation = connection.from_arrow(value)
        except BaseException:
            owner.close()
            raise
        try:
            return _OwnedDuckDBRelation(relation, owner), None
        except BaseException:
            relation = None
            owner.close()
            raise
    return duckdb.from_arrow(value), None
