"""Allocation-bounded registry for finalizer domains used during shutdown.

Domains register during normal runtime.  Shutdown freezes the registry before
terminal teardown and subsequently reads the prebuilt immutable view, so no new
finalizable subsystem can appear after quiescence has been established.
"""

from __future__ import annotations

from dataclasses import dataclass
from threading import Lock
from typing import Any, Callable

from .callable_contract import callable_contract
from .finalizer_escrow import ReservedFinalizerEscrow
from .fork_safety import quarantine_inherited_state


@dataclass(frozen=True, slots=True)
class FinalizerRuntimeDomain:
    """One loaded finalizer domain and its allocation-safe observation hooks."""

    name: str
    drain: Callable[[], int]
    snapshot: Callable[[], object]
    escrows: tuple[tuple[str, ReservedFinalizerEscrow[Any]], ...] = ()
    contract_generation: int = 1


_MAX_FINALIZER_DOMAINS = 128
_REGISTRY_LOCK = Lock()
_FORK_LOCK_BANK = (Lock(), Lock())
_FORK_LOCK_BANK_INDEX = 0
_FORK_FRESH_LOCK: Lock | None = None
_REGISTRY: list[FinalizerRuntimeDomain] = []
_REGISTRY_NAMES: dict[str, FinalizerRuntimeDomain] = {}
_REGISTRY_EPOCH = bytearray(8)
_REGISTRY_CORRUPTED = False
_REGISTRY_FROZEN = False
_FROZEN_DOMAINS: tuple[FinalizerRuntimeDomain, ...] | None = None
_FROZEN_ESCROWS: tuple[tuple[str, ReservedFinalizerEscrow[Any]], ...] | None = None
_FROZEN_ACTIVITY_NATIVE_CAPSULES: tuple[object, ...] | None = None


def _fixed_increment(counter: bytearray) -> bool:
    """Increment a fixed-width little-endian counter without wrapping."""
    if all(value == 255 for value in counter):
        return False
    for index, value in enumerate(counter):
        if value != 255:
            counter[index] = value + 1
            return True
        counter[index] = 0
    return False


def _fixed_value(counter: bytearray) -> int:
    return int.from_bytes(counter, "little", signed=False)


def _write_fixed(counter: bytearray, target: bytearray, offset: int) -> int:
    for value in counter:
        target[offset] = value
        offset += 1
    return offset


def _same_callable_contract(
    left: Callable[..., object] | None, right: Callable[..., object] | None
) -> bool:
    return callable_contract(left) == callable_contract(right)


def _same_escrow_contract(
    left: tuple[tuple[str, object], ...], right: tuple[tuple[str, object], ...]
) -> bool:
    if len(left) != len(right):
        return False
    for (left_name, left_escrow), (right_name, right_escrow) in zip(left, right):
        left_type = type(left_escrow)
        right_type = type(right_escrow)
        if left_name != right_name:
            return False
        if (left_type.__module__, left_type.__qualname__) != (
            right_type.__module__,
            right_type.__qualname__,
        ):
            return False
        if getattr(left_escrow, "capacity", None) != getattr(right_escrow, "capacity", None):
            return False
    return True


def _same_domain(left: FinalizerRuntimeDomain, right: FinalizerRuntimeDomain) -> bool:
    return (
        left.name == right.name
        and left.contract_generation == right.contract_generation
        and _same_callable_contract(left.drain, right.drain)
        and _same_callable_contract(left.snapshot, right.snapshot)
        and _same_escrow_contract(left.escrows, right.escrows)
    )


def register_finalizer_domain(
    name: str,
    *,
    drain: Callable[[], int],
    snapshot: Callable[[], object],
    escrows: tuple[tuple[str, ReservedFinalizerEscrow[Any]], ...] = (),
    contract_generation: int = 1,
) -> None:
    """Register a domain once while normal runtime allocation is still allowed."""
    global _REGISTRY_CORRUPTED
    if not name or not callable(drain) or not callable(snapshot):
        raise ValueError("invalid finalizer runtime domain")
    if type(contract_generation) is not int or contract_generation <= 0:
        raise ValueError("finalizer domain contract_generation must be a positive exact integer")
    domain = FinalizerRuntimeDomain(name, drain, snapshot, escrows, contract_generation)
    with _REGISTRY_LOCK:
        existing = _REGISTRY_NAMES.get(name)
        if existing is not None:
            if existing is domain or (
                existing.drain is domain.drain
                and existing.snapshot is domain.snapshot
                and existing.escrows == domain.escrows
            ):
                return
            if not _same_domain(existing, domain):
                raise RuntimeError(
                    f"finalizer domain {name!r} was re-registered with different ownership hooks"
                )
            if _REGISTRY_FROZEN:
                raise RuntimeError("finalizer registry is frozen for runtime shutdown")
            if not _fixed_increment(_REGISTRY_EPOCH):
                _REGISTRY_CORRUPTED = True
                raise RuntimeError("finalizer registry epoch exhausted")
            # A semantic module reload must replace stale callbacks/escrows;
            # retaining the old domain would make new owners invisible to shutdown.
            for index, item in enumerate(_REGISTRY):
                if item.name == name:
                    _REGISTRY[index] = domain
                    break
            _REGISTRY_NAMES[name] = domain
            return
        if _REGISTRY_FROZEN:
            raise RuntimeError("finalizer registry is frozen for runtime shutdown")
        if len(_REGISTRY) >= _MAX_FINALIZER_DOMAINS:
            raise RuntimeError("finalizer registry capacity exhausted")
        if not _fixed_increment(_REGISTRY_EPOCH):
            _REGISTRY_CORRUPTED = True
            raise RuntimeError("finalizer registry epoch exhausted")
        _REGISTRY.append(domain)
        try:
            _REGISTRY_NAMES[name] = domain
        except BaseException:
            if _REGISTRY and _REGISTRY[-1] is domain:
                _REGISTRY.pop()
            raise


def freeze_finalizer_registry() -> tuple[FinalizerRuntimeDomain, ...]:
    """Freeze registration and prebuild immutable shutdown views exactly once."""
    global _REGISTRY_FROZEN, _FROZEN_DOMAINS, _FROZEN_ESCROWS, _FROZEN_ACTIVITY_NATIVE_CAPSULES
    with _REGISTRY_LOCK:
        if _FROZEN_DOMAINS is None:
            domains = tuple(_REGISTRY)
            escrows: list[tuple[str, ReservedFinalizerEscrow[Any]]] = []
            for domain in domains:
                escrows.extend(domain.escrows)
            frozen_escrows = tuple(escrows)
            capsules: list[object] = []
            for domain in domains:
                for _name, reserved_escrow in domain.escrows:
                    for counter in reserved_escrow.activity_counters():
                        capsule = counter.native_capsule
                        if capsule is None:
                            raise RuntimeError("finalizer activity counter lacks native authority")
                        capsules.append(capsule)
            frozen_capsules = tuple(capsules)
            # Publish every immutable view only after all allocations succeeded.
            _FROZEN_DOMAINS = domains
            _FROZEN_ESCROWS = frozen_escrows
            _FROZEN_ACTIVITY_NATIVE_CAPSULES = frozen_capsules
        _REGISTRY_FROZEN = True
        return _FROZEN_DOMAINS


def finalizer_registry_frozen() -> bool:
    """Report whether finalizer-domain registration has been frozen."""
    with _REGISTRY_LOCK:
        return _REGISTRY_FROZEN


def finalizer_domains() -> tuple[FinalizerRuntimeDomain, ...]:
    """Return the frozen view during teardown, otherwise a detached live view."""
    frozen = _FROZEN_DOMAINS
    if frozen is not None:
        return frozen
    with _REGISTRY_LOCK:
        return tuple(_REGISTRY)


def finalizer_registry_epoch() -> int:
    """Return the fixed-width registry epoch for diagnostics."""
    with _REGISTRY_LOCK:
        return _fixed_value(_REGISTRY_EPOCH)


def finalizer_activity_token() -> tuple[object, ...]:
    """Return an ABA-resistant token spanning every currently loaded domain.

    Publication/progress epochs are backed by fixed-width escrow counters.  Any
    allocation failure while constructing this diagnostic token propagates to
    shutdown, which treats an unobservable domain as non-quiescent.
    """
    domains = finalizer_domains()
    with _REGISTRY_LOCK:
        registry_epoch = _fixed_value(_REGISTRY_EPOCH)
        registry_corrupted = _REGISTRY_CORRUPTED
    token: list[object] = [registry_epoch, registry_corrupted]
    for domain in domains:
        publication_epoch = 0
        progress_epoch = 0
        active = 0
        publication_failures = 0
        for _name, escrow in domain.escrows:
            snapshot = escrow.capacity_snapshot()
            active += snapshot.active
            publication_failures += snapshot.publication_failures
            publication_epoch += snapshot.publication_epoch
            progress_epoch += snapshot.progress_epoch
        token.append(
            (
                domain.name,
                active,
                publication_failures,
                publication_epoch,
                progress_epoch,
            )
        )
    return tuple(token)


def finalizer_activity_is_quiescent(token: tuple[object, ...]) -> bool:
    """Return true only when the token proves there are zero publicable owners."""
    if len(token) < 2 or bool(token[1]):
        return False
    for item in token[2:]:
        if not isinstance(item, tuple) or len(item) < 5:
            return False
        if item[1] != 0 or item[2] != 0:
            return False
    return True


_ACTIVITY_RECORD_BYTES = 8 + 8 + 8 + 8
_REGISTRY_ACTIVITY_BYTES = 8 + 1


def finalizer_activity_buffer_size() -> int:
    """Return exact bytes needed for allocation-free quiescence observation."""
    domains = finalizer_domains()
    records = 0
    for domain in domains:
        records += len(domain.escrows)
    return _REGISTRY_ACTIVITY_BYTES + records * _ACTIVITY_RECORD_BYTES


def write_finalizer_activity_into(target: bytearray) -> bool:
    """Fill a preallocated activity buffer and report immediate quiescence.

    When the ABI is available all atomic counters are copied in one native call
    directly into ``target``. No counter value is materialized as ``PyLong``.
    """
    required = finalizer_activity_buffer_size()
    if len(target) != required:
        raise ValueError("finalizer activity buffer has wrong size")
    with _REGISTRY_LOCK:
        offset = _write_fixed(_REGISTRY_EPOCH, target, 0)
        corrupted = _REGISTRY_CORRUPTED
        target[offset] = 1 if corrupted else 0
        offset += 1
        domains = _FROZEN_DOMAINS
        capsules = _FROZEN_ACTIVITY_NATIVE_CAPSULES
        if domains is None or capsules is None:
            return False
    quiescent = not corrupted
    from .native_runtime import native_core

    return bool(native_core.atomic_epoch_write_activity(capsules, target, offset)) and quiescent


def registered_finalizer_escrows() -> tuple[tuple[str, ReservedFinalizerEscrow[Any]], ...]:
    """Return every process-global reserved escrow from currently loaded domains."""
    frozen = _FROZEN_ESCROWS
    if frozen is not None:
        return frozen
    with _REGISTRY_LOCK:
        result: list[tuple[str, ReservedFinalizerEscrow[Any]]] = []
        for domain in _REGISTRY:
            result.extend(domain.escrows)
        return tuple(result)


def _reset_finalizer_registry_for_tests() -> None:
    """Reopen registration only for isolated test-runtime resets."""
    global _REGISTRY_FROZEN, _FROZEN_DOMAINS, _FROZEN_ESCROWS
    with _REGISTRY_LOCK:
        _REGISTRY_FROZEN = False
        _FROZEN_DOMAINS = None
        _FROZEN_ESCROWS = None


def _prepare_registry_for_fork() -> None:
    global _FORK_FRESH_LOCK
    _FORK_FRESH_LOCK = _FORK_LOCK_BANK[_FORK_LOCK_BANK_INDEX]


def _clear_registry_fork_preparation() -> None:
    global _FORK_FRESH_LOCK
    _FORK_FRESH_LOCK = None


def _reset_registry_after_fork() -> None:
    global _REGISTRY_LOCK, _FORK_FRESH_LOCK, _FORK_LOCK_BANK_INDEX
    prepared = _FORK_FRESH_LOCK
    if prepared is None:
        return
    quarantine_inherited_state("finalizer-registry", _REGISTRY_LOCK)
    _REGISTRY_LOCK = prepared
    _FORK_FRESH_LOCK = None
    _FORK_LOCK_BANK_INDEX = 1 - _FORK_LOCK_BANK_INDEX


from .fork_manager import register_fork_handler as _register_fork_handler  # noqa: E402

_register_fork_handler(
    "finalizer-registry",
    before=_prepare_registry_for_fork,
    after_in_parent=_clear_registry_fork_preparation,
    after_in_child=_reset_registry_after_fork,
)


__all__ = [
    "FinalizerRuntimeDomain",
    "finalizer_activity_buffer_size",
    "finalizer_activity_is_quiescent",
    "finalizer_activity_token",
    "write_finalizer_activity_into",
    "finalizer_domains",
    "finalizer_registry_epoch",
    "finalizer_registry_frozen",
    "freeze_finalizer_registry",
    "register_finalizer_domain",
    "registered_finalizer_escrows",
]
