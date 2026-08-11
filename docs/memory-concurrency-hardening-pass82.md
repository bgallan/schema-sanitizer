# Pass 82 — exact logical ownership, FD-open receipts, and ABA hardening

Pass 82 continues the pass80/pass81 transition from amount-derived cleanup to exact,
retry-idempotent resource ownership. The focus is the remaining split-commit windows
in logical external-runtime admission, operation-local thread borrowing, FD open/close
accounting, and deferred memory-ledger teardown.

## Changes

### Exact operation-local external-runtime borrows

- `_OperationThreadBorrowBudget` now owns authenticated child claims keyed by
  claim-id + capability instead of treating `_borrowed` as authoritative.
- `_OperationThreadBorrowLease` is the production child authority and supports
  exact target-based shrink/release.
- Each exact borrow pre-reserves a prepared finalizer cleanup slot before claim
  publication. `__del__` only hands the exact claim to the bounded safe-point
  cleanup path; it does not directly block on budget locks.
- Legacy amount borrowing remains isolated for historical private tests only and
  cannot consume exact claims.

### Shared external-runtime logical/physical claims

- Shared physical and logical claims are represented by prearmed exact owners before
  their coordinator claim is published. If an asynchronous unwind occurs after claim
  commit but before the caller receives the wrapper, detached cleanup can still set the
  claim to target zero.
- Partial logical-envelope publication is rolled back completely. A released governor
  lease can no longer remain paired with a positive `logical_width` and be reused by a
  later operation.
- Logical release/shrink mirrors are reconciled from the underlying exact governor
  owner. A retry after `lease.release()` committed but before coordinator metadata was
  cleared recognizes that target zero already committed instead of replaying release or
  reporting false corruption.
- Native physical mirrors are likewise refreshed from the exact native receipt before
  admission/resizing.
- External-runtime cleanup treats owner existence as authority. `native_amount`,
  `physical_amount`, `logical_width`, and borrowed counts are mirrors/diagnostics, not
  predicates deciding whether cleanup is required.
- Exact native cleanup uses absolute target zero rather than stale release deltas.

### FD receipt owns both permits and physical-open state

- The ABI3 FD receipt now exposes exact metadata `(receipt_id, generation, amount, opened)` plus authenticated `mark_opened` / `mark_closed` mutators.
- `ProcessFdPermitLease.opened()` is therefore part of the same authority that owns the
  permits. A receipt cannot shrink below descriptors it still proves open.
- `FileDescriptorCapability` reconciles its Python `_opened` mirror from the exact
  receipt before admission/release.
- If an asynchronous exception lands after native open-accounting committed but before
  Python publishes its mirror, the context-manager close path detects the unmirrored
  exact open and retires it after the OS descriptor is proven closed.
- Native global FD-open accounting remains canonical; the Python counter is a mirror
  used for source-only compatibility and diagnostics.

### Deferred memory close completion

- Releasing the final exact `OperationMemoryLease` now invokes
  `_maybe_finish_deferred_close()`.
- A `close()` that deferred host-wide/cross-process ownership while exact leases were
  alive therefore completes automatically when the final owner disappears; callers do
  not need a second explicit `close()`.

### Receipt provenance and real generation validation

- Memory, external-runtime permits, and FD permit receipts expose metadata including
  monotonic receipt identity and generation.
- Python exact mutators fetch the current metadata and send `expected_generation` with
  the mutation. Native mutators reject stale generations before changing authority.
- Receipt ID allocators fail closed at exhaustion and never wrap/reuse zero.
- Generation counters also fail closed before `UINT64_MAX` would wrap, preserving zero
  as the optional-argument sentinel and preventing stale/ABA protection from silently
  disappearing.
- Fork provenance guards from pass81 remain enforced at the native receipt boundary.

## Fault-injection / regression coverage

`tests/memory/test_memory_safety_pass82.py` adds 11 tests covering:

1. exact operation-thread borrow cleanup after owner loss;
1. rollback after logical envelope publication but before claim commit;
1. physical shared claim loss before caller handoff;
1. exact-owner cleanup when the mirrored amount is deliberately zero;
1. automatic completion of a deferred memory close;
1. FD receipt ownership of `opened` and prevention of release while open;
1. interruption immediately after exact FD-open accounting commit;
1. propagation of expected generation into external permit mutation;
1. fail-closed receipt-ID/generation exhaustion and stale-generation guards;
1. mirror reconstruction from exact owners;
1. logical target-zero retry after the underlying governor release already committed.

## Validation

- `tests/memory/test_memory_safety_pass60.py` through `pass82.py`:
  **273 passed, 1 skipped**.
- Pass82-specific tests: **11 passed**.
- `python -m compileall -q src tests/memory/test_memory_safety_pass82.py`: passed.
- Direct syntax/warning validation passed for all modified ABI translation units with:
  `g++ -std=c++20 -Wall -Wextra -Werror -fsyntax-only` (plus project/Python includes):
  - `cpp/src/api/python_abi3/runtime/ordered_executor_probe.cc`
  - `cpp/src/api/python_abi3/options/prepare.cc`
  - `cpp/src/api/python_abi3/_core_abi3_module.cc`
- The complete `pytest -q tests/memory` suite still cannot collect because
  `schema_sanitizer._core_abi3` has not been built in this environment.
- Full project CMake configuration remains unavailable here: the repository requires
  CMake >= 4.3 while the environment provides CMake 3.31.6.
