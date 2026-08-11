# Pass 77 — corruption quarantine and one-shot fork-state hardening

Pass 77 generalizes the invariant introduced in pass 76: once authoritative
accounting disagrees with an auxiliary cache/counter, **new admission closes
irreversibly while exact cleanup authority remains usable**.

## Changes

- Async task-domain leases now retire slot and operation ownership independently.
  Counter disagreement latches `_ASYNC_CORRUPTED`, closes admission, and keeps
  unresolved lease components retryable. An incomplete `release()` raises so
  `StageConcurrencyAdmission` retains the exact domain capability instead of
  dropping it after a partial cleanup.
- `TerminalOwnershipLedger` treats its fixed slot bank as the authority.
  `_owners` is an advisory cache; mismatch quarantines publication while exact
  slot/category retirement remains legal.
- The static control-plane registry recomputes its bounded authoritative total
  from `_ENTRIES`. Aggregate mismatch latches corruption, closes reserve(), and
  exact rollback repairs the aggregate cache without clearing the corruption
  latch. Read-side byte accounting also returns the authoritative total.
- Native `OperationMemoryLedger` stores bytes and the irreversible corruption
  latch in one `std::atomic<uint64_t>` state word. An over-release atomically
  commits both the reduced byte count and quarantine bit, so no concurrent
  reserve CAS can slip between accounting corruption and admission closure.
  Cleanup remains legal after quarantine.
- `fork_manager` centrally suppresses managed prepared-swap callbacks beyond
  the two preallocated fork generations, preventing A/B/A ancestor-state reuse
  throughout managed runtime state.
- The direct `fork_safety` bootstrap has the same third-generation guard, so its
  own two-lock bank cannot recycle an ancestor-active lock outside the manager.
- Partition-resource and provider-session `ContextVar` replacements are now
  preallocated in two banks at import time; their `before_fork` callbacks only
  select references.
- `concurrency_contracts.before_fork` no longer clears or mutates child-bank
  dictionaries. Each bank is one-shot in a child lineage and arrives fresh.

## Regression coverage

`tests/memory/test_memory_safety_pass77.py` fault-injects each corruption class,
checks cleanup-after-quarantine and stage-broker retry rooting, verifies the
third nested fork is suppressed in both the dispatcher and bootstrap, and
validates that at-fork preparation no longer constructs `ContextVar`s or clears
contract maps.

When `_core_abi3` is available, the pass77 suite also executes the native
readmission regression through the Python ABI. In the current validation host,
the package build backend could not run because CMake 3.31 is installed while
the project requires CMake 4.3; the modified native source was therefore also
validated directly with the system compiler and a standalone linked smoke test.
