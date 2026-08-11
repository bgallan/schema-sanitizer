# Pass 79 — external residency transactions and exact-owner hardening

## Implemented

- Serialized the external-runtime resident-identity / stack-debt subledger with an allocation-free `std::atomic_flag` writer gate.
- Moved validation of `stack_debt >= resident_identity` inside exclusive writer authority so validation applies to the state that is actually published.
- Made all four legacy individual residency mutations participate in the same writer transaction and mutation epoch.
- Replaced saturating resident/debt debits with checked fail-closed retirement; impossible resident underflow quarantines the residency domain.
- Re-check residency quarantine after acquiring the writer gate, closing the check-before-wait race.
- Publish compound residency updates in invariant-preserving order: debt growth before identity growth; identity shrink before debt shrink.
- Runtime diagnostics now read resident identity and stack debt under the same writer gate, eliminating torn diagnostic pairs.
- Added `ProcessPhysicalThreadPermitLease` and migrated native ordered-executor worker startup to RAII permit ownership. Thread-construction failure and normal worker exit now retire the permit through the owner destructor rather than manually paired amount releases.
- Marked production `OperationMemoryLedger` instances as requiring exact Python lease authority. `OperationMemoryLease.resize()` can no longer silently fall back to aggregate amount mutation for a real ledger.
- On the rare registration-failure + rollback-failure constructor path, retry exact lease registration before retaining the legacy amount-based finalizer fallback. This recovers authenticated ownership after transient allocation/control-plane faults while preserving historical focused-double compatibility.
- Added Pass79 source/fault tests covering writer serialization, post-gate validation, checked resident/debt debit, RAII native thread ownership, production resize authority, and exact registration recovery.

## Validation

- `python -m compileall -q src`: PASS.
- Pass69–Pass79 focused compatibility set used for this change: **79 passed**.
- Pass79-specific tests: **5 passed**.
- Full `tests/memory` collection cannot run in this environment because `schema_sanitizer._core_abi3` is not built/installed.
- Native CMake configuration cannot run in this environment: project requires CMake >= 4.3 while the available CMake is 3.31.6.

## Remaining compatibility boundary

The public/native ABI still exposes amount-based acquire/release functions for compatibility and Python governance. Internal ordered-executor workers now use RAII ownership. The emergency `OperationMemoryLease` amount finalizer remains only as a last-resort compatibility/recovery boundary when registration, rollback, and exact-registration recovery all fail; removing that final boundary completely requires a pre-published native/tokenized reservation identity rather than a Python-only publication change.
