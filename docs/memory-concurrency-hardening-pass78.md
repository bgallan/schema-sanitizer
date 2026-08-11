# Concurrency / memory hardening pass 78

Pass 78 extends the fail-closed ownership model introduced in passes 76–77. The core rule is now applied to more resource domains: **derived counters never become release authority; a mismatch quarantines new admission while exact owners retain cleanup authority**.

## Changes

### Process thread / file-descriptor governor

- Rebuilds `_in_use`, external, and teardown counters from authenticated active leases before admission-sensitive decisions.
- Any mismatch is repaired from exact ownership, recorded, and latches a sticky admission quarantine.
- Exact lease shrink/release remains available after quarantine so cleanup cannot be stranded.

### Dynamic control-plane budget

- `_owners` is authoritative; `_reserved` and `_active` are verified/rebuilt caches.
- Counter divergence latches corruption and blocks new tickets while exact ticket release remains valid.
- Fork child reset roots inherited lock/owner/native-shadow state before swapping to prepared state.

### Operation memory lease resize

- Each Python lease now tracks logical size separately from a conservative physical high-watermark.
- Logical shrink no longer releases aggregate physical bytes before capability authority changes.
- Growth only reserves bytes above the physical high-watermark; final capability retirement releases the retained physical charge.
- This removes the async-exception split-commit window where one lease could release aggregate bytes still owned by another lease.

### Temporary storage governor

- Device aggregate counters are verified against exact capability ownership before admission.
- Legacy amount-based reservations use a separate explicit legacy subledger for compatibility.
- Legacy releases can only debit legacy authority and cannot consume bytes owned by an exact capability.
- Cross-process cache values are reconciled against the exact cross-process account and corruption fails closed.
- If lower-layer cross-process release commits exact authority and then raises, higher-level capability authority also commits before the original exception is propagated, preventing retry from stealing another owner.

### Cross-process storage

- Release commits process-local exact authority before the fallible shared-journal tail; failures leave conservative shared debt and require reconciliation.
- Growth remains host-first and marks reconciliation pending if interrupted before local publication.
- Recovery only shrinks stale host authority; it never manufactures a larger host contribution.
- Reconciliation is process+device aware: it sums every authenticated local account for that device, so recovery of one account cannot erase a sibling account's ownership.
- Same-process account transitions are serialized with the bounded registry lock because the host journal stores one aggregate process+device record.

### Cleanup dispatcher and retry accounting

- CleanupDispatcher rebuilds owned/subsystem count and byte caches from its exact `_owned_index`; mismatch opens a sticky circuit and rejects new admission while cleanup drains.
- ReleaseGuardian rebuilds retained-byte accounting from exact retained items; underflow no longer resets retained bytes to zero.
- RetryScheduler reconstructs global and per-subsystem byte/count accounting from exact pending/ready/active/successor/emergency owner mappings and pauses admission on divergence.

### Fork hardening

- Fork quarantine now has two physically separate, one-shot generation slabs.
- Labels are deduplicated within a generation, not across generations, so the second nested child can root state inherited from the first child even when handler labels are identical.
- Third and later nested generations remain inert/fail-closed.
- Additional prepared-swap handlers root inherited locks/containers before installing fresh child banks.

### Native thread / FD permits

- Detected amount underflow latches sticky native corruption for thread or FD permits and blocks subsequent acquisition.
- Acquisition rechecks quarantine after successful CAS. A grant racing with quarantine is never exposed to the caller; it is retained as conservative terminal debt instead of amount-rollback that could debit another owner.
- Thread aggregate/subledger publication remains conserving under the mutation epoch.
- Public amount-based ABI is retained for compatibility; exact identity remains enforced by the higher-level lease/RAII owners.

## Regression coverage

`tests/memory/test_memory_safety_pass78.py` adds fault injection for:

- governor low-counter corruption and cleanup after quarantine;
- dynamic control-plane cache corruption;
- operation-memory shrink interruption safety;
- temporary-storage low-counter corruption;
- cleanup-dispatcher cache corruption;
- ReleaseGuardian and RetryScheduler derived-accounting corruption;
- cross-process storage post-authority failure and reconciliation;
- same-device sibling cross-process accounts during reconciliation;
- generation-scoped fork quarantine;
- native permit sticky quarantine source contract;
- legacy temporary-storage release isolation from exact capabilities;
- cross-process temporary-storage tail failure after lower exact commit.

## Validation in this environment

- Hardening regression passes 54–78: **329 passed, 1 skipped**.
- Pass 78 regression file: **13 passed**.
- `python -m compileall -q src/schema_sanitizer`: pass.
- `python meta/ci/check_primary_cleanup.py`: pass.
- `c++ -std=c++20 -Icpp/src -Icpp/include -fsyntax-only cpp/src/internal/runtime/operation_task_arena.cc`: pass with GCC 14.2.
- The one cumulative skip requires the compiled `_core_abi3` extension. This environment has CMake 3.31.6 while the project requires CMake 4.3, so the ABI3 target cannot be built here.
- A broader selection of older memory tests has the same 16 pre-existing failures on pass 77 and pass 78 in this environment (native-core absence plus historical source-contract/stub incompatibilities); no additional failures were introduced by pass 78.
