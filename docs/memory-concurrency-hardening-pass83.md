# Pass 83 — control-plane exactness and interruption hardening

Pass 83 continues the ownership-token work from passes 80–82. The focus is no
longer aggregate native resource accounting alone: control-plane publications,
runtime probes and local FD state machines must also remain recoverable across
asynchronous exceptions.

## Implemented hardening

### 1. External runtime claim cardinality is membership-authoritative

`_EXTERNAL_RUNTIME_TOTAL_PHYSICAL_CLAIMS` and
`_EXTERNAL_RUNTIME_TOTAL_LOGICAL_CLAIMS` are now diagnostic mirrors only.
Admission and cleanup reconcile them from the exact per-pool claim dictionaries.
A signal between claim insertion/removal and scalar publication therefore cannot:

- make cleanup fail with a false aggregate underflow;
- make a live claim invisible to capacity accounting; or
- manufacture admission capacity.

The historical counters remain for bounded diagnostics/source compatibility, but
claim membership is the authority.

### 2. External claim finalizers do not block behind third-party configuration

Physical and logical detached-finalizer callbacks no longer wait indefinitely on
`config_inflight`. If configuration is active, they publish the exact claim at
absolute target zero (a tombstone) and return. The configuration owner drains all
zero-target claims after dropping the inflight latch.

This preserves bounded shutdown/finalizer progress even if a third-party runtime
callback is slow or stuck, while the coordinator dictionary continues to retain
conservative ownership until cleanup can commit.

### 3. Residency probes are generation validated

External runtime identity/stack-debt probes now use optimistic validation:

1. wait for a stable configuration generation;
1. capture `config_generation`;
1. execute arbitrary runtime probe callbacks outside project locks;
1. publish the sample only if the generation is still unchanged and stable;
1. otherwise retry the probe.

A sample obtained before a worker-pool reconfiguration can no longer overwrite
newer resident identity or stack-debt state.

### 4. FD open admission uses exact in-flight attempt identity

`FileDescriptorCapability` now owns a set of exact `_FdOpenAttempt` objects.
`_opening` remains only a compatibility/diagnostic mirror.

This removes the previous split update:

`_opening -= 1` -> asynchronous exception -> `_opened += 1`

Each attempt is retired idempotently by identity. If the exact receipt commit
raises after physically/accountingly opening, the receipt is queried while the
capability lock is held and the attempt is marked committed before cleanup.
Raw descriptors and all scandir variants therefore close the physical object and
retire exact receipt state without an over-abort path masking the primary error.

### 5. Deferred memory-ledger close is a post-commit tail

The exact child lease release is now irrevocably successful once its exact owner
has retired. If the subsequent deferred ledger-close tail fails (journal/fsync,
cross-process release, advisory handling), the child release does not raise a
replayable error.

Instead the ledger retains `_cross_process_release_deferred=True` and records the
post-release observation failure so `close()`/ledger finalization can retry the
host-wide tail without attempting to release the already-destroyed child owner.

### 6. Uncertain FD-close debt repairs observability from exact slots

The preallocated uncertain-close debt slots are authoritative. The scalar count
is rebuilt from occupied slots, including snapshots and duplicate-retention
retries. A duplicate exact slot also republishes the terminal owner
idempotently.

Thus an interruption after slot publication but before count/terminal-owner
publication remains conservative and self-repairing.

### 7. Exact ABI mutators return post-commit OCC state

Exact native mutators now return authoritative post-commit state in the same ABI
call:

- operation-memory reservation resize/release: `(generation, bytes)`;
- external-runtime permit resize: `(generation, amount)`;
- FD permit resize/open/close: `(generation, amount, opened)`.

Python consumes these results when available, avoiding a second metadata read
after the commit. Older ABI/test doubles remain supported through metadata-query
fallbacks. Generation remains the expected-generation OCC token passed into each
mutation.

### 8. Minor cleanup

Removed the duplicate `configured_width` assignment in the external runtime
configuration helper.

## Pass83 fault/regression coverage

`tests/memory/test_memory_safety_pass83.py` adds 12 tests covering:

1. physical claim cleanup with a stale-low aggregate mirror;
1. logical claim cleanup with a stale-low aggregate mirror;
1. non-blocking physical finalizer while configuration is inflight;
1. non-blocking logical finalizer while configuration is inflight;
1. residency probe retry after `config_generation` changes mid-probe;
1. exact memory release when the deferred close tail fails afterwards;
1. uncertain FD debt count/terminal-owner repair from an existing exact slot;
1. exact FD opening-attempt membership as authority;
1. config-owner draining of a finalizer tombstone;
1. external exact resize consuming returned post-commit state;
1. FD exact mutator consuming returned post-commit state;
1. ABI source contract for post-commit generation/state returns.

## Validation

- `tests/memory/test_memory_safety_pass60.py` through `pass83.py`:
  **285 passed, 1 skipped**.
- Pass83-specific tests: **12 passed**.
- `python -m compileall -q src tests/memory/test_memory_safety_pass83.py`: passed.
- Direct warning/syntax validation passed with
  `g++ -std=c++20 -Wall -Wextra -Werror -fsyntax-only` plus project, Python and
  vendored third-party includes for:
  - `cpp/src/api/python_abi3/runtime/ordered_executor_probe.cc`
  - `cpp/src/api/python_abi3/options/prepare.cc`
  - `cpp/src/api/python_abi3/_core_abi3_module.cc`
- The complete `pytest -q tests/memory` suite still cannot collect because
  `schema_sanitizer._core_abi3` is not built in this environment.
- Full CMake configuration is unavailable here because the repository requires
  CMake >= 4.3 while the environment provides CMake 3.31.6.
