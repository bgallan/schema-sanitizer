# Pass 73 — concurrency and memory hardening

Pass 73 closes post-commit ownership gaps found during the pass72 audit. The central invariant is now explicit: once a primary resource release commits, any remaining finalizer/control-plane work may only acknowledge secondary bookkeeping and must never replay the primary release.

## Main changes

- Added `acknowledge_prepared_finalizer_cleanup()` as an ACK-only prepared-finalizer transition. It disarms the primary callback before exact escrow retirement, so failed retirement can be retried safely through GC/safe-point publication.
- Applied ACK-only finalizer retirement to Remote I/O permits/submission reservations/capacity registrations, Provider request leases, and process-resource leases.
- Made ProviderThrottle and process-resource control-plane retirement a separate retryable commit. Exact ledger entries remain rooted with `resource_released=True` until their control ticket is retired; retries cannot return capacity twice.
- Hardened ProviderThrottle state eviction so a state is never removed when its control-plane ticket did not confirm retirement.
- Hardened aggregate cross-process memory:
  - finalizer-ticket retirement falls back to a preallocated ACK sentinel;
  - explicit `resize`/`release` fail closed on stale exact capability instead of silently succeeding;
  - local reservation state changes only after the coordinator commit;
  - construction rollback no longer discards an unretired finalizer generation.
- Hardened stage-admission construction escrow with an ACK-only sentinel when reserved-ticket retirement cannot commit.
- Reworked path-claim admission state to distinguish uncommitted construction from counted live admission; removed the saturating decrement and added ACK-only finalizer retirement for rollback/release tails.
- Rooted temporary-storage resize replacement capability before post-commit validation and moved lower-level capability cleanup outside the pool condition lock.
- Reworked release-guardian control-ticket retirement so a successful primary release remains rooted and retries only the secondary control-plane tail.
- Replaced saturating decrements on quiescence-critical counters with checked decrements plus sticky protocol-violation state in:
  - temporary janitor;
  - remote chunk prefetch lifecycle;
  - cleanup dispatcher;
  - retry scheduler / release guardian;
  - provider session key gates;
  - Remote I/O callback barriers;
  - temporary-storage resize inflight tracking.
    Clean shutdown can no longer be reported after a protocol underflow was observed.

## Regression coverage

Added `tests/memory/test_memory_safety_pass73.py` with fault injection for:

- prepared-finalizer ACK failure after primary commit;
- Remote I/O permit secondary-tail retry without double release;
- ProviderThrottle control-ticket failure without double decrement;
- cross-process stale capability fail-closed semantics;
- cross-process ACK-sentinel transfer;
- stage-construction ACK-sentinel transfer;
- path-claim construction rollback without false decrement;
- removal of saturating quiescence decrements;
- temporary-storage replacement rooting / out-of-lock cleanup.

## Validation performed in this source-only environment

- `python -m compileall -q src tests` — PASS
- `python meta/ci/check_primary_cleanup.py` — PASS
- pass57 through pass73 memory-safety suites in one process — **230 passed**, 2 Python `fork()` deprecation warnings
- pass46 focused exact-capability/finalizer regression subset — **7 passed**
- pass68 through pass72 were also run independently before the combined gate — all PASS

The environment does not contain the compiled `schema_sanitizer._core_abi3` extension. Older tests whose sole failure is direct native-core import therefore remain unavailable here. A pass48 compatibility test that already fails in the unmodified pass72 source (legacy tuple return from a test double versus the newer registration object contract) is likewise not introduced by pass73.
