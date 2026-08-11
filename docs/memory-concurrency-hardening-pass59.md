# Memory & concurrency hardening — Pass 59

Pass 59 closes second-generation ownership and liveness gaps left after Pass 58.
The focus is end-to-end resource authority: physical remote resources, logical
FD/permit credit, result ownership, native producer backpressure and
cross-process memory ceilings must all commit or retry as one coherent model.

## Strengthened invariants

1. **Remote provider cleanup is commit-after-physical-close**

   - `RemoteProviderSessionPool` no longer removes an entry or releases its FD
     and control-memory credit when `client.close()` / manager `__aexit__()`
     fails.
   - Failed entries remain in the authoritative pool mapping and the next pool
     close retries the exact resource.
   - Physical close has its own committed state, so a later failure while
     releasing a logical lease cannot cause the SDK resource to be closed twice.
   - Pool control-plane metadata is charged to operation memory and entry count
     has a hard ceiling even when no operation ledger is active.
   - Historical Pass 19 tests that required FD credit to be returned after a
     failed physical close were corrected to enforce the stronger invariant.

1. **Retryable cleanup across all remote/pair owners**

   - `_AzureServiceOwner` marks service and credential closure independently and
     only publishes `_closed` after both resources have committed.
   - A credential whose rollback close fails during Azure service construction
     is retained in a bounded terminal registry and retried by an owned Task.
     Cancellation cannot turn that live credential into a false shutdown
     commit.
   - `RuntimeConcurrencyPairAdmission.close()` preserves a failed admission and
     retries it; ContextVar reset and admission close have independent commit
     state.
   - `RemoteDirectoryDownloadSession` allocates local semaphore/control state
     before provider acquisition and clears its provider only after close
     succeeds.
   - Remote shutdown uses an explicitly owned cleanup Task plus `asyncio.wait`
     rather than `wait_for`, so cancellation-resistant provider cleanup cannot
     extend a nominal timeout indefinitely.

1. **Remote-I/O permits now include active FD admission**

   - Every submitted remote operation acquires governed active-connection FD
     credit before its remote-I/O permit.
   - Both owners are attached to the same stage domain and release
     transactionally.
   - The provider pool continues to account persistent/control-plane descriptors
     separately, preventing SDK connection activity from bypassing the process
     FD governor.
   - FD acquisition is non-blocking with respect to the coordinator event-loop:
     temporary pressure is polled asynchronously under a bounded deadline.

1. **No hidden Azure SDK concurrency**

   - Azure async blob download/upload paths force `max_concurrency=1`.
   - Cross-object/chunk parallelism therefore comes from Schema-Sanitizer's
     governed scheduler rather than an invisible SDK worker/connection fanout.

1. **Native retained-byte admission is memory-first and waiter-bounded**

   - `SubmitCharged()` obtains retained-byte credit before worker startup or
     queue-slot publication.
   - Producers waiting for bytes use a separate bounded waiter bank and therefore
     cannot consume `queued_total` as phantom queue slots.
   - Waiter-capacity rejection has its own observable metric.
   - Retained-byte release wakes the bounded waiter set. This is necessary for
     heterogeneous charges: a 50-byte waiter cannot consume the only wake while
     a 20-byte waiter that already fits remains asleep.
   - The retained epoch/CV transition remains serialized, preserving Pass 58's
     lost-wakeup fix.

1. **Backpressure deadline and per-saturation timeout are distinct**

   - The 30-second per-saturation hard timeout remains bounded.
   - A new `backpressure_deadline_millis_from()` preserves the operation's wider
     async timeout (up to 24h) instead of reusing the 30-second clipped helper.
   - All four production arena-construction paths publish both values.
   - Dynamic deadline/timeout setters continue to wake live waiters so shortened
     deadlines take effect immediately.

1. **Externally governed async results require proof**

   - `AsyncResultMemoryContract.EXTERNALLY_GOVERNED` is no longer documentary.
   - The contract supplies an `external_ownership_proof`; the scheduler refuses
     to release its bridge lease unless the target governor proves adoption.
   - Source discovery proves that `DiscoveredDirectoryInput` carries a live
     metadata owner before its scheduler bridge charge is released.

1. **Cross-process memory ceilings follow live owners**

   - Each contribution generation records the capacity observed by its owning
     operation.
   - Effective process capacity is the minimum ceiling among live owners, not a
     historical low-water mark.
   - Releasing the restrictive owner can safely widen the physical lease again.
   - New acquire/resize operations explicitly reject logical totals above the
     live-owner ceiling even when an older physical over-reservation means no
     immediate physical growth would otherwise occur.

1. **Cgroup discovery is streaming and allocation-bounded**

   - Python no longer materializes complete `/proc/self/cgroup` or
     `/proc/self/mountinfo` files with `read_text().splitlines()`.
   - Parsing is binary/streaming with explicit line, aggregate-byte and record
     ceilings.
   - Exceeding any parser envelope fails closed to an unknown cgroup view rather
     than allocating proportionally to an extreme mount table.

1. **Full-object byte materialization is explicitly bounded**

   - Internal S3, Azure and GCS `download_bytes` helpers require a positive
     `maximum_bytes` argument.
   - Shared bounded reader/chunk collectors reserve transient + retained memory
     before materialization and return lease-retaining bytes.
   - The old unbounded `await body.read()` / bytearray accumulation paths are no
     longer available as accidental future bypasses.

1. **Adversarial and sanitizer coverage**

   - Pass 59 adds retry tests for physical-vs-logical provider cleanup,
     pair-admission retry, result-ownership proof, live-owner capacity recovery,
     bounded proc parsing, FD/remote permit composition and native admission
     ordering.
   - The native sanitizer probe now includes a heterogeneous-backpressure case:
     two live blockers fill retained capacity, 50-byte and 20-byte producers
     wait, exactly 20 bytes are released, and only the fitting producer must
     progress while neither waiter occupies a queue slot.
   - The probe supports targeted `--case` execution so the new concurrency
     contracts can be validated independently of unrelated environment-sensitive
     rounds.

## Validation performed in this environment

- `tests/memory/test_memory_safety_pass54.py` through `pass59.py`: **86 passed**.
- Corrected Pass 19 provider-pool retry tests: **2 passed**.
- Expanded compatible hardening subset (Pass 19 excluding native-extension-only
  tests plus Pass 54–59): **102 passed, 5 deselected**. The deselected tests
  require `_core_abi3`, which is not built in this container.
- Modified Python tree: `compileall` / `py_compile` passed.
- `operation_task_arena.cc` and `prepare.cc`: C++23 syntax checks passed.
- The three modified Python-ABI arena construction units also pass C++23 syntax
  checks with the repository nanoarrow and system Python include paths.
- Targeted ThreadSanitizer executions pass with `halt_on_error=1` for both
  `arena_backpressure_deadline` and `arena_heterogeneous_backpressure`.
- The complete existing native probe reaches and passes all new Pass 59 cases,
  then still encounters the pre-existing environment-sensitive
  `shared_arena startup timed out: started=4` round in this container.
- `ruff` was not available in the container, so no Ruff result is claimed.
