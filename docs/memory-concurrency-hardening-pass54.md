# Memory & Concurrency Hardening — Pass 54

Pass 54 closes the remaining gaps found after Pass 53 where independently
bounded subsystems could still violate a process-wide lifetime, liveness, or
capacity invariant when composed under pressure.

## Core invariants added or strengthened

### 1. Native retained-byte backpressure is lock-safe

`OperationTaskArena::SubmitCharged` never waits for retained-byte capacity while
holding a worker queue mutex. Admission is performed as a transaction:

1. inspect/prepare the target;
1. release the queue mutex before waiting for retained-byte progress;
1. reacquire it after the epoch changes;
1. revalidate stop/worker state;
1. publish the task only after the byte reservation can commit.

This removes the producer/worker lock cycle where the worker that could free
retained bytes needed the same slot mutex held by the waiting producer.

CPU runnable admission is also taken before a worker commits a dequeue. A task
therefore remains visible in its queue while the worker is waiting for CPU
capacity, preserving fairness, stealing visibility, and queue backpressure.
Cross-worker stealing cannot skip a compatible broad task at the front merely
to reach a dedicated output task later in the same victim queue.

### 2. Async result memory is admitted before materialization

The bounded async scheduler now supports `expected_retained_bytes(index)` in
addition to exact `retained_bytes(result)` reconciliation. When a result size is
predictable, the scheduler reserves the expected resident credit before
`fetch()` runs, then reconciles the lease to the actual retained size before
publishing the result.

The fallback estimator no longer silently truncates large containers after its
bounded sample: unseen elements receive an extrapolated conservative charge.
Callers that know object/page/content sizes supply the preflight estimator.

### 3. Async terminal ownership is physically bounded

Terminal cancellation debt uses a preallocated capsule bank whose capacity is
at least the process-global async task capacity. It owns:

- outstanding tasks;
- the scheduler admission capability;
- the result queue;
- late result leases/drain state.

There is no unbounded `gather()` fallback. Cancellation has a bounded grace
period, and late results remain owned until workers actually terminate.

### 4. Async fairness is work-conserving

Operations retain a bounded guaranteed share but may borrow otherwise idle task
slots. Intentional first-pass fair-share shortfalls are not reported as hard
admission rejections; only the post-borrow shortfall is diagnosed.

### 5. `StageConcurrencyAdmission` is a real multidomain transaction

Additional domains have one deterministic global ordering. Acquisition follows
that order; rollback is reverse-order; close releases secondary domains before
publishing base memory/thread/control capacity as free. Remote I/O and async
task admissions now attach to this capability in production rather than only in
tests.

### 6. Shutdown is phase-aware and deadline-bounded

Async shutdown is split into:

1. close new async admission;
1. short initial grace drain;
1. close producers;
1. final async drain;
1. close consumers/remaining resources.

A timeout in an early grace phase is not a permanent failure if the final
snapshot is quiescent. Lifecycle waits in operation contexts, resources,
remote-prefetch startup, and coordinator handshakes use bounded deadlines; no
state-machine wait is allowed to block indefinitely.

### 7. Cgroup limits resolve the process's actual hierarchy

Python and native code resolve cgroup v1/v2 paths from `/proc/self/cgroup` and
`/proc/self/mountinfo` instead of assuming the process lives at the root of
`/sys/fs/cgroup`. Python caches the resolved view briefly and refreshes across
PID/cgroup migration. CPU, memory, PIDs, and pressure consumers use the
resolved paths.

### 8. Python and native threads share one physical-thread domain

All known project-owned physical thread creation paths consume the same native
process-global permit counter:

- governed Python `Thread` hosts;
- `OperationTaskArena` workers;
- native cleanup/reaper workers;
- private `OrderedExecutor` `JThread` workers.

Python reserves before irreversible `Thread.start()`, marks running/stopped
around the actual host lifetime, and releases exactly once after physical exit.
Native OS thread observation is fail-closed when it cannot be obtained, and is
used as reconciliation rather than as an independent admission counter.

Physical thread capacity also accounts conservatively for live cgroup memory
headroom/stack reservation instead of deriving capacity only from a static
memory ceiling.

### 9. Dynamic CPU shrink applies to a single wide arena

`ProcessCpuGovernor` refreshes current CPU capacity and does not bypass
admission merely because only one arena is registered. A single arena wider
than the current CPU capacity is governed too, so affinity/cgroup reductions
become effective without killing existing worker threads.

### 10. Terminal thread/FD debt avoids dynamic dictionaries

Governed-thread retirement debt and uncertain-FD-close debt use fixed-capacity
preallocated banks with bounded scans. Terminal ownership preservation no
longer depends on dictionary insertion or O(n) tuple construction under memory
pressure.

### 11. Native allocation metadata is a process-global slab

Live native allocation records share one fixed process-global slab. Pools do
not pre-reserve private registry tables; records are acquired on demand with
bounded two-choice probing and per-pool logical limits. This removes
first-created-pool capacity capture while retaining a hard physical metadata
bound.

If the process memory ceiling shrinks, new metadata admission is restricted
without making records created under the previous ceiling unreclaimable.

### 12. Remote sorting has governed scratch

Remote/provider discovery routes use a governed sort helper that reserves a
conservative scratch budget before invoking the in-memory sort. This closes the
remaining O(n) temporary-reference window left after Pass 53 bounded discovery
materialization.

### 13. Payload-pair certification no longer relies on a one-byte sentinel

The public pair bootstrap uses an operation-sized window derived from the real
I/O chunk budget, is explicitly marked as bootstrap, and is closed before
output payload observation. Payload certification requires observations from
real stage activity plus a native-core call while the operation capability is
active; bootstrap activity alone cannot satisfy the payload/release gate.

### 14. Finalizer paths remain noexcept during interpreter teardown

`SinkOutput` and `_ArrowStream` put even finalization/PID checks inside their
exception boundary so module-global teardown cannot leak `TypeError` from
`__del__`. Rich cleanup still runs only from governed safe points.

### 15. Speculative lookahead replacement transfers ownership first

When a prepared lookahead entry becomes stale because options changed, its
shared operation context is forked/transferred before the stale preparation is
closed and rebuilt. Replacement therefore does not transiently acquire a
second independent project-thread/memory domain.

## Validation added in Pass 54

`tests/memory/test_memory_safety_pass54.py` covers the new source and runtime
invariants, including:

- no retained-byte wait while holding a slot mutex;
- CPU admission before dequeue commit;
- async preflight/reconciliation and conservative estimation;
- bounded terminal async debt and work-conserving fairness;
- deterministic stage-domain ordering;
- real remote stage composition;
- stronger 56/56/native observation contract;
- bounded lifecycle waits and phase-aware shutdown;
- cgroup hierarchy resolution in Python and C++;
- shared Python/C++ physical-thread permit lifetime;
- coverage of every known native thread-creation path;
- fixed terminal debt banks;
- dynamic single-arena CPU limiting;
- process-global registry slab and governed sort scratch.

Historical scheduler probes were also updated so they distinguish **physical
arena width** from **simultaneously runnable CPU capacity**. They no longer
require deliberate CPU oversubscription merely to exercise stealing/fairness.
