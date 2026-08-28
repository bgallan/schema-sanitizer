# Concurrency lifecycle invariants

This document is the canonical implementation contract for memory ownership,
concurrency admission, finalization, and shutdown. It describes the current
model by invariant and replaces version-by-version hardening notes. Modules
ending in `_impl`, native ABI details, and the types named here are internal and
are not public compatibility API.

For configuration and supported runtime behavior, see
[Resources and concurrency](../operations/resources-and-concurrency.md).

## Index

- [Authority model](#authority-model)
- [Acquisition and publication transactions](#acquisition-and-publication-transactions)
- [Transfers and native receipts](#transfers-and-native-receipts)
- [Rooted finalizers and replay safety](#rooted-finalizers-and-replay-safety)
- [Generations, bounded state, and corruption](#generations-bounded-state-and-corruption)
- [Resident memory and cross-process coordination](#resident-memory-and-cross-process-coordination)
- [Temporary storage and physical artifacts](#temporary-storage-and-physical-artifacts)
- [Threads, file descriptors, and external runtimes](#threads-file-descriptors-and-external-runtimes)
- [Schedulers, backpressure, and retained results](#schedulers-backpressure-and-retained-results)
- [Cgroups, fork, and process provenance](#cgroups-fork-and-process-provenance)
- [Shutdown and quiescence](#shutdown-and-quiescence)
- [Validation contract](#validation-contract)

## [Authority model](#index)

Every governed resource is represented by an exact owner. The owner may be a
Python capability, a native RAII receipt, a bounded generation slot, or a
ledger entry authenticated by identity, generation, owner process, and a
private capability. It is created before the resource can escape and remains
reachable until physical and logical cleanup commits.

Counters, byte amounts, mapping sizes, queue indexes, and diagnostic mirrors
are not release authority. They are checked or reconstructed from exact owners.
Stale mirrors may reduce capacity while being reconciled, but they must never:

- release a different owner's resource;
- make a live owner invisible;
- admit work beyond exact capacity; or
- turn an already committed release into a replayable debit.

Release is target-based and idempotent wherever an interruption can occur. A
successful retry either reaches the same target or acknowledges that the exact
owner already reached it. Amount-only mutation remains confined to explicit
compatibility or lower-level implementation boundaries and cannot consume an
exact owner's authority.

The universal failure rule is:

> Before the primary commit, failure leaves the complete exact owner intact.
> After the primary commit, any remaining work is a separately owned,
> retryable tail and cannot repeat the primary effect.

## [Acquisition and publication transactions](#index)

Construction allocates wrappers, index capacity, finalizer capacity, callback
storage, and diagnostic result objects before the first irreversible resource
commit. After a commit, the authoritative tail mutates pre-existing storage or
returns a preconstructed result; it does not depend on a new tuple, mapping
entry, callback, or Python integer allocation.

`CompositeParallelAdmission` and `StageConcurrencyAdmission` compose resource
domains in one transaction. The published order is resident memory, physical
execution capacity, control plane, runnable CPU, remote I/O, asynchronous task
capacity, and provider-specific capacity. Extension domains have a stable order
after the built-ins. Cleanup runs in reverse and retains the complete suffix if
one release fails.

Memory is acquired before helper-worker capacity. Cleanup returns the worker
before its protecting memory credit, preventing that credit from being
readmitted while the helper is still live. Stage construction reserves a
bounded rooted rollback owner before acquiring any domain. Each acquired lease
is immediately attached to an existing owner slot; it never exists only in a
temporary list awaiting publication.

Thread start, file open, remote client creation, provider configuration, queue
claim, and result publication each have an explicit commit point:

- registry authority and capacity are reserved before `Thread.start()`;
- an FD reservation exists before physical open, and opened state is committed
  only after open succeeds;
- remote cleanup escrow exists before client or credential construction;
- queue storage and delivery callbacks exist before removing a waiter from its
  authoritative queue;
- expected result bytes are reserved before fetch or materialization when the
  size is knowable;
- publication after an irreversible effect is no-throw or transfers its exact
  owner to a bounded recovery domain.

Telemetry, notification, and progress reporting are derived work. Failure to
publish them records retry debt or dirty level-triggered state; it cannot undo a
committed resource transition.

## [Transfers and native receipts](#index)

Ownership transfer changes the authenticated owner without changing the
physical charge. The predecessor becomes inert only when the exact ledger
confirms that the successor owns the same generation. If interruption occurs
around the swap, authoritative inspection determines which rooted owner remains
armed; both may stay reachable conservatively, but only one capability can
authenticate.

Native commits that can outlive the calling Python bytecode return an owning
receipt in the same call:

- operation-memory reservations;
- external-runtime physical-thread permits;
- file-descriptor permits and physical-open state; and
- native managed-thread permits held by RAII objects.

Receipt construction is transactional with admission. Mutation uses an
absolute target and expected generation, and returns post-commit generation and
state from preallocated Python result objects. Receipts carry monotonic identity
and creator PID; IDs and generations fail closed instead of wrapping. Receipt
destruction after `fork()` is harmless, while mutation or inspection from the
wrong process is rejected.

Python diagnostic views expose byte counts, configured width, permit amount, and
opened count. Production cleanup queries or mutates the receipt directly; it
never infers authority from a diagnostic view.

## [Rooted finalizers and replay safety](#index)

Rich cleanup does not run directly in `__del__`. A finalizer-capable constructor
pre-reserves a slot in a fixed `ReservedFinalizerEscrow` and roots a separate
`RootedFinalizerAuthority` before exposing the wrapper or primary resource. The
authority contains only exact cleanup state and never roots the wrapper whose
collection must trigger handoff.

The destructor performs a non-blocking, generation-specific arm. Normal safe
points drain rooted owners under governed execution. If a wrapper disappears
between owner publication and ticket mirroring, owner-first lookup still finds
the generation. Reserving the same unarmed owner again is idempotent; a second
active generation for that owner is rejected.

Two state machines are relevant:

```text
rooted authority: RESERVED -> PRIMARY_ARMED -> ACK_ONLY -> RETIRED
escrow slot:      RESERVED -> PUBLISHED -> CLAIMED -> PROCESSED
                                      -> RECYCLE_PENDING -> FREE
```

`ACK_ONLY` is irreversible and is published before fallible secondary
retirement. Once primary cleanup commits, a retry may retire finalizer or
control-plane capacity but cannot invoke primary cleanup again. A
`FinalizerReplayCapability` records the exact postcondition for resource APIs
whose callback can be interrupted between release and clearing its arguments.
Replaying the same capability acknowledges completion; another capability is
rejected.

An exception while a slot is `CLAIMED` restores the owner to a processable
state. Once `PROCESSED` is visible, later safe points perform only bookkeeping.
Production cleanup callbacks are nevertheless target-based and idempotent,
because Python cannot make callback return and the following state publication
one indivisible external transaction.

`RECYCLE_PENDING` means the owner is gone but free-ring bookkeeping has not
completed. It is repairable capacity, not resource corruption. The capacity
identity is:

```text
active + available + recycle_pending + retired == capacity
```

Admission and safe points may scavenge owner-free pending slots. Authentication
or publication failures remain fail-closed and observable.

## [Generations, bounded state, and corruption](#index)

`BoundedGenerationPool.acquire_for(owner)` roots the exact owner before handing
back an ABA-resistant token. `release_for(owner)` retires by identity and is
retry-idempotent. Fixed owner slots are authoritative; free rings, indexes,
counts, and tokens are reconstructible mirrors.

Generation arithmetic is prepared before publication. Tokens use bounded
non-zero namespaces, never wrap, and permanently retire a physical slot before
reuse could make a stale token authoritative. The same owner-first rule applies
to runtime-service registrations, external-runtime claims, cross-process memory
contributions, path-claim admission, retry items, and finalizer generations.

Dynamic coordination structures have hard item and byte ceilings. This includes
control-plane capabilities, waiters, retries, cleanup calls, terminal owners,
provider keys, session-construction gates, cross-process records, and diagnostic
rings. Keys and labels derived from input are length-bounded or digested before
retention.

Corruption closes new admission but preserves cleanup:

- exact owner tables or slots are scanned to reconstruct derived accounting;
- a true underflow, impossible state, or authentication mismatch latches a
  protocol violation or quarantine;
- already-authenticated owners may still shrink, release, or move to terminal
  quarantine;
- a slot retired while its free-ring is corrupt is not returned for reuse; and
- clean shutdown is impossible while sticky corruption or unresolved ownership
  remains.

Ordinary mirror drift that can be repaired unambiguously from exact owners is
recorded and reconciled; it is not, by itself, permission to poison an otherwise
sound exact ledger.

## [Resident memory and cross-process coordination](#index)

One `OperationMemoryLedger` owns Python and native bytes for an operation. Each
exact lease also participates in the native process resident authority. A lease
tracks logical ownership and a conservative physical high-water mark or native
receipt, so an interrupted shrink cannot expose aggregate capacity still owned
by the lease.

Ledger close is a state transition. New reservations stop when close begins,
while live leases may drain. Releasing the final exact child triggers the
deferred ledger-close tail. If host-wide cleanup fails after child retirement,
the child release remains committed and the rooted ledger owner retries only
cross-process and advisory tails.

Optional cross-process memory coordination uses an authenticated per-process
account plus a bounded, descriptor-pinned journal. Direction determines commit
order:

- growth persists the host-wide reservation before growing the exact direct
  ledger;
- shrink or release reduces the exact direct ledger before shrinking the
  persistent journal.

A failed second step leaves conservative over-reservation and explicit
reconciliation debt. Reconciliation derives the host contribution from exact
local owners and may reduce stale authority; it never manufactures a larger
contribution. Live capacity is the minimum ceiling of current owners rather
than a historical low-water mark.

Coordination documents are size- and record-bounded. Updates validate schema,
semantics, PID start tokens, descriptor and path identity, and complete bytes
before replacing the prior state. Oversized, malformed, unknown-version, or
partially readable state fails closed and does not truncate the last valid
journal.

## [Temporary storage and physical artifacts](#index)

Temporary storage uses exact process capabilities in addition to operation and
per-filesystem ownership. Capability identity, not device plus byte amount,
authorizes resize and release. Growth, shrink, cross-device replacement, local
accounting, cross-process accounting, and control-plane retirement retain
independent commit state, so retry skips every component that already committed.

Cross-device resize roots the replacement before retiring the predecessor.
Lower-level journal or account operations occur outside the pool condition
lock. A post-commit exception activates or roots the prepublished capability;
failed rollback remains a bounded orphan driven by a later safe point.

The physical path and its lease are one ownership transaction. Deletion must be
confirmed before returning storage. Failed deletion transfers both to the
bounded quarantine janitor; failed janitor admission leaves the caller's owner
retryable. Directory traversal is streaming and bounded, and coordination roots
are pinned and revalidated before mutation.

Cross-process storage aggregates authenticated accounts by process instance and
device. Same-process transitions serialize around that aggregate. Recovery
sums all sibling accounts before shrinking stale host authority, so reconciling
one account cannot erase another account's bytes.

## [Threads, file descriptors, and external runtimes](#index)

One native atomic total is the physical admission commit point for managed and
external-runtime thread permits. Domain counters are subledgers and must
conserve:

```text
total physical permits == managed permits + external-runtime permits
```

Snapshots retry across an in-flight writer count and mutation epoch until the
subledgers are stable. Runnable CPU is independent from resident physical
threads, so affinity or cgroup shrink can stop dequeues without destroying a
pool.

File-descriptor receipts own both reservation and opened count. Exact
`_FdOpenAttempt` membership, rather than a scalar opening count, identifies an
in-flight physical open. After open or close, code reconciles from the receipt
before cleanup, covering interruption between the native commit and Python
mirror update. A receipt cannot shrink below descriptors it proves open.

Physical close always precedes opened-state retirement and permit return. An
uncertain close occupies a preallocated exact debt slot. Slot occupancy is the
authority; telemetry is rebuilt from it. Duplicate publication repairs missing
metadata and observability without closing or releasing the descriptor again.

External runtimes are coordinated by stable pool identity. One bounded
coordinator entry owns exact physical and logical claims, verified configured
width, configuration generation, resident identity, and stack debt. Third-party
getter, setter, and probe callbacks execute outside project locks. Followers
wait on explicit configuration state, and same-thread reentrancy fails closed.

Configuration is read back before acceptance. A pool may operate at a narrower
verified width, but overlapping claims cannot silently re-expand it. A fresh
generation may grow only after new capacity admission. Unknown or failed
resident probes preserve conservative identity and stack debt; explicit zero is
the only observation that retracts identity. Probe publication is accepted only
if the configuration generation stayed stable.

Active claims, configured width, resident worker identity, and stack debt are
different facts. Only explicit resident identity may offset OS-observed threads,
and stack debt must never be lower than resident identity. Target-zero claims
encountering in-flight third-party configuration become bounded tombstones;
they retain their finalizer generation until the underlying envelope actually
reaches zero.

## [Schedulers, backpressure, and retained results](#index)

All queues are bounded in items, bytes, waiters, and worker ownership. A
successful resource release performs its authoritative debit before derived
scheduling repair. Availability is level-triggered: lost notification leaves a
dirty level and a pre-reserved path can redrive delivery without depending on
the scheduler whose capacity it is announcing.

The retry scheduler separates pending, ready, active, successor, emergency,
dead-letter, and parked ownership. A key transitions to `RUNNING` under the same
condition lock used by cancellation; cancellation that wins before that commit
cannot be followed by a callback. At most one coalesced successor exists per
running key. Generation identity and bounded composite keys prevent ABA,
cross-type equality, and unbounded metadata retention.

The cleanup dispatcher uses per-subsystem queues and Deficit Round Robin under
global and per-subsystem ceilings. Failed worker permits and release capabilities
remain exact owners in bounded fail-closed slots. A poison or parked owner
cannot block unrelated runnable cleanup.

Asynchronous ordered scheduling uses a fixed O(window) result ring rather than
an unbounded pending mapping. `AsyncResultMemoryContract` distinguishes
preflight size, postflight reconciliation, and externally governed ownership.
External ownership requires a sealed `GovernedResultOwnership` capability; a
boolean assertion cannot cause bridge memory to be released.

Cancellation-resistant async tasks enter a fixed terminal-debt bank in
`BUILDING` before child publication. They then move through active, claimed, and
retry-pending states under exact generations. A failed reaper returns the same
generation to retry state, and a bounded round-robin cursor prevents one debt
from starving all later owners.

Native retained-byte backpressure is memory-first and waiter-bounded. Producers
do not occupy queue slots or hold worker-queue locks while waiting. Epoch and
condition-variable transitions prevent lost wakeups; operation deadlines and
dynamic timeout changes wake blocked producers. Heterogeneous requests permit
only bounded small-request bypass, ensuring an older large request eventually
receives accumulated capacity.

Remote I/O uses the same principles for synchronous and asynchronous waiters:
one weighted authority, bounded submission before event-loop handoff, removable
waiters, finite deadlines, and bounded bypass. Provider expiry indexing keeps
one mutable heap node per live endpoint rather than historical nodes.

## [Cgroups, fork, and process provenance](#index)

Cgroup observations use explicit `VALUE`, `UNBOUNDED`, and `UNKNOWN` states.
Python and native readers resolve `/proc/self/cgroup` against mount information,
walk every visible constraining ancestor, and validate membership around the
complete sample. Migration, incomplete ancestry, truncation, parser limits,
integer overflow, malformed data, or unreadable control files produce
`UNKNOWN`, not unlimited capacity.

Proc and mount parsing is streaming with limits on records, line size, and total
bytes. Linux PID fallback accounts for threads belonging to the same real UID
only when authoritative cgroup PID headroom is unavailable. External process
threads and descriptors remain part of the observed envelope.

At-fork preparation selects fresh locks, contexts, and registries from
preallocated banks. It does not allocate, import modules, execute rich cleanup,
or acquire locks that may be held by vanished parent threads. Inherited owners
are quarantined by generation and remain reachable without being released in
the child. When prepared generations are exhausted, further nested fork state
fails closed instead of recycling ancestor-active synchronization primitives.

Every lease and receipt validates creator PID before touching inherited locks
or mutating authority. The initialized child is poisoned for normal runtime use
and must `exec()` or be replaced by a `spawn` or `forkserver` process.

## [Shutdown and quiescence](#index)

Runtime shutdown is terminal, single-flight, and driven by one absolute
monotonic deadline. It first freezes public, async, remote, and service
admission. Producers close before the cleanup consumers that own their fallback
paths. Worker registrations remain in `RETIRING` until physical exit and exact
permit retirement have both committed.

Finalizer domains are registered during normal import; shutdown does not import
new cleanup subsystems under pressure. It drains them around producer and
consumer phases and compares monotonic publication and progress epochs. Equal
cardinality is insufficient: a retire-and-publish ABA advances the activity
token and prevents false quiescence.

Terminal success requires all exact resource and control-plane owners to drain,
including:

- operation and process resident-memory ownership;
- temporary-storage leases, quarantined artifacts, and janitor work;
- Python and native thread and FD reservations, physically open FDs, and
  uncertain-close debt;
- remote waiters, submissions, permits, provider cleanup, and external-runtime
  claims;
- retry, cleanup-dispatcher, guardian, notifier, async debt, and native reaper
  ownership;
- finalizer active, published, recycle-pending, retired, and overflow state; and
- sticky protocol violations, parked work, dead letters, or reconciliation
  debt that make clean completion impossible.

The integral runtime snapshot reads under a process-wide diagnostic epoch and
uses an additive schema. Snapshot failure is observability failure, not proof of
quiescence. A deadline may return explicit incomplete ownership, but shutdown
must never wait forever or label bounded terminal debt as successfully drained.

## [Validation contract](#index)

Concurrency claims are executable contracts, not documentation flags. Concrete
implementation callables register the mechanisms for transferable resident
credit, composite slot-and-byte admission, control-plane budgeting, stage
admission, cancellation, native payload entry, file-descriptor admission, and
external-runtime claims. Validation fails if a required implementation is
missing or if execution did not observe it.

The public matrix contains seven inputs (`csv`, `json`, `json_array`, `jsonl`,
`xml`, `parquet`, and `python`) and seven outputs (`csv`, `jsonl`, `parquet`,
`pyarrow`, `pandas`, `polars`, and `duckdb`). All 49 pairs must
exercise their real payload and format-specific primary stages. Bootstrap-only
activity cannot certify a payload path.

Orthogonal transport and lifetime profiles prevent evidence from an unrelated
route from satisfying a pair:

- input: local path, remote chunks, directory source plan, materialized memory,
  Python iterator, and staged remote;
- output: local file, staged remote commit, stream, and analytical adapter.

Every profile requires resident credit, composite admission, control-plane
ownership, native payload entry, and cancellation checkpoints. Path and remote
routes additionally require exact FD evidence; remote, directory, iterator, and
staged routes require complete stage admission; analytical adapters require an
external-runtime claim.

Native health validation is fail-closed. It requires a compatible diagnostic
schema, stable mutation epochs, zero underflow and protocol-violation counters,
and conservation between aggregate and domain subledgers. Missing native
diagnostics cannot certify a release.

Regression coverage must exercise normal completion, construction failure,
pre- and post-commit interruption, cancellation, timeout, release failure,
abandoned GC handoff, fork rejection, capacity shrink, mirror drift,
authentication failure, generation exhaustion, and shutdown. Native resource
and queue protocols are additionally stressed under ASan/UBSan and
ThreadSanitizer. A new resource owner is incomplete until these terminal paths
converge to zero ownership or explicit bounded conservative debt.
