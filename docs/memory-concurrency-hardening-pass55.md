# Memory & concurrency hardening — recovered Pass 55

This pass is reconstructed from the last downloadable Pass 54 checkpoint and the
surviving design/validation record from the collapsed Pass 55/56 conversation.
It is intentionally marked **recovered**: it preserves the recovered invariants,
but its archive is not expected to be byte-identical to the original lost Pass 55.

## Hardening restored in this pass

1. **Preallocated terminal-owner authority**

   - Terminal ownership no longer depends on a dynamically growing authoritative
     dictionary at failure/OOM time.
   - A fixed record bank is allocated before terminal debt can exist.
   - Publish/retire/category-retire paths perform bounded scans and do not build
     temporary key collections.

1. **Terminal metadata is accounted in bytes**

   - Snapshots expose `metadata_bytes` and `total_attributed_bytes`.
   - Shutdown accounting includes both retained payload attribution and ledger
     metadata attribution.
   - Uncertain-FD terminal ownership uses an explicit byte attribution rather
     than treating descriptor units as bytes.

1. **Diagnostic rejection remains non-throwing under pressure**

   - Terminal-owner rejection is latched fail-closed even if a diagnostic counter
     has hostile/failing arithmetic semantics.

1. **Tri-state cgroup observations**

   - Limit reads distinguish `VALUE`, `UNBOUNDED`, and `UNKNOWN`.
   - `UNKNOWN` is never silently interpreted as unlimited capacity.

1. **Effective hierarchical cgroup limits**

   - Memory and PID constraints are evaluated across the process cgroup and all
     constraining ancestors up to the controller mount root.
   - Memory headroom uses the minimum `(limit - usage)` across bounded ancestors.
   - Adaptive pressure uses the highest usage/limit ratio across ancestors.
   - Unresolved Linux cgroup observations fail closed for new thread capacity.

## Compatibility

The older value-only `read_cgroup_integer()` API is retained for compatibility;
it intentionally maps both `UNKNOWN` and `UNBOUNDED` to `None`. New admission
code uses the tri-state/effective APIs whenever the distinction is safety-critical.
