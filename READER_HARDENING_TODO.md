# Reader hardening TODO

## Objective

Make every native reader fail predictably when processing malformed or hostile
input. A rejected input may return a structured parsing or out-of-memory error,
but it must not crash the process, allocate outside the global memory budget, or
consume disproportionate CPU.

This document covers XML, CSV, JSON, Parquet, their streaming scanners, and the
shared ingestion pipeline.

Progress note (2026-08-01, second pass): XML hostile-markup scanning now has
bounded regression coverage and charged-peak/release tests; CSV record, field,
cell, segment, and reconciled-header limits share the operation pool; JSON
cross-chunk scanner metadata and root-field caches use budgeted PMR storage;
and Parquet adds shared parallel reservations plus optional page CRC32
verification. Supported Parquet codecs now have truncated/corrupt regressions,
and repetition/definition-level output is budgeted before vector allocation.
All four deterministic native fuzz targets complete 1,000-run campaigns. XML
worker failure now drains charged trees, JSON path groups release exhausted
children and budget coordination metadata, and fuzz campaigns have
engine-specific per-input time/RSS guards. The consolidated workflow now runs
the deterministic corpus on every supported sanitizer platform and schedules a
weekly 10,000-run-per-reader ASan/UBSan campaign. Actual sanitizer execution,
early releasing committed XML row trees, and the remaining observability work
remain open. Reader failures now preserve structural offsets and limits without
echoing payload values or XML names. A reproducible Release-vs-Release A/B
harness and evidence report were added. The current 2,000-row run keeps XML
within 1.05-1.13x and Parquet preflight within 1.04x of the previous pass, but
CSV remains about 1.95x and JSONL 1.47-1.88x slower, so the performance checkbox
remains open rather than hiding the regression. The same exercise also confirms
that 10,000 streamed XML rows exhaust 64 MiB safely until committed trees can be
released earlier.

Progress note (2026-08-01, third pass): committed XML row trees now carry a
batch releaser and are dropped immediately after ordered materialization,
including cancellation and error unwinding; a 10,000-row regression completes
under 64 MiB in serial and parallel modes with zero charged bytes after close.
Public reader exceptions now expose privacy-safe structured context (`format`,
`source`, `stage`, offsets/indices, and applicable limits) while preserving the
existing exception classes. Clang ASan/UBSan reran all 10 canonical reader
corpus entries plus 1,000 mutations per CSV, JSON, XML, and Parquet frontend,
and GCC TSan completed 100 ordered-executor concurrency rounds without a
finding. Row metadata was compacted into one budgeted PMR vector and eager CSV
capacity reservation was removed. The latest matched Release A/B run improves
JSONL multi-threaded overhead to 1.17x and keeps Parquet preflight at 0.96x, but
CSV remains 1.39-1.44x and JSONL single-threaded 1.33x, so performance remains
an explicit open item.

Progress note (2026-08-01, fourth pass): operation diagnostics now aggregate
charged-memory current/peak/limit values, reader depth, decoded bytes, records,
nodes, Parquet compression totals, and privacy-safe cancellation reasons across
inference, materialization, grouped files, and serial/parallel plans. A public
rejection matrix verifies identical error class, stage, and byte offset for
malformed CSV, JSONL, XML document, and XML row-tag inputs across local files,
directories, HTTP staging, and both threading modes while preserving existing
outputs. Parallel CSV worker results now store cells in one flat budgeted block.
The matched Release A/B run records CSV at 1.21-1.30x, JSONL at 1.37-1.46x, XML
at 0.96-1.03x, and Parquet preflight at 0.98x; a checked performance envelope
now rejects unreviewed regressions. A documented linear-work contract and
matched Release scaling evidence cover hostile-but-valid CSV, JSONL, and XML in
serial and parallel modes, with a cross-platform CI smoke; Parquet is covered
structurally by bounded ranges, pages, decompression, levels, and sanitizer
fuzzing. Scheduled fuzzing emits canonical evidence and a release-independent
limit-review artifact, but the periodic limit-review item remains open until
real production telemetry is supplied to that process.

Progress note (2026-08-01, fourth-pass continuation): local and remote directory
discovery now share one operation-scoped metadata quota across synchronous,
asynchronous, grouped, and concurrent provider paths. File records are charged
before retention, local listings no longer build an uncharged path list, and
parent scans retain only requested directory entries. Blocking and asynchronous
HTTP control bodies are capped before full materialization, including GCS list,
metadata, status, delete, media-upload, and resumable-upload responses. A fixed
64 KiB directory-runtime allowance preserves useful parser behavior under tiny
test budgets; scalable growth remains bounded. The cross-language atomic-budget
checkbox remains open because Python metadata/control-response quotas and the
native governed pool still do not debit one shared resident ledger.

Progress note (2026-08-01, fourth-pass budget propagation fix): source-selected
registry metadata wrappers now receive the resolved explicit
`memory_limit_bytes` value for path, Python-stream, and in-memory-text inputs.
Previously those wrappers could silently fall back to automatic host-memory
sizing; under concurrent process pressure that produced spurious metadata
limits as small as 1-2 KiB despite a caller-supplied 64 MiB budget. Deterministic
8 KiB regressions cover all three native source branches and public Parquet
atomic cleanup, while three pressure campaigns complete 144 concurrent
64 MiB conversions. The fix was rebuilt with ASan/UBSan and TSan; all canonical
reader corpus entries, 4,000 mutation runs, and 100 concurrency rounds remain
clean. This closes the propagation bypass but not the cross-language atomic
ledger, so the shared operation-wide-budget checkbox remains open.

Progress note (2026-08-02, fifth pass): one native atomic resident-memory ledger
now spans Python and C++ for each public operation. Native pools reserve actual
upstream allocation bytes including allocator overhead; materialized text/bytes
inputs, directory metadata, retained HTTP control bodies, and remote transfer
windows acquire lifetime leases from the same counter. Registry warm-up children,
source groups, remote tasks, inference, materialization, writers, and concurrent
native workers reuse the root ledger rather than deriving independent limits.
Five cross-language regressions cover atomic contention, Python/native
coexistence, stream lifetime, retained control responses, and reservation before
blocking reads. Clang ASan/UBSan reran those paths plus 4,000 reader mutations
without findings, and GCC TSan completed 100 rounds of 32 concurrent ledger
clients without a race. The documented boundary now excludes only fixed or
opaque runtime overhead, disk contents/page cache, and analytical results after
ownership transfer.

## Security contract

- [x] Treat every input byte and every declared size, count, offset, nesting
  level, encoding, and compression ratio as untrusted.
- [x] Make `memory_limit_bytes` a shared operation-wide budget across readers,
  inference, materialization, staging, and concurrent workers.
- [x] Keep parsing time linear in input size unless a documented format feature
  requires otherwise.
- [x] Reject malformed input consistently in single-threaded, multi-threaded,
  document, streaming, local-file, and remote-file paths.
- [x] Convert allocation failures and safety-limit violations into stable public
  exceptions; never let them terminate the process.
- [x] Apply internal security ceilings before configurable schema or output
  limits. Output limits such as `arrow_max_depth` are not parser safeguards.
- [x] Document which allocations are deliberately outside the operation budget,
  including analytical result ownership after it is returned to Python.

## P0: fix confirmed XML availability issues

### Bound nesting before recursion

- [x] Add an internal XML nesting ceiling enforced before descending into a
  child element.
- [x] Enforce the same ceiling in document parsing and `xml_row_tag` streaming.
- [x] Track depth with an unsigned, overflow-checked type.
- [x] Replace recursive element parsing with an explicit stack where practical.
- [x] Replace the recursive `build_xml_node_model` traversal with an iterative
  post-order traversal.
- [x] Keep the internal safety ceiling independent from `arrow_max_depth`; the
  latter should continue to control output projection only.
- [x] Return an `Invalid` error containing the input offset and configured or
  internal limit when nesting is excessive.

Acceptance criteria:

- [x] XML immediately below the limit succeeds in document and row-tag modes.
- [x] XML at and above the rejected boundary returns a Python exception without
  `SIGSEGV`, abort, hang, or sanitizer finding.
- [x] A subprocess regression exercises at least 20,000 nested elements.
- [x] The result and error are identical with `multi_threading` enabled and
  disabled.

### Charge the complete XML representation to memory

- [x] Allocate XML nodes, names, text, attributes, child collections, grouped
  values, projected fields, and decoded entity output from the operation's
  budgeted memory resource.
- [x] Ensure every parallel XML worker shares the same global budget rather than
  receiving an independent allowance.
- [x] Include container capacity and allocator overhead in estimates or charge
  actual allocations through the shared resource.
- [x] Avoid retaining both raw source bytes and a fully materialized tree when
  execution no longer needs both.
- [x] Release completed row trees as soon as the ordered consumer has committed
  them.
- [x] Bound the number of nodes, attributes per element, total attributes, and
  decoded text bytes as secondary safeguards.
- [x] Translate `std::bad_alloc` from every XML construction path into the
  public out-of-memory status.

Acceptance criteria:

- [x] A sub-1-MiB XML document whose node expansion exceeds a 1-MiB operation
  budget fails with a controlled out-of-memory exception.
- [x] Peak charged bytes never exceed `memory_limit_bytes`, apart from explicitly
  documented fixed runtime overhead.
- [x] Parallel parsing cannot multiply the configured budget by worker count.
- [x] Cancelling or failing an XML batch releases all charged memory.

### Make entity decoding linear and Unicode-safe

- [x] Rewrite entity decoding as a single-pass state machine.
- [x] Never search the remaining input again for every unmatched `&`.
- [x] Define one consistent policy for unknown, incomplete, and malformed
  entities; prefer rejecting malformed XML.
- [x] Reject numeric entities above `U+10FFFF`.
- [x] Reject surrogate code points in `U+D800..U+DFFF`.
- [x] Reject NUL and characters forbidden by the supported XML version.
- [x] Validate raw input as well as decoded entities for well-formed UTF-8.
- [x] Apply identical decoding and validation to element text and attributes.

Acceptance criteria:

- [x] Long runs of unmatched ampersands complete in bounded linear scanning time.
- [x] `&#x110000;`, `&#xD800;`, `&#xFFFFFFFF;`, and `&#0;` are rejected.
- [x] Successful XML conversion cannot emit invalid UTF-8 to JSONL, Arrow, or
  Parquet.
- [x] Chunk boundaries inside an entity do not alter the result.

## P1: make XML parsing strict and scanner behavior consistent

- [x] Validate XML names rather than accepting every byte up to whitespace or a
  delimiter.
- [x] Reject duplicate attributes on the same element.
- [x] Reject raw `<` and forbidden control characters in attribute values.
- [x] Validate comments, CDATA, processing instructions, closing tags, and
  trailing document content consistently.
- [x] Keep `DOCTYPE` and `ENTITY` declarations disabled in both parser modes.
- [x] Preserve the current guarantee that XML parsing never resolves external
  resources or performs network or filesystem access.
- [x] Make the streaming scanner retain its progress while waiting for an
  unterminated comment, CDATA section, quoted tag, or processing instruction so
  refills cannot repeatedly rescan the entire buffer.
- [x] Share token and name validation between `XmlParser` and
  `XmlRowTagScanner` to prevent parser differentials.
- [x] Clearly document the supported XML subset if full XML conformance is not
  intended.

Acceptance criteria:

- [x] Document and row-tag modes accept and reject the same syntax.
- [x] DTD and external-entity regression tests cover mixed case and chunk
  boundaries.
- [x] Malformed markup has bounded memory and linear processing time.

## P1: harden CSV validation

- [x] Reject unterminated quoted fields at end of file by default.
- [x] Keep CSV ingestion intentionally strict; implicit repair and lenient mode
  are unsupported and documented rather than silently repairing malformed rows.
- [x] Preserve the existing maximum-cell count and test the exact boundary.
- [x] Bound raw record size, decoded record size, individual field size, and
  cross-chunk segment count through the global memory budget.
- [x] Detect decoded-size arithmetic overflow before allocating arena storage.
- [x] Reject bytes after a closing quote unless they are permitted whitespace,
  a delimiter, or the record terminator.
- [x] Test embedded newlines, doubled quotes, empty final fields, alternative
  delimiters, BOMs, invalid encodings, and truncated multibyte input.
- [x] Ensure header parsing applies the same strictness and resource limits as
  data rows.
- [x] Reject duplicate non-empty source headers deterministically before they
  can collapse during object materialization.
- [x] Reject distinct source headers that collide only after configured name
  reconciliation.

Acceptance criteria:

- [x] A truncated quoted record produces a structured parse error.
- [x] Single-threaded and multi-threaded paths produce identical CSV errors and
  source offsets.
- [x] No record can cause an allocation greater than the remaining global
  budget.

## P1: verify JSON validation across optimized paths

- [x] Preserve the existing internal nesting ceiling and streaming depth checks.
- [x] Validate UTF-8, escape sequences, surrogate pairs, numbers, and trailing
  content even for fields skipped by projection or schema filtering.
- [x] Ensure lazy and optimized readers cannot accept malformed values merely
  because those values are not materialized.
- [x] Charge token indexes, decoded strings, cross-chunk values, scanner stacks,
  row metadata, and root-field object caches to the global operation budget.
- [x] Charge schema-inference field-name scratch, parser evidence, and
  long-lived path-group coordination structures to the global operation
  budget; final returned logical-schema ownership remains outside the budget as
  documented.
- [x] Test large objects, duplicate keys, very long numbers, invalid exponents,
  invalid escapes, isolated surrogates, and values crossing chunk boundaries.
- [x] Confirm that JSON Lines recovers or stops according to the selected error
  policy without losing source offsets.

Acceptance criteria:

- [x] Malformed projected and unprojected fields are rejected consistently.
- [x] Depth 512 boundary tests cannot crash either scanner or materializer.
- [x] JSON document and JSON Lines paths obey the same scanner and temporary-memory
  contract.

## P1: bind Parquet safeguards to the operation budget

- [x] Preserve the existing footer, metadata, Thrift depth, schema, page,
  dictionary, validity, decompression, and expansion-ratio ceilings.
- [x] Derive effective page, row-group, decompression, metadata, and reader
  buffer limits from the operation `memory_limit_bytes` budget.
- [x] Share decompression and materialization reservations across concurrently
  decoded columns and row groups.
- [x] Reject overlapping, overflowing, backward, footer-overlapping, or
  out-of-file offsets before seeking or allocating.
- [x] Exercise every supported compression codec with truncated and corrupt
  payloads; exercise Snappy and GZIP with high-expansion payloads and reject
  oversized uncompressed pages under the same effective operation limit.
- [x] Validate page checksums when present and expose checksum failures as data
  errors.
- [x] Bound nested repetition/definition-level expansion before constructing
  Arrow offsets and validity buffers.
- [x] Review the 1-GiB hard ceilings so that a lower operation budget always
  takes precedence.

Acceptance criteria:

- [x] A decompression bomb is rejected before exceeding the global budget.
- [x] Parallel column decoding cannot reserve the full limit independently per
  worker.
- [x] Mutated footer, page header, offsets, levels, and compressed payloads fail
  without sanitizer findings.

## P1: expand fuzzing to complete reader pipelines

- [x] Keep the lightweight parser fuzzers for fast coverage.
- [x] Add an XML streaming fuzzer covering `XmlRowTagScanner`, arbitrary chunk
  boundaries, entity decoding, and model construction.
- [x] Extend the CSV frontend fuzzer from record scanning and cell decoding
  through compiled-plan frontend materialization.
- [x] Extend the JSON scanner/on-demand fuzzer, which fully walks parsed values,
  through schema-filtered and skipped-field frontend execution.
- [x] Continue fuzzing Parquet footer parsing and native Arrow streaming for all
  compiled codecs.
- [x] Vary `memory_limit_bytes`, batch sizes, chunk sizes, error policies, and
  multithreading in bounded combinations.
- [x] Store every fixed crash, timeout, excessive-allocation, and parser
  differential as a minimized regression corpus entry.
- [x] Run fuzz targets under ASan and UBSan; run concurrency regressions under
  TSan where supported.
- [x] Add per-input elapsed-time, maximum-input-size, and resident-memory
  guards so deterministic CI campaigns report the run and input size on a
  resource regression rather than hanging.

Acceptance criteria:

- [x] Every public reader has at least one native frontend or stream-level
  end-to-end fuzz target.
- [x] CI runs the deterministic regression corpus on every supported platform.
- [x] Longer mutation campaigns run in a consolidated scheduled workflow.

## P2: shared observability and maintenance

- [x] Standardize reader errors with format, source, byte offset, row or element
  index, safety limit, and observed value where safe.
- [x] Report peak charged memory, parser depth, decoded bytes, records or nodes,
  decompression ratio, and cancellation reason in diagnostics.
- [x] Never include sensitive input contents in exceptions or telemetry by
  default.
- [x] Add a security-support statement describing accepted input trust levels
  and responsible disclosure.
- [x] Document hard security ceilings separately from performance defaults.
- [x] Benchmark valid inputs before and after hardening to prevent accidental
  performance regressions.
- [ ] Revisit reader limits periodically using production telemetry and fuzzing
  evidence rather than release-numbered documents.

## Definition of done

- [x] All confirmed XML crash, CPU-amplification, Unicode, and memory-budget
  regressions pass.
- [x] CSV rejects truncated quoting in strict mode.
- [x] JSON optimized paths validate skipped malformed content.
- [x] Parquet resource reservations respect the shared operation budget.
- [x] Every reader passes its end-to-end corpus under the available sanitizers.
- [ ] `pre-commit run -a` and the consolidated CI workflow pass.
- [x] Public documentation accurately describes strictness, limits, memory
  accounting, and the analytical-output exception to memory ownership.

## Fifth-pass continuation validation (2026-08-02)

The shared cross-language ledger was rebuilt and revalidated after extracting
operation input lifetime, discovery budgeting, analytical adapter
materialization, Parquet page scratch, and JSON frontend construction into their
intended architectural owners. The refactor restores the repository's module
size and ownership checks without weakening their thresholds:
`execution_context.py` is 477 lines, `source_discovery.py` is 499 lines, and the
JSON text frontend is 550 lines. The rebuilt Release module passes a focused
500-test reader, memory, concurrency, diagnostics, registry, and maintenance
matrix with four optional skips; all 332 layout checks pass independently.
Clang ASan/UBSan reran 1,000 mutations for each CSV, JSON, XML, and Parquet
frontend without a finding, while GCC TSan repeated 100 rounds of 32 concurrent
ledger clients without a race. Source hygiene validates 543 Python files, 38
JSON files, and the project TOML. These matrices overlap and therefore are not
reported as one artificially summed test count.

Two items intentionally remain open. Production telemetry is unavailable in
this environment, so the periodic limit-review process cannot yet be exercised
against real workloads. The hosted workflow cannot be claimed as passing
locally: `pre_commit` and `pyarrow` are not installed, although the workflow
topology, deterministic corpus, sanitizer builds, and all locally executable
checks are covered by repository tests.
