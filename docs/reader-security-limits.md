# Reader security limits

This document separates immutable denial-of-service ceilings from performance
settings derived from `memory_limit_bytes`. Hard ceilings cannot be raised by a
caller. The effective limit is always the lower of the hard ceiling and the
operation-derived budget.

## Coordinated operation budgets

`memory_limit_bytes` is the only public memory/resource control. Native parsing,
inference, materialization, writers, concurrent workers, retained in-memory
input, directory metadata, remote control bodies, and transfer windows reserve
from one atomic operation ledger; no worker or Python staging task receives an
independent copy of the limit. Allocation failures and safety-limit violations
are translated to stable public exceptions.

The exact charged domains, fixed 64 KiB directory-metadata sublimit allowance,
disk-permit separation, and deliberately untracked runtime/output ownership are
documented in [Reader memory accounting](reader-memory-accounting.md) and
[SECURITY.md](../SECURITY.md).

## XML hard ceilings

| Resource | Ceiling |
|---|---:|
| Element nesting | 512 |
| Nodes | 1,000,000 |
| Attributes on one element | 4,096 |
| Total attributes | 1,000,000 |
| Decoded text | 512 MiB |

The supported XML subset is UTF-8 XML 1.0 without DTDs, general entities,
external entities, XInclude, network access, or filesystem resolution. The five
predefined entities and valid numeric character references are supported.
Document mode and `xml_row_tag` mode use the same syntax and Unicode checks.

## CSV hard ceilings

| Resource | Ceiling |
|---|---:|
| Cells in one record | 65,536 |
| Raw record bytes | 256 MiB |
| Decoded record bytes | 256 MiB |
| One decoded field | 64 MiB |
| Cross-chunk segments | 65,536 |

CSV parsing is intentionally strict. Unterminated quoting, quotes inside an
unquoted field, invalid UTF-8, and non-whitespace bytes after a closing quote
are rejected. There is no implicit repair or lenient mode. Distinct source
headers that become equal after name reconciliation are rejected.

## JSON hard ceilings

| Resource | Ceiling |
|---|---:|
| Nesting | 512 |
| Fields in one object | 65,536 |
| Buffered JSON value | 128 MiB |
| JSON Lines record | 128 MiB |
| Cross-chunk segments | 65,536 |

Projected-out and schema-filtered values still receive full lexical validation.
Parser security ceilings are operation-fatal and are not converted into
`skip_row` or `emit_null_row` recoveries.

## Parquet hard ceilings

| Resource | Ceiling |
|---|---:|
| Footer | 64 MiB |
| One metadata binary value | 16 MiB |
| Materialized metadata | 256 MiB |
| Compact-Thrift skip depth | 64 |
| Page header | 1 MiB |
| Compressed or uncompressed page verification | 256 MiB |
| Decompressed bytes per row group | 1 GiB |
| Derived bytes per row group | 1 GiB |
| Decompression expansion ratio | 4,096:1 |
| Schema elements / schema-path entries | 65,536 |
| Columns per row group | 65,536 |
| Row groups | 1,000,000 |
| Pages per column | 1,000,000 |
| Dictionary entries | 16,777,216 |

Footer, metadata, page, decompression, derived-buffer, row-group, and native
reader-window limits are additionally reduced from `memory_limit_bytes`.
Parallel column decoders reserve scratch and estimated output from one atomic
shared coordinator. Negative, overflowing, backward, out-of-file, and
footer-overlapping ranges are rejected before seeking or allocation. Page CRC32
checksums are validated when present.

## Privacy-safe failures

Reader exceptions and operation telemetry identify the format, failure category,
byte offset, counts, and applicable limit where available. Public Python reader
exceptions preserve their existing classes and expose safe machine-readable
fields through `exc.detail`: `format`, `source`, `stage`, `byte_offset`,
`row_index`, `element_index`, `row_group_index`, `limit_name`, `limit_value`, and
`observed_value` when the native failure provides them. In-memory sources are
reported as placeholders such as `<memory>` rather than copied payloads.

Diagnostics do not include raw records, field values, XML element names, JSON
strings, CSV cells, or Parquet payload bytes by default. This keeps failures
actionable without copying potentially sensitive input into logs.

## Performance defaults are not security ceilings

Chunk sizes, batch sizes, worker counts, grouping thresholds, and coalescing
windows are performance choices derived from the same operation budget. They
may change between releases and must not be used as security boundaries. The
hard ceilings above are the format-safety boundaries; a smaller operation
budget always wins.
