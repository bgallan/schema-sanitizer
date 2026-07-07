# Native Parquet Reader TODO

The Parquet input pipeline is production-safe because native reader failures
fall back to PyArrow. The native reader itself is not yet a complete PyArrow
replacement. Remaining work:

1. Materialize all native-planned encodings:

   - [x] RLE dictionary byte-array/string pages.
   - [x] RLE dictionary pages for non-byte-array physical types.
   - [x] DELTA_BINARY_PACKED integer pages.
   - [x] DELTA_LENGTH_BYTE_ARRAY byte-array/string pages.
   - [x] BYTE_STREAM_SPLIT float/double pages.
   - [x] Boolean bit-packed PLAIN values.

1. Complete physical and logical type coverage:

   - [x] Decimal logical types.
   - Fixed-size binary details.
   - [x] Date and timestamp units/timezone metadata.
   - [x] Unsigned integer logical types.
   - Nullability edge cases across all supported types.

1. Support nested and repeated columns:

   - Structs.
   - Lists.
   - Repeated fields.
   - Full definition/repetition level reconstruction.

1. Broaden row-group and page coverage:

   - [x] Multi-row-group parity tests.
   - [x] Multiple data pages per column.
   - [x] Mixed null/non-null spans across pages.
   - Empty row groups.
   - [x] Empty files.

1. Expand input source support:

   - File-like objects.
   - Buffers.
   - Filesystem-backed non-local paths.
   - Directory/dataset reads.

1. Match production reader behavior:

   - Projection support.
   - Batch-size control.
   - Predicate/filter integration if direct native reads become dataset-aware.

1. Harden large-file behavior:

   - Page-buffer reuse.
   - Streaming materialization instead of whole-row-group buffering where possible.
   - [x] Memory-budget checks for decoded/materialized buffers.

1. Add cross-writer compatibility:

   - PyArrow-written files.
   - Spark-written files.
   - DuckDB-written files.
   - BigQuery-exported Parquet variants.

1. Keep fallback observability strong:

   - Preserve clear route labels for native vs PyArrow reads.
   - Log unsupported native-reader cases at a useful level.
   - Add counters/diagnostics if native reader adoption becomes user-visible.
