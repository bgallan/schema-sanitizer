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
   - [x] Fixed-size binary details.
   - [x] Date and timestamp units/timezone metadata.
   - [x] Unsigned integer logical types.
   - [x] Nullability edge cases across all supported types.

1. Support nested and repeated columns:

   - [x] Required non-repeated structs with scalar leaves.
   - [x] Nullable top-level non-repeated structs with scalar leaves.
   - [x] Bounded definition/repetition level diagnostics for repeated leaves.
   - [x] Simple top-level list row-offset reconstruction diagnostics.
   - [x] Simple top-level integer lists with DELTA_BINARY_PACKED elements.
   - [x] Simple top-level string lists with DELTA_LENGTH_BYTE_ARRAY elements.
   - [x] Simple top-level fixed-width lists with PLAIN elements.
   - [x] Simple top-level dictionary-encoded scalar lists.
   - [x] Simple top-level boolean lists with PLAIN bit-packed elements.
   - [x] Simple top-level float lists with BYTE_STREAM_SPLIT elements.
   - [x] Simple top-level string/binary lists with PLAIN byte-array elements.
   - [x] Simple top-level logical/fixed-size scalar lists.
   - [x] Simple top-level lists with nullable scalar elements.
   - [x] Simple top-level lists across multiple data pages.
   - [x] Empty files with simple top-level list schemas.
   - [x] Simple top-level scalar lists.
   - [x] Production fallback for complex/nested lists (list/list, list/map, nested list leaves).
   - [x] Native materialization for top-level list-of-struct with scalar leaves.
   - [x] Native materialization for top-level list-of-list with scalar leaves.
   - [x] Native materialization for top-level list-of-list-of-list with scalar leaves.
   - [x] Native materialization for arbitrary-depth top-level list chains with scalar leaves.
   - [x] Native materialization for top-level map with scalar key/value leaves.
   - [x] Native materialization for top-level list-of-map with scalar key/value leaves.
   - [x] Native materialization for top-level list-of-struct with scalar list children.
   - [x] Native materialization for top-level map with scalar list values.
   - [x] Native materialization for top-level list-of-struct with scalar list-chain children.
   - [x] Native materialization for top-level map with scalar list-chain values.
   - Native materialization for arbitrary recursive mixed repeated struct/map shapes.
     - [x] Top-level list-of-struct with scalar map children.
     - [x] Top-level list-of-struct with scalar map children containing scalar list values.
     - [x] Top-level list-of-struct with scalar map children containing scalar list-chain values.
     - [x] Top-level list-of-struct with scalar map children containing scalar struct values.
     - [x] Top-level list-of-struct with scalar map children containing scalar struct values with scalar list children.
     - [x] Top-level list-of-struct with scalar map children containing scalar struct values with scalar list-chain children.
     - [x] Top-level map with scalar struct values.
     - [x] Top-level map with scalar struct values containing scalar list children.
     - [x] Top-level map with scalar struct values containing scalar list-chain children.
     - [x] Top-level list-of-map with scalar struct values.
     - [x] Top-level list-of-map with scalar struct values containing scalar list children.
     - [x] Top-level list-of-map with scalar struct values containing scalar list-chain children.
     - [x] Top-level struct with scalar map children.
     - [x] Top-level struct with scalar map children containing scalar list values.
     - [x] Top-level struct with scalar map children containing scalar list-chain values.
     - [x] Top-level struct with scalar map children containing scalar struct values.
     - [x] Top-level struct with scalar map children containing scalar struct values with scalar list children.
     - [x] Top-level struct with scalar map children containing scalar struct values with scalar list-chain children.
     - [x] Recursive path planner started for native readiness and output-layout classification.
     - [x] Recursive native list-chain Arrow array assembly shared across supported nested parents.
     - [x] Recursive native list-chain Arrow schema assembly shared across supported nested parents.
     - [x] Recursive planner drives native nested repetition-layout validation for supported list-struct, map, and list-map routes.
     - [x] Recursive native scalar list/list-chain outputs share one Arrow array construction path.
     - [x] Supported nested list/map/struct wrappers share common Arrow list/struct shell builders.
     - [x] Recursive list-chain materializer consumes both generic and legacy repeated-level layouts.
     - [x] Recursive struct-child assembly drives scalar and scalar-list-chain children for supported struct nodes.
     - [x] Recursive map-entry assembly drives supported top-level, list-map, list-struct-map, and struct-map entries.
     - [x] Recursive struct-child assembly materializes supported map children directly, removing row-group map-name grouping loops.
     - [x] Recursive Arrow schema construction now drives supported top-level maps, list-of-struct, list-of-map, and top-level struct shapes.
     - [x] Recursive Arrow schema construction is single-headed for all native-supported output shapes.
     - [x] Recursive row-group output-field materialization is single-headed for all native-supported output shapes.
     - [x] Recursive materialization now supports ordinary nested struct children under non-repeated top-level structs using footer definition-level thresholds.
     - Native recursive Arrow array construction for mathematically arbitrary mixed repeated struct/map/list shapes.
       Remaining honest effort: large. The next real finish line is a recursive
       materialization tree (`struct`/`list`/`map`/`leaf`) that owns buffer
       allocation, repetition/definition-level interpretation, Arrow schema
       construction, and Arrow array construction. Until that exists, supported
       shapes still depend on several branch-specific materializers. Expected
       effort is multiple focused engineering days, plus generated deep-shape
       fixtures, because every recursive level must preserve memory safety and
       exact null/empty-list semantics.
     - [x] Start recursive materialization tree construction from Parquet paths.
     - [x] Persist recursive materialization trees in native output layout.
     - [x] Merge per-leaf recursive trees into one validated output-field tree.
     - [x] Drive native struct/list allocation counts from recursive output-field trees.
     - [x] Store and validate source column indices on recursive materialization leaves.
   - Repeated fields.
   - Full definition/repetition level reconstruction.
   - [x] Production fallback for nested/repeated files through PyArrow.

1. Broaden row-group and page coverage:

   - [x] Multi-row-group parity tests.
   - [x] Simple top-level lists across multiple row groups.
   - [x] Multiple data pages per column.
   - [x] Mixed null/non-null spans across pages.
   - [x] Native footer parsing for empty row groups.
   - [x] Production fallback for empty row groups through PyArrow.
   - [x] Empty files.

1. Expand input source support:

   - [x] File-like objects through PyArrow fallback.
   - [x] Buffers through PyArrow fallback.
   - [x] Local `file://` filesystem URIs through the direct Arrow path.
   - [x] Remote filesystem/cloud single-file and directory URIs through
     bounded local staging and shared Arrow-source execution.
   - [x] Directory/dataset reads through the direct Arrow path.

1. Match production reader behavior:

   - [x] Native projection support for top-level scalar columns.
   - [x] Native projection support for supported top-level struct columns.
   - [x] Native projection support for supported top-level list columns.
   - [x] Native projection support for empty supported file schemas.
   - [x] Projection support through PyArrow fallback when native cannot satisfy
     projected column reads.
   - [x] Projection support through PyArrow fallback for complex/nested lists.
   - [x] Batch-size control, including PyArrow fallback when native row-group
     batches would exceed the requested batch size.
   - [x] Predicate/filter integration through PyArrow dataset scanner fallback.
   - Native predicate pushdown if direct native reads become dataset-aware.

1. Harden large-file behavior:

   - [x] Reuse the native input file handle across row groups.
   - [x] Reuse native page payload buffers during row-group materialization.
   - Streaming materialization instead of whole-row-group buffering where possible.
   - [x] Memory-budget checks for decoded/materialized buffers.

1. Add cross-writer compatibility:

   - [x] PyArrow-written files through PyArrow fallback.
   - [x] Spark-compatible INT96 timestamp variants through PyArrow fallback.
   - [x] Spark-flavored nested PyArrow fixtures through PyArrow fallback.
   - Spark-written files.
   - [x] DuckDB-written files through PyArrow fallback.
   - [x] BigQuery-compatible standard scalar/logical variants without Arrow
     schema metadata through PyArrow fallback.
   - [x] BigQuery-export-like nested/repeated fixtures without Arrow schema
     metadata through PyArrow fallback.
   - BigQuery-exported Parquet variants.

1. Keep fallback observability strong:

   - [x] Preserve clear route labels for native vs PyArrow reads.
   - [x] Log unsupported native-reader cases at a useful level.
   - [x] Add counters/diagnostics if native reader adoption becomes user-visible.
