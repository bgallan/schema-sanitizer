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
     - [x] Recursive map-entry struct materialization now uses generic footer definition-level thresholds for top-level map and list-map nested struct values.
     - [x] Recursive list-struct element materialization now supports nested ordinary struct children using footer definition-level thresholds.
     - [x] Recursive list-struct element materialization now supports nested ordinary struct children with scalar list/list-chain children using generic footer-derived repeated layouts.
     - [x] Native readiness and stream creation now share merged recursive output-field layout validation, so unsupported mixed recursive layouts fall back before native batch consumption.
     - [x] Top-level recursive struct materialization now supports nested ordinary struct children with scalar list/list-chain children using generic footer-derived repeated layouts and row-level struct validity reconstruction.
     - [x] Map and list-map recursive value materialization now supports nested ordinary struct children with scalar list/list-chain children using map-entry-aware generic repeated layouts.
     - [x] Recursive list-node materialization now supports list-of-struct children under map and list-map values, including scalar/list children inside the struct element.
     - [x] Recursive complex-list schema construction now supports nested list children instead of failing after the first complex list node.
     - [x] Recursive complex-list array materialization now supports `list<list<struct<...>>>` shapes at top level, inside map values, and inside list-map values, including scalar-list children inside the struct element.
     - [x] Recursive complex-list array materialization now supports direct map children under nested list nodes, including `list<list<map<...>>>`, map values containing `list<list<map<...>>>`, list-map values containing `list<list<map<...>>>`, and struct-valued entries in those nested maps.
     - [x] Recursive struct materialization now supports map children inside generic nested map-value structs, covering shapes such as `list<list<map<string, struct<map<...>>>>>`.
     - [x] Added generated deep-shape parity coverage for top-level struct, list-struct, map-value struct, and list-map-value struct parents containing nested `list<list<struct<map<...>>>>` paths.
     - [x] Native map materialization now receives a repeated-layout context object instead of raw enum/index pairs, starting the migration away from branch-specific map/list plumbing.
     - [x] Expanded generated deep-shape parity coverage with map values that contain list/map children and struct fields that contain map/list/list/struct/map paths.
     - [x] Recursive child and struct materialization now pass a child-layout context object instead of separate list-index, map-context, and capability flags.
     - [x] Added generated parity coverage for a top-level struct containing map/list/map/struct/list/list/struct/map recursion.
     - [x] Top-level scalar list, list-of-struct, list-of-map, and complex-list outputs now share one recursive list-output materializer.
     - [x] Removed the old branch-specific scalar-list/list-struct/list-map output materializers after migrating their layout handling into recursive context helpers.
     - [x] Top-level scalar list, list-of-struct, list-of-map, and complex-list schemas now share one recursive list-schema builder.
     - [x] Removed the old branch-specific scalar-list/list-struct/list-map schema construction helpers after unifying list schema construction.
     - [x] Recursive map array materialization now builds top-level, struct-map, list-map, list-struct-map, and generic map shells from one repeated-layout context helper.
     - [x] Recursive struct node materialization now obtains length/null-count/validity from one context helper for map-value, list-element, and row-group struct contexts.
     - [x] Recursive map-value struct length now comes from repeated-layout metadata instead of a separate definition/repetition recount.
     - [x] Recursive list-element struct length now comes from child-context repeated-layout metadata instead of field-shape counters.
     - [x] Recursive map contexts now carry explicit repeated-layout indexes, so map shell construction no longer derives layout indexes from branch-specific enum names.
     - [x] Recursive list-element struct validity now uses the active child repeated-layout index instead of the legacy first-list validity path.
     - [x] Recursive map-value struct validity now uses one level-driven scanner for top-level, struct-map, list-map, list-struct-map, and generic nested map contexts.
     - [x] Recursive list shells now use one repeated-layout helper for top-level list output, nested complex-list nodes, and scalar list-chain materialization.
     - [x] Direct struct children under recursive list nodes now materialize through the shared recursive struct-node materializer instead of a list-specific struct assembly branch.
     - [x] Recursive map-entry level thresholds now live in map layout contexts, removing the branch-specific map-entry definition/repetition switch from validity reconstruction.
     - [x] Recursive map-value struct layout-column selection now uses repeated-layout depth instead of top-level/list-map/struct-map path classifiers.
     - [x] Recursive map runtime context no longer carries branch-specific enum names; legacy entry thresholds are construction-time data only.
     - [x] Recursive list-struct child validity now uses the shared repeated-layout accessor instead of reading repeated-layout storage directly.
     - [x] Recursive child dispatch now uses an explicit parent-context enum instead of `allow_map_child` / `allow_struct_map_value` booleans.
     - [x] Recursive child contexts are now created through named transitions and boundary aggregates instead of a raw boolean factory.
     - [x] Recursive list/map shell Arrow array configuration now goes through one repeated-layout-view helper.
     - [x] Recursive map-entry and struct-child appending now share one child-appender loop with caller-provided node context.
     - [x] Top-level struct layout-column selection now prefers non-repeated leaves using generic repeated-level metadata instead of struct-map path classifiers.
     - [x] Recursive map-entry and top-level struct boundary contexts now use named constructors.
     - [x] Direct map children under map-entry contexts now advance through recursive repeated-layout context, covering map-valued map entries.
     - [x] Recursive planner materializability now comes from a materialization-tree walk instead of the old branch-specific path whitelist.
     - [x] Repeated-layout validators now trust the recursive planner materializability decision instead of rechecking a separate path classifier.
     - [x] Added recursive parity coverage for top-level map-value structs with map children, map-valued maps, map-valued map-valued maps, and list-map values containing maps.
     - [x] Added generated recursive parity coverage for struct-map-map, list-struct-map-map, map-list-map-map, list-map-list-map-map, map-list-struct-map-map, and list-list-struct-map-map shapes.
     - [x] Nested map child layout selection now lives on recursive child context transitions instead of the central child dispatcher.
     - [x] Recursive Arrow schema leaf nullability now uses footer-derived path definition levels instead of hard-coded top-level shape formulas.
     - [x] Native readiness for repeated paths now uses recursive planner materializability and decoded repeated-layout state instead of the old repeated-path whitelist.
     - [x] Repeated-layout decoding now prefers the generic footer-derived repeated-level path for every supported repeated column, leaving branch-specific decoders as fallback only.
     - [x] Row-group repeated-layout validation now walks recursive list/map nodes and compares only the shared structural layout under each repeated node, instead of dispatching through top-level list/map/list-map validators.
     - [x] Map-entry definition/repetition thresholds now always come from footer-derived repeated-level metadata; legacy fixed top-level/list-map/struct-map offsets were removed.
     - [x] Recursive output-field schema and array materialization now dispatch from the recursive tree root kind instead of cached branch-specific shape flags.
     - [x] Recursive output-field grouping no longer stores list/map/struct/list-depth classifier flags; compatibility is enforced through recursive tree merging.
     - [x] Added native parity coverage for deep multi-column list-chain struct leaves that were previously blocked by generic nested-list depth guards.
     - [x] Recursive list-node materialization now delegates leaf, list, map, and struct children through the same child dispatcher using an explicit list-element context.
     - [x] Removed the remaining list-struct-specific child materializer entry point after migrating list child transitions to recursive context helpers.
     - [x] Added deeper generated parity coverage for list/map/list/list/struct/list/map/struct/list/map recursion under the native route.
     - [x] Repeated leaf value counts now prefer deepest decoded repeated-layout metadata instead of branch-specific top-level list/list-map/list-struct shape counters.
     - [x] Repeated leaf parent definition levels now use footer-derived generic repeated path levels instead of legacy top-level shape formulas.
     - [x] Native repeated-path support detection now delegates to the recursive materialization planner instead of enumerating supported top-level repeated shapes.
     - [x] Generic repeated-layout decoding now derives repeated boundaries from footer path definition levels instead of legacy top-level shape formulas.
     - [x] Repeated native read planning now routes supported repeated paths directly into the generic recursive repeated-layout decoder at runtime.
     - [x] Removed unreachable branch-specific repeated-layout dispatch from the runtime repeated read-planning path.
     - [x] Removed obsolete top-level repeated depth classifier wrappers after the recursive planner became the native materializability gate.
     - [x] Leaf value materialization now resolves element counts from decoded recursive repeated-layout metadata instead of legacy top-level shape classifiers.
     - [x] Legacy nested/deep repeated-layout decoders were deleted; the generic recursive decoder is the single runtime repeated-layout source, with legacy fields populated only as synced compatibility views.
     - [x] List BYTE_STREAM_SPLIT value materialization now uses footer-derived parent definition levels like the other list value decoders.
     - [x] List leaf value materializers now validate against decoded recursive leaf layout metadata instead of the legacy first repeated-layout flag.
     - [x] List leaf value counts no longer fall back to synced legacy layout fields; runtime materialization requires decoded recursive repeated-layout metadata.
     - [x] Native repeated-layout lookup now reads only recursive repeated-layout storage; synced legacy repeated fields are no longer fallback inputs.
     - [x] Native reader memory budgeting now sizes all repeated levels from recursive repeated-layout storage instead of special-casing the first three legacy levels.
     - [x] Native readiness now requires decoded recursive repeated-layout storage for the full repetition depth.
     - [x] Footer diagnostics now expose all recursive repeated-level layouts as a depth-preserving array instead of only the first three synced legacy views.
     - [x] Added recursive sibling repeated-branch coverage to prove struct children reuse layout cursors independently for separate repeated subtrees.
     - [x] Recursive materialization nodes now persist their repeated-layout index; list/map materialization and repeated-layout validation consume node metadata instead of a caller-maintained next-index cursor.
     - [x] Recursive struct nodes now persist their validity domain and repeated parent layout; struct validity and layout-column selection consume node metadata instead of runtime parent-kind hints.
     - [x] Removed the recursive child-layout context from materialization traversal after list/map layout indexes and struct validity domains became node-owned.
     - [x] Replaced representative struct layout-column selectors with one recursive node-path/footer-metadata selector used by root and nested structs.
     - [x] Replaced list/map array layout-column selection with recursive node-path/footer-metadata selection instead of first-leaf or repeated-count heuristics.
     - [x] Added adversarial recursive fixtures for mixed nullable struct siblings under root/list/map repeated ancestors.
     - [x] Added projected recursive multi-row-group coverage to verify accepted nested shapes reset offsets and validity independently per row group.
     - [x] Added generated-style recursive null/empty matrix coverage for list/map/struct combinations across every supported container level.
     - [x] Added deterministic generated extreme recursive fixtures for depth-8 list chains, alternating list/map recursion, and wide branch-count stress.
     - [x] Recursive metadata validation now verifies list/map layout indexes match their recursive repeated depth, not just that they are locally in range.
     - [x] Recursive metadata validation now enforces materializer arity assumptions for struct/list/map nodes before native readiness is accepted.
     - [x] Added multi-root recursive projection coverage to verify independent recursive output trees do not share traversal state.
     - [x] Recursive native-readiness planning now uses a status-returning recursive support validator instead of a permissive boolean walker, so unsupported recursive grammar failures are reported precisely.
     - [x] Added independent recursive-root subset projection coverage with reordered projected roots across multiple row groups.
     - [x] Scalar list-chain schema construction now uses the same recursive list schema builder as complex lists; the old private chain schema helper was removed.
     - [x] Scalar list-chain array materialization now uses the same recursive list-node materializer as complex lists; the old chain array shortcut was removed.
     - [x] Root list outputs now use the same recursive list-node materializer as nested list nodes.
     - [x] Added generated depth-10 scalar list-chain coverage to prove high-depth scalar lists stay on the native recursive route after removing the special path.
     - [x] Root struct outputs now use the same recursive struct-node materializer as nested structs; the old top-level optional-struct validity helper was removed.
     - [x] Added required/optional root-struct parity coverage to prove top-level struct nullability still follows footer definition levels.
     - [x] Recursive struct-node and map-entry child assembly now share the same child-appender loop, removing the last manual struct child traversal.
     - Native recursive Arrow array construction for mathematically arbitrary mixed repeated struct/map/list shapes.
       Remaining honest effort: medium-small. The recursive tree now drives
       native readiness, output layout, Arrow schema construction, Arrow array
       construction, repeated-layout decoding, repeated-layout validation, and
       the final materializability gate. List/map repeated-layout indexes and
       struct validity domains are stored on recursive materialization nodes
       and consumed by materialization/validation. Scalar and complex list
       chains now share the same recursive schema and array builders, and root
       structs use the same recursive materializer as nested structs. The hard
       runtime split has been removed for schema-sanitizer's native writer
       grammar. Remaining work is mostly proving and documenting the exact
       production support contract instead of keeping the stronger
       mathematical-arbitrariness claim.
       Current concrete blocker: mathematically arbitrary mixed recursive
       shapes are now covered by hand-written stress cases plus deterministic
       generated depth/branch/null-empty fixtures for the supported recursive
       path grammar. The remaining work is to downgrade the literal
       "mathematically arbitrary" wording to a bounded production support
       statement, then add compatibility fixtures for externally written
       Parquet files that use equivalent but non-native list/map encodings.
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
