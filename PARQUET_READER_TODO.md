# Native Parquet Reader TODO

The Parquet input pipeline is production-safe when PyArrow is installed:
native reader skips/failures fall back to PyArrow, successful and failed
fallback attempts are observable, diagnostics preserve the native reason
that triggered recovery, local-path fallback is laddered from native to
PyArrow Dataset and then to `ParquetFile.iter_batches` when filters are absent,
and final diagnostics expose explicit contract booleans for native success,
safe fallback success, and failed fallback outcomes. `last_parquet_pipeline_contract_status()`
turns those fields into a compact fail-closed gate for the most recent read, and
`native_parquet_writer_contract_status()` provides a preflight gate for
schema-sanitizer-native files before relying on the native stream, and
`parquet_preflight_contract_status()` combines that writer-native gate with
PyArrow availability and the same runtime filter contract used by the reader, so
operators can fail closed before reading if neither native nor fallback can cover
the file. `parquet_contract_certification_status()`
now combines the preflight gate, writer-native gate, applicable nested contract,
and optional recursive projection coverage audits into one fail-closed production
certificate for native-or-PyArrow coverage. Native recursive diagnostics now also
have a compact `native_parquet_nested_contract_status()`
gate for the schema-sanitizer writer grammar: every decoded row group, layout
fingerprint, definition/repetition level fingerprint, repeated-ancestor fingerprint,
leaf contract, root contract, and ownership map must be stable/collision-free before
`satisfied=True`. Schema-sanitizer-native files whose nested contract is applicable
but not satisfied are treated as native-read blockers and fall back to PyArrow
instead of being streamed natively. The native reader itself is not yet a complete PyArrow
replacement. Remaining work:

1. Materialize all native-planned encodings:

   - [x] Native Snappy page decompression for footer verification and runtime
     materialization.
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
   - [x] Native materialization for recursive mixed repeated struct/map shapes
     within the bounded schema-sanitizer native writer grammar.
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
     - [x] Footer diagnostics now expose the recursive output layout tree per row group, including root kind, leaf/struct/list/map counts, node depth, branching, source-column indices, physical leaf paths, repeated-node paths, a deterministic physical recursive shape signature, and a structural signature independent of leaf column indexes for production debugging.
     - [x] Added deterministic recursive shape-fuzzer coverage across generated list/map/struct chains of varying depth, root kinds, branch counts, nulls, empties, and map/list/struct leaves.
     - [x] Added bounded Cartesian recursive grammar coverage for every `list`/`map`/`struct` operation word up to depth 3 plus deeper frontier shapes, with multi-row-group native round trips and shape-signature assertions.
     - [x] Added adversarial generated recursive null/empty/full matrix coverage that injects null outer containers, empty repeated nodes, null list elements/map values, and sparse structs across every container kind while asserting native structural diagnostics.
     - [x] Added per-row-group recursive phase-matrix coverage that isolates all-null, empty-only, sparse, and full values into separate row groups for deep list/map/struct shapes, proving offsets, validity, and structural signatures reset cleanly between row groups.
     - [x] Added projected multi-root recursive phase-matrix coverage that combines several generated deep roots in one file, projects reordered subsets across every phase row group, and asserts native footer layout fields match the requested projection order.
     - [x] Added a public native recursive layout summary that folds row-group recursive diagnostics into a compact stability report for shape-signature drift, structural-signature drift, leaf-path drift, repeated-node-path drift, and projected-root invariance checks.
     - [x] Added generated recursive projection-with-noise coverage that keeps one deep target root projected while many unprojected Cartesian recursive roots are present, proving unprojected arbitrary nested branches do not perturb the projected native root layout.
     - [x] Recursive layout summaries now expose stable per-field/layout fingerprints, order-independent canonical fingerprints, field-name lookup maps, leaf/repeated-node ownership maps, and duplicate physical leaf/repeated-node ownership across projected roots, so production diagnostics can compare arbitrary nested layouts without scanning full row-group JSON.
     - [x] Footer diagnostics and recursive summaries now expose component-wise leaf/repeated-node paths in addition to legacy dot-joined labels, avoiding false collisions for arbitrary nested field names that themselves contain dots or separator-like characters.
     - [x] Footer diagnostics and recursive summaries now include per-leaf maximum definition/repetition levels plus path-definition-level vectors, with drift detection and canonical fingerprints for required/optional/nullability profiles across row groups.
     - [x] Footer diagnostics and recursive summaries now expose path-repetition-level vectors and canonical leaf repetition-path fingerprints, making repeated-container topology drift visible for mathematically arbitrary list/map nesting across row groups and projections.
     - [x] Recursive summaries now expose per-row-group layout, leaf-level, and repetition-path fingerprints plus stability flags, making row-group segmentation drift visible without scanning full footer JSON.
     - [x] Recursive summaries now expose leaf-to-repeated-ancestor fingerprints and per-row-group stability flags, tying each physical leaf back to the named list/map containers that own its repetition levels so deep topology drift is diagnosable beyond raw repetition vectors.
     - [x] Leaf-to-repeated-ancestor fingerprints now include the complete leaf repetition-level vector as well as named ancestor ownership, catching deep list/map topology drift even when sampled ancestor levels happen to match.
     - [x] Recursive summaries now expose per-leaf recursive contracts combining component paths, max definition/repetition levels, path-level vectors, and repeated ancestors, plus canonical row-group fingerprints and drift detection for those contracts.
     - [x] Recursive summaries now expose root-level recursive contracts that aggregate leaf contracts, repeated containers, level topology, shape signatures, and depth/branching metrics per projected top-level field, with canonical fingerprints and row-group drift detection.
     - [x] Added a public recursive projection-contract audit that compares projected field/leaf/root contracts against the full-file recursive layout, detecting missing, unexpected, reordered, or drifted arbitrary nested roots without hand-diffing footer JSON.
     - [x] Added a recursive projection-chain contract audit that verifies `full -> source projection -> target projection` preserves the same field/leaf/root contracts as direct `full -> target projection`, catching non-subset, reordering, and contract-drift bugs in chained deep nested projections.
     - [x] Added a recursive projection-partition contract audit that verifies multiple disjoint projected reads exactly cover the full recursive root set, catching missing, duplicated, unknown, or drifted arbitrary nested root contracts before production recomposition.
     - [x] Added a recursive projection-coverage contract audit for partial or intentionally overlapping projected reads, reporting gaps/overlaps separately while still failing on unknown columns or field/leaf/root contract drift for every requested arbitrary nested root.
     - [x] Added recursive segmentation-invariance coverage that writes the same deep null/empty/sparse/full nested table with single, paired, irregular, and per-row row-group cuts, then asserts native projected reads and canonical fingerprints are identical.
     - [x] Added deep requiredness-level matrix coverage for recursive native-writer structs/lists with mixed required roots, required list elements, nullable repeated values, and optional deep chains, asserting native round trip and stable definition/repetition-level diagnostics.
     - [x] Added deterministic seeded recursive fuzz coverage with irregular bounded tree shapes, varied branch widths, repeated sibling subtrees, null/empty/sparse/full values, projected native round trips, and fingerprint assertions.
     - [x] Added recursive projection-permutation coverage that writes several independent irregular deep roots into one file, projects reordered subsets, and asserts every projected root keeps the same canonical fingerprint and ownership maps as the full native footer.
     - [x] Native recursive Arrow array construction for the bounded
       schema-sanitizer native writer grammar across mixed repeated
       struct/map/list shapes.
       Production support contract: the recursive tree now drives native
       readiness, output layout, Arrow schema construction, Arrow array
       construction, repeated-layout decoding, repeated-layout validation, and
       the final materializability gate for schema-sanitizer native-writer
       paths. List/map repeated-layout indexes and struct validity domains are
       stored on recursive materialization nodes and consumed by
       materialization/validation. Scalar and complex list chains now share the
       same recursive schema and array builders, root list/struct outputs use
       the same recursive materializers as nested list/struct nodes, and the
       old branch-specific runtime split has been removed.
       Compatibility boundary: mathematically arbitrary external encodings are
       not promised on the native route. Externally written files that encode
       equivalent list/map/struct semantics with non-native path conventions
       remain production-supported through PyArrow fallback with explicit
       native-readiness diagnostics.
     - [x] Start recursive materialization tree construction from Parquet paths.
     - [x] Persist recursive materialization trees in native output layout.
     - [x] Merge per-leaf recursive trees into one validated output-field tree.
     - [x] Drive native struct/list allocation counts from recursive output-field trees.
     - [x] Store and validate source column indices on recursive materialization leaves.
   - [x] Repeated fields within the bounded schema-sanitizer native writer
     grammar.
   - [x] Full definition/repetition level reconstruction within the bounded
     schema-sanitizer native writer grammar.
   - [x] Production fallback for nested/repeated files through PyArrow.

1. Broaden row-group and page coverage:

   - [x] Native Snappy page verification and value decoding for common external
     writer payloads.
   - [x] Multi-row-group parity tests.
   - [x] Simple top-level lists across multiple row groups.
   - [x] Multiple data pages per column.
   - [x] Mixed null/non-null spans across pages.
   - [x] Native footer parsing for empty row groups.
   - [x] Production fallback for empty row groups through PyArrow.
   - [x] Empty files.

1. Expand input source support:

   - [x] File-like objects through PyArrow fallback.
   - [x] Local file-backed streams can enter the native path by resolving the
     stream's local filename before falling back to PyArrow for anonymous
     streams.
   - [x] Buffers through PyArrow fallback.
   - [x] Buffer-backed Parquet bytes can be staged to a temporary local
     `.parquet` file so native-writer buffers can use the native reader while
     external buffers still keep PyArrow fallback with native diagnostics.
   - [x] Local `file://` filesystem URIs through the direct Arrow path.
   - [x] Remote filesystem/cloud single-file and directory URIs through
     bounded local staging and shared Arrow-source execution.
   - [x] Directory/dataset reads through the direct Arrow path.

1. Match production reader behavior:

   - [x] Native projection support for top-level scalar columns.
   - [x] Native projection support for supported top-level struct columns.
   - [x] Native projection support for supported top-level list columns.
   - [x] Native projection planning decodes page headers/payloads only for
     selected top-level fields, so unsupported/corrupt/large unprojected
     columns do not block projected native reads.
   - [x] Native projected footer diagnostics are exposed through the same ABI
     helper as full-footers, keeping readiness checks aligned with projected
     stream creation.
   - [x] Native projection support for empty supported file schemas.
   - [x] Projection support through PyArrow fallback when native cannot satisfy
     projected column reads.
   - [x] Projection support through PyArrow fallback for complex/nested lists.
   - [x] Batch-size control, including PyArrow fallback when native row-group
     batches would exceed the requested batch size.
   - [x] Predicate/filter integration through PyArrow dataset scanner fallback.
   - [x] Parquet API tests collect and skip cleanly when PyArrow is absent,
     preserving the package's optional-PyArrow contract in CI.
   - Native predicate pushdown if direct native reads become dataset-aware.

1. Harden large-file behavior:

   - [x] Reuse the native input file handle across row groups.
   - [x] Reuse native page payload buffers during row-group materialization.
   - Streaming materialization instead of whole-row-group buffering where possible.
   - [x] Projected native planning skips unprojected page decoding, reducing
     CPU and memory before row-group materialization.
   - [x] Buffer-backed native-writer reads can stage once to a temporary local
     file instead of forcing the non-path PyArrow iter-batches route.
   - [x] Memory-budget checks for decoded/materialized buffers.

1. Add cross-writer compatibility:

   - [x] Native reader/writer Snappy codec support for schema-sanitizer files.
   - [x] PyArrow/Spark/BigQuery-like Snappy page payloads are decoded and
     verified by native footer diagnostics before falling back for non-native
     writer/layout reasons.
   - [x] PyArrow-written files through PyArrow fallback.
   - [x] Spark-compatible INT96 timestamp variants through PyArrow fallback.
   - [x] Spark-flavored nested PyArrow fixtures through PyArrow fallback.
   - [x] PyArrow legacy/non-compliant nested list/map encoding fixtures through
     PyArrow fallback.
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
   - [x] Expose a defensive Parquet route/fallback observability snapshot with
     route counts, native-reader reason counts, last route, and last native
     diagnostic details.
   - [x] Harden safe-fallback behavior so ordinary native footer/native stream
     exceptions (`RuntimeError`, `ValueError`, `OSError`, etc.) never escape the
     pipeline before PyArrow gets a chance to read; diagnostics now preserve the
     original native reason plus `fallback_expected`, `fallback_route`,
     `fallback_attempted`, and `fallback_succeeded`.
   - [x] Record successful PyArrow fallback route annotations for both dataset
     scanner and `ParquetFile.iter_batches` routes, so production observability
     can prove that unsupported native reads actually recovered through fallback.
   - [x] Record failed PyArrow fallback attempts before re-raising, including
     `fallback_attempted=True`, `fallback_succeeded=False`, `fallback_route`,
     and `fallback_error`, so the safe pipeline never hides whether recovery
     was attempted or whether PyArrow itself rejected the input.
   - [x] Add fallback attempt history and a local-path fallback ladder: native
     reader -> PyArrow Dataset scanner -> `ParquetFile.iter_batches` when no
     filters are present. This makes the pipeline resilient to Dataset-specific
     open/scan failures while preserving fail-closed behavior for filters, which
     require the Dataset scanner route.
   - [x] Add aggregate fallback attempt/success/failure counters per PyArrow
     route in the defensive observability snapshot, so production can prove the
     safe fallback contract over more than the last read.
   - [x] Record the Parquet `created_by` marker and
     `native_writer_detected`/`native_writer_contract_satisfied` diagnostics on
     native successes, making the schema-sanitizer native-writer route explicit
     instead of implicit in `native_stream`.
   - [x] Add `parquet_contract_certification_status()` as a single fail-closed
     certificate that combines safe pipeline preflight, schema-sanitizer native
     writer certification, applicable nested-recursive contract status, and
     optional projection-coverage audits for arbitrary nested roots.
   - [x] Add batch-size preflight/certification to the schema-sanitizer-native
     writer gate, so a native file is only certified for runtime parameters that
     the native reader can actually serve without splitting row groups; otherwise
     the safe PyArrow fallback remains available but the native guarantee fails
     closed.
   - [x] Add filter-aware preflight/certification to the schema-sanitizer-native
     writer gate: predicate filters intentionally fail the native guarantee and
     certify only the PyArrow Dataset fallback route, matching runtime behavior
     and preventing a filtered read from being mis-certified as native.
   - [x] Add a runtime-readiness gate (`parquet_contract_runtime_readiness_status()`
     plus `meta/ci/check_parquet_contract_runtime.py`) so CI and production
     startup checks fail closed when PyArrow fallback or native footer/stream
     hooks are unavailable instead of relying on silently skipped runtime tests.
   - [x] Add `meta/ci/check_parquet_contract_runtime_suite.py`, a stricter
     PyArrow/native CI gate that executes selected end-to-end Parquet contract
     tests for native schema-sanitizer reads, safe fallback, and arbitrary nested
     grammar coverage, and fails closed if any selected test is skipped.
   - [x] Add a grouped runtime-suite manifest and pre-pytest selection validator
     so the PyArrow/native CI gate also fails closed if a contract family loses
     coverage, a selected nodeid is duplicated, or a selected runtime test is
     renamed/removed before the suite executes.
   - [x] Add per-contract-family execution verification to the runtime suite,
     so the PyArrow/native CI gate fails closed if any selected contract group
     does not produce passing reports, even when aggregate pytest totals look
     plausible.
   - [x] Add a durable JSON runtime-contract certificate emitted by
     `meta/ci/check_parquet_contract_runtime_suite.py --certificate-output`,
     covering manifest selection, runtime readiness, per-group execution, and
     the three top-level guarantees, and upload it from CI for release/audit
     review.
