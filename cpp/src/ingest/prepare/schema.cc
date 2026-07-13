// Resolves the final logical schema used by prepared ingestion.

#include "ingest/prepare/prepare_internal.hh"

#include "internal/planning/field_name_sanitizer.hh"
#include "internal/planning/schema_evolution.hh"

namespace sanitize::ingest_internal {

sanitize::Result<LogicalSchema>
resolve_ingest_logical_schema(const PreparedOptions &opts,
                              const LogicalSchema &inferred_schema) {
  const bool has_contract = static_cast<bool>(opts.spec.arrow_schema_contract);
  if (!has_contract) {
    return internal::reorder_schema_fields(inferred_schema, nullptr,
                                           opts.spec.field_order);
  }

  auto contract_schema = internal::sanitize_logical_schema_field_names(
      *opts.spec.arrow_schema_contract, opts);
  if (opts.spec.schema_evolution == SchemaEvolutionMode::kStrict) {
    if (contract_schema.fields.empty()) {
      return sanitize::Status::Invalid(
          "Strict schema evolution requires a non-empty schema contract");
    }
    return internal::reorder_schema_fields(contract_schema, &contract_schema,
                                           opts.spec.field_order);
  }

  return internal::evolve_schema(contract_schema, inferred_schema,
                                 opts.spec.schema_evolution,
                                 opts.spec.field_order);
}

} // namespace sanitize::ingest_internal
