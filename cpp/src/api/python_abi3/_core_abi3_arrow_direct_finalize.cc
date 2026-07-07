// Schema-contract and evolution handling for direct Arrow ingestion.

#include "api/python_abi3/_core_abi3_arrow_direct.hh"

#include "internal/planning/field_name_sanitizer.hh"
#include "internal/planning/schema_evolution.hh"
#include "sanitize/core/logical_schema.hh"
#include "sanitize/core/status.hh"
#include "sanitize/options/options.hh"

namespace core_abi3_internal {

sanitize::Result<sanitize::LogicalSchema>
finalize_direct_arrow_schema(const sanitize::LogicalSchema &input_schema,
                             const sanitize::PreparedOptions &opts) {
  auto inferred_logical =
      sanitize::internal::sanitize_logical_schema_field_names(input_schema,
                                                              opts);

  const bool has_contract = static_cast<bool>(opts.spec.arrow_schema_contract);
  if (opts.spec.schema_evolution == sanitize::SchemaEvolutionMode::kStrict &&
      !has_contract) {
    return sanitize::Status::Invalid(
        "Strict schema evolution requires a schema contract");
  }

  sanitize::LogicalSchema contract_logical;
  if (has_contract) {
    contract_logical = sanitize::internal::sanitize_logical_schema_field_names(
        *opts.spec.arrow_schema_contract, opts);
  }

  if (has_contract) {
    if (opts.spec.schema_evolution == sanitize::SchemaEvolutionMode::kStrict) {
      if (contract_logical.fields.empty()) {
        return sanitize::Status::Invalid(
            "Strict schema evolution requires a non-empty schema contract");
      }
      return sanitize::internal::reorder_schema_fields(
          contract_logical, &contract_logical, opts.spec.field_order);
    }
    return sanitize::internal::evolve_schema(contract_logical, inferred_logical,
                                             opts.spec.schema_evolution,
                                             opts.spec.field_order);
  }
  return sanitize::internal::reorder_schema_fields(inferred_logical, nullptr,
                                                   opts.spec.field_order);
}

} // namespace core_abi3_internal
