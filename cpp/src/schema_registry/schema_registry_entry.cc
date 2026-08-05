// Public schema-registry merge orchestration.

#include "sanitize/schema_registry/schema_registry.hh"

#include "internal/planning/schema_evolution.hh"
#include "sanitize/metadata/file_metadata.hh"
#include "schema_registry/schema_registry_internal.hh"

#include <optional>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

namespace sanitize {
using schema_registry_internal::DriftEvent;

Result<bool>
schema_registry_has_canonical_schema(std::string_view registry_json) {
  SAN_ASSIGN_OR_RAISE(
      auto schema,
      schema_registry_internal::canonical_schema_from_registry_json(
          registry_json));
  return schema && !schema->fields.empty();
}

Result<SchemaRegistryMergeResult> merge_schema_registry_with_previous(
    const SchemaRegistryMergeInput &input,
    std::optional<LogicalSchema> previous_schema) {
  if (input.field_name_policy.empty()) {
    return Status::Invalid("schema registry merge: field_name_policy is empty");
  }
  if (input.default_key_name.empty()) {
    return Status::Invalid("schema registry merge: default_key_name is empty");
  }

  std::string detected_at = input.detected_at;
  if (detected_at.empty()) {
    SAN_ASSIGN_OR_RAISE(detected_at, current_utc_iso_timestamp());
  }

  std::vector<DriftEvent> drifts;
  drifts.reserve(input.inferred_schema.fields.size());
  LogicalSchema schema;
  if ((!previous_schema || previous_schema->fields.empty()) &&
      input.schema_evolution == SchemaEvolutionMode::kStrict) {
    return Status::Invalid(
        "Strict schema evolution requires a non-empty canonical schema");
  }
  if (!previous_schema || previous_schema->fields.empty()) {
    schema = input.inferred_schema;
    schema_registry_internal::normalize_integer_float_schema(schema);
    for (const auto &field : schema.fields) {
      drifts.push_back(DriftEvent{
          .source_path = field.name,
          .output_name = field.name,
          .drift_type = "newly_added",
          .previous_schema = std::nullopt,
          .new_schema = schema_registry_internal::field_type_string(field)});
    }
  } else {
    schema_registry_internal::normalize_integer_float_schema(*previous_schema);
    LogicalSchema inferred_schema = input.inferred_schema;
    schema_registry_internal::normalize_integer_float_schema(inferred_schema);
    if (input.schema_evolution == SchemaEvolutionMode::kStrict) {
      SAN_ASSIGN_OR_RAISE(
          schema,
          internal::evolve_schema(*previous_schema, inferred_schema,
                                  input.schema_evolution, input.field_order));
    } else {
      schema.fields = schema_registry_internal::merge_registry_fields(
          previous_schema->fields, inferred_schema.fields, "", drifts,
          detected_at, input.default_key_name);
    }
  }
  schema_registry_internal::normalize_integer_float_schema(schema);
  if (input.schema_evolution != SchemaEvolutionMode::kStrict &&
      input.field_order == FieldOrderPolicy::kAlphabetically) {
    schema =
        internal::reorder_schema_fields(schema, nullptr, input.field_order);
  }

  SchemaRegistryMergeResult out;
  out.schema = std::move(schema);
  out.drifts_json =
      schema_registry_internal::drift_events_json(drifts, detected_at);
  out.registry_json = schema_registry_internal::registry_json(
      out.schema, input.registry_json, input.field_name_policy,
      !drifts.empty());
  out.detected_at = std::move(detected_at);
  return out;
}

Result<SchemaRegistryMergeResult>
merge_schema_registry(const SchemaRegistryMergeInput &input) {
  SAN_ASSIGN_OR_RAISE(
      auto previous_schema,
      schema_registry_internal::canonical_schema_from_registry_json(
          input.registry_json));
  return merge_schema_registry_with_previous(input, std::move(previous_schema));
}

Result<SchemaRegistryMergeResult> merge_schema_registry_with_previous_schema(
    const SchemaRegistryMergeInput &input,
    const LogicalSchema &previous_schema) {
  return merge_schema_registry_with_previous(input, previous_schema);
}
} // namespace sanitize
