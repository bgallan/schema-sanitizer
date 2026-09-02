// Declares native schema-registry merging for incremental pipelines.
// Decoded canonical schemas, durable registry JSON, and drift events are
// reconciled through one deterministic evolution contract.

#pragma once

#include <string>
#include <string_view>

#include "sanitize/core/logical_schema.hh"
#include "sanitize/core/status.hh"
#include "sanitize/options/options.hh"

namespace sanitize {

struct SchemaRegistryMergeInput {
  LogicalSchema inferred_schema;
  std::string registry_json;
  std::string field_name_policy = "lower_snake";
  std::string detected_at;
  std::string default_key_name = "default_key";
  FieldOrderPolicy field_order = FieldOrderPolicy::kAlphabetically;
  SchemaEvolutionMode schema_evolution = SchemaEvolutionMode::kAdditive;
};

struct SchemaRegistryMergeResult {
  LogicalSchema schema;
  std::string registry_json;
  std::string drifts_json;
  std::string detected_at;
};

/// Merges an inferred schema into a registry-backed canonical output schema.
Result<SchemaRegistryMergeResult>
merge_schema_registry(const SchemaRegistryMergeInput &input);

/// Merges an inferred schema into an already-decoded previous canonical schema.
/// The registry JSON is still carried for durable metadata serialization, but
/// the merge itself does not need to parse it again.
Result<SchemaRegistryMergeResult> merge_schema_registry_with_previous_schema(
    const SchemaRegistryMergeInput &input,
    const LogicalSchema &previous_schema);

/// Returns whether a registry JSON document carries a usable canonical schema.
Result<bool>
schema_registry_has_canonical_schema(std::string_view registry_json);

} // namespace sanitize
