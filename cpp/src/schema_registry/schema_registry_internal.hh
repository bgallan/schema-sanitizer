// Declares internal helpers for the native schema-registry engine.

#pragma once

#include "sanitize/core/logical_schema.hh"
#include "sanitize/core/status.hh"

#include <cstdint>
#include <optional>
#include <string>
#include <string_view>
#include <vector>

namespace sanitize::schema_registry_internal {

enum class TopLevelKind : std::uint8_t { kScalar, kStruct, kList };

struct DriftEvent {
  std::string source_path;
  std::string output_name;
  std::string drift_type;
  std::optional<std::string> previous_schema;
  std::string new_schema;
};

// Returns a scalar/container compatibility class for a logical type.
TopLevelKind top_level_kind(const LogicalType &type) noexcept;

// Returns whether two logical types have compatible top-level shapes.
bool same_top_level_kind(const LogicalType &left,
                         const LogicalType &right) noexcept;

// Returns a stable unversioned name for names such as field_v2_string.
std::optional<std::string> variant_base_name(std::string_view name);

// Returns the numeric version from names such as field_v2_string.
std::optional<int> variant_version(std::string_view name);

// Returns the stable semantic suffix used in a generated version name.
std::string variant_semantic_type(const LogicalType &type);

// Returns the dirty source segment for one output segment.
std::string source_segment_for_output(std::string_view output_segment);

// Joins nested field names into a source path.
std::string join_path(std::string_view parent, std::string_view child);

// Returns whether two logical type trees are exactly equivalent.
bool logical_type_equal(const LogicalType &left, const LogicalType &right);

// Collapses integer/float sibling variants so float is the durable numeric
// representation for one source field family.
void normalize_integer_float_schema(LogicalSchema &schema);

// Renders a field's logical type in a compact Arrow-like form.
std::string field_type_string(const LogicalField &field);

// Renders a logical type in a compact Arrow-like form.
std::string logical_type_string(const LogicalType &type);

// Parses the canonical schema embedded in a previous registry document.
Result<std::optional<LogicalSchema>>
canonical_schema_from_registry_json(std::string_view registry_json);

// Serializes drift events as compact JSON.
std::string drift_events_json(const std::vector<DriftEvent> &events,
                              std::string_view detected_at);

// Serializes the current registry document as compact JSON.
std::string registry_json(const LogicalSchema &schema,
                          std::string_view previous_registry_json,
                          std::string_view field_name_policy, bool has_drifts);

} // namespace sanitize::schema_registry_internal
