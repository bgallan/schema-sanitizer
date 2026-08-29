// Declares shared internal helpers for the native schema-registry engine.
// Type comparison, semantic naming, path construction, JSON encoding, and drift
// models remain consistent across parsing, merging, and publication units.

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

/// Returns a scalar/container compatibility class for a logical type.
TopLevelKind top_level_kind(const LogicalType &type) noexcept;

/// Returns whether two logical types have compatible top-level shapes.
bool same_top_level_kind(const LogicalType &left,
                         const LogicalType &right) noexcept;

/// Returns the stable semantic suffix used in a generated version name.
std::string variant_semantic_type(const LogicalType &type);

/// Returns the borrowed dirty source segment for one output segment.
std::string_view
source_segment_for_output(std::string_view output_segment) noexcept;

/// Joins nested field names into a source path.
std::string join_path(std::string_view parent, std::string_view child);

/// Returns whether two logical type trees are exactly equivalent.
bool logical_type_equal(const LogicalType &left, const LogicalType &right);

/// Merges incoming fields into the previous canonical field tree.
std::vector<LogicalField> merge_registry_fields(
    const std::vector<LogicalField> &base_fields,
    const std::vector<LogicalField> &incoming_fields,
    std::string_view parent_path, std::vector<DriftEvent> &drifts,
    std::string_view detected_at, std::string_view default_key_name);

/// Collapses integer/float sibling variants so float is the durable numeric
/// representation for one source field family.
void normalize_integer_float_schema(LogicalSchema &schema);

/// Renders a field's logical type in a compact Arrow-like form.
std::string field_type_string(const LogicalField &field);

/// Renders a logical type in a compact Arrow-like form.
std::string logical_type_string(const LogicalType &type);

/// Parses the canonical schema embedded in a previous registry document.
Result<std::optional<LogicalSchema>>
canonical_schema_from_registry_json(std::string_view registry_json);

/// Serializes drift events as compact JSON.
std::string drift_events_json(const std::vector<DriftEvent> &events,
                              std::string_view detected_at);

/// Serializes the current registry document as compact JSON.
std::string registry_json(const LogicalSchema &schema,
                          std::string_view previous_registry_json,
                          std::string_view field_name_policy, bool has_drifts);

} // namespace sanitize::schema_registry_internal
