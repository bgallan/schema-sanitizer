// Declares output field-name sanitization helpers.

#pragma once

#include <cstddef>
#include <string>
#include <string_view>
#include <vector>

#include "sanitize/core/logical_schema.hh"
#include "sanitize/options/options.hh"

namespace sanitize::internal {

// Returns whether a raw policy string preserves source names.
bool uses_preserve_policy(std::string_view field_name_policy) noexcept;

// Returns whether a policy string asks for lower snake-case names.
bool uses_lower_snake_policy(std::string_view field_name_policy) noexcept;

// Appends a deterministic collision suffix to a clean base name.
std::string clean_with_suffix(std::string_view dirty, std::string_view base,
                              std::size_t length);

// Returns whether a top-level name is reserved for generated ETL metadata.
bool is_reserved_etl_column_name(std::string_view name) noexcept;

// Returns a sanitized base field name according to the configured policy.
std::string clean_field_name_base(std::string_view dirty,
                                  const sanitize::PreparedOptions &opts);

// Returns a sanitized base field name according to a raw policy value.
std::string clean_field_name_base(std::string_view dirty,
                                  std::string_view field_name_policy);

// Returns whether a dirty source key can address an output field name.
bool field_name_matches_output(std::string_view dirty, std::string_view clean,
                               const sanitize::PreparedOptions &opts);

// Returns whether a dirty source key can address an output field name for a raw
// policy value.
bool field_name_matches_output(std::string_view dirty, std::string_view clean,
                               std::string_view field_name_policy);

// Returns the source-name probe for a flattened output field, if applicable.
std::string_view unflattened_output_name(std::string_view output_name) noexcept;

// Returns final sanitized names for one sibling field scope.
std::vector<std::string>
clean_sibling_field_names(const std::vector<std::string_view> &dirty_names,
                          const sanitize::PreparedOptions &opts);

// Sanitizes all logical schema field names recursively.
sanitize::LogicalSchema
sanitize_logical_schema_field_names(const sanitize::LogicalSchema &schema,
                                    const sanitize::PreparedOptions &opts);

} // namespace sanitize::internal
