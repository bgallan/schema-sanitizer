// Implements output field-name sanitization helpers.

#include "internal/planning/field_name_sanitizer.hh"

#include <algorithm>
#include <cstddef>
#include <memory>
#include <string>
#include <string_view>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include "internal/planning/field_name_collision.hh"
#include "internal/planning/field_name_policy.hh"
#include "internal/planning/variant_field_names.hh"

namespace sanitize::internal {
namespace {

constexpr std::size_t kInitialSuffixLength = 6;
constexpr std::size_t kMaxSuffixLength = 16;
constexpr std::string_view kFlattenedSuffix = "_flattened";
constexpr std::string_view kSanitizedFlattenedSuffix = "flattened";

// Returns whether prepared options preserve source names.
bool prepared_uses_preserve_policy(
    const sanitize::PreparedOptions &opts) noexcept {
  return uses_preserve_policy(opts.spec.field_name_policy);
}

sanitize::LogicalType
sanitize_logical_type_names(const sanitize::LogicalType &type,
                            const sanitize::PreparedOptions &opts);

// Sanitizes all fields in one sibling scope while resolving collisions.
std::vector<sanitize::LogicalField>
sanitize_logical_fields(const std::vector<sanitize::LogicalField> &fields,
                        const sanitize::PreparedOptions &opts) {
  std::vector<std::string_view> dirty_names;
  dirty_names.reserve(fields.size());
  for (const auto &field : fields)
    dirty_names.push_back(field.name);

  const std::vector<std::string> clean_names =
      clean_sibling_field_names(dirty_names, opts);

  std::vector<sanitize::LogicalField> out;
  out.reserve(fields.size());
  for (std::size_t i = 0; i < fields.size(); ++i) {
    sanitize::LogicalField field;
    field.name = clean_names[i];
    field.nullable = fields[i].nullable;
    if (fields[i].type) {
      field.type = std::make_unique<sanitize::LogicalType>(
          sanitize_logical_type_names(*fields[i].type, opts));
    }
    out.push_back(std::move(field));
  }
  return out;
}

// Sanitizes nested logical type field names recursively.
sanitize::LogicalType
sanitize_logical_type_names(const sanitize::LogicalType &type,
                            const sanitize::PreparedOptions &opts) {
  sanitize::LogicalType out(type.kind);
  switch (type.kind) {
  case sanitize::LogicalKind::kStruct:
    out.fields = sanitize_logical_fields(type.fields, opts);
    return out;
  case sanitize::LogicalKind::kList:
    if (type.value) {
      out.value = std::make_unique<sanitize::LogicalType>(
          sanitize_logical_type_names(*type.value, opts));
    }
    return out;
  default:
    return type;
  }
}

} // namespace

bool is_reserved_etl_column_name(std::string_view name) noexcept {
  return name == "schema_registry" || name == "schema_drifts" ||
         name == "source_file" || name == "ingestion_timestamp";
}

std::string clean_field_name_base(std::string_view dirty,
                                  std::string_view field_name_policy) {
  if (uses_preserve_policy(field_name_policy))
    return std::string(dirty);

  std::string out;
  out.reserve(dirty.size());
  if (uses_lower_snake_policy(field_name_policy)) {
    bool previous_underscore = false;
    for (unsigned char c : dirty) {
      char clean = lower_snake(c);
      if (clean == '_') {
        if (!previous_underscore && !out.empty())
          out.push_back(clean);
        previous_underscore = true;
      } else {
        out.push_back(clean);
        previous_underscore = false;
      }
    }
    while (!out.empty() && out.back() == '_')
      out.pop_back();
    if (out.empty())
      out = "field";
    if (out.front() >= '0' && out.front() <= '9') {
      std::string prefixed;
      prefixed.reserve(out.size() + std::string_view("field_").size());
      prefixed = "field_";
      prefixed += out;
      return prefixed;
    }
    return out;
  }

  for (unsigned char c : dirty) {
    const char clean = lower_alpha(c);
    if (clean != '\0')
      out.push_back(clean);
  }
  if (out.empty())
    out = "field";
  return out;
}

std::string clean_field_name_base(std::string_view dirty,
                                  const sanitize::PreparedOptions &opts) {
  if (dirty == opts.spec.default_key_name)
    return std::string(dirty);
  if (prepared_uses_preserve_policy(opts))
    return std::string(dirty);
  return clean_field_name_base(dirty, opts.spec.field_name_policy);
}

bool field_name_matches_output(std::string_view dirty, std::string_view clean,
                               std::string_view field_name_policy) {
  const std::string base = clean_field_name_base(dirty, field_name_policy);
  if (clean == base)
    return true;
  if (uses_lower_snake_policy(field_name_policy)) {
    const std::string_view version_base = versioned_field_base(clean);
    if (!version_base.empty() && version_base == base)
      return true;
  }
  if (uses_preserve_policy(field_name_policy))
    return false;

  for (std::size_t len = kInitialSuffixLength; len <= kMaxSuffixLength; ++len) {
    if (clean == clean_with_suffix(dirty, base, len))
      return true;
  }
  return false;
}

bool field_name_matches_output(std::string_view dirty, std::string_view clean,
                               const sanitize::PreparedOptions &opts) {
  if (dirty == opts.spec.default_key_name && clean == dirty)
    return true;
  return field_name_matches_output(dirty, clean, opts.spec.field_name_policy);
}

std::string_view
unflattened_output_name(std::string_view output_name) noexcept {
  if (output_name.ends_with(kFlattenedSuffix))
    return output_name.substr(0, output_name.size() - kFlattenedSuffix.size());
  if (output_name.ends_with(kSanitizedFlattenedSuffix)) {
    return output_name.substr(0, output_name.size() -
                                     kSanitizedFlattenedSuffix.size());
  }
  return {};
}

std::vector<std::string>
clean_sibling_field_names(const std::vector<std::string_view> &dirty_names,
                          const sanitize::PreparedOptions &opts) {
  std::vector<std::string> out(dirty_names.size());
  if (prepared_uses_preserve_policy(opts)) {
    for (std::size_t i = 0; i < dirty_names.size(); ++i)
      out[i] = std::string(dirty_names[i]);
    return out;
  }

  std::vector<std::string> bases;
  bases.reserve(dirty_names.size());
  std::unordered_map<std::string, std::size_t> base_counts;
  base_counts.reserve(dirty_names.size());
  for (std::string_view dirty : dirty_names) {
    std::string base = clean_field_name_base(dirty, opts);
    base_counts[base] += 1;
    bases.push_back(std::move(base));
  }

  std::vector<std::size_t> order(dirty_names.size());
  for (std::size_t i = 0; i < order.size(); ++i)
    order[i] = i;
  std::ranges::sort(order, [&](std::size_t lhs, std::size_t rhs) {
    if (dirty_names[lhs] == dirty_names[rhs])
      return lhs < rhs;
    return dirty_names[lhs] < dirty_names[rhs];
  });

  std::unordered_set<std::string> used;
  used.reserve(dirty_names.size());
  for (std::size_t idx : order) {
    const std::string &base = bases[idx];
    std::string candidate = base;
    if (base_counts[base] > 1)
      candidate =
          clean_with_suffix(dirty_names[idx], base, kInitialSuffixLength);

    for (std::size_t len = kInitialSuffixLength;
         used.contains(candidate) && len <= kMaxSuffixLength; ++len) {
      candidate = clean_with_suffix(dirty_names[idx], base, len);
    }
    while (used.contains(candidate))
      candidate.push_back('z');

    out[idx] = candidate;
    used.insert(std::move(candidate));
  }

  return out;
}

sanitize::LogicalSchema
sanitize_logical_schema_field_names(const sanitize::LogicalSchema &schema,
                                    const sanitize::PreparedOptions &opts) {
  sanitize::LogicalSchema out;
  out.fields = sanitize_logical_fields(schema.fields, opts);
  for (std::size_t i = 0; i < schema.fields.size(); ++i) {
    if (is_reserved_etl_column_name(schema.fields[i].name))
      out.fields[i].name = schema.fields[i].name;
  }
  return out;
}

} // namespace sanitize::internal
