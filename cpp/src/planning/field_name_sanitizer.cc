// Implements output and logical-schema field-name sanitization.

#include "internal/planning/field_name_sanitizer.hh"

#include "internal/planning/variant_field_names.hh"
#include "internal/string_lookup.hh"
#include "sanitize/detail/hash.hh"

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <ranges>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

namespace sanitize::internal {
namespace {

constexpr std::size_t kInitialSuffixLength = 6;
constexpr std::size_t kMaxSuffixLength = 16;
constexpr std::string_view kFlattenedSuffix = "_flattened";
constexpr std::string_view kSanitizedFlattenedSuffix = "flattened";
constexpr std::string_view kPolicyPreserve = "preserve";
constexpr std::string_view kPolicyLowerSnake = "lower_snake";

char lower_alpha(unsigned char c) noexcept {
  if (c >= 'A' && c <= 'Z')
    return static_cast<char>(c + ('a' - 'A'));
  if (c >= 'a' && c <= 'z')
    return static_cast<char>(c);
  return '\0';
}

char lower_snake(unsigned char c) noexcept {
  if (c >= 'A' && c <= 'Z')
    return static_cast<char>(c + ('a' - 'A'));
  if ((c >= 'a' && c <= 'z') || (c >= '0' && c <= '9') || c == '_')
    return static_cast<char>(c);
  return '_';
}

bool prepared_uses_preserve_policy(
    const sanitize::PreparedOptions &opts) noexcept {
  return uses_preserve_policy(opts.spec.field_name_policy);
}

sanitize::LogicalType
sanitize_logical_type_names(const sanitize::LogicalType &type,
                            const sanitize::PreparedOptions &opts);

std::vector<sanitize::LogicalField>
sanitize_logical_fields(const std::vector<sanitize::LogicalField> &fields,
                        const sanitize::PreparedOptions &opts) {
  std::vector<std::string_view> dirty_names;
  dirty_names.reserve(fields.size());
  for (const auto &field : fields) {
    dirty_names.push_back(field.name);
  }

  const std::vector<std::string> clean_names =
      clean_sibling_field_names(dirty_names, opts);
  std::vector<sanitize::LogicalField> out;
  out.reserve(fields.size());
  for (std::size_t index = 0; index < fields.size(); ++index) {
    sanitize::LogicalField field;
    field.name = clean_names[index];
    field.nullable = fields[index].nullable;
    if (fields[index].type) {
      field.type = std::make_unique<sanitize::LogicalType>(
          sanitize_logical_type_names(*fields[index].type, opts));
    }
    out.push_back(std::move(field));
  }
  return out;
}

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

bool uses_preserve_policy(std::string_view field_name_policy) noexcept {
  return field_name_policy == kPolicyPreserve;
}

bool uses_lower_snake_policy(std::string_view field_name_policy) noexcept {
  return field_name_policy == kPolicyLowerSnake;
}

std::string clean_with_suffix(std::string_view dirty, std::string_view base,
                              std::size_t length) {
  std::string out;
  out.reserve(base.size() + length);
  out.append(base);
  const std::size_t suffix_start = out.size();
  uint64_t hash = sanitize::detail::hash_key64(dirty);
  for (std::size_t i = 0; i < length; ++i) {
    out.push_back(static_cast<char>('a' + (hash % 26u)));
    hash /= 26u;
    if (hash == 0) {
      hash = sanitize::detail::hash_key64(
          std::string_view(out).substr(suffix_start));
    }
  }
  return out;
}

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
      const char clean = lower_snake(c);
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
      out.reserve(out.size() + std::string_view("field_").size());
      out.insert(0, "field_");
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
  if (prepared_uses_preserve_policy(opts)) {
    std::vector<std::string> preserved;
    preserved.reserve(dirty_names.size());
    for (std::string_view dirty : dirty_names)
      preserved.emplace_back(dirty);
    return preserved;
  }

  std::vector<std::string> bases;
  bases.reserve(dirty_names.size());
  BorrowedStringLookupMap<std::size_t> base_counts;
  base_counts.reserve(dirty_names.size());
  for (std::string_view dirty : dirty_names) {
    bases.push_back(clean_field_name_base(dirty, opts));
    auto [count, _] =
        base_counts.try_emplace(std::string_view(bases.back()), 0U);
    ++count->second;
  }
  if (base_counts.size() == dirty_names.size())
    return bases;

  std::vector<std::string> out(dirty_names.size());
  std::vector<std::size_t> order(dirty_names.size());
  for (std::size_t i = 0; i < order.size(); ++i)
    order[i] = i;
  std::ranges::sort(order, [&](std::size_t lhs, std::size_t rhs) {
    if (dirty_names[lhs] == dirty_names[rhs])
      return lhs < rhs;
    return dirty_names[lhs] < dirty_names[rhs];
  });

  BorrowedStringLookupSet used;
  used.reserve(dirty_names.size());
  for (std::size_t idx : order) {
    const std::string &base = bases[idx];
    std::string candidate = base;
    const auto count = base_counts.find(base);
    if (count != base_counts.end() && count->second > 1) {
      candidate =
          clean_with_suffix(dirty_names[idx], base, kInitialSuffixLength);
    }

    for (std::size_t len = kInitialSuffixLength;
         used.contains(candidate) && len <= kMaxSuffixLength; ++len) {
      candidate = clean_with_suffix(dirty_names[idx], base, len);
    }
    while (used.contains(candidate))
      candidate.push_back('z');

    out[idx] = std::move(candidate);
    used.emplace(out[idx]);
  }

  return out;
}

sanitize::LogicalSchema
sanitize_logical_schema_field_names(const sanitize::LogicalSchema &schema,
                                    const sanitize::PreparedOptions &opts) {
  sanitize::LogicalSchema out;
  out.fields = sanitize_logical_fields(schema.fields, opts);
  for (std::size_t index = 0; index < schema.fields.size(); ++index) {
    if (is_reserved_etl_column_name(schema.fields[index].name)) {
      out.fields[index].name = schema.fields[index].name;
    }
  }
  return out;
}

} // namespace sanitize::internal
