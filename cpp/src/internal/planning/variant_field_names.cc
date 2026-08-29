// Implements helpers for hybrid schema-registry versioned field names.
// The helpers normalize private planning state without leaking wire or layout
// details into public APIs.

#include "internal/planning/variant_field_names.hh"

#include <charconv>
#include <cstddef>
#include <limits>
#include <string>
#include <system_error>
#include <utility>

namespace sanitize::internal {

std::optional<VersionedFieldName>
parse_versioned_field_name(std::string_view name) noexcept {
  if (name.size() < 7)
    return std::nullopt;

  const std::size_t marker = name.rfind("_v");
  if (marker == std::string_view::npos || marker == 0 ||
      marker + 3 >= name.size()) {
    return std::nullopt;
  }

  const std::size_t semantic_separator = name.find('_', marker + 2);
  if (semantic_separator == std::string_view::npos ||
      semantic_separator == marker + 2 ||
      semantic_separator + 1 >= name.size()) {
    return std::nullopt;
  }

  int version = 0;
  const char *version_begin = name.data() + marker + 2;
  const char *version_end = name.data() + semantic_separator;
  const auto [ptr, ec] = std::from_chars(version_begin, version_end, version);
  if (ec != std::errc() || ptr != version_end || version < 2)
    return std::nullopt;

  const std::string_view semantic_type = name.substr(semantic_separator + 1);
  bool previous_underscore = false;
  for (const char value : semantic_type) {
    const bool is_lower_alpha = value >= 'a' && value <= 'z';
    if (!is_lower_alpha && value != '_')
      return std::nullopt;
    if (value == '_' && previous_underscore)
      return std::nullopt;
    previous_underscore = value == '_';
  }
  if (semantic_type.front() == '_' || semantic_type.back() == '_')
    return std::nullopt;

  return VersionedFieldName{
      .base = name.substr(0, marker),
      .version = version,
      .semantic_type = semantic_type,
  };
}

std::string make_versioned_field_name(std::string_view base, int version,
                                      std::string_view semantic_type) {
  char digits[std::numeric_limits<int>::digits10 + 3];
  const auto [end, ec] =
      std::to_chars(digits, digits + sizeof(digits), version);
  if (ec != std::errc{})
    std::unreachable();

  std::string out;
  out.reserve(base.size() + 2U + static_cast<std::size_t>(end - digits) + 1U +
              semantic_type.size());
  out.append(base);
  out.append("_v");
  out.append(digits, end);
  out.push_back('_');
  out.append(semantic_type);
  return out;
}

std::string_view versioned_field_base(std::string_view name) noexcept {
  const auto parsed = parse_versioned_field_name(name);
  return parsed ? parsed->base : std::string_view{};
}

std::string_view variant_family_base(std::string_view name) noexcept {
  const std::string_view base = versioned_field_base(name);
  return base.empty() ? name : base;
}

bool same_variant_family(std::string_view lhs, std::string_view rhs) noexcept {
  return lhs != rhs && variant_family_base(lhs) == variant_family_base(rhs);
}

bool in_variant_family(std::string_view name,
                       std::string_view family_base) noexcept {
  return variant_family_base(name) == family_base;
}

} // namespace sanitize::internal
