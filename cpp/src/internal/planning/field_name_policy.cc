// Implements field-name policy normalization helpers.

#include "internal/planning/field_name_policy.hh"

namespace sanitize::internal {
namespace {

constexpr std::string_view kPolicyPreserve = "preserve";
constexpr std::string_view kPolicyLowerSnake = "lower_snake";

} // namespace

bool uses_preserve_policy(std::string_view field_name_policy) noexcept {
  return field_name_policy == kPolicyPreserve;
}

bool uses_lower_snake_policy(std::string_view field_name_policy) noexcept {
  return field_name_policy == kPolicyLowerSnake;
}

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

} // namespace sanitize::internal
