// Declares field-name policy normalization helpers.

#pragma once

#include <string_view>

namespace sanitize::internal {

// Returns whether a raw policy string preserves source names.
bool uses_preserve_policy(std::string_view field_name_policy) noexcept;

// Returns whether a policy string asks for lower snake-case names.
bool uses_lower_snake_policy(std::string_view field_name_policy) noexcept;

// Converts one ASCII byte to lowercase a-z or drops it.
char lower_alpha(unsigned char c) noexcept;

// Converts one ASCII byte to lowercase snake-case content.
char lower_snake(unsigned char c) noexcept;

} // namespace sanitize::internal
