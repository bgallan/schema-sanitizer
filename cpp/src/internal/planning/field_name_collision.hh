// Declares deterministic field-name collision suffix helpers.

#pragma once

#include <cstddef>
#include <string>
#include <string_view>

namespace sanitize::internal {

// Returns a deterministic lowercase alphabetic hash suffix.
std::string alpha_hash_suffix(std::string_view dirty, std::size_t length);

// Appends a deterministic collision suffix to a clean base name.
std::string clean_with_suffix(std::string_view dirty, std::string_view base,
                              std::size_t length);

} // namespace sanitize::internal
