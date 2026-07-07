// Provides stable field-name hash helpers used by planning and lookup.

#pragma once

#include <cstdint>
#include <string_view>

namespace sanitize::detail {

// Returns the ASCII lowercase form of a byte.
static inline unsigned char ascii_lower(unsigned char c) noexcept {
  return (c >= 'A' && c <= 'Z') ? static_cast<unsigned char>(c + 32) : c;
}

// FNV-1a 64-bit. Reserve 0 as an empty sentinel.
static inline uint64_t hash_key64(std::string_view s) noexcept {
  uint64_t h = 14695981039346656037ull;
  for (unsigned char c : s) {
    h ^= static_cast<uint64_t>(c);
    h *= 1099511628211ull;
  }
  return h == 0 ? 1ull : h;
}

// Case-folded (ASCII) hash used for boolean token matching without allocating.
static inline uint64_t hash_key64_casefold(std::string_view s) noexcept {
  uint64_t h = 14695981039346656037ull;
  for (unsigned char c : s) {
    h ^= static_cast<uint64_t>(ascii_lower(c));
    h *= 1099511628211ull;
  }
  return h == 0 ? 1ull : h;
}

} // namespace sanitize::detail
