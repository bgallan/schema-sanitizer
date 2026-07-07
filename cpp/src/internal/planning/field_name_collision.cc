// Implements deterministic field-name collision suffix helpers.

#include "internal/planning/field_name_collision.hh"

#include <cstdint>

#include "sanitize/detail/hash.hh"

namespace sanitize::internal {

std::string alpha_hash_suffix(std::string_view dirty, std::size_t length) {
  uint64_t h = sanitize::detail::hash_key64(dirty);
  std::string out;
  out.reserve(length);
  for (std::size_t i = 0; i < length; ++i) {
    out.push_back(static_cast<char>('a' + (h % 26u)));
    h /= 26u;
    if (h == 0) {
      // Re-mix deterministically if the requested suffix is longer than the
      // remaining base-26 digits.
      h = sanitize::detail::hash_key64(out);
    }
  }
  return out;
}

std::string clean_with_suffix(std::string_view dirty, std::string_view base,
                              std::size_t length) {
  std::string out(base);
  out += alpha_hash_suffix(dirty, length);
  return out;
}

} // namespace sanitize::internal
