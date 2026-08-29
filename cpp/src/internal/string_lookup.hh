// Declares shared heterogeneous string lookup containers for internal hot
// paths. They avoid temporary string allocations while retaining standard
// hash-table lookup semantics.

#pragma once

#include <cstddef>
#include <functional>
#include <string>
#include <string_view>
#include <unordered_map>
#include <unordered_set>

namespace sanitize::internal {

struct TransparentStringHash {
  using is_transparent = void;

  /// Hashes a borrowed string without allocating an owned key.
  [[nodiscard]] std::size_t operator()(std::string_view value) const noexcept {
    return std::hash<std::string_view>{}(value);
  }

  /// Hashes an owned string identically to a borrowed string.
  template <class Allocator>
  [[nodiscard]] std::size_t operator()(
      const std::basic_string<char, std::char_traits<char>, Allocator> &value)
      const noexcept {
    return (*this)(std::string_view(value));
  }

  /// Hashes a null-terminated key identically to other string views.
  [[nodiscard]] std::size_t operator()(const char *value) const noexcept {
    return (*this)(std::string_view(value));
  }
};

template <class T>
using StringLookupMap =
    std::unordered_map<std::string, T, TransparentStringHash, std::equal_to<>>;

// Stores borrowed keys whose source strings outlive the lookup table.
template <class T>
using BorrowedStringLookupMap =
    std::unordered_map<std::string_view, T, TransparentStringHash,
                       std::equal_to<>>;

using BorrowedStringLookupSet =
    std::unordered_set<std::string_view, TransparentStringHash,
                       std::equal_to<>>;

} // namespace sanitize::internal
