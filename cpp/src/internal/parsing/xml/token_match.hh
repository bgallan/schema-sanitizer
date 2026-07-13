// Shared XML whitespace and token matching.

#pragma once

#include <algorithm>
#include <cstddef>
#include <string_view>

namespace sanitize::internal::xml_tokens {

/// Return whether one byte is XML whitespace as defined by XML 1.0.
[[nodiscard]] constexpr bool is_xml_whitespace(char value) noexcept {
  return value == ' ' || value == '\t' || value == '\n' || value == '\r';
}

/// Return whether text contains only XML whitespace characters.
[[nodiscard]] inline bool xml_is_ws(std::string_view text) noexcept {
  return std::ranges::all_of(text, is_xml_whitespace);
}

/// Return whether text has token at the requested byte offset.
[[nodiscard]] inline bool starts_with_at(std::string_view text, std::size_t pos,
                                         std::string_view token) noexcept {
  return pos <= text.size() && text.substr(pos).starts_with(token);
}

/// Convert one ASCII byte to lowercase without locale-dependent machinery.
[[nodiscard]] constexpr unsigned char
ascii_lower(unsigned char value) noexcept {
  return value >= 'A' && value <= 'Z'
             ? static_cast<unsigned char>(value + ('a' - 'A'))
             : value;
}

/// Return whether text has token at the byte offset, ignoring ASCII case.
[[nodiscard]] inline bool
starts_with_ascii_ci_at(std::string_view text, std::size_t pos,
                        std::string_view token) noexcept {
  if (pos > text.size() || text.size() - pos < token.size()) {
    return false;
  }
  const std::string_view candidate = text.substr(pos, token.size());
  return std::ranges::equal(candidate, token, [](char left, char right) {
    return ascii_lower(static_cast<unsigned char>(left)) ==
           ascii_lower(static_cast<unsigned char>(right));
  });
}

} // namespace sanitize::internal::xml_tokens
