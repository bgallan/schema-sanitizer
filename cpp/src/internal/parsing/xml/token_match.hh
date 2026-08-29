// Declares shared XML safety limits, whitespace, name validation, and token
// matching. The parser validates bounded input while preserving offsets,
// zero-copy views, and deterministic diagnostics.

#pragma once

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <string_view>

namespace sanitize::internal::xml_tokens {

inline constexpr std::uint32_t kMaxXmlNestingDepth = 512U;
inline constexpr std::size_t kMaxXmlNodes = 1'000'000U;
inline constexpr std::size_t kMaxXmlAttributesPerElement = 4'096U;
inline constexpr std::size_t kMaxXmlTotalAttributes = 1'000'000U;
inline constexpr std::size_t kMaxXmlDecodedBytes = std::size_t{512} << 20U;

/// Return whether one byte is XML whitespace as defined by XML 1.0.
[[nodiscard]] constexpr bool is_xml_whitespace(char value) noexcept {
  return value == ' ' || value == '\t' || value == '\n' || value == '\r';
}

/// Return whether text contains only XML whitespace characters.
[[nodiscard]] inline bool xml_is_ws(std::string_view text) noexcept {
  return std::ranges::all_of(text, is_xml_whitespace);
}

/// Return whether one ASCII byte can start an XML name in the supported subset.
[[nodiscard]] constexpr bool
is_ascii_xml_name_start(unsigned char value) noexcept {
  return (value >= 'A' && value <= 'Z') || (value >= 'a' && value <= 'z') ||
         value == '_' || value == ':';
}

/// Return whether one ASCII byte can continue an XML name.
[[nodiscard]] constexpr bool
is_ascii_xml_name_char(unsigned char value) noexcept {
  return is_ascii_xml_name_start(value) || (value >= '0' && value <= '9') ||
         value == '-' || value == '.';
}

/// Validate one XML name after the containing input has passed UTF-8
/// validation.
[[nodiscard]] inline bool is_valid_xml_name(std::string_view name) noexcept {
  if (name.empty()) {
    return false;
  }
  const auto first = static_cast<unsigned char>(name.front());
  if (first < 0x80U && !is_ascii_xml_name_start(first)) {
    return false;
  }
  for (std::size_t index = 1; index < name.size(); ++index) {
    const auto byte = static_cast<unsigned char>(name[index]);
    if (byte < 0x80U && !is_ascii_xml_name_char(byte)) {
      return false;
    }
  }
  return true;
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
