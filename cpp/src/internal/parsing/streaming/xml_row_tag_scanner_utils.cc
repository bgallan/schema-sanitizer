// Implements stateless helpers for streaming XML row-tag scanning.

#include "internal/parsing/streaming/xml_row_tag_scanner_utils.hh"

#include <algorithm>
#include <cctype>

namespace sanitize::internal::xml_row_tag_scanner_utils {

bool xml_is_ws(std::string_view text) {
  return std::all_of(text.begin(), text.end(), [](char c) {
    return std::isspace(static_cast<unsigned char>(c)) != 0;
  });
}

bool starts_with_at(std::string_view text, std::size_t pos,
                    std::string_view token) {
  if (pos > text.size() || text.size() - pos < token.size()) {
    return false;
  }
  return text.compare(pos, token.size(), token) == 0;
}

bool starts_with_ascii_ci_at(std::string_view text, std::size_t pos,
                             std::string_view token) {
  if (pos > text.size() || text.size() - pos < token.size()) {
    return false;
  }
  for (std::size_t i = 0; i < token.size(); ++i) {
    const auto a = static_cast<unsigned char>(text[pos + i]);
    const auto b = static_cast<unsigned char>(token[i]);
    if (std::tolower(a) != std::tolower(b)) {
      return false;
    }
  }
  return true;
}

} // namespace sanitize::internal::xml_row_tag_scanner_utils
