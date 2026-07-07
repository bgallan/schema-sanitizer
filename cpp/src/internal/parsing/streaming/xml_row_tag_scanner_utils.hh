// Declares stateless helpers for streaming XML row-tag scanning.

#pragma once

#include <cstddef>
#include <string_view>

namespace sanitize::internal::xml_row_tag_scanner_utils {

/// Return whether text contains only XML whitespace characters.
bool xml_is_ws(std::string_view text);

/// Return whether text has token at the requested byte offset.
bool starts_with_at(std::string_view text, std::size_t pos,
                    std::string_view token);

/// Return whether text has token at the byte offset, ignoring ASCII case.
bool starts_with_ascii_ci_at(std::string_view text, std::size_t pos,
                             std::string_view token);

} // namespace sanitize::internal::xml_row_tag_scanner_utils
