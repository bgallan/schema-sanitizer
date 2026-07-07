// Implements small JSON object/string writers for internal diagnostic payloads.

#include "internal/json/json_write.hh"

#include <array>
#include <charconv>
#include <cstddef>
#include <cstdint>
#include <string>
#include <string_view>

namespace sanitize::internal::json_write {

void append_string(std::string &out, std::string_view value) {
  static constexpr std::string_view kHex = "0123456789abcdef";

  out.push_back('"');
  for (unsigned char byte : value) {
    switch (byte) {
    case '"':
      out += "\\\"";
      break;
    case '\\':
      out += "\\\\";
      break;
    case '\b':
      out += "\\b";
      break;
    case '\f':
      out += "\\f";
      break;
    case '\n':
      out += "\\n";
      break;
    case '\r':
      out += "\\r";
      break;
    case '\t':
      out += "\\t";
      break;
    default:
      if (byte < 0x20) {
        out += "\\u00";
        out.push_back(kHex[(byte >> 4) & 0xF]);
        out.push_back(kHex[byte & 0xF]);
      } else {
        out.push_back(static_cast<char>(byte));
      }
      break;
    }
  }
  out.push_back('"');
}

void append_key(std::string &out, bool &first, std::string_view key) {
  if (!first)
    out.push_back(',');
  first = false;
  append_string(out, key);
  out.push_back(':');
}

void append_string_field(std::string &out, bool &first, std::string_view key,
                         std::string_view value) {
  append_key(out, first, key);
  append_string(out, value);
}

void append_int_field(std::string &out, bool &first, std::string_view key,
                      int64_t value) {
  append_key(out, first, key);
  std::array<char, 32> buffer{};
  auto [ptr, ec] =
      std::to_chars(buffer.data(), buffer.data() + buffer.size(), value);
  if (ec == std::errc()) {
    out.append(buffer.data(), static_cast<std::size_t>(ptr - buffer.data()));
    return;
  }
  out += std::to_string(value);
}

} // namespace sanitize::internal::json_write
