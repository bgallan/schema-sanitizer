// Implements small JSON object/string writers for internal diagnostic payloads.

#include "internal/json_encoding/token_writer.hh"

#include <array>
#include <charconv>
#include <cmath>
#include <cstddef>
#include <cstdint>
#include <string>
#include <string_view>

namespace sanitize::internal::json_encoding {

namespace {

[[nodiscard]] constexpr bool requires_json_escape(unsigned char byte) noexcept {
  return byte < 0x20 || byte == '"' || byte == '\\';
}

template <class String>
void append_escaped_byte(String &out, unsigned char byte) {
  static constexpr std::string_view kHex = "0123456789abcdef";
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
    out += "\\u00";
    out.push_back(kHex[(byte >> 4) & 0xF]);
    out.push_back(kHex[byte & 0xF]);
    break;
  }
}

} // namespace

template <class String>
void append_string_impl(String &out, std::string_view value) {
  out.push_back('"');
  std::size_t run_start = 0;
  for (std::size_t index = 0; index < value.size(); ++index) {
    const auto byte = static_cast<unsigned char>(value[index]);
    if (!requires_json_escape(byte)) {
      continue;
    }
    if (index > run_start) {
      out.append(value.data() + run_start, index - run_start);
    }
    append_escaped_byte(out, byte);
    run_start = index + 1;
  }
  if (run_start < value.size()) {
    out.append(value.data() + run_start, value.size() - run_start);
  }
  out.push_back('"');
}

void append_string(std::string &out, std::string_view value) {
  append_string_impl(out, value);
}

void append_string(std::pmr::string &out, std::string_view value) {
  append_string_impl(out, value);
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

void append_double_field(std::string &out, bool &first, std::string_view key,
                         double value) {
  append_key(out, first, key);
  if (!std::isfinite(value)) {
    out += "0";
    return;
  }
  std::array<char, 64> buffer{};
  auto [ptr, ec] = std::to_chars(buffer.data(), buffer.data() + buffer.size(),
                                 value, std::chars_format::general);
  if (ec == std::errc()) {
    out.append(buffer.data(), static_cast<std::size_t>(ptr - buffer.data()));
    return;
  }
  out += "0";
}

} // namespace sanitize::internal::json_encoding
