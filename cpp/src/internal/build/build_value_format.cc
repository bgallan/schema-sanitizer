// Formats ValueView values used by build conversion fallback paths.

#include "internal/build/build_value_format.hh"

#include <cmath>
#include <cstdint>
#include <sstream>
#include <string>
#include <string_view>

#include "sanitize/core/status.hh"

namespace sanitize::internal {
namespace {

using sanitize::Status;
using sanitize::ValueView;

// Converts value to jsonish.
std::string value_to_jsonish(ValueView value);

// Escapes json string.
std::string escape_json_string(std::string_view value) {
  std::string out;
  out.reserve(value.size() + 2);
  out.push_back('"');
  for (unsigned char c : value) {
    switch (c) {
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
      if (c < 0x20) {
        static constexpr std::string_view hex = "0123456789abcdef";
        out += "\\u00";
        out.push_back(hex[c >> 4]);
        out.push_back(hex[c & 0x0f]);
      } else {
        out.push_back(static_cast<char>(c));
      }
    }
  }
  out.push_back('"');
  return out;
}

// Converts double to string.
std::string double_to_string(double value) {
  if (!std::isfinite(value))
    return "null";
  std::ostringstream oss;
  oss.precision(17);
  oss << value;
  return oss.str();
}

std::string value_to_jsonish(ValueView value) {
  if (value.is_null())
    return "null";
  if (value.is_string())
    return escape_json_string(value.as_string_view());
  if (value.is_bool())
    return value.as_bool() ? "true" : "false";
  if (value.is_int())
    return std::to_string(value.as_int());
  if (value.is_float())
    return double_to_string(value.as_float());
  if (value.is_array()) {
    std::string out;
    out.push_back('[');
    bool first = true;
    auto st = value.for_each_array_element([&](ValueView element) -> Status {
      if (!first)
        out.push_back(',');
      first = false;
      out += value_to_jsonish(element);
      return Status::OK();
    });
    if (!st.ok())
      return "null";
    out.push_back(']');
    return out;
  }
  if (value.is_object()) {
    std::string out;
    out.push_back('{');
    bool first = true;
    auto st = value.for_each_object_field(
        [&](std::string_view key, uint64_t, ValueView field) -> Status {
          if (!first)
            out.push_back(',');
          first = false;
          out += escape_json_string(key);
          out.push_back(':');
          out += value_to_jsonish(field);
          return Status::OK();
        });
    if (!st.ok())
      return "null";
    out.push_back('}');
    return out;
  }
  return "null";
}

} // namespace

std::string value_to_scalar_string(ValueView value) {
  if (value.is_null())
    return "";
  if (value.is_string())
    return std::string(value.as_string_view());
  if (value.is_bool())
    return value.as_bool() ? "true" : "false";
  if (value.is_int())
    return std::to_string(value.as_int());
  if (value.is_float())
    return double_to_string(value.as_float());
  return value_to_jsonish(value);
}

} // namespace sanitize::internal
