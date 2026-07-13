/* Native parsed-JSON compaction helpers for Python ABI3 wrappers. */
#include "api/python_abi3/json/_core_abi3_json_tools.hh"
#include "api/python_abi3/json/json_number_write.hh"

#include "internal/json_encoding/token_writer.hh"
#include "internal/parsing/json/ondemand/document.hh"

#include <array>
#include <charconv>
#include <cmath>
#include <limits>
#include <locale>
#include <memory_resource>
#include <sstream>
#include <string>
#include <string_view>

namespace core_abi3_internal {
namespace {

sanitize::Status append_json_value(std::string &out, sanitize::ValueView value);

sanitize::Result<std::string>
compact_json_document_impl(std::string_view text) {
  std::pmr::monotonic_buffer_resource arena;
  sanitize::internal::JsonOnDemandDoc doc(&arena);
  auto value_result = doc.ParseValue(text);
  if (!value_result.ok()) {
    return value_result.status();
  }
  std::string compact;
  const auto status = append_json_value(compact, *value_result);
  if (!status.ok()) {
    return status;
  }
  return compact;
}

sanitize::Result<std::string>
json_array_document_to_jsonl_impl(std::string_view text) {
  std::pmr::monotonic_buffer_resource arena;
  sanitize::internal::JsonOnDemandDoc doc(&arena);
  auto value_result = doc.ParseValue(text);
  if (!value_result.ok()) {
    return value_result.status();
  }
  auto value = *value_result;
  if (!value.is_array()) {
    return sanitize::Status::Invalid("json_array input must be a JSON array");
  }

  std::string out;
  std::size_t index = 0;
  SAN_RETURN_NOT_OK(value.for_each_array_element(
      [&](sanitize::ValueView child) -> sanitize::Status {
        if (!child.is_object()) {
          return sanitize::Status::Invalid(
              "json_array requires object elements; invalid element " +
              std::to_string(index));
        }
        SAN_RETURN_NOT_OK(append_json_value(out, child));
        out.push_back('\n');
        ++index;
        return sanitize::Status::OK();
      }));
  return out;
}

sanitize::Status append_json_object(std::string &out,
                                    sanitize::ValueView value) {
  out.push_back('{');
  bool first = true;
  SAN_RETURN_NOT_OK(value.for_each_object_field(
      [&](std::string_view key, uint64_t, sanitize::ValueView child) {
        if (!first) {
          out.push_back(',');
        }
        first = false;
        sanitize::internal::json_encoding::append_string(out, key);
        out.push_back(':');
        return append_json_value(out, child);
      }));
  out.push_back('}');
  return sanitize::Status::OK();
}

sanitize::Status append_json_array(std::string &out,
                                   sanitize::ValueView value) {
  out.push_back('[');
  bool first = true;
  SAN_RETURN_NOT_OK(
      value.for_each_array_element([&](sanitize::ValueView child) {
        if (!first) {
          out.push_back(',');
        }
        first = false;
        return append_json_value(out, child);
      }));
  out.push_back(']');
  return sanitize::Status::OK();
}

sanitize::Status append_json_value(std::string &out,
                                   sanitize::ValueView value) {
  if (value.is_null()) {
    out += "null";
  } else if (value.is_bool()) {
    out += value.as_bool() ? "true" : "false";
  } else if (value.is_int()) {
    out += std::to_string(value.as_int());
  } else if (value.is_float()) {
    append_json_double(out, value.as_float());
  } else if (value.is_string()) {
    sanitize::internal::json_encoding::append_string(out,
                                                     value.as_string_view());
  } else if (value.is_object()) {
    return append_json_object(out, value);
  } else if (value.is_array()) {
    return append_json_array(out, value);
  }
  return sanitize::Status::OK();
}

} // namespace

void append_json_double(std::string &out, double value) {
  if (std::isnan(value)) {
    out += "NaN";
    return;
  }
  if (std::isinf(value)) {
    out += value < 0 ? "-Infinity" : "Infinity";
    return;
  }
#if !defined(__APPLE__)
  if constexpr (requires(char *first, char *last, double v) {
                  std::to_chars(first, last, v, std::chars_format::general,
                                std::numeric_limits<double>::max_digits10);
                }) {
    std::array<char, 64> buffer{};
    auto [ptr, ec] = std::to_chars(buffer.data(), buffer.data() + buffer.size(),
                                   value, std::chars_format::general,
                                   std::numeric_limits<double>::max_digits10);
    if (ec == std::errc()) {
      out.append(buffer.data(), static_cast<std::size_t>(ptr - buffer.data()));
      return;
    }
  }
#endif
  std::ostringstream stream;
  stream.imbue(std::locale::classic());
  stream.precision(std::numeric_limits<double>::max_digits10);
  stream << value;
  out += stream.str();
}

sanitize::Result<std::string> compact_json_document(std::string_view text) {
  return compact_json_document_impl(text);
}

sanitize::Result<std::string>
json_array_document_to_jsonl(std::string_view text) {
  return json_array_document_to_jsonl_impl(text);
}

} // namespace core_abi3_internal
