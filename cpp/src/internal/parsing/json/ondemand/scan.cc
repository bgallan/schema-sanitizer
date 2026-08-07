// Scans nested JSON token spans without materializing parsed values.

#include "internal/parsing/json/ondemand/scan.hh"

#include "sanitize/core/status.hh"

#include <cstdint>
#include <string>
#include <string_view>

namespace sanitize::internal::json_scan {

namespace {

enum class ContainerSeparator : uint8_t {
  kContinue,
  kDone,
};

sanitize::Status parse_error_at(const Cursor &c, std::string_view message) {
  return sanitize::Status::Invalid(message, std::to_string(c.offset()));
}

sanitize::Status skip_value_at_depth(Cursor &c, std::size_t depth);

sanitize::Status nesting_error(const Cursor &c) {
  return parse_error_at(
      c, "JSON parse error: nesting exceeds safety limit 512 at byte ");
}

sanitize::Status skip_object_member(Cursor &c, std::size_t depth) {
  if (c.p >= c.end || *c.p != '"') {
    return parse_error_at(c, "JSON parse error: expected string key at byte ");
  }
  SAN_RETURN_NOT_OK(skip_string(c));
  skip_ws(c);
  SAN_RETURN_NOT_OK(expect(c, ':'));
  skip_ws(c);
  SAN_RETURN_NOT_OK(skip_value_at_depth(c, depth));
  skip_ws(c);
  return sanitize::Status::OK();
}

sanitize::Result<ContainerSeparator> consume_object_separator(Cursor &c) {
  if (c.p >= c.end) {
    return parse_error_at(c, "JSON parse error: unterminated object at byte ");
  }
  if (*c.p == ',') {
    ++c.p;
    skip_ws(c);
    return ContainerSeparator::kContinue;
  }
  if (*c.p == '}') {
    ++c.p;
    return ContainerSeparator::kDone;
  }
  return parse_error_at(c, "JSON parse error: expected ',' or '}' at byte ");
}

sanitize::Status skip_object_at_depth(Cursor &c, std::size_t depth) {
  SAN_RETURN_NOT_OK(expect(c, '{'));
  skip_ws(c);
  if (c.p < c.end && *c.p == '}') {
    ++c.p;
    return sanitize::Status::OK();
  }
  std::size_t fields = 0;
  while (true) {
    if (fields >= kMaxJsonObjectFields) {
      return sanitize::Status::Invalid(
          "JSON object field count exceeds safety limit: ",
          std::to_string(fields + 1U), " > ",
          std::to_string(kMaxJsonObjectFields));
    }
    ++fields;
    SAN_RETURN_NOT_OK(skip_object_member(c, depth));
    SAN_ASSIGN_OR_RAISE(ContainerSeparator separator,
                        consume_object_separator(c));
    if (separator == ContainerSeparator::kContinue) {
      continue;
    }
    return sanitize::Status::OK();
  }
}

sanitize::Status skip_array_at_depth(Cursor &c, std::size_t depth) {
  SAN_RETURN_NOT_OK(expect(c, '['));
  skip_ws(c);
  if (c.p < c.end && *c.p == ']') {
    ++c.p;
    return sanitize::Status::OK();
  }
  while (true) {
    SAN_RETURN_NOT_OK(skip_value_at_depth(c, depth));
    skip_ws(c);
    if (c.p >= c.end) {
      return parse_error_at(c, "JSON parse error: unterminated array at byte ");
    }
    if (*c.p == ',') {
      ++c.p;
      skip_ws(c);
      continue;
    }
    if (*c.p == ']') {
      ++c.p;
      return sanitize::Status::OK();
    }
    return parse_error_at(c, "JSON parse error: expected ',' or ']' at byte ");
  }
}

sanitize::Status skip_value_at_depth(Cursor &c, std::size_t depth) {
  skip_ws(c);
  if (c.p >= c.end) {
    return parse_error_at(c, "JSON parse error: unexpected end at byte ");
  }
  const char ch = *c.p;
  if (ch == 'n')
    return skip_literal(c, "null", 4);
  if (ch == 't')
    return skip_literal(c, "true", 4);
  if (ch == 'f')
    return skip_literal(c, "false", 5);
  if (ch == '"')
    return skip_string(c);
  if (ch == '{' || ch == '[') {
    if (depth >= kMaxJsonNestingDepth) {
      return nesting_error(c);
    }
    return ch == '{' ? skip_object_at_depth(c, depth + 1U)
                     : skip_array_at_depth(c, depth + 1U);
  }
  if (ch == '-' || (ch >= '0' && ch <= '9'))
    return skip_number(c);
  return parse_error_at(c, "JSON parse error: invalid value at byte ");
}

} // namespace

sanitize::Status skip_object(Cursor &c) {
  if (kMaxJsonNestingDepth == 0) {
    return nesting_error(c);
  }
  return skip_object_at_depth(c, 1U);
}

sanitize::Status skip_array(Cursor &c) {
  if (kMaxJsonNestingDepth == 0) {
    return nesting_error(c);
  }
  return skip_array_at_depth(c, 1U);
}

sanitize::Status skip_value(Cursor &c) { return skip_value_at_depth(c, 0U); }

} // namespace sanitize::internal::json_scan
