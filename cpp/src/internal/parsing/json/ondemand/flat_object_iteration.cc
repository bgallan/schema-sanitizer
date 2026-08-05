// Implements single-pass flat JSON object classification for inference.

#include "internal/parsing/json/ondemand/document.hh"

#include "internal/parsing/json/ondemand/scan.hh"
#include "sanitize/core/primitives.hh"

#include <string>
#include <string_view>

namespace sanitize::internal {
namespace {

[[nodiscard]] bool integer_token_fits_int64(std::string_view token) noexcept {
  const bool negative = !token.empty() && token.front() == '-';
  if (negative) {
    token.remove_prefix(1);
  }
  if (token.size() < 19) {
    return true;
  }
  if (token.size() > 19) {
    return false;
  }
  constexpr std::string_view kInt64Max = "9223372036854775807";
  constexpr std::string_view kInt64MinMagnitude = "9223372036854775808";
  return token <= (negative ? kInt64MinMagnitude : kInt64Max);
}

} // namespace

sanitize::Result<JsonOnDemandDoc::FlatValue>
JsonOnDemandDoc::ParseFlatChildValue(json_scan::Cursor &cursor,
                                     std::string_view text,
                                     std::size_t base_offset) {
  using Kind = FlatValue::Kind;
  if (cursor.p >= cursor.end) {
    return ParseError("JSON parse error: unexpected end", cursor.offset());
  }

  const char ch = *cursor.p;
  if (ch == 'n') {
    SAN_RETURN_NOT_OK(json_scan::skip_literal(cursor, "null", 4));
    return FlatValue{.kind = Kind::kNull, .string_value = {}};
  }
  if (ch == 't') {
    SAN_RETURN_NOT_OK(json_scan::skip_literal(cursor, "true", 4));
    return FlatValue{.kind = Kind::kBool, .string_value = {}};
  }
  if (ch == 'f') {
    SAN_RETURN_NOT_OK(json_scan::skip_literal(cursor, "false", 5));
    return FlatValue{.kind = Kind::kBool, .string_value = {}};
  }
  if (ch == '"') {
    const char *start = cursor.p;
    SAN_RETURN_NOT_OK(json_scan::skip_string(cursor));
    const auto token =
        std::string_view(start, static_cast<std::size_t>(cursor.p - start));
    SAN_ASSIGN_OR_RAISE(
        auto parsed, ParseValue(token, base_offset + static_cast<std::size_t>(
                                                         start - text.data())));
    return FlatValue{.kind = Kind::kString,
                     .string_value = parsed.as_string_view()};
  }
  if (ch == '-' || (ch >= '0' && ch <= '9')) {
    const char *start = cursor.p;
    SAN_RETURN_NOT_OK(json_scan::skip_number(cursor));
    const char *end = cursor.p;
    const std::string_view number(start, static_cast<std::size_t>(end - start));
    bool is_float = false;
    for (const char character : number) {
      if (character == '.' || character == 'e' || character == 'E') {
        is_float = true;
        break;
      }
    }
    if (!is_float && integer_token_fits_int64(number)) {
      return FlatValue{.kind = Kind::kInt, .string_value = {}};
    }
    double floating = 0.0;
    if (!parse_ascii_float64_strict(number, &floating)) {
      return ParseError("JSON parse error: invalid float",
                        base_offset +
                            static_cast<std::size_t>(start - text.data()));
    }
    return FlatValue{.kind = Kind::kFloat, .string_value = {}};
  }
  if (ch == '{' || ch == '[') {
    const char closing = ch == '{' ? '}' : ']';
    auto probe = cursor;
    ++probe.p;
    json_scan::skip_ws(probe);
    const bool empty = probe.p < probe.end && *probe.p == closing;
    SAN_RETURN_NOT_OK(json_scan::skip_value(cursor));
    if (ch == '{') {
      return FlatValue{.kind = empty ? Kind::kEmptyObject : Kind::kNestedObject,
                       .string_value = {}};
    }
    return FlatValue{.kind = empty ? Kind::kEmptyArray : Kind::kNestedArray,
                     .string_value = {}};
  }
  return ParseError("JSON parse error: invalid value", cursor.offset());
}

sanitize::Status JsonOnDemandDoc::EmitFlatObjectField(json_scan::Cursor &cursor,
                                                      std::string_view text,
                                                      void *ctx,
                                                      FlatObjectEachFn fn,
                                                      std::size_t base_offset) {
  SAN_ASSIGN_OR_RAISE(auto key, ParseObjectKey(cursor, text, base_offset));
  json_scan::skip_ws(cursor);
  SAN_RETURN_NOT_OK(json_scan::expect(cursor, ':'));
  json_scan::skip_ws(cursor);
  SAN_ASSIGN_OR_RAISE(auto value,
                      ParseFlatChildValue(cursor, text, base_offset));
  return fn(ctx, key, value);
}

sanitize::Status
JsonOnDemandDoc::ForEachFlatObjectFieldC(std::string_view text, void *ctx,
                                         FlatObjectEachFn fn,
                                         std::size_t base_offset) {
  if (!fn) {
    return sanitize::Status::Invalid(
        "ForEachFlatObjectFieldC: callback is null");
  }
  auto cursor = MakeCursor(text, base_offset);
  bool done = false;
  std::size_t fields = 0;
  SAN_RETURN_NOT_OK(EnterObjectIterator(cursor, &done));
  while (!done) {
    if (fields >= json_scan::kMaxJsonObjectFields) {
      return sanitize::Status::Invalid(
          "JSON object field count exceeds safety limit: ",
          std::to_string(fields + 1U), " > ",
          std::to_string(json_scan::kMaxJsonObjectFields));
    }
    ++fields;
    SAN_RETURN_NOT_OK(EmitFlatObjectField(cursor, text, ctx, fn, base_offset));
    SAN_RETURN_NOT_OK(AdvanceObjectIterator(cursor, &done));
  }
  return sanitize::Status::OK();
}

} // namespace sanitize::internal
