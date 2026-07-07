// Implements on-demand JSON ValueView parsing and nested iteration.

#include "internal/parsing/json_ondemand.hh"
#include "internal/parsing/json_ondemand_scan.hh"
#include "internal/parsing/json_string_decode.hh"

#include "sanitize/core/primitives.hh"
#include "sanitize/core/status.hh"

#include <cassert>
#include <charconv>
#include <string>
#include <string_view>
#include <system_error>

namespace sanitize::internal {

using json_scan::Cursor;
using json_scan::scan_string;
using json_scan::skip_literal;
using json_scan::skip_number;
using json_scan::skip_value;
using json_scan::skip_ws;
using json_string_decode::decode_json_string_slice;
using json_string_decode::DecodeErrors;

struct JsonOnDemandDoc::OdObject {
  const JsonOnDemandDoc *doc = nullptr;
  std::string_view text;
  std::size_t base_offset = 0;
};

struct JsonOnDemandDoc::OdArray {
  const JsonOnDemandDoc *doc = nullptr;
  std::string_view text;
  std::size_t base_offset = 0;
};

namespace {

// Dispatches object iteration through an on-demand object view.
sanitize::Status od_obj_for_each(const void *self, void *ctx,
                                 ValueView::ObjectEachFn fn) {
  const auto *obj = static_cast<const JsonOnDemandDoc::OdObject *>(self);
  if (!obj || !obj->doc)
    return sanitize::Status::Invalid("JSON object view is null");
  return obj->doc->ForEachObjectFieldImpl(obj, ctx, fn);
}

// Dispatches array iteration through an on-demand array view.
sanitize::Status od_arr_for_each(const void *self, void *ctx,
                                 ValueView::ArrayEachFn fn) {
  const auto *arr = static_cast<const JsonOnDemandDoc::OdArray *>(self);
  if (!arr || !arr->doc)
    return sanitize::Status::Invalid("JSON array view is null");
  return arr->doc->ForEachArrayElementImpl(arr, ctx, fn);
}

constexpr ValueView::ObjectVTable kObjVt{&od_obj_for_each};
constexpr ValueView::ArrayVTable kArrVt{&od_arr_for_each};

constexpr DecodeErrors kStringDecodeErrors{
    .truncated_escape = "JSON parse error: truncated escape",
    .incomplete_unicode_escape = "JSON parse error: incomplete \\uXXXX escape",
    .invalid_unicode_hex = "JSON parse error: invalid hex in \\uXXXX",
    .missing_low_surrogate = "JSON parse error: missing low surrogate",
    .invalid_low_surrogate_hex =
        "JSON parse error: invalid hex in low surrogate",
    .invalid_low_surrogate_range =
        "JSON parse error: invalid low surrogate range",
    .unexpected_low_surrogate = "JSON parse error: unexpected low surrogate",
    .invalid_escape = "JSON parse error: invalid escape",
};

} // namespace

JsonOnDemandDoc::JsonOnDemandDoc(std::pmr::memory_resource *upstream)
    : arena_(upstream) {
  // Crash-fast if the pipeline violated allocator invariants.
  // (In debug builds this is a clear assertion failure; in release builds
  // a null upstream is a logic error and must not be masked.)
  assert(upstream != nullptr);
}

void *JsonOnDemandDoc::ArenaAlloc(std::size_t n, std::size_t align) {
  return arena_.allocate(n, align);
}

char *JsonOnDemandDoc::ArenaAllocChars(std::size_t n) {
  return static_cast<char *>(ArenaAlloc(n, alignof(char)));
}

sanitize::Status JsonOnDemandDoc::ParseError(std::string_view msg,
                                             std::size_t offset) {
  return sanitize::Status::Invalid(std::string(msg), " at byte ",
                                   std::to_string(offset));
}

json_scan::Cursor
JsonOnDemandDoc::MakeCursor(std::string_view text,
                            std::size_t base_offset) noexcept {
  Cursor c;
  c.p = text.data();
  c.end = text.data() + text.size();
  c.base = base_offset;
  c.text_begin = text.data();
  return c;
}

sanitize::Status JsonOnDemandDoc::ExpectEnd(Cursor &cursor) {
  skip_ws(cursor);
  if (cursor.p != cursor.end) {
    return ParseError("JSON parse error: trailing characters", cursor.offset());
  }
  return sanitize::Status::OK();
}

sanitize::Result<ValueView>
JsonOnDemandDoc::ParseLiteralValue(Cursor &cursor, const char *literal,
                                   std::size_t literal_size, ValueView value) {
  SAN_RETURN_NOT_OK(skip_literal(cursor, literal, literal_size));
  SAN_RETURN_NOT_OK(ExpectEnd(cursor));
  return value;
}

sanitize::Result<ValueView>
JsonOnDemandDoc::ParseStringValue(Cursor &cursor, std::string_view text,
                                  std::size_t base_offset) {
  const char *begin = nullptr;
  const char *end = nullptr;
  bool has_escapes = false;
  SAN_RETURN_NOT_OK(scan_string(cursor, begin, end, has_escapes));
  SAN_RETURN_NOT_OK(ExpectEnd(cursor));
  if (!has_escapes) {
    return ValueView::String(
        std::string_view(begin, static_cast<std::size_t>(end - begin)));
  }
  const auto input_size = static_cast<std::size_t>(end - begin);
  char *out = ArenaAllocChars(input_size ? input_size : 1);
  SAN_ASSIGN_OR_RAISE(
      auto decoded, decode_json_string_slice(out, begin, end, text, base_offset,
                                             kStringDecodeErrors));
  return ValueView::String(decoded);
}

sanitize::Result<ValueView>
JsonOnDemandDoc::ParseObjectValue(Cursor &cursor, std::string_view text,
                                  std::size_t base_offset) {
  const char *start = cursor.p;
  SAN_RETURN_NOT_OK(skip_value(cursor));
  const char *end = cursor.p;
  SAN_RETURN_NOT_OK(ExpectEnd(cursor));
  auto *obj =
      static_cast<OdObject *>(ArenaAlloc(sizeof(OdObject), alignof(OdObject)));
  new (obj) OdObject{
      .doc = this,
      .text = std::string_view(start, static_cast<std::size_t>(end - start)),
      .base_offset =
          base_offset + static_cast<std::size_t>(start - text.data())};
  return ValueView::ObjectView(obj, &kObjVt);
}

sanitize::Result<ValueView>
JsonOnDemandDoc::ParseArrayValue(Cursor &cursor, std::string_view text,
                                 std::size_t base_offset) {
  const char *start = cursor.p;
  SAN_RETURN_NOT_OK(skip_value(cursor));
  const char *end = cursor.p;
  SAN_RETURN_NOT_OK(ExpectEnd(cursor));
  auto *arr =
      static_cast<OdArray *>(ArenaAlloc(sizeof(OdArray), alignof(OdArray)));
  new (arr) OdArray{
      .doc = this,
      .text = std::string_view(start, static_cast<std::size_t>(end - start)),
      .base_offset =
          base_offset + static_cast<std::size_t>(start - text.data())};
  return ValueView::ArrayView(arr, &kArrVt);
}

sanitize::Result<ValueView>
JsonOnDemandDoc::ParseNumberValue(Cursor &cursor, std::string_view text,
                                  std::size_t base_offset) {
  const char *start = cursor.p;
  SAN_RETURN_NOT_OK(skip_number(cursor));
  const char *end = cursor.p;
  SAN_RETURN_NOT_OK(ExpectEnd(cursor));

  std::string_view number(start, static_cast<std::size_t>(end - start));
  bool is_float = false;
  for (const char character : number) {
    if (character == '.' || character == 'e' || character == 'E') {
      is_float = true;
      break;
    }
  }
  if (!is_float) {
    int64_t value = 0;
    auto result =
        std::from_chars(number.data(), number.data() + number.size(), value);
    if (result.ec == std::errc() &&
        result.ptr == number.data() + number.size()) {
      return ValueView::Int(value);
    }
    // Fallback to float if an integer literal is outside int64 range.
    is_float = true;
  }
  if (!is_float) {
    return ParseError("JSON parse error: invalid number",
                      base_offset +
                          static_cast<std::size_t>(start - text.data()));
  }

  double value = 0.0;
  if (!parse_ascii_float64_strict(number, &value)) {
    return ParseError("JSON parse error: invalid float",
                      base_offset +
                          static_cast<std::size_t>(start - text.data()));
  }
  return ValueView::Float(value);
}

sanitize::Result<ValueView>
JsonOnDemandDoc::ParseValue(std::string_view text, std::size_t base_offset) {
  Cursor c = MakeCursor(text, base_offset);
  skip_ws(c);

  if (c.p >= c.end)
    return ParseError("JSON parse error: empty input", c.offset());

  const char ch = *c.p;
  if (ch == 'n') {
    return ParseLiteralValue(c, "null", 4, ValueView::Null());
  }
  if (ch == 't') {
    return ParseLiteralValue(c, "true", 4, ValueView::Bool(true));
  }
  if (ch == 'f') {
    return ParseLiteralValue(c, "false", 5, ValueView::Bool(false));
  }
  if (ch == '"') {
    return ParseStringValue(c, text, base_offset);
  }
  if (ch == '{') {
    return ParseObjectValue(c, text, base_offset);
  }
  if (ch == '[') {
    return ParseArrayValue(c, text, base_offset);
  }
  if (ch == '-' || (ch >= '0' && ch <= '9')) {
    return ParseNumberValue(c, text, base_offset);
  }

  return ParseError("JSON parse error: invalid value", c.offset());
}

sanitize::Status
JsonOnDemandDoc::ForEachObjectFieldImpl(const OdObject *obj, void *ctx,
                                        ValueView::ObjectEachFn fn) const {
  // This object wrapper is allocated in this->arena_ but may outlive stack
  // frames. We can safely cast away const to call the public API (which uses
  // the same arena).
  return const_cast<JsonOnDemandDoc *>(this)->ForEachObjectFieldC(
      obj->text, ctx, fn, obj->base_offset);
}

sanitize::Status
JsonOnDemandDoc::ForEachArrayElementImpl(const OdArray *arr, void *ctx,
                                         ValueView::ArrayEachFn fn) const {
  return const_cast<JsonOnDemandDoc *>(this)->ForEachArrayElementC(
      arr->text, ctx, fn, arr->base_offset);
}

sanitize::Result<std::size_t> json_skip_value(std::string_view text,
                                              std::size_t start,
                                              std::size_t base_offset) {
  if (start > text.size()) {
    return sanitize::Status::Invalid("json_skip_value: start out of range");
  }
  Cursor c;
  c.p = text.data() + start;
  c.end = text.data() + text.size();
  c.base = base_offset;
  c.text_begin = text.data();

  // Skip leading whitespace.
  skip_ws(c);
  SAN_RETURN_NOT_OK(skip_value(c));
  return static_cast<std::size_t>(c.p - text.data());
}

} // namespace sanitize::internal
