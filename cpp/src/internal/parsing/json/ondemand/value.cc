// Implements on-demand JSON container views and value dispatch.

#include "internal/parsing/json/ondemand/document.hh"

#include "internal/parsing/json/ondemand/scan.hh"
#include "sanitize/core/status.hh"

#include <string_view>

namespace sanitize::internal {

using json_scan::Cursor;
using json_scan::skip_value;
using json_scan::skip_ws;

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

sanitize::Status od_obj_for_each(const void *self, void *ctx,
                                 ValueView::ObjectEachFn fn) {
  const auto *obj = static_cast<const JsonOnDemandDoc::OdObject *>(self);
  if (!obj || !obj->doc)
    return sanitize::Status::Invalid("JSON object view is null");
  return obj->doc->ForEachObjectFieldImpl(obj, ctx, fn);
}

sanitize::Status od_arr_for_each(const void *self, void *ctx,
                                 ValueView::ArrayEachFn fn) {
  const auto *arr = static_cast<const JsonOnDemandDoc::OdArray *>(self);
  if (!arr || !arr->doc)
    return sanitize::Status::Invalid("JSON array view is null");
  return arr->doc->ForEachArrayElementImpl(arr, ctx, fn);
}

constexpr ValueView::ObjectVTable kObjVt{&od_obj_for_each};
constexpr ValueView::ArrayVTable kArrVt{&od_arr_for_each};

} // namespace

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
JsonOnDemandDoc::ParseValue(std::string_view text, std::size_t base_offset) {
  Cursor cursor = MakeCursor(text, base_offset);
  skip_ws(cursor);

  if (cursor.p >= cursor.end)
    return ParseError("JSON parse error: empty input", cursor.offset());

  const char ch = *cursor.p;
  if (ch == 'n')
    return ParseLiteralValue(cursor, "null", 4, ValueView::Null());
  if (ch == 't')
    return ParseLiteralValue(cursor, "true", 4, ValueView::Bool(true));
  if (ch == 'f')
    return ParseLiteralValue(cursor, "false", 5, ValueView::Bool(false));
  if (ch == '"')
    return ParseStringValue(cursor, text, base_offset);
  if (ch == '{')
    return ParseObjectValue(cursor, text, base_offset);
  if (ch == '[')
    return ParseArrayValue(cursor, text, base_offset);
  if (ch == '-' || (ch >= '0' && ch <= '9'))
    return ParseNumberValue(cursor, text, base_offset);

  return ParseError("JSON parse error: invalid value", cursor.offset());
}

sanitize::Status
JsonOnDemandDoc::ForEachObjectFieldImpl(const OdObject *obj, void *ctx,
                                        ValueView::ObjectEachFn fn) const {
  return const_cast<JsonOnDemandDoc *>(this)->ForEachObjectFieldC(
      obj->text, ctx, fn, obj->base_offset);
}

sanitize::Status
JsonOnDemandDoc::ForEachArrayElementImpl(const OdArray *arr, void *ctx,
                                         ValueView::ArrayEachFn fn) const {
  return const_cast<JsonOnDemandDoc *>(this)->ForEachArrayElementC(
      arr->text, ctx, fn, arr->base_offset);
}

} // namespace sanitize::internal
