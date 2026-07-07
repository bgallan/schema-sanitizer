// Implements scalar and nested ValueView accessors and callbacks.

#include "sanitize/core/value_view.hh"

#include <cstdint>
#include <string_view>

#include "internal/runtime/assert.hh"

namespace sanitize {

bool ValueView::as_bool() const {
  SCHEMA_SANITIZER_DCHECK(tag_ == Tag::kBool);
  return tag_ == Tag::kBool ? b_ : false;
}

int64_t ValueView::as_int() const {
  SCHEMA_SANITIZER_DCHECK(tag_ == Tag::kInt);
  return tag_ == Tag::kInt ? i_ : 0;
}

double ValueView::as_float() const {
  SCHEMA_SANITIZER_DCHECK(tag_ == Tag::kFloat);
  return tag_ == Tag::kFloat ? d_ : 0.0;
}

std::string_view ValueView::as_string_view() const {
  SCHEMA_SANITIZER_DCHECK(tag_ == Tag::kString);
  return tag_ == Tag::kString ? s_ : std::string_view{};
}

} // namespace sanitize
