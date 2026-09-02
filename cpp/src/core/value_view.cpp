// Implements scalar and nested ValueView accessors and traversal callbacks.
// Each accessor validates the active logical kind while object and array walks
// preserve the non-owning lifetime and error contract of the underlying value.

#include "sanitize/core/value_view.hh"

#include <cstdint>
#include <string_view>

#include "core/debug_assert.hh"

namespace sanitize {

bool ValueView::as_bool() const {
  SANITIZE_DCHECK(tag_ == Tag::kBool);
  return tag_ == Tag::kBool ? b_ : false;
}

int64_t ValueView::as_int() const {
  SANITIZE_DCHECK(tag_ == Tag::kInt);
  return tag_ == Tag::kInt ? i_ : 0;
}

double ValueView::as_float() const {
  SANITIZE_DCHECK(tag_ == Tag::kFloat);
  return tag_ == Tag::kFloat ? d_ : 0.0;
}

std::string_view ValueView::as_string_view() const {
  SANITIZE_DCHECK(tag_ == Tag::kString);
  return tag_ == Tag::kString ? s_ : std::string_view{};
}

Status ValueView::container_is_empty(bool *out) const {
  if (!out)
    return Status::Invalid("ValueView::container_is_empty: out is null");
  *out = false;

  bool seen_child = false;
  Status status = Status::OK();
  if (is_object()) {
    status = for_each_object_field(
        [&](std::string_view, uint64_t, ValueView) -> Status {
          seen_child = true;
          return Status(StatusCode::kCancelled, {});
        });
  } else if (is_array()) {
    status = for_each_array_element([&](ValueView) -> Status {
      seen_child = true;
      return Status(StatusCode::kCancelled, {});
    });
  } else {
    return Status::OK();
  }

  if (!seen_child && !status.ok())
    return status;
  *out = !seen_child;
  return Status::OK();
}

} // namespace sanitize
