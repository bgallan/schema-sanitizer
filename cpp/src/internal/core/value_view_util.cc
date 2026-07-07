// Implements predicates for non-owning ValueView instances.

#include "internal/core/value_view_util.hh"

namespace sanitize::internal {

sanitize::Status value_view_container_is_empty(sanitize::ValueView value,
                                               bool *out) {
  if (!out)
    return sanitize::Status::Invalid(
        "value_view_container_is_empty: out is null");
  *out = false;

  bool seen_child = false;
  sanitize::Status status = sanitize::Status::OK();
  if (value.is_object()) {
    status = value.for_each_object_field(
        [&](std::string_view, uint64_t,
            sanitize::ValueView) -> sanitize::Status {
          seen_child = true;
          return sanitize::Status::Cancelled("value view child found");
        });
  } else if (value.is_array()) {
    status = value.for_each_array_element(
        [&](sanitize::ValueView) -> sanitize::Status {
          seen_child = true;
          return sanitize::Status::Cancelled("value view child found");
        });
  } else {
    return sanitize::Status::OK();
  }

  if (!seen_child && !status.ok())
    return status;
  *out = !seen_child;
  return sanitize::Status::OK();
}

} // namespace sanitize::internal
