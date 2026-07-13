// Implements inference depth-limit helpers.

#include "internal/inference/depth.hh"

#include <cstdint>
#include <string_view>

namespace sanitize::internal {
namespace {

bool exceeds_depth_limit(const ValueView &v, const PreparedOptions &opts,
                         DepthState depth) {
  if (v.is_array()) {
    const DepthState next{.arrow = depth.arrow + 1, .parquet = depth.parquet};
    if (next.arrow > opts.spec.arrow_max_depth ||
        next.parquet > opts.spec.parquet_max_depth)
      return true;
    bool exceeded = false;
    v.for_each_array_element([&](ValueView el) -> sanitize::Status {
      if (!exceeded)
        exceeded = exceeds_depth_limit(el, opts, next);
      return sanitize::Status::OK();
    });
    return exceeded;
  }

  if (v.is_object()) {
    const DepthState next{.arrow = depth.arrow + 1,
                          .parquet = depth.parquet + 1};
    if (next.arrow > opts.spec.arrow_max_depth ||
        next.parquet > opts.spec.parquet_max_depth)
      return true;
    bool exceeded = false;
    v.for_each_object_field(
        [&](std::string_view, uint64_t, ValueView vv) -> sanitize::Status {
          if (!exceeded)
            exceeded = exceeds_depth_limit(vv, opts, next);
          return sanitize::Status::OK();
        });
    return exceeded;
  }

  return false;
}

} // namespace

DepthState enter_value_depth(DepthState parent, const ValueView &v) {
  if (v.is_array())
    return DepthState{.arrow = parent.arrow + 1, .parquet = parent.parquet};
  if (v.is_object())
    return DepthState{.arrow = parent.arrow + 1, .parquet = parent.parquet + 1};
  return parent;
}

bool should_flatten_nested(const ValueView &v, const PreparedOptions &opts,
                           DepthState parent_depth) {
  if (!v.is_object() && !v.is_array())
    return false;
  return exceeds_depth_limit(v, opts, parent_depth);
}

} // namespace sanitize::internal
