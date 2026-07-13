// Declares inference depth-limit helpers.

#pragma once

#include "sanitize/core/value_view.hh"
#include "sanitize/options/options.hh"

namespace sanitize::internal {

struct DepthState {
  int arrow = 0;
  int parquet = 0;
};

// Returns depth after entering a child value from a parent depth state.
DepthState enter_value_depth(DepthState parent, const ValueView &v);

// Returns whether a named nested value should be represented as a string.
bool should_flatten_nested(const ValueView &v, const PreparedOptions &opts,
                           DepthState parent_depth);

} // namespace sanitize::internal
