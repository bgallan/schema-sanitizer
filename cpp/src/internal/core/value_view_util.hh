// Provides small predicates for non-owning ValueView instances.

#pragma once

#include "sanitize/core/status.hh"
#include "sanitize/core/value_view.hh"

namespace sanitize::internal {

// Returns whether a ValueView is an object or array with no children.
sanitize::Status value_view_container_is_empty(sanitize::ValueView value,
                                               bool *out);

} // namespace sanitize::internal
